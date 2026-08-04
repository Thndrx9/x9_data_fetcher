import json
import queue
import re
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Set, Tuple
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")

# How long before an hour boundary to pre-create the next hour's tables
# (no inserts yet — just DDL, so it's not competing with live writes right
# at the boundary itself).
PRE_OPEN_SECONDS = 60

# How long before an hour boundary to start writing every incoming tick to
# BOTH the closing hour's file and the next hour's file. Ticks in this
# window are deliberately duplicated across both hourly files so a boundary
# race can never silently drop one; merge_daily() in parquet_archiver.py
# collapses the duplicates back to one copy each at end of day.
DUAL_WRITE_SECONDS = 9

_VALID_PREFIXES = ("depth", "quote")

# ---------------------------------------------------------------------------
# Per-symbol table strategy
# ---------------------------------------------------------------------------
# Each symbol gets its own table:  depth_RELIANCE, depth_TCS, depth_WIPRO ...
# (or quote_RELIANCE / quote_TCS / ... for the quote writer)
#
# New symbol mid-day:
#   → first enqueue() for that symbol hits _ensure_table()
#   → CREATE TABLE IF NOT EXISTS <prefix>_SYMBOL runs once
#   → rows start inserting immediately — no restart needed
#
# New symbol mid-week:
#   → same flow — existing DB opened, new table created inside it
#   → other symbols' tables untouched
#
# Process restart mid-week:
#   → _load_existing_tables() reads sqlite_master and pre-populates
#     known_tables so CREATE TABLE is skipped for existing ones
#
# Hourly rollover (top of every IST hour):
#   → old DB closed, new DB opened
#   → known_tables reset to empty — tables re-created as ticks arrive
#   → the just-closed file is picked up and archived to Parquet by the
#     separate parquet_archiver.py process (never by this writer itself)
#
# Two independent TickWriter instances are created — one with
# prefix="depth", one with prefix="quote" — each with its own thread,
# queue, and SQLite connections. They never share state; this file is a
# single source of shared logic, not a merge of the two writers' data.
# ---------------------------------------------------------------------------


def _safe_symbol(symbol: str) -> str:
    """Strip anything that isn't alphanumeric or underscore — safe for table names."""
    return "".join(c for c in symbol if c.isalnum() or c == "_")


def _hourly_db_path(base_dir: Path, at: "datetime | None" = None) -> Path:
    """Returns e.g. <base_dir>/market_2026-06-12_14.db — rolls over every hour.

    Hour is zero-padded (00-23) so filenames sort correctly and
    parquet_archiver.py can parse the hour back out unambiguously.

    `at` defaults to now (IST) — pass a future datetime (e.g. now + 1h) to
    compute the *next* hour's path ahead of time for pre-opening.
    """
    stamp = (at or datetime.now(IST)).strftime("%Y-%m-%d_%H")
    return base_dir / f"market_{stamp}.db"


def _seconds_to_next_hour(now: "datetime | None" = None) -> float:
    """Seconds remaining until the next IST clock-hour boundary."""
    now = now or datetime.now(IST)
    next_hour = (now.replace(minute=0, second=0, microsecond=0)
                 + timedelta(hours=1))
    return (next_hour - now).total_seconds()


def _open_db(db_path: Path, tag: str) -> sqlite3.Connection:
    """Open (or create) the hourly SQLite file and set performance pragmas."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not db_path.exists()
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    # Wait up to 5s for a momentary lock to clear instead of raising
    # "database is locked" immediately — must be set before journal_mode,
    # since enabling WAL itself briefly takes an exclusive lock and is
    # exactly the kind of short-lived contention this is meant to absorb.
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")    # batched disk writes
    conn.execute("PRAGMA synchronous=NORMAL")  # no fsync on every commit
    conn.execute("PRAGMA cache_size=500")      # 500 × 4 KB = 2 MB RAM
    conn.execute("PRAGMA page_size=4096")
    conn.commit()
    action = "created" if is_new else "opened"
    print(f"{tag} DB {action}: {db_path.name}", flush=True)
    return conn


_MALFORMED_TABLE_RE = re.compile(r"^(depth|quote)_\1")  # e.g. quote_quote..., depth_depth...


def _load_existing_tables(conn: sqlite3.Connection, prefix: str, tag: str) -> Set[str]:
    """
    On startup or restart, read which <prefix>_* tables already exist in the
    DB. Prevents redundant CREATE TABLE calls for symbols already seen.

    Tables matching a doubled prefix (quote_quote_..., depth_depth_...) are
    a known artifact of a since-fixed bug in the pre-open step and are
    quietly excluded here rather than propagated — carrying them into
    known_tables would faithfully round-trip them into every future hourly
    file forever (strip one layer, _ensure_table re-adds it, net no-op),
    which is exactly what kept them alive across restarts after the
    original fix stopped new ones from being created.
    """
    cur = conn.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{prefix}_%'"
    )
    all_names = {row[0] for row in cur.fetchall()}
    tables = {name for name in all_names if not _MALFORMED_TABLE_RE.match(name)}
    skipped = len(all_names) - len(tables)
    if tables:
        print(f"{tag} found {len(tables)} existing tables in DB", flush=True)
    if skipped:
        print(
            f"{tag}[WARN] ignored {skipped} malformed table(s) with a "
            f"doubled prefix (pre-existing corruption, not re-created)",
            flush=True,
        )
    return tables


def _ensure_table(
    conn: sqlite3.Connection, sym: str, known_tables: Set[str], prefix: str
) -> Tuple[str, bool]:
    """
    Return (table_name, created) for sym, creating the table the first time it
    is seen. After creation the name is added to known_tables so this is
    called only once per symbol per DB file. `created` lets the caller batch
    a single summary log line per flush instead of printing per symbol —
    printing here directly produced ~200 lines every hourly rollover.
    """
    table = f"{prefix}_{_safe_symbol(sym)}"
    if table in known_tables:
        return table, False

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            timestamp INTEGER,
            ingest_ns INTEGER,
            raw_json  TEXT
        )
    """)
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table} ON {table} (timestamp)"
    )
    conn.commit()
    known_tables.add(table)
    return table, True


@lru_cache(maxsize=512)
def _insert_sql(table: str) -> str:
    """Cache INSERT SQL per table — 3 columns (timestamp, ingest_ns, raw_json), built once per symbol."""
    placeholders = ",".join(["?"] * 3)
    return f"INSERT INTO {table} VALUES ({placeholders})"


def _parse_row(row: dict) -> tuple:
    """
    Store only the broker-provided timestamp, ingest timestamp, and raw JSON.
    """
    raw = row.get("raw_json", "{}")
    d = json.loads(raw) if isinstance(raw, str) else raw

    return (
        d.get("timestamp"),
        row.get("ingest_ns"),
        raw if isinstance(raw, str) else json.dumps(raw),
    )


# ---------------------------------------------------------------------------
# Writer class — shared by both the depth and quote writers via `prefix`
# ---------------------------------------------------------------------------

class TickWriter:
    """
    Buffered single-writer for tick-level rows — depth snapshots or quote
    ticks, depending on `prefix`.

    Storage layout inside <base_dir>/market_YYYY-MM-DD_HH.db (rolls over hourly):
        <prefix>_RELIANCE   ← one table per symbol
        <prefix>_TCS
        <prefix>_WIPRO      ← created automatically on first tick

    New symbols can be added mid-day or mid-week with no restart.

    Public interface:
        writer = TickWriter(base_dir, prefix="depth", known_symbols=["ABB", "TCS"])
        writer.enqueue(symbol, row)
        writer.shutdown()
    """

    def __init__(
        self,
        base_dir: str,
        prefix: str,
        known_symbols=None,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
    ):
        if prefix not in _VALID_PREFIXES:
            raise ValueError(f"prefix must be one of {_VALID_PREFIXES}, got {prefix!r}")

        self.prefix = prefix
        self.tag = f"[TICK_WRITER:{prefix.upper()}]"

        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.flush_batch_size   = max(1,   int(flush_batch_size))
        self.flush_interval_sec = max(0.2, float(flush_interval_sec))

        # Canonical universe of symbols to pre-create tables for ahead of
        # each rollover — driven directly from the configured symbol list,
        # not reverse-parsed from existing table names. Only ever mutated
        # from inside the writer thread (see _flush), so no lock needed.
        self.known_symbols: Set[str] = (
            {str(s).upper() for s in known_symbols} if known_symbols else set()
        )

        self._q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run_guarded, name=f"{prefix}-tick-writer", daemon=True
        )
        self._thread.start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue(self, symbol: str, row: dict) -> None:
        """Add a tick to the write queue. Non-blocking."""
        self._q.put((symbol.upper(), dict(row)))

    def shutdown(self, timeout: float | None = None) -> None:
        """Drain the queue, flush remaining rows, close DB, stop thread."""
        self._stop.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            print(
                f"{self.tag}[ERROR] shutdown timed out before final flush completed",
                flush=True,
            )

    # ------------------------------------------------------------------
    # Internal — everything below runs inside the writer thread only
    # ------------------------------------------------------------------

    def _run_guarded(self) -> None:
        """
        Wraps _run() so an unhandled exception is logged loudly instead of
        the writer thread just dying silently — the two writers run on
        independent threads, so one crashing must not take the other (or
        the process) down without at least a clear signal in the log.
        """
        try:
            self._run()
        except Exception:
            print(
                f"{self.tag}[FATAL] writer thread crashed — no further "
                f"{self.prefix} ticks are being written until restart:\n"
                f"{traceback.format_exc()}",
                flush=True,
            )

    def _run(self) -> None:
        buffered: Dict[str, List[dict]] = {}
        last_flush = time.monotonic()

        current_db_path = _hourly_db_path(self.base_dir)
        conn = _open_db(current_db_path, self.tag)
        known_tables = _load_existing_tables(conn, self.prefix, self.tag)  # pre-load on restart

        # Pre-opened next-hour connection, live only during the window from
        # T-60s to the boundary. None outside that window.
        next_conn = None
        next_db_path = None
        next_known_tables: Set[str] = set()
        next_buffered: Dict[str, List[dict]] = {}

        while True:
            should_exit = self._stop.is_set() and self._q.empty()
            if should_exit:
                break

            now_dt = datetime.now(IST)
            secs_left = _seconds_to_next_hour(now_dt)

            # --- T-60s: pre-open the next hour's file and create tables,
            # no rows written yet. ---
            if next_conn is None and secs_left <= PRE_OPEN_SECONDS:
                next_db_path = _hourly_db_path(
                    self.base_dir, at=now_dt + timedelta(seconds=secs_left + 1)
                )
                next_conn = _open_db(next_db_path, self.tag)
                next_known_tables = set()
                pre_created = 0
                # Driven directly from self.known_symbols (the canonical
                # symbol list) rather than reverse-parsed from existing
                # table names — no bare-symbol extraction here means no way
                # to reintroduce the doubled-prefix corruption a table-name
                # parsing approach was previously vulnerable to. Also fixes
                # a gap the table-name approach had: a symbol with no ticks
                # yet this hour had no table to parse from, so it silently
                # never got pre-created either — iterating the real symbol
                # list instead means every configured symbol is pre-created
                # every hour regardless of whether it's ticked yet.
                for sym in self.known_symbols:
                    _, created = _ensure_table(next_conn, sym, next_known_tables, self.prefix)
                    if created:
                        pre_created += 1
                if pre_created:
                    print(
                        f"{self.tag} pre-created {pre_created} table(s) on "
                        f"{next_db_path.name} ahead of rollover",
                        flush=True,
                    )

            # Check for hourly rollover BEFORE pulling the next row off the
            # queue. This must happen first: if a row is popped and appended
            # to `buffered` ahead of this check, a tick that arrives right at
            # the hour boundary gets appended under the OLD hour's buffer,
            # and then flushed straight into the OLD hour's (about to close)
            # connection instead of the new one — misfiling it by up to one
            # row. Checking first means any row popped this iteration is
            # appended only after the writer has already switched to
            # whichever connection is actually current.
            #
            # The DUAL_WRITE_SECONDS window below is the actual fix for that
            # residual race: for the last few seconds before a boundary,
            # every tick is written to both the closing and next files, so
            # even if this check's timing is off by a poll cycle, the tick
            # already landed in whichever file turns out to be current.
            new_db_path = _hourly_db_path(self.base_dir, at=now_dt)
            if new_db_path != current_db_path:
                self._flush(conn, buffered, known_tables, self.prefix, self.tag, self.known_symbols)
                conn.close()

                if next_conn is not None and next_db_path == new_db_path:
                    # Normal path: the pre-opened connection is exactly the
                    # new current hour — hand it straight over.
                    self._flush(next_conn, next_buffered, next_known_tables, self.prefix, self.tag, self.known_symbols)
                    conn = next_conn
                    known_tables = next_known_tables
                else:
                    # Fallback (e.g. writer started < 60s before a boundary,
                    # so there was no time to pre-open): open fresh as before.
                    if next_conn is not None:
                        self._flush(next_conn, next_buffered, next_known_tables, self.prefix, self.tag, self.known_symbols)
                        next_conn.close()
                    conn = _open_db(new_db_path, self.tag)
                    known_tables = _load_existing_tables(conn, self.prefix, self.tag)

                current_db_path = new_db_path
                next_conn = None
                next_db_path = None
                next_known_tables = set()
                next_buffered = {}
                buffered = {}
                print(
                    f"{self.tag} hourly rollover → {current_db_path.name}",
                    flush=True,
                )

            try:
                symbol, row = self._q.get(timeout=0.25)
                buffered.setdefault(symbol, []).append(row)
                # --- T-9s to T-0: also write this tick into the next hour's
                # file, straight from the live stream — no buffering-and-
                # copying-back-off-disk, just the same row appended a second
                # time into the other buffer. ---
                if next_conn is not None and secs_left <= DUAL_WRITE_SECONDS:
                    next_buffered.setdefault(symbol, []).append(row)
                self._q.task_done()
            except queue.Empty:
                pass

            now = time.monotonic()
            due_time  = (now - last_flush) >= self.flush_interval_sec
            due_batch = any(
                len(rows) >= self.flush_batch_size for rows in buffered.values()
            )

            if due_time or due_batch:
                self._flush(conn, buffered, known_tables, self.prefix, self.tag, self.known_symbols)
                if next_conn is not None and next_buffered:
                    self._flush(next_conn, next_buffered, next_known_tables, self.prefix, self.tag, self.known_symbols)
                last_flush = now

        # final flush before exit
        self._flush(conn, buffered, known_tables, self.prefix, self.tag, self.known_symbols)
        conn.close()
        if next_conn is not None:
            self._flush(next_conn, next_buffered, next_known_tables, self.prefix, self.tag, self.known_symbols)
            next_conn.close()

    @staticmethod
    def _flush(
        conn: sqlite3.Connection,
        buffered: Dict[str, List[dict]],
        known_tables: Set[str],
        prefix: str,
        tag: str,
        known_symbols: Set[str],
    ) -> None:
        """
        For each symbol:
          1. ensure its table exists (CREATE once, skipped after that)
          2. parse all buffered rows
          3. executemany into that symbol's table
        One commit at the end covers all symbols.

        `known_symbols` is the canonical set future rollovers pre-create
        tables from — any symbol seen here that isn't in it yet (e.g. a
        genuinely new mid-day listing) gets added, so it's proactively
        pre-created on future rollovers too, not just created reactively
        on demand every time it happens to tick.
        """
        any_rows = False
        new_tables = 0

        for sym, rows in buffered.items():
            if not rows:
                continue

            known_symbols.add(sym)
            table, created = _ensure_table(conn, sym, known_tables, prefix)
            if created:
                new_tables += 1
            sql   = _insert_sql(table)
            parsed: List[tuple] = []

            for row in rows:
                try:
                    parsed.append(_parse_row(row))
                except Exception as exc:
                    print(
                        f"{tag}[ERROR] parse failed for {sym}: {exc}",
                        flush=True,
                    )

            if parsed:
                try:
                    conn.executemany(sql, parsed)
                    any_rows = True
                except Exception as exc:
                    print(
                        f"{tag}[ERROR] insert failed for {sym}: {exc}",
                        flush=True,
                    )

            buffered[sym] = []

        if new_tables:
            print(f"{tag} created {new_tables} new table(s) this flush", flush=True)

        if any_rows:
            try:
                conn.commit()           # one commit covers all symbols
            except Exception as exc:
                print(f"{tag}[ERROR] commit failed: {exc}", flush=True)