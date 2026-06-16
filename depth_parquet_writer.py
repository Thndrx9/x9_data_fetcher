import json
import os
import queue
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from market_time import MARKET_CLOSE, is_trading_day, now_kolkata


IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Per-symbol table strategy
# ---------------------------------------------------------------------------
# Each symbol gets its own table:  depth_RELIANCE, depth_TCS, depth_WIPRO ...
#
# New symbol mid-day:
#   → first enqueue() for that symbol hits _ensure_table()
#   → CREATE TABLE IF NOT EXISTS depth_SYMBOL runs once
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
# Weekly rollover (Monday midnight IST):
#   → old DB closed, new DB opened
#   → known_tables reset to empty — tables re-created as ticks arrive
# ---------------------------------------------------------------------------


def _safe_symbol(symbol: str) -> str:
    """Strip anything that isn't alphanumeric or underscore — safe for table names."""
    return "".join(c for c in symbol if c.isalnum() or c == "_")


def _weekly_db_path(base_dir: Path) -> Path:
    """Returns e.g. <base_dir>/market_2026_W24.db — rolls over every Monday."""
    week = datetime.now(IST).strftime("%Y_W%W")
    return base_dir / f"market_{week}.db"


def _open_db(db_path: Path) -> sqlite3.Connection:
    """Open (or create) the weekly SQLite file and set performance pragmas."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not db_path.exists()
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")    # batched disk writes
    conn.execute("PRAGMA synchronous=NORMAL")  # no fsync on every commit
    conn.execute("PRAGMA cache_size=500")      # 500 × 4 KB = 2 MB RAM
    conn.execute("PRAGMA page_size=4096")
    conn.commit()
    action = "created" if is_new else "opened"
    print(f"[DEPTH_WRITER] DB {action}: {db_path.name}", flush=True)
    return conn


def _load_existing_tables(conn: sqlite3.Connection) -> Set[str]:
    """
    On startup or restart, read which depth_* tables already exist in the DB.
    Prevents redundant CREATE TABLE calls for symbols already seen this week.
    """
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'depth_%'"
    )
    tables = {row[0] for row in cur.fetchall()}
    if tables:
        print(f"[DEPTH_WRITER] found {len(tables)} existing tables in DB", flush=True)
    return tables


def _ensure_table(conn: sqlite3.Connection, sym: str, known_tables: Set[str]) -> str:
    """
    Return the table name for sym, creating the table the first time it is seen.
    After creation the name is added to known_tables so this is called only once
    per symbol per DB file.
    """
    table = f"depth_{_safe_symbol(sym)}"
    if table in known_tables:
        return table

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
    print(f"[DEPTH_WRITER] new table: {table}", flush=True)
    return table


@lru_cache(maxsize=256)
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
# Writer class — same public interface as before
# ---------------------------------------------------------------------------

class DepthParquetWriter:
    """
    Buffered single-writer for depth snapshots.

    Storage layout inside <base_dir>/market_YYYY_WXX.db:
        depth_RELIANCE   ← one table per symbol
        depth_TCS
        depth_WIPRO      ← created automatically on first tick

    New symbols can be added mid-day or mid-week with no restart.

    Public interface unchanged:
        writer = DepthParquetWriter(base_dir)
        writer.enqueue(symbol, row)
        writer.shutdown()
    """

    def __init__(
        self,
        base_dir: str,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
    ):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.flush_batch_size   = max(1,   int(flush_batch_size))
        self.flush_interval_sec = max(0.2, float(flush_interval_sec))

        self._q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="depth-parquet-writer", daemon=True
        )
        self._thread.start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue(self, symbol: str, row: dict) -> None:
        """Add a depth tick to the write queue. Non-blocking."""
        self._q.put((symbol.upper(), dict(row)))

    def shutdown(self, timeout: float | None = None) -> None:
        """Drain the queue, flush remaining rows, close DB, stop thread."""
        self._stop.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            print(
                "[DEPTH_WRITER][ERROR] shutdown timed out before final flush completed",
                flush=True,
            )

    # ------------------------------------------------------------------
    # Internal — everything below runs inside the writer thread only
    # ------------------------------------------------------------------

    def _run(self) -> None:
        buffered: Dict[str, List[dict]] = {}
        last_flush = time.monotonic()

        current_db_path = _weekly_db_path(self.base_dir)
        conn = _open_db(current_db_path)
        known_tables = _load_existing_tables(conn)  # pre-load on restart

        while True:
            should_exit = self._stop.is_set() and self._q.empty()
            if should_exit:
                break

            try:
                symbol, row = self._q.get(timeout=0.25)
                buffered.setdefault(symbol, []).append(row)
                self._q.task_done()
            except queue.Empty:
                pass

            now = time.monotonic()
            due_time  = (now - last_flush) >= self.flush_interval_sec
            due_batch = any(
                len(rows) >= self.flush_batch_size for rows in buffered.values()
            )

            if due_time or due_batch:
                # weekly rollover — close old DB, open new one
                new_db_path = _weekly_db_path(self.base_dir)
                if new_db_path != current_db_path:
                    self._flush(conn, buffered, known_tables)
                    conn.close()
                    current_db_path = new_db_path
                    conn = _open_db(current_db_path)
                    known_tables = set()   # new DB — no tables yet
                    print(
                        f"[DEPTH_WRITER] weekly rollover → {current_db_path.name}",
                        flush=True,
                    )

                self._flush(conn, buffered, known_tables)
                last_flush = now

        # final flush before exit
        self._flush(conn, buffered, known_tables)
        conn.close()

    @staticmethod
    def _flush(
        conn: sqlite3.Connection,
        buffered: Dict[str, List[dict]],
        known_tables: Set[str],
    ) -> None:
        """
        For each symbol:
          1. ensure its table exists (CREATE once, skipped after that)
          2. parse all buffered rows
          3. executemany into that symbol's table
        One commit at the end covers all symbols.
        """
        any_rows = False

        for sym, rows in buffered.items():
            if not rows:
                continue

            table = _ensure_table(conn, sym, known_tables)
            sql   = _insert_sql(table)
            parsed: List[tuple] = []

            for row in rows:
                try:
                    parsed.append(_parse_row(row))
                except Exception as exc:
                    print(
                        f"[DEPTH_WRITER][ERROR] parse failed for {sym}: {exc}",
                        flush=True,
                    )

            if parsed:
                try:
                    conn.executemany(sql, parsed)
                    any_rows = True
                except Exception as exc:
                    print(
                        f"[DEPTH_WRITER][ERROR] insert failed for {sym}: {exc}",
                        flush=True,
                    )

            buffered[sym] = []

        if any_rows:
            try:
                conn.commit()           # one commit covers all symbols
            except Exception as exc:
                print(f"[DEPTH_WRITER][ERROR] commit failed: {exc}", flush=True)