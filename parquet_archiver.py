"""
Archives closed hourly SQLite files (produced by tick_writer.py's two
TickWriter instances — prefix="depth" and prefix="quote") into
zstd-compressed Parquet, one chunk per symbol per hour, then merges those
into daily and weekly files.

Design constraints this follows (see project notes):
  - Runs as its OWN OS process, never a thread inside the live fetcher —
    started via subprocess.Popen from start_data.py, not imported into it.
  - Raw JSON is stored as-is in a single string column — no parsing into
    typed columns. Simplicity/safety over cleverness.
  - Never touches the SQLite file the live writer is still writing to:
    a file is only ever a candidate once its hour is strictly in the past
    (or force_all=True is passed, which only start_data.py's end-of-day
    hook should do, once it KNOWS the writer has already shut down).
  - Verify row counts before deleting anything, always. If verification
    fails for any table in a file, the WHOLE file is left alone and
    retried on the next pass — nothing is ever partially cleaned up.

Layout produced:
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-<HH>depth.parquet         (hourly chunks)
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-<HH>quote.parquet
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-depth.parquet              (after daily merge — deduped)
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-quote.parquet
    <archive_root>/<YYYY>-W<WW>/<SYMBOL>-<YYYY>-W<WW>-depth.parquet (after weekly merge — its
    <archive_root>/<YYYY>-W<WW>/<SYMBOL>-<YYYY>-W<WW>-quote.parquet  own week folder, not the
                                                                     symbol's folder)
"""

import fcntl
import json
import os
import re
import shutil
import signal
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from x9_data_fetcher.fo_symbols import get_fo_underlyings, normalize_symbol
from x9_data_fetcher.market_time import is_trading_day, market_hours_for
from x9_data_fetcher.console import colorize as _colorize

_builtin_print = print


def print(*args, **kwargs):  # noqa: A001 — shadow builtin so every existing
    # print() call in this file picks up the shared color scheme without
    # having to edit each call site individually.
    if args and isinstance(args[0], str):
        args = (_colorize(args[0]),) + args[1:]
    _builtin_print(*args, **kwargs)

IST = ZoneInfo("Asia/Kolkata")

_TAG = "[ARCHIVER]"

# Matches the hourly filenames written by tick_writer.py's
# _hourly_db_path(): market_2026-06-12_14.db
_HOURLY_FILE_RE = re.compile(r"^market_(\d{4}-\d{2}-\d{2})_(\d{2})\.db$")

# Matches per-symbol tables inside those files: depth_RELIANCE, quote_TCS
_TABLE_RE = re.compile(r"^(depth|quote)_(.+)$")

COMPRESSION = "zstd"

# Round-based retry for merge_daily/merge_weekly (see each function). Kept
# much lower than backfill_manager.py's MAX_BACKFILL_ROUNDS=9: that one
# retries flaky NETWORK calls to a broker, where a later attempt
# genuinely has a good chance of succeeding. A merge failure here is
# local disk I/O (Parquet write, then read back to verify) — a write
# error or row-count mismatch is far more likely to be a real, persistent
# problem (disk full, corrupted chunk, permissions) than something that
# clears up between attempts, so there's little value in hammering it 9
# times.
MERGE_MAX_ROUNDS = 3

# Pause between retry rounds (merges, typed build, candle conversion).
RETRY_WAIT_SEC = float(os.getenv("X9_ARCHIVER_RETRY_WAIT_SEC", "30"))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _source_dirs() -> List[Path]:
    """
    The directories the live writers rotate hourly SQLite files into.
    X9_QUOTE_OUTPUT_DIR / X9_OHLC_OUTPUT_DIR alias to the same setting —
    mirrors the fallback chain start_data.py already uses.

    NOTE: these often point at the SAME directory (both default to "data"),
    since depth and quote/ohlc writers can share one physical SQLite file
    (their tables are prefixed depth_/quote_ so they never collide inside
    it). Callers must dedupe by resolved path — see run_once().
    """
    depth_dir = os.getenv("X9_DEPTH_OUTPUT_DIR", "data")
    quote_dir = os.getenv("X9_QUOTE_OUTPUT_DIR", os.getenv("X9_OHLC_OUTPUT_DIR", "data"))
    return [Path(depth_dir), Path(quote_dir)]


def _archive_root() -> Path:
    return Path(os.getenv("X9_ARCHIVE_DIR", "archive"))


def _poll_interval_sec() -> float:
    return float(os.getenv("X9_ARCHIVER_POLL_INTERVAL_SEC", "300"))


def _nice_level() -> int:
    return int(os.getenv("X9_ARCHIVER_NICE", "15"))


# ---------------------------------------------------------------------------
# Cross-process lock and retry wait
# ---------------------------------------------------------------------------

_stop = False  # set by the standalone process's signal handler

_thread_lock = threading.RLock()
_lock_depth = 0
_lock_handle = None


@contextmanager
def _archive_lock():
    """
    Only one archive operation at a time, across the standalone archiver
    process and start_data.py's end-of-day pass. Re-entrant within a thread.
    """
    global _lock_depth, _lock_handle
    with _thread_lock:
        if _lock_depth == 0:
            root = _archive_root()
            root.mkdir(parents=True, exist_ok=True)
            handle = open(root / ".archiver.lock", "w")
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if _stop:
                        handle.close()
                        raise RuntimeError("stopping while waiting for archive lock")
                    time.sleep(0.2)
            _lock_handle = handle
        _lock_depth += 1
        try:
            yield
        finally:
            _lock_depth -= 1
            if _lock_depth == 0:
                _lock_handle.close()  # closing releases the flock
                _lock_handle = None


def _wait_before_retry() -> bool:
    """Sleep RETRY_WAIT_SEC. Returns False if asked to stop meanwhile."""
    deadline = time.monotonic() + RETRY_WAIT_SEC
    while time.monotonic() < deadline:
        if _stop:
            return False
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    return not _stop


# ---------------------------------------------------------------------------
# Filename / hour helpers
# ---------------------------------------------------------------------------

def _parse_hourly_filename(name: str) -> Optional[Tuple[date, int]]:
    m = _HOURLY_FILE_RE.match(name)
    if not m:
        return None
    day = date.fromisoformat(m.group(1))
    hour = int(m.group(2))
    return day, hour


def _current_hour_key(now: Optional[datetime] = None) -> Tuple[date, int]:
    now = now or datetime.now(IST)
    return now.date(), now.hour


# ---------------------------------------------------------------------------
# Hourly chunk archiving
# ---------------------------------------------------------------------------

def _list_symbol_tables(conn: sqlite3.Connection) -> List[Tuple[str, str, str]]:
    """Returns [(table_name, prefix, symbol), ...] for depth_*/quote_* tables."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    out = []
    for (name,) in rows:
        m = _TABLE_RE.match(name)
        if m:
            out.append((name, m.group(1), m.group(2)))
    return out


def _row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _chunk_path(archive_root: Path, prefix: str, symbol: str, day: date, hour: int) -> Path:
    return archive_root / symbol / f"{day.isoformat()}-{hour:02d}{prefix}.parquet"


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, engine="pyarrow", compression=COMPRESSION, index=False)
    tmp.replace(path)  # atomic on same filesystem — no half-written chunk visible


def _archive_table(
    conn: sqlite3.Connection,
    table: str,
    prefix: str,
    symbol: str,
    day: date,
    hour: int,
    archive_root: Path,
    tag: str,
) -> bool:
    """Archive one symbol's table from this hourly file. Returns True iff
    the resulting Parquet chunk's row count matches the source exactly."""
    src_count = _row_count(conn, table)

    df = pd.read_sql_query(
        f"SELECT timestamp, ingest_ns, raw_json FROM {table} ORDER BY timestamp",
        conn,
    )
    path = _chunk_path(archive_root, prefix, symbol, day, hour)

    try:
        _write_parquet(df, path)
    except Exception as exc:
        print(f"{tag}[ERROR] write failed for {table}: {exc}", flush=True)
        return False

    try:
        written_count = len(pd.read_parquet(path, columns=["timestamp"]))
    except Exception as exc:
        print(f"{tag}[ERROR] verify-read failed for {table}: {exc}", flush=True)
        return False

    if written_count != src_count:
        print(
            f"{tag}[ERROR] row count mismatch for {table}: "
            f"sqlite={src_count} parquet={written_count} — chunk kept for "
            f"inspection but source file will NOT be deleted this pass",
            flush=True,
        )
        return False

    return True


def _delete_sqlite_file(db_path: Path, tag: str) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        p = db_path.with_name(db_path.name + suffix) if suffix else db_path
        try:
            if p.exists():
                p.unlink()
        except Exception as exc:
            print(f"{tag}[ERROR] failed to remove {p}: {exc}", flush=True)
    print(f"{tag} archived + removed {db_path.name}", flush=True)


def _archive_db_file(db_path: Path, archive_root: Path) -> bool:
    """Archive every symbol table in one closed hourly file. Deletes the
    file only if every table verified successfully. Returns True if the
    file was (fully) archived and deleted."""
    parsed = _parse_hourly_filename(db_path.name)
    if parsed is None:
        print(f"{_TAG}[WARN] unrecognized filename, skipping: {db_path.name}", flush=True)
        return False
    day, hour = parsed

    try:
        # read-only URI connection: the archiver must never write to a
        # file the live writer owns, even accidentally
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except Exception as exc:
        print(f"{_TAG}[ERROR] could not open {db_path.name}: {exc}", flush=True)
        return False

    try:
        tables = _list_symbol_tables(conn)
        if not tables:
            print(f"{_TAG} {db_path.name} has no symbol tables — deleting empty file", flush=True)
            conn.close()
            _delete_sqlite_file(db_path, _TAG)
            return True

        all_ok = True
        for table, prefix, symbol in tables:
            ok = _archive_table(conn, table, prefix, symbol, day, hour, archive_root, _TAG)
            all_ok = all_ok and ok
    finally:
        conn.close()

    if all_ok:
        _delete_sqlite_file(db_path, _TAG)
    else:
        print(
            f"{_TAG}[WARN] {db_path.name} had failures — left in place, will retry next pass",
            flush=True,
        )
    return all_ok


def run_once(force_all: bool = False) -> None:
    with _archive_lock():
        _run_once_locked(force_all)


def _run_once_locked(force_all: bool) -> None:
    """
    One archiving pass: find every closed hourly .db file across the
    configured source dirs and archive it.

    force_all=True also archives the file matching the CURRENT hour. Only
    safe to pass when the caller already knows the live writer has stopped
    (e.g. start_data.py right after fetcher.shutdown() at market close) —
    never from the standalone loop below.
    """
    archive_root = _archive_root()
    current_key = _current_hour_key()
    seen: set = set()
    scanned = 0
    archived = 0

    for base_dir in _source_dirs():
        if not base_dir.exists():
            print(f"{_TAG}[WARN] source dir does not exist: {base_dir.resolve()}", flush=True)
            continue
        for db_path in sorted(base_dir.glob("market_*.db")):
            resolved = db_path.resolve()
            if resolved in seen:
                continue  # depth/quote dirs can point at the same file
            seen.add(resolved)
            scanned += 1

            parsed = _parse_hourly_filename(db_path.name)
            if parsed is None:
                continue
            # >= rather than == : tick_writer.py now pre-opens and starts
            # writing to the NEXT hour's file up to PRE_OPEN_SECONDS before
            # the boundary (see tick_writer.py), so a file whose hour is
            # still in the future relative to this process's clock can
            # already be open and under active DDL from a live writer.
            # Treating only an exact match as "still being written" left
            # that pre-opened file unprotected and racing with the writer's
            # own CREATE TABLE calls — the direct cause of a
            # "database schema has changed" crash.
            if not force_all and parsed >= current_key:
                continue  # current hour, or a pre-opened future hour — never touch it

            if _archive_db_file(db_path, archive_root):
                archived += 1

    if scanned == 0:
        print(f"{_TAG} pass complete — no candidate files found", flush=True)
    else:
        print(f"{_TAG} pass complete — scanned {scanned}, archived {archived}", flush=True)


# ---------------------------------------------------------------------------
# Daily merge — concatenates a day's hourly chunks into one file per symbol
# ---------------------------------------------------------------------------

def _daily_path(archive_root: Path, prefix: str, symbol: str, day: date) -> Path:
    return archive_root / symbol / f"{day.isoformat()}-{prefix}.parquet"


_HOURLY_CHUNK_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-(\d{2})(depth|quote)\.parquet$")

# Weekly merges now live in archive_root/<YYYY-Www>/ folders, siblings of
# the per-symbol folders directly under archive_root. Anything matching
# this name is a week folder, not a symbol, and must be skipped wherever
# archive_root's immediate children are assumed to all be symbols.
_WEEK_FOLDER_RE = re.compile(r"^\d{4}-W\d{2}$")


def _merge_daily_one(
    symbol_dir: Path, symbol: str, prefix: str, day: date, archive_root: Path
) -> Tuple[str, object]:
    """
    Attempt to merge one symbol/prefix's hourly chunks for `day` into the
    daily file. Returns (status, info):
      - ("no_chunks", None)             — nothing to merge, not a failure
      - ("merged", (written_count, expected_dupes)) — success
      - ("failed", reason_str)          — caller decides whether to retry
    """
    # Regex-filtered rather than a bare glob, so an already-merged daily
    # file sitting in the same flat folder (e.g. 2026-07-31-quote.parquet,
    # no hour digits) is never mistaken for an hourly chunk and re-merged
    # into itself.
    chunks = sorted(
        p for p in symbol_dir.glob(f"{day.isoformat()}-*{prefix}.parquet")
        if _HOURLY_CHUNK_RE.match(p.name)
    )
    if not chunks:
        return "no_chunks", None

    out_path = _daily_path(archive_root, prefix, symbol, day)
    # A daily file from an earlier merge is merged in too, never overwritten
    # by only the chunks that turned up afterwards.
    sources = ([out_path] if out_path.exists() else []) + chunks

    try:
        frames = [pd.read_parquet(c) for c in sources]
        src_count = sum(len(f) for f in frames)
        concatenated = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    except Exception as exc:
        return "failed", f"read failed: {exc}"

    # Full-row dedup: the boundary overlap writes the exact same row
    # (identical timestamp, ingest_ns, raw_json) into both files, so an
    # exact-duplicate match is the safest possible key — it can never
    # accidentally collapse two genuinely different ticks that merely
    # share a timestamp.
    merged = concatenated.drop_duplicates(keep="first").reset_index(drop=True)
    expected_dupes = len(concatenated) - len(merged)

    try:
        _write_parquet(merged, out_path)
        written_count = len(pd.read_parquet(out_path, columns=["timestamp"]))
    except Exception as exc:
        return "failed", f"write/verify failed: {exc}"

    if written_count != src_count - expected_dupes:
        return "failed", (
            f"row count mismatch: chunks={src_count} "
            f"dupes_dropped={expected_dupes} merged={written_count}"
        )

    for c in chunks:
        c.unlink()
    return "merged", (written_count, expected_dupes)


def merge_daily(day: date, archive_root: Optional[Path] = None) -> None:
    """
    For every symbol with hourly chunks for `day`, concatenate them into one
    daily Parquet file, drop the duplicate rows produced by the writer's
    dual-write boundary overlap (see tick_writer.py — the last
    DUAL_WRITE_SECONDS of ticks before
    each hourly rollover are deliberately written to both the closing and
    the next hour's file, so a boundary race can never silently drop a
    tick), verify the resulting row count, then delete the hourly chunks.
    Symbols with no chunks for `day` are skipped. Safe to call more than
    once — already-merged symbols simply have no hourly chunks left to find.

    Round-based retry, same shape as backfill_manager.py's BackfillManager
    — each round retries only the (symbol, prefix) pairs that failed the
    previous round, up to MERGE_MAX_ROUNDS. Per-item success is silent;
    one summary line is printed at the end instead of one line per pair
    (this used to print one "merged ..." line per symbol per prefix — up
    to ~400 lines/day for 200 symbols).
    """
    archive_root = archive_root or _archive_root()
    tag = "[ARCHIVER:DAILY]"
    if not archive_root.exists():
        return

    # Build the initial pending set — every (symbol, prefix) pair that
    # actually has hourly chunks for `day`. No chunks means nothing to
    # merge for that pair, so it's never added and never counted as
    # pending/failed.
    items: List[Tuple[str, str]] = []
    for symbol_dir in sorted(archive_root.iterdir()):
        if not symbol_dir.is_dir() or _WEEK_FOLDER_RE.match(symbol_dir.name):
            continue
        symbol = symbol_dir.name
        for prefix in ("depth", "quote"):
            has_chunks = any(
                _HOURLY_CHUNK_RE.match(p.name)
                for p in symbol_dir.glob(f"{day.isoformat()}-*{prefix}.parquet")
            )
            if has_chunks:
                items.append((symbol, prefix))

    if not items:
        return

    merged_total = 0
    rows_total = 0
    dupes_total = 0
    last_failure_reason: Dict[Tuple[str, str], str] = {}
    round_num = 0

    while items and round_num < MERGE_MAX_ROUNDS:
        if round_num and not _wait_before_retry():
            break
        round_num += 1
        round_total = len(items)
        if round_num == 1:
            print(
                f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} — "
                f"merging {round_total} symbol/prefix pair(s)",
                flush=True,
            )
        else:
            print(
                f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} — "
                f"retrying {round_total} pair(s) that failed round {round_num - 1}",
                flush=True,
            )

        still_pending: List[Tuple[str, str]] = []
        round_success = 0

        for symbol, prefix in items:
            symbol_dir = archive_root / symbol
            status, info = _merge_daily_one(symbol_dir, symbol, prefix, day, archive_root)
            if status == "merged":
                written_count, dupes = info
                merged_total += 1
                rows_total += written_count
                dupes_total += dupes
                last_failure_reason.pop((symbol, prefix), None)
                round_success += 1
            elif status == "no_chunks":
                # Shouldn't happen given the up-front filter, but treat as
                # done rather than pending if a chunk vanishes mid-pass.
                round_success += 1
            else:
                still_pending.append((symbol, prefix))
                last_failure_reason[(symbol, prefix)] = str(info)

        items = still_pending
        if items:
            if round_num < MERGE_MAX_ROUNDS:
                print(
                    f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} done — "
                    f"success={round_success} pending={len(items)} "
                    f"→ retrying pending in round {round_num + 1}",
                    flush=True,
                )
            else:
                print(
                    f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} done — "
                    f"success={round_success} pending={len(items)}",
                    flush=True,
                )
        else:
            print(
                f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} done — "
                f"success={round_success} pending=0 → all recovered, stopping early",
                flush=True,
            )

    summary = (
        f"{tag} merge complete — {merged_total} merged ({rows_total} rows, "
        f"{dupes_total} boundary duplicate(s) dropped)"
    )
    if items:
        summary += f", {len(items)} still failing"
    print(summary, flush=True)

    if items:
        names = ", ".join(
            f"{symbol}/{prefix} ({last_failure_reason.get((symbol, prefix), 'unknown')})"
            for symbol, prefix in items
        )
        print(
            f"{tag}[WARN] {len(items)} pair(s) still failing after "
            f"{round_num} round(s), hourly chunks kept: {names}",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Weekly merge — concatenates a week's daily files into one file per symbol
# ---------------------------------------------------------------------------

def _iso_week_path(archive_root: Path, prefix: str, symbol: str, any_day_in_week: date) -> Path:
    iso_year, iso_week, _ = any_day_in_week.isocalendar()
    week_folder = f"{iso_year}-W{iso_week:02d}"
    return archive_root / week_folder / f"{symbol}-{week_folder}-{prefix}.parquet"


def _days_in_iso_week(any_day_in_week: date) -> List[date]:
    monday = any_day_in_week - timedelta(days=any_day_in_week.weekday())
    return [monday + timedelta(days=i) for i in range(7)]


def is_last_trading_day_of_week(day: date) -> bool:
    """True if no later day in day's ISO week (Mon-Sun) is a trading day."""
    for d in _days_in_iso_week(day):
        if d > day and is_trading_day(d):
            return False
    return True


def _merge_weekly_one(
    symbol_dir: Path,
    symbol: str,
    prefix: str,
    week_days: List[date],
    archive_root: Path,
    any_day_in_week: date,
) -> Tuple[str, object]:
    """
    Attempt to merge one symbol/prefix's daily files for the week into the
    weekly file. Same (status, info) contract as _merge_daily_one.
    """
    daily_paths = [
        symbol_dir / f"{d.isoformat()}-{prefix}.parquet"
        for d in week_days
        if (symbol_dir / f"{d.isoformat()}-{prefix}.parquet").exists()
    ]
    if not daily_paths:
        return "no_chunks", None

    out_path = _iso_week_path(archive_root, prefix, symbol, any_day_in_week)
    try:
        frames = [pd.read_parquet(p) for p in daily_paths]
        if out_path.exists():
            # week already partly merged (late daily file) — keep what's there
            frames.insert(0, pd.read_parquet(out_path))
            merged = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
        else:
            merged = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        src_count = len(merged)
    except Exception as exc:
        return "failed", f"read failed: {exc}"

    try:
        _write_parquet(merged, out_path)
        written_count = len(pd.read_parquet(out_path, columns=["timestamp"]))
    except Exception as exc:
        return "failed", f"write/verify failed: {exc}"

    if written_count != src_count:
        return "failed", f"row count mismatch: dailies={src_count} merged={written_count}"

    for p in daily_paths:
        p.unlink()
    return "merged", written_count


def merge_weekly(any_day_in_week: date, archive_root: Optional[Path] = None) -> None:
    """
    For every symbol/table with daily files in any_day_in_week's ISO week,
    concatenate them into one weekly Parquet file, verify, then delete the
    daily files. Intended to be called on the last trading day of the week
    (see is_last_trading_day_of_week) — but merges whatever daily files
    exist for the week regardless of which day it's called on.

    Same round-based retry + single summary line as merge_daily.
    """
    archive_root = archive_root or _archive_root()
    tag = "[ARCHIVER:WEEKLY]"
    week_days = _days_in_iso_week(any_day_in_week)

    if not archive_root.exists():
        return

    items: List[Tuple[str, str]] = []
    for symbol_dir in sorted(archive_root.iterdir()):
        if not symbol_dir.is_dir() or _WEEK_FOLDER_RE.match(symbol_dir.name):
            continue
        symbol = symbol_dir.name
        for prefix in ("depth", "quote"):
            has_dailies = any(
                (symbol_dir / f"{d.isoformat()}-{prefix}.parquet").exists()
                for d in week_days
            )
            if has_dailies:
                items.append((symbol, prefix))

    if not items:
        return

    merged_total = 0
    rows_total = 0
    last_failure_reason: Dict[Tuple[str, str], str] = {}
    round_num = 0

    while items and round_num < MERGE_MAX_ROUNDS:
        if round_num and not _wait_before_retry():
            break
        round_num += 1
        round_total = len(items)
        if round_num == 1:
            print(
                f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} — "
                f"merging {round_total} symbol/prefix pair(s)",
                flush=True,
            )
        else:
            print(
                f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} — "
                f"retrying {round_total} pair(s) that failed round {round_num - 1}",
                flush=True,
            )

        still_pending: List[Tuple[str, str]] = []
        round_success = 0

        for symbol, prefix in items:
            symbol_dir = archive_root / symbol
            status, info = _merge_weekly_one(
                symbol_dir, symbol, prefix, week_days, archive_root, any_day_in_week
            )
            if status == "merged":
                merged_total += 1
                rows_total += info
                last_failure_reason.pop((symbol, prefix), None)
                round_success += 1
            elif status == "no_chunks":
                round_success += 1
            else:
                still_pending.append((symbol, prefix))
                last_failure_reason[(symbol, prefix)] = str(info)

        items = still_pending
        if items:
            if round_num < MERGE_MAX_ROUNDS:
                print(
                    f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} done — "
                    f"success={round_success} pending={len(items)} "
                    f"→ retrying pending in round {round_num + 1}",
                    flush=True,
                )
            else:
                print(
                    f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} done — "
                    f"success={round_success} pending={len(items)}",
                    flush=True,
                )
        else:
            print(
                f"{tag} round {round_num}/{MERGE_MAX_ROUNDS} done — "
                f"success={round_success} pending=0 → all recovered, stopping early",
                flush=True,
            )

    summary = f"{tag} merge complete — {merged_total} merged ({rows_total} rows)"
    if items:
        summary += f", {len(items)} still failing"
    print(summary, flush=True)

    if items:
        names = ", ".join(
            f"{symbol}/{prefix} ({last_failure_reason.get((symbol, prefix), 'unknown')})"
            for symbol, prefix in items
        )
        print(
            f"{tag}[WARN] {len(items)} pair(s) still failing after "
            f"{round_num} round(s), daily files kept: {names}",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Daily typed-column export (additive) — a second, parallel copy of each
# day's already-merged daily Parquet (produced by merge_daily above), with
# raw_json parsed into typed columns instead of one opaque JSON string
# column. Purely reads merge_daily's finished output; never reads, writes,
# or deletes anything in the existing hourly/daily/weekly layout above.
#
# Layout produced:
#   <archive_root>/<DD-MM-YYYY>/<DD-MM-YYYY>_quote.parquet
#   <archive_root>/<DD-MM-YYYY>/<DD-MM-YYYY>_depth.parquet
#
# Call build_daily_typed(day) AFTER merge_daily(day) has run for that day.
# Old typed date-folders are pruned automatically each call — retention
# is X9_TYPED_DAILY_RETENTION_DAYS days (default 3).
# ---------------------------------------------------------------------------

_DEPTH_LEVELS = 5

# Fields present on every quote/depth tick payload seen in production data.
_QUOTE_COMMON_FIELDS = [
    "symbol", "exchange", "mode", "ltp", "ltt", "volume",
    "open", "high", "low", "close",
]
# Present only on the "trade" variant of quote payloads — NULL elsewhere.
_QUOTE_TRADE_FIELDS = [
    "last_trade_quantity", "average_price",
    "total_buy_quantity", "total_sell_quantity",
]
# Present on the "full" variant of quote payloads, and on all depth
# payloads — NULL on the "trade"/"daily_close" quote variants.
_QUOTE_FULL_FIELDS = [
    "last_quantity", "oi", "upper_circuit", "lower_circuit",
]
# Present only on the rare synthetic daily-close variant — NULL elsewhere.
_QUOTE_DAILY_CLOSE_FIELDS = ["interval", "source"]


def _typed_retention_days() -> int:
    return int(os.getenv("X9_TYPED_DAILY_RETENTION_DAYS", "3"))


def _typed_day_folder(day: date) -> str:
    return day.strftime("%d-%m-%Y")


def _typed_daily_dir(archive_root: Path, day: date) -> Path:
    return archive_root / _typed_day_folder(day)


def _typed_combined_path(archive_root: Path, day: date, prefix: str) -> Path:
    return _typed_daily_dir(archive_root, day) / f"{_typed_day_folder(day)}_{prefix}.parquet"


def _classify_quote_record(d: dict) -> str:
    if "source" in d:
        return "daily_close"
    if "last_trade_quantity" in d:
        return "trade"
    return "full"


# Fixed, explicit schemas — every symbol's table is built to conform to
# these exactly, so a ParquetWriter can stream one symbol's table at a
# time straight to disk instead of holding every symbol in memory before
# writing (that all-symbols-at-once approach is what OOM-killed the
# service on a 398-symbol/prefix archive — see build_daily_typed below).
_QUOTE_TYPED_SCHEMA = pa.schema(
    [
        ("timestamp", pa.int64()),
        ("ingest_ns", pa.int64()),
        ("symbol", pa.string()),
        ("record_type", pa.string()),
        ("exchange", pa.string()),
        ("mode", pa.int64()),
        ("ltp", pa.float64()),
        ("ltt", pa.int64()),
        ("volume", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("last_trade_quantity", pa.int64()),
        ("average_price", pa.float64()),
        ("total_buy_quantity", pa.float64()),
        ("total_sell_quantity", pa.float64()),
        ("last_quantity", pa.int64()),
        ("oi", pa.int64()),
        ("upper_circuit", pa.float64()),
        ("lower_circuit", pa.float64()),
        ("interval", pa.string()),
        ("source", pa.string()),
    ]
)

_DEPTH_TYPED_SCHEMA = pa.schema(
    [
        ("timestamp", pa.int64()),
        ("ingest_ns", pa.int64()),
        ("symbol", pa.string()),
        ("exchange", pa.string()),
        ("mode", pa.int64()),
        ("ltp", pa.float64()),
        ("ltt", pa.int64()),
        ("volume", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("last_quantity", pa.int64()),
        ("oi", pa.int64()),
        ("upper_circuit", pa.float64()),
        ("lower_circuit", pa.float64()),
    ]
    + [
        (f"{side}_{field}_{i}", pa.int64() if field in ("qty", "orders") else pa.float64())
        for side in ("buy", "sell")
        for i in range(1, _DEPTH_LEVELS + 1)
        for field in ("price", "qty", "orders")
    ]
)


def _quote_columns(daily_path: Path, symbol: str) -> Dict[str, list]:
    """Read one already-merged daily quote Parquet file and return its
    parsed-from-raw_json data as a dict of plain Python lists, column-name
    -> values, matching _QUOTE_TYPED_SCHEMA exactly. Plain lists (not a
    pandas DataFrame) so a value the JSON payload doesn't have for that
    row stays a real None straight through to pa.table(), instead of
    pandas silently upcasting a missing int column to float NaN."""
    df = pd.read_parquet(daily_path, columns=["timestamp", "ingest_ns", "raw_json"])
    fields = (
        _QUOTE_COMMON_FIELDS + _QUOTE_TRADE_FIELDS + _QUOTE_FULL_FIELDS + _QUOTE_DAILY_CLOSE_FIELDS
    )
    cols: Dict[str, list] = {f: [] for f in fields}
    record_types: List[str] = []
    for s in df["raw_json"]:
        d = json.loads(s)
        record_types.append(_classify_quote_record(d))
        for f in fields:
            cols[f].append(d.get(f))
    cols["timestamp"] = df["timestamp"].tolist()
    cols["ingest_ns"] = df["ingest_ns"].tolist()
    cols["symbol"] = [symbol] * len(df)
    cols["record_type"] = record_types
    return cols


def _depth_columns(daily_path: Path, symbol: str) -> Dict[str, list]:
    """Same idea as _quote_columns but for depth payloads: top-level
    quote-style fields plus fixed buy/sell price-ladder columns, matching
    _DEPTH_TYPED_SCHEMA exactly. A depth level beyond _DEPTH_LEVELS is
    dropped; a missing level is None (→ NULL) — either way this never
    raises on one odd payload."""
    df = pd.read_parquet(daily_path, columns=["timestamp", "ingest_ns", "raw_json"])
    top_fields = _QUOTE_COMMON_FIELDS + _QUOTE_FULL_FIELDS
    level_cols = [
        f"{side}_{field}_{i}"
        for side in ("buy", "sell")
        for i in range(1, _DEPTH_LEVELS + 1)
        for field in ("price", "qty", "orders")
    ]
    cols: Dict[str, list] = {f: [] for f in top_fields + level_cols}
    for s in df["raw_json"]:
        d = json.loads(s)
        for f in top_fields:
            cols[f].append(d.get(f))
        depth = d.get("depth") or {}
        for side in ("buy", "sell"):
            levels = depth.get(side) or []
            for i in range(_DEPTH_LEVELS):
                lvl = levels[i] if i < len(levels) else {}
                cols[f"{side}_price_{i + 1}"].append(lvl.get("price"))
                cols[f"{side}_qty_{i + 1}"].append(lvl.get("quantity"))
                cols[f"{side}_orders_{i + 1}"].append(lvl.get("orders"))
    cols["timestamp"] = df["timestamp"].tolist()
    cols["ingest_ns"] = df["ingest_ns"].tolist()
    cols["symbol"] = [symbol] * len(df)
    return cols


def _typed_symbol_dirs(archive_root: Path) -> List[Path]:
    return [
        p for p in sorted(archive_root.iterdir())
        if p.is_dir() and not _WEEK_FOLDER_RE.match(p.name)
    ]


def _unmerged_sources(archive_root: Path, day: date) -> List[str]:
    """Data for `day` that hasn't reached a daily file yet: hourly SQLite
    files still waiting to be archived, and unmerged hourly chunks."""
    found = []
    seen: set = set()
    for base_dir in _source_dirs():
        for db_path in sorted(base_dir.glob(f"market_{day.isoformat()}_*.db")):
            if db_path.resolve() not in seen:
                seen.add(db_path.resolve())
                found.append(f"{db_path.name} (not archived)")
    for symbol_dir in _typed_symbol_dirs(archive_root):
        for p in sorted(symbol_dir.glob(f"{day.isoformat()}-*.parquet")):
            if _HOURLY_CHUNK_RE.match(p.name):
                found.append(f"{symbol_dir.name}/{p.name} (not merged)")
    return found


def _build_typed_prefix(
    archive_root: Path, day: date, prefix: str, schema: pa.Schema, columns_fn
) -> List[str]:
    """
    Build one combined typed file. Streams one symbol at a time to disk (all
    symbols in memory at once is what can trigger the OOM killer). The file
    is published only if EVERY symbol with a daily file made it in and the
    row count verifies. Returns failure reasons; empty means success (or no
    daily files for this prefix, in which case nothing is written).
    """
    out_path = _typed_combined_path(archive_root, day, prefix)
    tmp_path = out_path.with_suffix(".parquet.tmp")
    failures: List[str] = []
    expected_rows = 0
    writer: Optional[pq.ParquetWriter] = None
    try:
        for symbol_dir in _typed_symbol_dirs(archive_root):
            symbol = symbol_dir.name
            daily_path = _daily_path(archive_root, prefix, symbol, day)
            if not daily_path.exists():
                continue
            try:
                table = pa.table(columns_fn(daily_path, symbol), schema=schema)
            except Exception as exc:
                failures.append(f"{symbol}/{prefix} (read/parse failed: {exc})")
                continue
            if writer is None:
                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(tmp_path, schema, compression=COMPRESSION)
            writer.write_table(table)
            expected_rows += table.num_rows
            del table  # release this symbol's data before the next one
        if writer is not None:
            writer.close()
            writer = None
            if not failures:
                written = pq.ParquetFile(tmp_path).metadata.num_rows
                if written != expected_rows:
                    failures.append(
                        f"combined {prefix} (row count mismatch: "
                        f"source={expected_rows} typed={written})"
                    )
                else:
                    tmp_path.replace(out_path)  # atomic on same filesystem
    except Exception as exc:
        failures.append(f"combined {prefix} (write failed: {exc})")
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        tmp_path.unlink(missing_ok=True)
    return failures


def build_daily_typed(day: date, archive_root: Optional[Path] = None) -> bool:
    """
    Additive export: parse every symbol's merged daily Parquet for `day` into
    typed columns and write ONE combined file per prefix:

        <archive_root>/<DD-MM-YYYY>/<DD-MM-YYYY>_quote.parquet
        <archive_root>/<DD-MM-YYYY>/<DD-MM-YYYY>_depth.parquet

    Reads the daily files only; call after merge_daily(day).

    Up to MERGE_MAX_ROUNDS rounds. Any existing typed tick files for `day`
    are removed first, and a file is only published when complete, so a typed
    file that exists is always a complete one. Returns True only if there was
    data and all of it was built.
    """
    archive_root = archive_root or _archive_root()
    tag = "[ARCHIVER:TYPED]"
    with _archive_lock():
        if not archive_root.exists():
            print(f"{tag}[WARN] archive dir not found: {archive_root.resolve()}", flush=True)
            return False

        for prefix in ("depth", "quote"):
            _typed_combined_path(archive_root, day, prefix).unlink(missing_ok=True)

        pending = {
            "depth": (_DEPTH_TYPED_SCHEMA, _depth_columns),
            "quote": (_QUOTE_TYPED_SCHEMA, _quote_columns),
        }
        failures: List[str] = []
        for round_num in range(1, MERGE_MAX_ROUNDS + 1):
            if round_num > 1:
                if not _wait_before_retry():
                    break
                merge_daily(day, archive_root)

            failures = _unmerged_sources(archive_root, day)
            if not failures:
                for prefix, (schema, columns_fn) in list(pending.items()):
                    errors = _build_typed_prefix(archive_root, day, prefix, schema, columns_fn)
                    if errors:
                        failures.extend(errors)
                    else:
                        del pending[prefix]
            if not failures:
                break
            print(
                f"{tag}[WARN] {_typed_day_folder(day)} round {round_num}/{MERGE_MAX_ROUNDS} "
                f"failed: {', '.join(failures)}",
                flush=True,
            )

        if failures:
            print(
                f"{tag}[ERROR] {_typed_day_folder(day)} typed tick files incomplete — "
                f"will retry at next startup or market close",
                flush=True,
            )
            return False

        built = [
            p for p in ("depth", "quote")
            if _typed_combined_path(archive_root, day, p).exists()
        ]
        if not built:
            print(f"{tag} no daily files found for {day.isoformat()} — nothing built", flush=True)
            return False
        print(
            f"{tag} {_typed_day_folder(day)} — built {len(built)} combined file(s) "
            f"in {_typed_daily_dir(archive_root, day).resolve()}",
            flush=True,
        )
        return True


# ---------------------------------------------------------------------------
# Candle conversion — older typed tick days become 1-minute candles
# ---------------------------------------------------------------------------
#
#   <archive_root>/<DD-MM-YYYY>/<DD-MM-YYYY>_candle1m.parquet
#
# Same rules as candle_archiver._BUILD_1M_CANDLES_SQL (the Postgres
# candle1m_<symbol> builder). Keep the two in step.

_CANDLE_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("ts_ms", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
    ]
)

_IST_OFFSET_MS = 19_800_000
_DAY_MS = 86_400_000
_SESSION_OPEN_MS = 33_300_000   # 09:15 IST since midnight
_FO_CUTOFF_MS = 54_900_000      # 15:15 IST since midnight
_VOLUME_GAP_MS = 90_000


def _build_candles(df: pd.DataFrame, day: date, is_fo: bool) -> pd.DataFrame:
    """1-minute candles for ONE symbol's typed quote ticks."""
    day_start = int(datetime(day.year, day.month, day.day, tzinfo=IST).timestamp() * 1000)
    # daily_close is the broker's synthetic EOD row, not a tick — it never
    # reaches the Postgres quote table the SQL builder reads.
    df = df[(df["record_type"] != "daily_close") & df["timestamp"].notna() & df["ltp"].notna()]
    ts = df["timestamp"].astype("int64")
    since_midnight = (ts + _IST_OFFSET_MS) % _DAY_MS
    keep = (ts >= day_start) & (ts < day_start + _DAY_MS) & (since_midnight >= _SESSION_OPEN_MS)
    if is_fo:
        keep &= since_midnight < _FO_CUTOFF_MS
    df = df[keep].sort_values(["timestamp", "ingest_ns"], kind="stable")
    if df.empty:
        return pd.DataFrame(columns=_CANDLE_SCHEMA.names[1:])

    ts = df["timestamp"].astype("int64")
    cum = df["volume"].fillna(0).astype("int64")
    # first tick of the day keeps its whole cumulative volume
    volume = cum.diff().fillna(cum).clip(lower=0)
    volume = volume.mask(ts.diff() > _VOLUME_GAP_MS, 0)
    since_open = (ts + _IST_OFFSET_MS) % _DAY_MS - _SESSION_OPEN_MS

    ticks = pd.DataFrame(
        {
            "ts_ms": day_start + _SESSION_OPEN_MS + since_open // 60_000 * 60_000,
            "ltp": df["ltp"],
            "volume": volume.astype("int64"),
        }
    )
    return (
        ticks.groupby("ts_ms", sort=True)
        .agg(
            open=("ltp", "first"),
            high=("ltp", "max"),
            low=("ltp", "min"),
            close=("ltp", "last"),
            volume=("volume", "sum"),
        )
        .reset_index()
    )


def _iter_symbol_ticks(path: Path):
    """Yield (symbol, ticks) one symbol at a time from a combined typed quote
    file (written symbol by symbol, so each symbol's rows are contiguous)."""
    columns = ["symbol", "record_type", "timestamp", "ingest_ns", "ltp", "volume"]
    parquet = pq.ParquetFile(path)
    symbol, parts = None, []
    for i in range(parquet.num_row_groups):
        group = parquet.read_row_group(i, columns=columns).to_pandas()
        for name, rows in group.groupby("symbol", sort=False):
            if name != symbol and parts:
                yield symbol, pd.concat(parts, ignore_index=True)
                parts = []
            symbol = name
            parts.append(rows)
    if parts:
        yield symbol, pd.concat(parts, ignore_index=True)


def _convert_once(archive_root: Path, day: date, fo_symbols: set) -> int:
    """Write and verify the candle file. Raises on any problem."""
    source = _typed_combined_path(archive_root, day, "quote")
    out_path = _typed_combined_path(archive_root, day, "candle1m")
    tmp_path = out_path.with_suffix(".parquet.tmp")
    rows = 0
    volume = 0
    try:
        with pq.ParquetWriter(tmp_path, _CANDLE_SCHEMA, compression=COMPRESSION) as writer:
            for symbol, ticks in _iter_symbol_ticks(source):
                candles = _build_candles(ticks, day, normalize_symbol(symbol) in fo_symbols)
                if candles.empty:
                    continue
                candles.insert(0, "symbol", symbol)
                writer.write_table(
                    pa.Table.from_pandas(candles, schema=_CANDLE_SCHEMA, preserve_index=False)
                )
                rows += len(candles)
                volume += int(candles["volume"].sum())
        if rows == 0:
            raise ValueError("no candles produced from the tick file")
        check = pq.read_table(tmp_path, columns=["volume"])
        written_volume = int(check.column("volume").to_pandas().sum())
        if check.num_rows != rows or written_volume != volume:
            raise ValueError(
                f"verification failed: rows {check.num_rows}/{rows}, "
                f"volume {written_volume}/{volume}"
            )
        tmp_path.replace(out_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return rows


def convert_day_to_candles(day: date, api_key: str, archive_root: Optional[Path] = None) -> bool:
    """
    Turn one day's typed quote ticks into 1-minute candles, then delete that
    day's typed quote and depth files. Tick files are deleted only after the
    candle file is written and verified. Up to MERGE_MAX_ROUNDS rounds.
    Per-symbol daily and weekly files are never touched.
    """
    archive_root = archive_root or _archive_root()
    tag = "[ARCHIVER:CANDLE]"
    label = _typed_day_folder(day)
    with _archive_lock():
        quote_path = _typed_combined_path(archive_root, day, "quote")
        if not quote_path.exists():
            return False
        for round_num in range(1, MERGE_MAX_ROUNDS + 1):
            if round_num > 1 and not _wait_before_retry():
                break
            try:
                fo_symbols = {normalize_symbol(s) for s in get_fo_underlyings(api_key)}
                # Ticks get deleted afterwards, so never guess the 15:15 cutoff.
                if not fo_symbols:
                    raise ValueError("F&O symbol list unavailable")
                rows = _convert_once(archive_root, day, fo_symbols)
                # depth first: if interrupted, the quote file still marks the
                # day as unfinished and the next pass completes it
                _typed_combined_path(archive_root, day, "depth").unlink(missing_ok=True)
                quote_path.unlink()
                print(f"{tag} {label} — {rows} candle(s) written, tick files removed", flush=True)
                return True
            except Exception as exc:
                print(
                    f"{tag}[WARN] {label} round {round_num}/{MERGE_MAX_ROUNDS} failed: {exc}",
                    flush=True,
                )
        print(
            f"{tag}[ERROR] {label} not converted — tick files kept, "
            f"will retry at next startup or market close",
            flush=True,
        )
        return False


# ---------------------------------------------------------------------------
# Closed-day pipeline — end of day, and catch-up at startup
# ---------------------------------------------------------------------------

def latest_closed_day(now: Optional[datetime] = None) -> date:
    """The most recent trading day whose session has already ended."""
    now = now or datetime.now(IST)
    day = now.date()
    if not (is_trading_day(day) and now.time() >= market_hours_for(day)[1]):
        day -= timedelta(days=1)
        while not is_trading_day(day):
            day -= timedelta(days=1)
    return day


def _is_converted(archive_root: Path, day: date) -> bool:
    return (
        _typed_combined_path(archive_root, day, "candle1m").exists()
        and not _typed_combined_path(archive_root, day, "quote").exists()
    )


def _has_daily_files(archive_root: Path, day: date, prefix: str) -> bool:
    return any(
        _daily_path(archive_root, prefix, d.name, day).exists()
        for d in _typed_symbol_dirs(archive_root)
    )


def _is_typed_complete(archive_root: Path, day: date) -> bool:
    """True if nothing more needs to be read from `day`'s daily files."""
    if _unmerged_sources(archive_root, day):
        return False
    if _is_converted(archive_root, day):
        return True
    return all(
        _typed_combined_path(archive_root, day, prefix).exists()
        or not _has_daily_files(archive_root, day, prefix)
        for prefix in ("depth", "quote")
    )


def _days_with_chunks(archive_root: Path, latest: date) -> List[date]:
    days = set()
    for symbol_dir in _typed_symbol_dirs(archive_root):
        for p in symbol_dir.glob("*.parquet"):
            m = _HOURLY_CHUNK_RE.match(p.name)
            if m:
                days.add(date.fromisoformat(m.group(1)))
    return sorted(d for d in days if d <= latest)


def _merge_finished_weeks(archive_root: Path, latest: date, kept: List[date]) -> None:
    """
    Weekly-merge every finished week that still has daily files. A week is
    postponed while any day still inside typed retention hasn't been built
    from those daily files (the merge deletes them).
    """
    weeks: Dict[Tuple[int, int], date] = {}
    for symbol_dir in _typed_symbol_dirs(archive_root):
        for p in symbol_dir.glob("*.parquet"):
            m = re.match(r"^(\d{4}-\d{2}-\d{2})-(depth|quote)\.parquet$", p.name)
            if m:
                d = date.fromisoformat(m.group(1))
                weeks[tuple(d.isocalendar())[:2]] = d

    for any_day in sorted(weeks.values()):
        week_days = _days_in_iso_week(any_day)
        if week_days[0] > latest or any(d > latest and is_trading_day(d) for d in week_days):
            continue  # week still in progress
        blocked = [
            d for d in week_days
            if d in kept and not _is_typed_complete(archive_root, d)
        ]
        if blocked:
            print(
                f"[ARCHIVER:WEEKLY][WARN] merge postponed — typed files not built for "
                f"{', '.join(d.isoformat() for d in blocked)}",
                flush=True,
            )
            continue
        merge_weekly(any_day, archive_root)


def process_closed_days(
    latest: date, api_key: str, rebuild_latest: bool = False,
    archive_root: Optional[Path] = None,
) -> None:
    """
    Bring the archive up to date through `latest` (a closed trading day):
      1. merge any leftover hourly chunks into daily files
      2. build missing typed tick files for days still inside retention
      3. convert every day OLDER than `latest` to candles — only once
         `latest`'s own tick files are complete, so there is always one full
         day of ticks
      4. prune typed folders past retention
      5. weekly-merge finished weeks

    rebuild_latest=True (end of day) always rebuilds `latest`'s tick files.
    """
    archive_root = archive_root or _archive_root()
    with _archive_lock():
        if not archive_root.exists():
            return

        merged_days = _days_with_chunks(archive_root, latest)
        for day in merged_days:
            merge_daily(day, archive_root)

        retention = max(1, _typed_retention_days())
        kept = [latest]
        day = latest
        while len(kept) < retention:
            day -= timedelta(days=1)
            if is_trading_day(day):
                kept.insert(0, day)

        for day in kept:
            if _is_converted(archive_root, day):
                continue
            has_data = bool(_unmerged_sources(archive_root, day)) or any(
                _has_daily_files(archive_root, day, p) for p in ("depth", "quote")
            )
            if not has_data:
                continue
            stale = day in merged_days or (rebuild_latest and day == latest)
            if stale or not _is_typed_complete(archive_root, day):
                build_daily_typed(day, archive_root)

        latest_ready = (
            _typed_combined_path(archive_root, latest, "quote").exists()
            and _is_typed_complete(archive_root, latest)
        )
        if latest_ready:
            for day in kept[:-1]:
                if _is_typed_complete(archive_root, day):
                    convert_day_to_candles(day, api_key, archive_root)
        else:
            print(
                f"[ARCHIVER:CANDLE] tick files for {_typed_day_folder(latest)} not complete "
                f"— older days left as ticks",
                flush=True,
            )

        prune_typed_daily(archive_root, reference_day=latest)
        _merge_finished_weeks(archive_root, latest, kept)


def run_end_of_day(day: date, api_key: str) -> None:
    """Call only after the live writers have shut down for the day."""
    with _archive_lock():
        run_once(force_all=True)
        process_closed_days(day, api_key, rebuild_latest=True)


def _trading_day_cutoff(reference_day: date, retention_trading_days: int) -> date:
    """
    Walks backward from `reference_day`, counting trading days (weekends
    and market holidays via is_trading_day() don't count), and returns the
    date of the `retention_trading_days`-th most recent trading day
    at-or-before `reference_day`. That returned date is the OLDEST day
    that should still be kept — anything strictly before it is prunable.

    e.g. retention_trading_days=3, reference_day=Wed → walks Wed(1),
    Tue(2), Mon(3) → returns Mon. If reference_day itself falls on a
    weekend/holiday it doesn't count, so the walk just continues further
    back until it finds retention_trading_days actual trading days.
    """
    day = reference_day
    counted = 0
    while True:
        if is_trading_day(day):
            counted += 1
            if counted >= retention_trading_days:
                return day
        day -= timedelta(days=1)


def prune_typed_daily(
    archive_root: Optional[Path] = None,
    retention_days: Optional[int] = None,
    reference_day: Optional[date] = None,
) -> None:
    """
    Deletes typed date-folders (<archive_root>/<DD-MM-YYYY>/) that fall
    before the `retention_days`-th most recent TRADING day at-or-before
    `reference_day` (default: today IST; default retention:
    X9_TYPED_DAILY_RETENTION_DAYS env var, else 3) — weekends and market
    holidays are skipped when counting back, via is_trading_day(), so a
    weekend never eats into the window. Only ever touches folders whose
    name matches the DD-MM-YYYY typed-folder pattern — never a symbol
    folder or a week folder (those use different name patterns and are
    left alone).
    """
    archive_root = archive_root or _archive_root()
    retention_days = retention_days if retention_days is not None else _typed_retention_days()
    reference_day = reference_day or datetime.now(IST).date()
    tag = "[ARCHIVER:TYPED]"

    if not archive_root.exists():
        return

    cutoff = _trading_day_cutoff(reference_day, retention_days)

    for entry in archive_root.iterdir():
        if not entry.is_dir():
            continue
        try:
            folder_day = datetime.strptime(entry.name, "%d-%m-%Y").date()
        except ValueError:
            continue  # not a typed date-folder (symbol dir or week folder) — leave alone
        if folder_day < cutoff:
            try:
                shutil.rmtree(entry)
                print(
                    f"{tag} pruned {entry.name} (before {retention_days}-trading-day cutoff {cutoff.isoformat()})",
                    flush=True,
                )
            except Exception as exc:
                print(f"{tag}[ERROR] failed to prune {entry.name}: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Standalone process entrypoint
# ---------------------------------------------------------------------------

def _handle_signal(signum, frame) -> None:
    global _stop
    _stop = True


def main() -> None:
    try:
        os.nice(_nice_level())
    except Exception as exc:
        print(f"{_TAG}[WARN] could not renice process: {exc}", flush=True)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    interval = _poll_interval_sec()
    print(
        f"{_TAG} started (pid={os.getpid()}, poll every {interval:.0f}s, "
        f"archive_root={_archive_root()})",
        flush=True,
    )

    # Catch up anything a missed or failed market close left behind. Runs in
    # this separate, reniced process so it never competes with live ingestion.
    try:
        run_once()
        process_closed_days(latest_closed_day(), os.getenv("API_KEY", ""))
    except Exception as exc:
        print(f"{_TAG}[ERROR] startup catch-up failed: {exc}", flush=True)

    while not _stop:
        try:
            run_once()
        except Exception as exc:
            print(f"{_TAG}[ERROR] archiving pass failed: {exc}", flush=True)
        for _ in range(int(interval * 10)):
            if _stop:
                break
            time.sleep(0.1)

    print(f"{_TAG} stopped (pid={os.getpid()})", flush=True)


if __name__ == "__main__":
    if __package__ is None or __package__ == "":
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    main()
