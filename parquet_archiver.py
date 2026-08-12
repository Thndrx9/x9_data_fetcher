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

import os
import re
import shutil
import signal
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from x9_data_fetcher.market_time import is_trading_day
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

    try:
        frames = [pd.read_parquet(c) for c in chunks]
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

    out_path = _daily_path(archive_root, prefix, symbol, day)
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

    try:
        frames = [pd.read_parquet(p) for p in daily_paths]
        src_count = sum(len(f) for f in frames)
        merged = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    except Exception as exc:
        return "failed", f"read failed: {exc}"

    out_path = _iso_week_path(archive_root, prefix, symbol, any_day_in_week)
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
# Standalone process entrypoint
# ---------------------------------------------------------------------------

_stop = False


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