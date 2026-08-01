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
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-<HH>depth.parquet   (hourly chunks)
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-<HH>quote.parquet
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-depth.parquet        (after daily merge — deduped)
    <archive_root>/<SYMBOL>/<YYYY-MM-DD>-quote.parquet
    <archive_root>/<SYMBOL>/<YYYY>-W<WW>-depth.parquet        (after weekly merge)
    <archive_root>/<SYMBOL>/<YYYY>-W<WW>-quote.parquet
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
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from x9_data_fetcher.market_time import is_trading_day

IST = ZoneInfo("Asia/Kolkata")

_TAG = "[ARCHIVER]"

# Matches the hourly filenames written by tick_writer.py's
# _hourly_db_path(): market_2026-06-12_14.db
_HOURLY_FILE_RE = re.compile(r"^market_(\d{4}-\d{2}-\d{2})_(\d{2})\.db$")

# Matches per-symbol tables inside those files: depth_RELIANCE, quote_TCS
_TABLE_RE = re.compile(r"^(depth|quote)_(.+)$")

COMPRESSION = "zstd"


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
            if not force_all and parsed == current_key:
                continue  # still being written — never touch it

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
    """
    archive_root = archive_root or _archive_root()
    tag = "[ARCHIVER:DAILY]"
    if not archive_root.exists():
        return

    for symbol_dir in sorted(archive_root.iterdir()):
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_dir.name

        for prefix in ("depth", "quote"):
            # Regex-filtered rather than a bare glob, so an already-merged
            # daily file sitting in the same flat folder (e.g.
            # 2026-07-31-quote.parquet, no hour digits) is never mistaken
            # for an hourly chunk and re-merged into itself.
            chunks = sorted(
                p for p in symbol_dir.glob(f"{day.isoformat()}-*{prefix}.parquet")
                if _HOURLY_CHUNK_RE.match(p.name)
            )
            if not chunks:
                continue

            try:
                frames = [pd.read_parquet(c) for c in chunks]
                src_count = sum(len(f) for f in frames)
                concatenated = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
            except Exception as exc:
                print(f"{tag}[ERROR] read failed for {symbol}/{prefix}/{day}: {exc}", flush=True)
                continue

            # Full-row dedup: the boundary overlap writes the exact same
            # row (identical timestamp, ingest_ns, raw_json) into both
            # files, so an exact-duplicate match is the safest possible key
            # — it can never accidentally collapse two genuinely different
            # ticks that merely share a timestamp.
            merged = concatenated.drop_duplicates(keep="first").reset_index(drop=True)
            expected_dupes = len(concatenated) - len(merged)

            out_path = _daily_path(archive_root, prefix, symbol, day)
            try:
                _write_parquet(merged, out_path)
                written_count = len(pd.read_parquet(out_path, columns=["timestamp"]))
            except Exception as exc:
                print(f"{tag}[ERROR] write/verify failed for {symbol}/{prefix}/{day}: {exc}", flush=True)
                continue

            if written_count != src_count - expected_dupes:
                print(
                    f"{tag}[ERROR] row count mismatch for {symbol}/{prefix}/{day}: "
                    f"chunks={src_count} dupes_dropped={expected_dupes} "
                    f"merged={written_count} — hourly chunks kept",
                    flush=True,
                )
                continue

            for c in chunks:
                c.unlink()
            print(
                f"{tag} merged {symbol}/{prefix}/{day} ({written_count} rows, "
                f"{expected_dupes} boundary duplicate(s) dropped) — hourly chunks removed",
                flush=True,
            )


# ---------------------------------------------------------------------------
# Weekly merge — concatenates a week's daily files into one file per symbol
# ---------------------------------------------------------------------------

def _iso_week_path(archive_root: Path, prefix: str, symbol: str, any_day_in_week: date) -> Path:
    iso_year, iso_week, _ = any_day_in_week.isocalendar()
    return archive_root / symbol / f"{iso_year}-W{iso_week:02d}-{prefix}.parquet"


def _days_in_iso_week(any_day_in_week: date) -> List[date]:
    monday = any_day_in_week - timedelta(days=any_day_in_week.weekday())
    return [monday + timedelta(days=i) for i in range(7)]


def is_last_trading_day_of_week(day: date) -> bool:
    """True if no later day in day's ISO week (Mon-Sun) is a trading day."""
    for d in _days_in_iso_week(day):
        if d > day and is_trading_day(d):
            return False
    return True


def merge_weekly(any_day_in_week: date, archive_root: Optional[Path] = None) -> None:
    """
    For every symbol/table with daily files in any_day_in_week's ISO week,
    concatenate them into one weekly Parquet file, verify, then delete the
    daily files. Intended to be called on the last trading day of the week
    (see is_last_trading_day_of_week) — but merges whatever daily files
    exist for the week regardless of which day it's called on.
    """
    archive_root = archive_root or _archive_root()
    tag = "[ARCHIVER:WEEKLY]"
    week_days = _days_in_iso_week(any_day_in_week)

    if not archive_root.exists():
        return

    for symbol_dir in sorted(archive_root.iterdir()):
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_dir.name

        for prefix in ("depth", "quote"):
            daily_paths = [
                symbol_dir / f"{d.isoformat()}-{prefix}.parquet"
                for d in week_days
                if (symbol_dir / f"{d.isoformat()}-{prefix}.parquet").exists()
            ]
            if not daily_paths:
                continue

            try:
                frames = [pd.read_parquet(p) for p in daily_paths]
                src_count = sum(len(f) for f in frames)
                merged = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
            except Exception as exc:
                print(f"{tag}[ERROR] read failed for {symbol}/{prefix}: {exc}", flush=True)
                continue

            out_path = _iso_week_path(archive_root, prefix, symbol, any_day_in_week)
            try:
                _write_parquet(merged, out_path)
                written_count = len(pd.read_parquet(out_path, columns=["timestamp"]))
            except Exception as exc:
                print(f"{tag}[ERROR] write/verify failed for {symbol}/{prefix}: {exc}", flush=True)
                continue

            if written_count != src_count:
                print(
                    f"{tag}[ERROR] row count mismatch for {symbol}/{prefix}: "
                    f"dailies={src_count} merged={written_count} — daily files kept",
                    flush=True,
                )
                continue

            for p in daily_paths:
                p.unlink()
            print(f"{tag} merged {prefix}/{symbol} ({written_count} rows) — daily files removed", flush=True)


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