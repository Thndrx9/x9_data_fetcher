"""
candle_archiver.py — archive aging-out raw ticks into 1-minute candles.

Problem this solves: pg_writer.purge_old_data() keeps only the last
LIVE_TICK_RETENTION_TRADING_DAYS (default 3) trading days of raw ticks
in quote_<symbol>/depth_<symbol>, deleting anything older every day at
market close. That multi-day window is genuinely needed — both this
repo's own BackfillManager (min_days, gap detection against outages
spanning more than a day) and the downstream x9 client's Tier-2 rollover
fallback (_fetch_1m_candles_from_main_db) depend on it — but keeping
that much *raw tick* data (potentially tens of thousands of rows per
symbol per day) just to serve those two consumers is far more storage
and query cost than either actually needs once a day is more than a
day or two old: both only ever want OHLCV at 1-minute resolution for
anything that isn't "yesterday or today", not individual ticks.

This module builds 1-minute candles (subtraction-method volume — see
_BUILD_1M_CANDLES_SQL's inline comment) from a day's raw ticks and
stores them in a NEW per-symbol table, candle1m_<symbol> — a distinct
name from quote_<symbol>/depth_<symbol>/daily_<symbol>, so it can't
collide with or interfere with any existing table, retention rule, or
reader. Running this BEFORE purge_old_data() each day means any day
that's about to fall out of raw-tick retention already has its candle
archive in place by the time its raw ticks are deleted — so shrinking
LIVE_TICK_RETENTION_TRADING_DAYS later (if desired) becomes safe rather
than lossy for anything reading multi-day history.

Wire this in once, in start_data.py's daily-close loop, immediately
before the existing purge_old_data() call:

    from x9_data_fetcher.candle_archiver import archive_aging_out_days
    ...
    await asyncio.to_thread(archive_aging_out_days)
    await asyncio.to_thread(purge_old_data)

Safe to call even if PostgreSQL isn't configured/reachable — logs and
returns rather than raising, matching purge_old_data()'s own contract
(so it can't take down the daily loop either).

Manual/backfill use (e.g. the first time this is deployed, to archive
days that are already sitting in quote_<symbol> from before this
existed):
    python3 -m x9_data_fetcher.candle_archiver --run
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras

from x9_data_fetcher.market_time import now_kolkata, tz_kolkata
from x9_data_fetcher.pg_writer import (
    _conn_params,
    _safe_print,
    _cutoff_ms,
    LIVE_TICK_RETENTION_TRADING_DAYS,
    _last_n_trading_days,
)

# This module only ever touches the PRIMARY database — the one holding raw
# live ticks (quote_<symbol>/depth_<symbol>). It never touches the separate
# history database (PG_HDBNAME) even though pg_writer.py's other functions
# (purge_old_data, table setup, etc.) loop over both — that history db holds
# PRE-BUILT candles straight from the broker API, not raw ticks, so there's
# nothing here for this module to read or archive there, and touching it
# needlessly caused lock contention with whatever else uses that database.
def _primary_dbname() -> str:
    return os.getenv("PG_DBNAME", "market").strip() or "market"

# Raw ticks for today and this many trading days back stay tick-level —
# only days OLDER than this get archived to candle1m_<symbol> and are
# fair game for purge_old_data() to delete afterward. 1 means "today
# and yesterday stay raw; day-before-yesterday and older get archived."
# Must stay comfortably below LIVE_TICK_RETENTION_TRADING_DAYS so a day
# always gets its candle archive built at least one full day before
# purge_old_data() would otherwise delete its raw ticks.
KEEP_RAW_TICK_TRADING_DAYS = int(
    os.getenv("X9_KEEP_RAW_TICK_TRADING_DAYS", "1").strip() or "1"
)


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ensure_candle1m_table(conn, symbol: str) -> str:
    """
    Create candle1m_<symbol> if it doesn't exist yet. Schema matches
    the downstream x9 client's own Candle1mCache table (ts_ms primary
    key, OHLCV) so the client can read this directly with no
    translation if it's ever pointed at this table as an additional
    fallback source, alongside its own local cache.
    """
    table = f"candle1m_{symbol}".lower()
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_quote_ident(table)} (
            ts_ms  BIGINT PRIMARY KEY,
            open   DOUBLE PRECISION,
            high   DOUBLE PRECISION,
            low    DOUBLE PRECISION,
            close  DOUBLE PRECISION,
            volume BIGINT
        )
    """)
    conn.commit()
    return table


# Same bucket/session-open math used throughout this codebase and the
# x9 client (backfill_manager.py's _fetch_1m_candles_from_main_db) —
# IST minute-bucket boundary via UTC-ms arithmetic, no per-row Python
# datetime conversion needed since this all happens server-side in SQL.
_BUILD_1M_CANDLES_SQL = """
    WITH b AS (
        SELECT
            timestamp,
            ltp,
            COALESCE(volume, 0) AS cum_volume,
            (
                (timestamp + 19800000) - MOD(timestamp + 19800000, 86400000)
                + 33300000
                + (GREATEST(MOD(timestamp + 19800000, 86400000) - 33300000, 0) / 60000) * 60000
                - 19800000
            ) AS bucket_ms
        FROM {table}
        WHERE timestamp >= %s AND timestamp < %s
          AND ltp IS NOT NULL
          AND MOD(timestamp + 19800000, 86400000) >= 33300000
    ),
    d AS (
        -- "volume" is the feed's CUMULATIVE volume-traded-today
        -- counter, not a per-tick quantity — diff consecutive
        -- readings to recover each tick's own contribution rather
        -- than summing last_quantity (which double-counts on any
        -- duplicate/depth-only re-broadcast tick that carries no new
        -- trade). This query is scoped to a single calendar day
        -- (the WHERE clause above), so no cross-day counter-reset to
        -- handle here. The day's first in-session tick has no prior
        -- row in this window — the counter starts at 0 at day open,
        -- so that tick's own cumulative value already IS its full
        -- incremental contribution (LAG → NULL → COALESCE to 0
        -- baseline). GREATEST guards against any out-of-order tick
        -- producing a spurious negative diff.
        SELECT
            timestamp,
            ltp,
            bucket_ms,
            GREATEST(cum_volume - COALESCE(LAG(cum_volume) OVER (ORDER BY timestamp ASC), 0), 0) AS tick_volume
        FROM b
    )
    SELECT
        bucket_ms,
        (array_agg(ltp ORDER BY timestamp ASC))[1]  AS open,
        MAX(ltp)                                     AS high,
        MIN(ltp)                                     AS low,
        (array_agg(ltp ORDER BY timestamp DESC))[1] AS close,
        SUM(tick_volume)                             AS volume
    FROM d
    GROUP BY bucket_ms
    ORDER BY bucket_ms
"""


def _build_1m_candles_for_day(conn, quote_table: str, day_start_ms: int, day_end_ms: int) -> list:
    """
    Build 1-minute OHLCV candles for one symbol/day from its raw
    quote_<symbol> ticks, using the subtraction (diffed cumulative
    volume) method — see _BUILD_1M_CANDLES_SQL's inline comment.
    Returns a list of (ts_ms, open, high, low, close, volume) tuples,
    empty if the table doesn't exist or has no ticks in this window.
    """
    cur = conn.cursor()
    try:
        cur.execute(_BUILD_1M_CANDLES_SQL.format(table=_quote_ident(quote_table)), (day_start_ms, day_end_ms))
        return cur.fetchall()
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return []


def _day_bounds_ms(day: date) -> tuple:
    start = datetime.combine(day, datetime.min.time(), tzinfo=tz_kolkata)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _quote_symbol_from_table(table: str) -> Optional[str]:
    if not table.startswith("quote_"):
        return None
    return table[len("quote_"):]


def _archive_db(dbname: str, keep_recent_trading_days: int, now: datetime) -> int:
    """
    Returns the number of candle rows written for this database (0 if
    everything was already archived, i.e. this db was already up to date).
    """
    tag = f"[CANDLE_ARCHIVER:{dbname}]"
    try:
        conn = psycopg2.connect(**_conn_params(dbname))
    except Exception as exc:
        _safe_print(f"{tag}[ERROR] connect failed: {exc}")
        return 0

    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'quote_%'"
        )
        quote_tables = [row[0] for row in cur.fetchall()]
        if not quote_tables:
            _safe_print(f"{tag} no quote_* tables found — nothing to archive")
            return 0

        # Candidate days: everything from LIVE_TICK_RETENTION_TRADING_DAYS
        # back up to (but not including) keep_recent_trading_days back —
        # i.e. exactly the days that still have raw ticks available right
        # now but are old enough that this run should archive them.
        # Going one day past the current retention window as a harmless
        # no-op safety margin, in case retention was ever widened after a
        # gap in running this archiver.
        all_days = _last_n_trading_days(now, LIVE_TICK_RETENTION_TRADING_DAYS + 1)
        cutoff_day = _last_n_trading_days(now, keep_recent_trading_days)[0] if keep_recent_trading_days > 0 else now.date()
        candidate_days = [d for d in all_days if d < cutoff_day]

        if not candidate_days:
            _safe_print(f"{tag} no days old enough to archive yet (keep_recent_trading_days={keep_recent_trading_days})")
            return 0

        total_symbols_archived = 0
        total_candles_written = 0

        # Which symbols already have a candle1m_<symbol> table — checked
        # once up front (cheap catalog lookup) rather than re-querying
        # pg_tables per symbol per day inside the hot loop below.
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'candle1m_%'"
        )
        existing_candle_tables = {row[0] for row in cur.fetchall()}

        for table in quote_tables:
            symbol = _quote_symbol_from_table(table)
            if not symbol:
                continue

            candle_table = f"candle1m_{symbol}".lower()
            table_exists = candle_table in existing_candle_tables
            symbol_had_new_data = False

            for day in candidate_days:
                day_start_ms, day_end_ms = _day_bounds_ms(day)

                # Idempotent skip — checked BEFORE running the expensive
                # tick-aggregation build, not after, so a day that's
                # already archived costs one cheap indexed lookup on
                # every subsequent run rather than repeating the full
                # LAG/GROUP BY build over that day's raw ticks forever.
                if table_exists:
                    cur.execute(
                        f"SELECT 1 FROM {_quote_ident(candle_table)} WHERE ts_ms >= %s AND ts_ms < %s LIMIT 1",
                        (day_start_ms, day_end_ms),
                    )
                    if cur.fetchone():
                        continue

                rows = _build_1m_candles_for_day(conn, table, day_start_ms, day_end_ms)
                if not rows:
                    continue

                if not table_exists:
                    _ensure_candle1m_table(conn, symbol)
                    existing_candle_tables.add(candle_table)
                    table_exists = True

                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {_quote_ident(candle_table)} (ts_ms, open, high, low, close, volume) "
                    f"VALUES %s ON CONFLICT (ts_ms) DO UPDATE SET "
                    f"open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                    f"close=EXCLUDED.close, volume=EXCLUDED.volume",
                    rows,
                    page_size=2000,
                )
                conn.commit()
                total_candles_written += len(rows)
                symbol_had_new_data = True

            if symbol_had_new_data:
                total_symbols_archived += 1

        _safe_print(
            f"{tag} archived {total_candles_written} candle row(s) across "
            f"{total_symbols_archived}/{len(quote_tables)} symbol(s) for "
            f"{len(candidate_days)} day(s) ({candidate_days[0]} → {candidate_days[-1]})"
        )
        return total_candles_written
    except Exception as exc:
        _safe_print(f"{tag}[ERROR] archive pass failed: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def archive_aging_out_days(
    keep_recent_trading_days: int = KEEP_RAW_TICK_TRADING_DAYS,
    now=None,
) -> int:
    """
    Build and store 1-minute candles (candle1m_<symbol>) in the PRIMARY
    database only, for every trading day that's older than
    `keep_recent_trading_days` but still has raw ticks present — i.e.
    every day about to (or already eligible to) fall out of
    purge_old_data()'s raw-tick retention window. Deliberately does NOT
    touch the separate history database (PG_HDBNAME) — that one holds
    pre-built candles from the broker API already, not raw ticks, so
    there's nothing there for this to read.

    Call this BEFORE purge_old_data() in the daily loop so a day's
    candle archive always exists before its raw ticks are deleted.
    Idempotent — already-archived days are skipped, so calling this
    daily only ever does new work for the newly-aging-out day. That
    same idempotency is also what makes this safe to call again at
    startup as an "up to date?" check — see run_startup_catch_up()
    below: a normal restart finds nothing new to do and returns 0
    almost instantly; only a real backlog (e.g. this process having
    missed one or more market closes while down) costs real work.

    Returns the total number of candle rows written — 0 means
    everything was already up to date, nothing to do. This is a plain
    read of what was written, not a fresh lookup, so it costs nothing
    extra beyond the archive pass itself.

    Safe to call even if PostgreSQL isn't configured/reachable — logs
    and returns 0 rather than raising.
    """
    now = now or now_kolkata()
    start = time.monotonic()
    _safe_print(
        f"[CANDLE_ARCHIVER] starting — archiving days older than "
        f"{keep_recent_trading_days} trading day(s) back"
    )
    total_written = _archive_db(_primary_dbname(), keep_recent_trading_days, now)
    _safe_print(f"[CANDLE_ARCHIVER] done in {time.monotonic() - start:.1f}s")
    return total_written


def run_startup_catch_up(
    keep_recent_trading_days: int = KEEP_RAW_TICK_TRADING_DAYS,
    now=None,
) -> int:
    """
    Startup entry point: check whether the candle archive is up to
    date and, if not, catch it up before returning — so a process that
    was down across one or more market closes doesn't leave a gap
    (those days would otherwise go straight to purge_old_data() with
    no candle archive ever built for them, once they age past
    keep_recent_trading_days).

    This is a thin, explicitly-named wrapper around
    archive_aging_out_days() — same idempotent logic, same safe-if-PG-
    unreachable behavior — kept separate only so the startup call site
    reads as "make sure this is up to date" rather than "run the daily
    job again", and so its log line is unambiguous either way:
        - returns 0  → archive was already up to date, nothing done
        - returns >0 → was behind, caught up now, returns row count
    """
    written = archive_aging_out_days(keep_recent_trading_days, now)
    if written:
        _safe_print(
            f"[CANDLE_ARCHIVER] startup check: was behind — archived "
            f"{written} candle row(s), now up to date"
        )
    else:
        _safe_print("[CANDLE_ARCHIVER] startup check: already up to date")
    return written


# ---------------------------------------------------------------------------
# Purge — candle1m_<symbol> retention.
#
# This deliberately lives here (not in pg_writer.py's purge_old_data())
# since this module owns the candle1m_<symbol> table's creation and
# schema. Reuses pg_writer's own cutoff math (_cutoff_ms) and connection
# helpers so this stays byte-for-byte consistent with how the tick-data
# purge computes its cutoff, but keeps the DELETE itself — and the
# ts_ms column name (candle1m_<symbol> doesn't use the "timestamp"
# column quote_/depth_/daily_ tables use) — self-contained here.
# ---------------------------------------------------------------------------

def _purge_candles_db(dbname: str, cutoff_ms: int) -> None:
    tag = f"[CANDLE_ARCHIVER:{dbname}]"
    try:
        conn = psycopg2.connect(**_conn_params(dbname))
    except Exception as exc:
        _safe_print(f"{tag}[ERROR] purge connect failed: {exc}")
        return

    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'candle1m_%'"
        )
        tables = [row[0] for row in cur.fetchall()]
        if not tables:
            _safe_print(f"{tag} no candle1m_* tables found — nothing to purge")
            return

        total_deleted = 0
        tables_affected = 0
        for table in tables:
            try:
                cur.execute(f"DELETE FROM {_quote_ident(table)} WHERE ts_ms < %s", (cutoff_ms,))
                deleted = cur.rowcount
                if deleted:
                    tables_affected += 1
                    total_deleted += deleted
            except Exception as exc:
                _safe_print(f"{tag}[ERROR] purge delete failed for {table}: {exc}")
                conn.rollback()
                cur = conn.cursor()  # cursor is dead after rollback — get a fresh one
                continue

        conn.commit()
        _safe_print(
            f"{tag} 1m candles: purged {total_deleted} row(s) across "
            f"{tables_affected}/{len(tables)} table(s) (cutoff={cutoff_ms})"
        )

        # Same rationale as pg_writer's own purge: reclaim disk space after
        # a large delete. Only worth the ACCESS EXCLUSIVE lock/rewrite cost
        # when something was actually deleted, and only runs once per
        # trading day right after close.
        if total_deleted:
            conn.autocommit = True
            vac_cur = conn.cursor()
            for table in tables:
                try:
                    vac_cur.execute(f"VACUUM FULL {_quote_ident(table)}")
                except Exception as exc:
                    _safe_print(f"{tag}[WARN] vacuum full failed for {table}: {exc}")
            _safe_print(f"{tag} 1m candles: vacuum full complete")

    except Exception as exc:
        _safe_print(f"{tag}[ERROR] purge pass failed: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def purge_old_candles(
    keep_trading_days: int = LIVE_TICK_RETENTION_TRADING_DAYS,
    now=None,
) -> None:
    """
    Delete candle1m_<symbol> rows older than `keep_trading_days` trading
    days, in the PRIMARY database only (never the history database —
    same reasoning as archive_aging_out_days() above: candle1m_<symbol>
    only ever exists in the primary db in the first place, since that's
    the only db this module ever writes to). Same cutoff window as
    pg_writer.purge_old_data() uses for raw tick data (quote_*/depth_*),
    computed the exact same way via pg_writer._cutoff_ms(), so both
    purges agree on what "3 trading days" means down to the millisecond.

    Call this at market close, alongside (order doesn't matter relative
    to) purge_old_data() — this only ever touches candle1m_* tables, so
    it can't interact with or be affected by the tick-data purge.

    Safe to call even if PostgreSQL isn't configured/reachable — logs
    and returns rather than raising, matching this module's other
    functions and pg_writer.purge_old_data()'s own contract.
    """
    now = now or now_kolkata()
    start = time.monotonic()
    cutoff_ms = _cutoff_ms(now, keep_trading_days)
    _safe_print(
        f"[CANDLE_ARCHIVER] starting 1m-candle purge — keep last "
        f"{keep_trading_days} trading day(s) (cutoff={cutoff_ms})"
    )
    _purge_candles_db(_primary_dbname(), cutoff_ms)
    _safe_print(f"[CANDLE_ARCHIVER] 1m-candle purge done in {time.monotonic() - start:.1f}s")


# ---------------------------------------------------------------------------
# CLI — manual/backfill run:
#     python3 -m x9_data_fetcher.candle_archiver --run
#     python3 -m x9_data_fetcher.candle_archiver --run --keep-recent-days 2
# ---------------------------------------------------------------------------

def _main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="candle_archiver maintenance CLI")
    ap.add_argument("--run", action="store_true", help="Run the archive pass once, now")
    ap.add_argument("--purge", action="store_true", help="Run the candle1m_* purge pass once, now")
    ap.add_argument("--keep-recent-days", type=int, default=KEEP_RAW_TICK_TRADING_DAYS,
                     help=f"Trading days to leave as raw ticks (default {KEEP_RAW_TICK_TRADING_DAYS})")
    ap.add_argument("--keep-trading-days", type=int, default=LIVE_TICK_RETENTION_TRADING_DAYS,
                     help=f"Trading days of candle1m_* data to keep before purge (default {LIVE_TICK_RETENTION_TRADING_DAYS})")
    args = ap.parse_args()

    if args.run:
        archive_aging_out_days(keep_recent_trading_days=args.keep_recent_days)
        return 0
    if args.purge:
        purge_old_candles(keep_trading_days=args.keep_trading_days)
        return 0

    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(_main())
