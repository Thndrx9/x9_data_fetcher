"""
Gap detection — figures out which (symbol, time-window) pairs are missing
data, and whether a gap is already recovered in the history Postgres DB.

This module does NOT fetch anything from the broker API and does NOT write
anything anywhere. It only answers: "what's missing?" Fetching/writing is
backfill_manager.py's job — it calls into this module, then acts on the
result.
"""

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

from x9_data_fetcher import connection_log
from x9_data_fetcher.market_time import (
    MARKET_CLOSE,
    MARKET_OPEN,
    is_trading_day,
    tz_kolkata,
)
from x9_data_fetcher.pg_writer import (
    _conn_params,
    _safe_print,
    live_any_row_in_range,
    live_timestamps_for_range,
)


HistoryWindow = Tuple[datetime, datetime]

# Second-level gap detection constants
_GAP_TOLERANCE_MS   = 90_000   # 90 s — absorbs minor tick-timing jitter
_CANDLE_INTERVAL_MS = 60_000   # 1-minute candle width


# ---------------------------------------------------------------------------
# Timestamp / session helpers
# ---------------------------------------------------------------------------

def _to_ms(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1_000_000_000_000_000:
            return int(numeric / 1_000_000)
        if numeric > 10_000_000_000:
            return int(numeric)
        return int(numeric * 1000)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return _to_ms(float(raw))
        except ValueError:
            pass
        normalized = raw.replace("Z", "+00:00")
        for fmt in (
            None,
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d",
        ):
            try:
                if fmt is None:
                    dt = datetime.fromisoformat(normalized)
                else:
                    dt = datetime.strptime(raw, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=tz_kolkata)
                return int(dt.astimezone(timezone.utc).timestamp() * 1000)
            except ValueError:
                continue
    return None


def _ms_to_ist(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(tz_kolkata)


def _floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _session_open(day: date) -> datetime:
    return datetime.combine(day, MARKET_OPEN, tzinfo=tz_kolkata)


def _session_last_candle(day: date) -> datetime:
    close_dt = datetime.combine(day, MARKET_CLOSE, tzinfo=tz_kolkata)
    return close_dt - timedelta(minutes=1)


def _previous_trading_day(day: date) -> Optional[date]:
    cursor = day - timedelta(days=1)
    for _ in range(30):
        if is_trading_day(cursor):
            return cursor
        cursor -= timedelta(days=1)
    return None


def _latest_completed_candle(now: datetime) -> Optional[datetime]:
    minute_start = _floor_minute(now) - timedelta(minutes=1)
    if is_trading_day(now.date()) and MARKET_OPEN <= minute_start.time() < MARKET_CLOSE:
        return minute_start
    if is_trading_day(now.date()) and now.time() >= MARKET_CLOSE:
        return _session_last_candle(now.date())
    previous_day = _previous_trading_day(now.date())
    if previous_day is None:
        return None
    return _session_last_candle(previous_day)


def _safe_symbol(symbol: str) -> str:
    """Match SQLite table naming convention."""
    return "".join(c for c in symbol if c.isalnum() or c == "_")


def _last_n_trading_days(reference: datetime, n: int) -> List[date]:
    """
    Return the last n trading days ending on reference.date() (inclusive
    if reference.date() is a trading day). Chronological order.
    """
    result: List[date] = []
    cursor = reference.date()
    for _ in range(n * 5):          # look back up to 5x n calendar days
        if is_trading_day(cursor):
            result.append(cursor)
            if len(result) == n:
                break
        cursor -= timedelta(days=1)
    return list(reversed(result))   # oldest -> newest


# NOTE: local SQLite scanning (_candidate_db_paths, _get_timestamps_for_day,
# _day_has_any_data) was removed here — hourly rollover now deletes each
# local market_*.db file shortly after archiving it, so historical lookback
# reads from the live PostgreSQL DB instead (see pg_writer.live_timestamps_for_range
# and pg_writer.live_any_row_in_range). Old version is in git history if ever
# needed for reference.


def _second_level_gaps(
    timestamps_ms: List[int],
    session_start: datetime,
    session_end: datetime,
) -> List[HistoryWindow]:
    """
    Walk a sorted timestamp sequence and return every missing window.

    Leading edge  - gap before the first tick
    Middle gaps   - consecutive pair separation > candle_interval + tolerance
    Trailing edge - gap after the last tick before session_end
    """
    if session_start > session_end:
        return []

    session_start_ms = int(session_start.astimezone(timezone.utc).timestamp() * 1000)
    session_end_ms   = int(session_end.astimezone(timezone.utc).timestamp() * 1000)

    # Keep only timestamps that fall inside (or just beyond) the session window
    ts = sorted(
        t for t in timestamps_ms
        if session_start_ms <= t <= session_end_ms + _GAP_TOLERANCE_MS
    )

    if not ts:
        return [(session_start, session_end)]

    gaps: List[HistoryWindow] = []

    # -- leading edge --------------------------------------------------------
    if ts[0] > session_start_ms + _GAP_TOLERANCE_MS:
        gap_end = _ms_to_ist(ts[0] - _CANDLE_INTERVAL_MS)
        if session_start <= gap_end:
            gaps.append((session_start, gap_end))

    # -- middle gaps ----------------------------------------------------------
    for i in range(len(ts) - 1):
        diff = ts[i + 1] - ts[i]
        if diff > _CANDLE_INTERVAL_MS + _GAP_TOLERANCE_MS:
            gap_start = _ms_to_ist(ts[i]     + _CANDLE_INTERVAL_MS)
            gap_end   = _ms_to_ist(ts[i + 1] - _CANDLE_INTERVAL_MS)
            if gap_start <= gap_end:
                gaps.append((gap_start, gap_end))

    # -- trailing edge ----------------------------------------------------------
    if ts[-1] < session_end_ms - _GAP_TOLERANCE_MS:
        gap_start = _ms_to_ist(ts[-1] + _CANDLE_INTERVAL_MS)
        if gap_start <= session_end:
            gaps.append((gap_start, session_end))

    return gaps


def _day_windows_from_log(
    quote_output_dir: str,
    day: date,
    session_start: datetime,
    session_end: datetime,
    now: datetime,
) -> Optional[List[HistoryWindow]]:
    """
    Derive gap windows for `day` purely from the connection log - no SQLite
    scanning needed.  These windows are system-wide (the feed was down for
    every symbol), so they're computed once per day and reused for every
    symbol needing backfill.

    Returns:
        None        - no log data for this day at all -> caller must fall
                       back to the per-symbol timestamp scan (old behaviour)
        []          - log shows a clean day (connected, no disconnects)
        [(s, e), ...] - exact outage windows derived from DISCONNECTED/
                       RECONNECTED pairs, or the full session if
                       DAY_NOT_STARTED was recorded
    """
    events = connection_log.get_events_for_day(quote_output_dir, day)
    if not events:
        return None   # no log info -> fall back to data scan

    if any(e[0] == "DAY_NOT_STARTED" for e in events):
        return [(session_start, session_end)]

    if not any(e[0] in ("DAY_STARTED", "RECONNECTED") for e in events):
        # log has rows but none indicate a successful connection - ambiguous,
        # safer to fall back to a real data scan
        return None

    windows: List[HistoryWindow] = []
    pending_disconnect_ms: Optional[int] = None

    # -- leading edge --------------------------------------------------------
    # If the very first event is DAY_STARTED and it happens after session
    # open, the system started mid-session (or restarted fresh with no
    # prior connection today) - everything from open to that first
    # connection is missing and was never recorded as a DISCONNECTED event
    # because there was no earlier connection to disconnect from.
    #
    # Boundaries are snapped to full MINUTE boundaries, not exact seconds.
    # Candles are stamped by their OPEN time (e.g. the 11:45 candle is
    # timestamped 11:45:00). If a disconnect/reconnect happens mid-minute
    # and the window used the exact second, the fetch filter would exclude
    # that candle entirely (its 11:45:00 stamp falls before an 11:45:50
    # window start) - silently losing a partially-affected candle. Snapping
    # to the minute containing the event ensures any touched candle is
    # always re-fetched in full from the authoritative history API.
    first_event, first_ts_ms, _first_mode = events[0]
    if first_event == "DAY_STARTED":
        first_dt          = _ms_to_ist(first_ts_ms)
        first_minute      = _floor_minute(first_dt)
        if first_minute > session_start:
            gap_start = session_start
            gap_end   = min(first_minute, session_end)
            if gap_start <= gap_end:
                windows.append((gap_start, gap_end))

    for event, ts_ms, _mode in events:
        if event == "DISCONNECTED":
            if pending_disconnect_ms is None:
                pending_disconnect_ms = ts_ms
        elif event in ("DAY_STARTED", "RECONNECTED"):
            if pending_disconnect_ms is not None:
                gap_start = _floor_minute(_ms_to_ist(pending_disconnect_ms))
                gap_end   = _floor_minute(_ms_to_ist(ts_ms))
                gap_start = max(gap_start, session_start)
                gap_end   = min(gap_end, session_end)
                if gap_start <= gap_end:
                    windows.append((gap_start, gap_end))
                pending_disconnect_ms = None

    # trailing disconnect never followed by a reconnect in the log
    if pending_disconnect_ms is not None:
        gap_start = max(_floor_minute(_ms_to_ist(pending_disconnect_ms)), session_start)
        gap_end   = session_end if day != now.date() else (_latest_completed_candle(now) or session_end)
        if gap_start <= gap_end:
            windows.append((gap_start, gap_end))

    return windows


def _find_symbol_gaps(
    live_conn,
    symbol: str,
    required_days: List[date],
    pre_startup_ts: Optional[datetime],
    now: datetime,
    quote_output_dir: Optional[str] = None,
    log_windows_cache: Optional[Dict[date, Optional[List[HistoryWindow]]]] = None,
) -> List[HistoryWindow]:
    """
    Return every (start, end) window missing for this symbol.

    Primary source - the connection log (DAY_STARTED / DISCONNECTED /
    RECONNECTED / DAY_NOT_STARTED).  If the log has data for a day, its
    windows are used directly and NO live-DB scan happens for that day -
    clean days cost nothing, known outages are queued straight to fetch.

    Fallback - for any day with no log data at all (log wasn't running,
    or predates this feature), fall back to the second-level timestamp
    scan of this symbol's own live-DB data (PostgreSQL), same as before.

    `log_windows_cache` lets the caller compute each day's log windows once
    and reuse them across all symbols, since log-derived windows are
    system-wide (identical for every symbol on that day).
    """
    table = f"quote_{_safe_symbol(symbol)}"
    gaps: List[HistoryWindow] = []
    if log_windows_cache is None:
        log_windows_cache = {}

    for day in required_days:
        if not is_trading_day(day):
            continue

        is_today = day == now.date()

        if is_today:
            if now.time() < MARKET_OPEN:
                continue
            session_end = _latest_completed_candle(now)
            if session_end is None:
                continue
            session_start = _session_open(day)
        else:
            session_start = _session_open(day)
            session_end   = _session_last_candle(day)

        # -- try the connection log first (cached per day across symbols) --
        first_time_for_day = day not in log_windows_cache
        if first_time_for_day:
            log_windows_cache[day] = (
                _day_windows_from_log(quote_output_dir, day, session_start, session_end, now)
                if quote_output_dir else None
            )
        log_windows = log_windows_cache[day]

        if log_windows is not None:
            # Log-derived windows are system-wide (same outage applies to
            # every symbol) - only print them the first time they're
            # computed for this day, not once per symbol that reuses them.
            if log_windows and first_time_for_day:
                for g in log_windows:
                    _safe_print(
                        f"[GAP_DETECTOR] {day} LOG gap (all symbols): "
                        f"{g[0].strftime('%H:%M')}->{g[1].strftime('%H:%M')}"
                    )
            elif not log_windows and first_time_for_day:
                # Log says "clean" - verify that's actually true before
                # trusting it. See live_any_row_in_range's docstring for why.
                day_start_ms = int(session_start.timestamp() * 1000)
                day_end_ms   = int(session_end.timestamp() * 1000)
                if live_conn is None or not live_any_row_in_range(live_conn, day_start_ms, day_end_ms):
                    _safe_print(
                        f"[GAP_DETECTOR][WARN] {day} connection log reports a clean "
                        "day, but zero rows exist for ANY symbol in that window "
                        "- overriding to a full-day gap (likely a silent auth "
                        "failure that never triggered a logged disconnect)"
                    )
                    log_windows_cache[day] = [(session_start, session_end)]
                    log_windows = log_windows_cache[day]
            if log_windows:
                gaps.extend(log_windows)
            # else: log confirms a clean day - nothing to do, no scan needed
            continue

        # -- no log data for this day - fall back to the data scan --------
        day_start_ms = int(
            datetime.combine(day, MARKET_OPEN, tzinfo=tz_kolkata).timestamp() * 1000
        )
        day_end_ms = int(
            datetime.combine(day, MARKET_CLOSE, tzinfo=tz_kolkata).timestamp() * 1000
        )
        timestamps = (
            live_timestamps_for_range(live_conn, table, day_start_ms, day_end_ms)
            if live_conn is not None else []
        )

        if not timestamps:
            _safe_print(
                f"[GAP_DETECTOR] {symbol} {day} - no data/log, fetching full session"
            )
            gaps.append((session_start, session_end))
            continue

        day_gaps = _second_level_gaps(timestamps, session_start, session_end)
        if day_gaps:
            for g in day_gaps:
                _safe_print(
                    f"[GAP_DETECTOR] {symbol} {day} SCAN gap: "
                    f"{g[0].strftime('%H:%M')}->{g[1].strftime('%H:%M')}"
                )
            gaps.extend(day_gaps)
        else:
            _safe_print(f"[GAP_DETECTOR] {symbol} {day} - data complete, skipping")

    return gaps


# ---------------------------------------------------------------------------
# Historical PostgreSQL helpers - is a gap already recovered in history DB?
# ---------------------------------------------------------------------------

def _history_connection(dbname: str):
    conn = psycopg2.connect(**_conn_params(dbname))
    conn.autocommit = True
    return conn


def _history_table_exists(conn, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s",
        (table.lower(),),
    )
    return cur.fetchone() is not None


def _history_timestamps_for_window(
    conn,
    table: str,
    window_start: datetime,
    window_end: datetime,
) -> List[int]:
    if not _history_table_exists(conn, table):
        return []

    start_ms = int(window_start.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = int(window_end.astimezone(timezone.utc).timestamp() * 1000)

    cur = conn.cursor()
    cur.execute(
        f"SELECT timestamp FROM {table} WHERE timestamp >= %s AND timestamp <= %s",
        (start_ms, end_ms),
    )
    timestamps: List[int] = []
    for (raw_ts,) in cur.fetchall():
        ts_ms = _to_ms(raw_ts)
        if ts_ms is not None:
            timestamps.append(ts_ms)
    return sorted(set(timestamps))


def _filter_gaps_already_in_history(
    conn,
    symbol: str,
    exchange: str,
    gaps: List[HistoryWindow],
) -> Tuple[List[HistoryWindow], int, int]:
    """
    Returns (remaining_gaps, fully_present_count, partially_present_count).

    Stays silent by design - with 100+ symbols this gets called once per
    symbol per run, so printing here would flood the log. The caller
    aggregates these counts into a single progress line + one summary.
    """
    table = f"quote_{_safe_symbol(symbol)}".lower()
    remaining: List[HistoryWindow] = []
    fully_present = 0
    partially_present = 0

    for window_start, window_end in gaps:
        timestamps = _history_timestamps_for_window(
            conn,
            table,
            window_start,
            window_end,
        )
        if not timestamps:
            remaining.append((window_start, window_end))
            continue

        missing_windows = _second_level_gaps(timestamps, window_start, window_end)
        if not missing_windows:
            fully_present += 1
            continue

        if missing_windows != [(window_start, window_end)]:
            partially_present += 1
        remaining.extend(missing_windows)

    return remaining, fully_present, partially_present


def _history_has_daily_row(conn, table: str, day: date) -> bool:
    """
    Is there already a daily_SYMBOL row for this trading day?

    daily_SYMBOL rows are always written with timestamp pinned to that
    day's 15:30:00 IST close (see DailyCloseManager) — a tight +/-1 minute
    window is enough to find it regardless of minor clock skew.
    """
    if not _history_table_exists(conn, table):
        return False

    window_start = datetime.combine(day, MARKET_CLOSE, tzinfo=tz_kolkata) - timedelta(minutes=1)
    window_end   = datetime.combine(day, MARKET_CLOSE, tzinfo=tz_kolkata) + timedelta(minutes=1)
    start_ms = int(window_start.astimezone(timezone.utc).timestamp() * 1000)
    end_ms   = int(window_end.astimezone(timezone.utc).timestamp() * 1000)

    cur = conn.cursor()
    cur.execute(
        f"SELECT 1 FROM {table} WHERE timestamp >= %s AND timestamp <= %s LIMIT 1",
        (start_ms, end_ms),
    )
    return cur.fetchone() is not None


def _missing_daily_days(
    conn,
    symbol: str,
    required_days: List[date],
) -> List[date]:
    """
    Returns the subset of `required_days` that have NO daily_SYMBOL row yet
    in the history Postgres DB — i.e. what DailyCloseManager.backfill_missing_days
    actually needs to fetch. Mirrors _filter_gaps_already_in_history's role
    for tick data, but for the daily-candle timeframe.
    """
    table = f"daily_{_safe_symbol(symbol)}".lower()
    return [day for day in required_days if not _history_has_daily_row(conn, table, day)]