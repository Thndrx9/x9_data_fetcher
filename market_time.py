import json
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

try:
    from zoneinfo import ZoneInfo

    tz_kolkata = ZoneInfo("Asia/Kolkata")
except ImportError:
    import pytz

    tz_kolkata = pytz.timezone("Asia/Kolkata")

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
PRE_CONNECT = dtime(9, 14)  # 1 minute before market open

# ── Trading calendar ────────────────────────────────────────────────────
_CALENDAR_FILE = Path(__file__).resolve().parent.parent / "trading_calendar.json"   # project root

_holidays: Set[date] = set()
# special_open maps a date to its own (start, end) market hours when known
# (e.g. Muhurat trading, 14:20-15:30), or to None when the date is a
# special trading day but no specific hours were recorded — in that case
# callers fall back to the normal MARKET_OPEN/MARKET_CLOSE as before.
_special_open: Dict[date, Optional[Tuple[dtime, dtime]]] = {}


def _parse_hhmm(s: Optional[str]) -> Optional[dtime]:
    if not s:
        return None
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return None


def _load_calendar() -> None:
    global _holidays, _special_open
    if not _CALENDAR_FILE.exists():
        _holidays = set()
        _special_open = {}
        return
    try:
        with open(_CALENDAR_FILE) as f:
            cal = json.load(f)
        _holidays = {date.fromisoformat(d) for d in cal.get("holidays", [])}

        _special_open = {}
        for entry in cal.get("special_open", []):
            if isinstance(entry, str):
                # Legacy/bare-date form — special trading day, hours unknown.
                _special_open[date.fromisoformat(entry)] = None
            elif isinstance(entry, dict) and "date" in entry:
                d = date.fromisoformat(entry["date"])
                start = _parse_hhmm(entry.get("start"))
                end = _parse_hhmm(entry.get("end"))
                _special_open[d] = (start, end) if (start and end) else None

        n_with_hours = sum(1 for v in _special_open.values() if v is not None)
        print(
            f"[X9_FETCHER] Trading calendar loaded: "
            f"{len(_holidays)} holidays, {len(_special_open)} special open day(s) "
            f"({n_with_hours} with known hours)",
            flush=True,
        )
    except Exception as exc:
        print(
            f"[X9_FETCHER][WARN] Failed to load trading calendar: {exc}", flush=True
        )
        _holidays = set()
        _special_open = {}


_load_calendar()


def reload_calendar() -> None:
    """Reload the trading calendar from disk (call after editing the JSON)."""
    _load_calendar()


def refresh_trading_calendar() -> None:
    """
    No automatic fetching. The trading calendar (trading_calendar.json) is
    maintained entirely by hand — add holidays and special sessions
    (Muhurat trading etc.) to it yourself, with real dates and, for a
    special session, its actual start/end hours. This function only
    reloads whatever is currently on disk, so an edit you make to the
    file takes effect without a restart the next time this runs.

    Kept as a function (rather than removed outright) so the call site in
    engine_runtime.py's startup path doesn't need to change.
    """
    _load_calendar()



# ── Core helpers ────────────────────────────────────────────────────────


def now_kolkata() -> datetime:
    return datetime.now(tz_kolkata)


def is_trading_day(dt: Optional[datetime] = None) -> bool:
    """
    True if the given date is a trading day:
    - Weekday that is NOT in the holidays list, OR
    - Any date explicitly listed in special_open (overrides weekends and
      overrides holidays — a Muhurat-trading date can be in both NSE's
      holiday list and special_open at once, and special_open must win).
    """
    if dt is None:
        dt = now_kolkata()
    d = dt.date() if isinstance(dt, datetime) else dt

    if d in _special_open:
        return True
    if d in _holidays:
        return False
    return d.weekday() < 5  # Mon-Fri


def market_hours_for(d: Optional[date] = None) -> Tuple[dtime, dtime]:
    """
    The (open, close) time for a given date: the special session's own
    hours if the date is in special_open with known hours, otherwise the
    normal MARKET_OPEN/MARKET_CLOSE. Does not check is_trading_day() —
    callers should do that first.
    """
    if d is None:
        d = now_kolkata().date()
    hours = _special_open.get(d)
    if hours:
        return hours
    return MARKET_OPEN, MARKET_CLOSE


def is_market_open(dt: Optional[datetime] = None) -> bool:
    if dt is None:
        dt = now_kolkata()
    if not is_trading_day(dt):
        return False
    d = dt.date() if isinstance(dt, datetime) else dt
    open_t, close_t = market_hours_for(d)
    return open_t <= dt.time() < close_t


def is_connectable(dt: Optional[datetime] = None) -> bool:
    """True if within pre-connect window or market hours on a trading day."""
    if dt is None:
        dt = now_kolkata()
    if not is_trading_day(dt):
        return False
    d = dt.date() if isinstance(dt, datetime) else dt
    open_t, close_t = market_hours_for(d)
    # Pre-connect is 1 minute before whatever today's open actually is —
    # the normal 9:14 on a regular day, or 1 minute before a special
    # session's own start time.
    pre_connect_minutes = open_t.hour * 60 + open_t.minute - 1
    pre_connect = dtime(pre_connect_minutes // 60, pre_connect_minutes % 60)
    return pre_connect <= dt.time() < close_t


def seconds_until_pre_connect(dt: Optional[datetime] = None) -> float:
    """
    Seconds until pre-connect (1 minute before market open) on today or
    the next trading day. Uses today's special-session open time if today
    is a special trading day with known hours, otherwise the normal 9:14.
    """
    if dt is None:
        dt = now_kolkata()

    d = dt.date() if isinstance(dt, datetime) else dt

    def _pre_connect_for(day: date) -> dtime:
        open_t, _close_t = market_hours_for(day)
        minutes = open_t.hour * 60 + open_t.minute - 1
        return dtime(minutes // 60, minutes % 60)

    # If today is a trading day and we haven't reached pre-connect yet → target today
    if is_trading_day(dt):
        pc_today = _pre_connect_for(d)
        if dt.time() < pc_today:
            target = dt.replace(
                hour=pc_today.hour,
                minute=pc_today.minute,
                second=0,
                microsecond=0,
            )
            return max(0.0, (target - dt).total_seconds())

    # Otherwise advance day-by-day to find the next trading day (max 15 days ahead)
    target_date = d + timedelta(days=1)
    for _ in range(15):
        if is_trading_day(target_date):
            pc = _pre_connect_for(target_date)
            target = dt.replace(
                year=target_date.year,
                month=target_date.month,
                day=target_date.day,
                hour=pc.hour,
                minute=pc.minute,
                second=0,
                microsecond=0,
            )
            return max(0.0, (target - dt).total_seconds())
        target_date += timedelta(days=1)

    # Fallback (shouldn't happen unless calendar blocks 15+ consecutive days)
    pc = _pre_connect_for(target_date)
    target = dt.replace(
        year=target_date.year,
        month=target_date.month,
        day=target_date.day,
        hour=pc.hour,
        minute=pc.minute,
        second=0,
        microsecond=0,
    )
    return max(0.0, (target - dt).total_seconds())


def trading_day_n_back(n: int, from_date: Optional[date] = None) -> date:
    """
    The date that is the n-th most recent trading day at-or-before
    from_date. n=0 -> from_date itself if it's a trading day, else the
    closest trading day before it. n=1 -> one trading day before that.
    Walks backward against the holiday/weekend calendar (bounded to a
    generous 30-day lookback per step to avoid an unbounded loop if the
    calendar is ever badly malformed).
    """
    if from_date is None:
        from_date = now_kolkata().date()

    d = from_date
    for _ in range(30):
        if is_trading_day(d):
            break
        d -= timedelta(days=1)

    remaining = n
    while remaining > 0:
        d -= timedelta(days=1)
        for _ in range(30):
            if is_trading_day(d):
                break
            d -= timedelta(days=1)
        remaining -= 1

    return d


def seconds_until_close(dt: Optional[datetime] = None) -> float:
    """
    Seconds until market close today — the special session's own close
    time if today is a special trading day with known hours, otherwise
    the normal 15:30.
    """
    if dt is None:
        dt = now_kolkata()

    d = dt.date() if isinstance(dt, datetime) else dt
    _open_t, close_t = market_hours_for(d)

    close_dt = dt.replace(
        hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0
    )
    return max(0.0, (close_dt - dt).total_seconds())
