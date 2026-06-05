import json
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Optional, Set

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
_CALENDAR_FILE = Path(__file__).resolve().parent / "trading_calendar.json"

_holidays: Set[date] = set()
_special_open: Set[date] = set()


def _load_calendar() -> None:
    global _holidays, _special_open
    if not _CALENDAR_FILE.exists():
        _holidays = set()
        _special_open = set()
        return
    try:
        with open(_CALENDAR_FILE) as f:
            cal = json.load(f)
        _holidays = {date.fromisoformat(d) for d in cal.get("holidays", [])}
        _special_open = {date.fromisoformat(d) for d in cal.get("special_open", [])}
        print(
            f"[X9_FETCHER] Trading calendar loaded: "
            f"{len(_holidays)} holidays, {len(_special_open)} special open days",
            flush=True,
        )
    except Exception as exc:
        print(
            f"[X9_FETCHER][WARN] Failed to load trading calendar: {exc}", flush=True
        )
        _holidays = set()
        _special_open = set()


_load_calendar()


def reload_calendar() -> None:
    """Reload the trading calendar from disk (call after editing the JSON)."""
    _load_calendar()


# ── Core helpers ────────────────────────────────────────────────────────


def now_kolkata() -> datetime:
    return datetime.now(tz_kolkata)


def is_trading_day(dt: Optional[datetime] = None) -> bool:
    """
    True if the given date is a trading day:
    - Weekday that is NOT in the holidays list, OR
    - Any date explicitly listed in special_open (overrides weekends).
    """
    if dt is None:
        dt = now_kolkata()
    d = dt.date() if isinstance(dt, datetime) else dt

    if d in _special_open:
        return True
    if d in _holidays:
        return False
    return d.weekday() < 5  # Mon-Fri


def is_market_open(dt: Optional[datetime] = None) -> bool:
    if dt is None:
        dt = now_kolkata()
    if not is_trading_day(dt):
        return False
    return MARKET_OPEN <= dt.time() < MARKET_CLOSE


def is_connectable(dt: Optional[datetime] = None) -> bool:
    """True if within pre-connect window or market hours on a trading day."""
    if dt is None:
        dt = now_kolkata()
    if not is_trading_day(dt):
        return False
    return PRE_CONNECT <= dt.time() < MARKET_CLOSE


def seconds_until_pre_connect(dt: Optional[datetime] = None) -> float:
    """Seconds until 9:14 on the next trading day."""
    if dt is None:
        dt = now_kolkata()

    # If today is a trading day and we haven't reached pre-connect yet → target today
    if is_trading_day(dt) and dt.time() < PRE_CONNECT:
        target = dt.replace(
            hour=PRE_CONNECT.hour,
            minute=PRE_CONNECT.minute,
            second=0,
            microsecond=0,
        )
        return max(0.0, (target - dt).total_seconds())

    # Otherwise advance day-by-day to find the next trading day (max 15 days ahead)
    target = dt.replace(
        hour=PRE_CONNECT.hour, minute=PRE_CONNECT.minute, second=0, microsecond=0
    ) + timedelta(days=1)

    for _ in range(15):
        if is_trading_day(target):
            return max(0.0, (target - dt).total_seconds())
        target += timedelta(days=1)

    # Fallback (shouldn't happen unless calendar blocks 15+ consecutive days)
    return max(0.0, (target - dt).total_seconds())


def seconds_until_close(dt: Optional[datetime] = None) -> float:
    """Seconds until market close (15:30) today."""
    if dt is None:
        dt = now_kolkata()

    close_dt = dt.replace(
        hour=MARKET_CLOSE.hour, minute=MARKET_CLOSE.minute, second=0, microsecond=0
    )
    return max(0.0, (close_dt - dt).total_seconds())
