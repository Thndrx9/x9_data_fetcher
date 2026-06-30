import asyncio
import json
import os
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib import request
from urllib.error import URLError

import psycopg2

from x9_data_fetcher.market_time import (
    MARKET_CLOSE,
    MARKET_OPEN,
    is_trading_day,
    now_kolkata,
    tz_kolkata,
)
from x9_data_fetcher.pg_writer import PgWriter, _conn_params


HistoryWindow = Tuple[datetime, datetime]

# Second-level gap detection constants
_GAP_TOLERANCE_MS   = 90_000   # 90 s — absorbs minor tick-timing jitter
_CANDLE_INTERVAL_MS = 60_000   # 1-minute candle width


# ---------------------------------------------------------------------------
# Timestamp helpers
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


# ---------------------------------------------------------------------------
# Gap detection helpers
# ---------------------------------------------------------------------------

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
    for _ in range(n * 5):          # look back up to 5× n calendar days
        if is_trading_day(cursor):
            result.append(cursor)
            if len(result) == n:
                break
        cursor -= timedelta(days=1)
    return list(reversed(result))   # oldest → newest


def _candidate_db_paths(base_dir: Path) -> List[Path]:
    return sorted(base_dir.glob("market_*.db"), key=lambda p: p.name, reverse=True)


def _get_timestamps_for_day(
    db_paths: List[Path], table: str, day: date
) -> List[int]:
    """
    Pull every raw timestamp (ms) stored for *symbol* on *day* across all
    SQLite DB files.  Returns a sorted, deduplicated list.
    """
    day_start_ms = int(
        datetime.combine(day, MARKET_OPEN, tzinfo=tz_kolkata).timestamp() * 1000
    )
    day_end_ms = int(
        datetime.combine(day, MARKET_CLOSE, tzinfo=tz_kolkata).timestamp() * 1000
    )
    timestamps: List[int] = []

    for db_path in db_paths:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            try:
                cur = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                )
                if not cur.fetchone():
                    continue
                cur = conn.execute(
                    f"SELECT timestamp FROM {table} "
                    f"WHERE timestamp >= ? AND timestamp < ?",
                    (day_start_ms, day_end_ms),
                )
                for (raw_ts,) in cur.fetchall():
                    ts_ms = _to_ms(raw_ts)
                    if ts_ms is not None:
                        timestamps.append(ts_ms)
            finally:
                conn.close()
        except sqlite3.Error as exc:
            print(f"[BACKFILL][WARN] error reading {db_path.name}: {exc}", flush=True)

    return sorted(set(timestamps))


def _second_level_gaps(
    timestamps_ms: List[int],
    session_start: datetime,
    session_end: datetime,
) -> List[HistoryWindow]:
    """
    Walk a sorted timestamp sequence and return every missing window.

    Leading edge  — gap before the first tick
    Middle gaps   — consecutive pair separation > candle_interval + tolerance
    Trailing edge — gap after the last tick before session_end
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

    # ── leading edge ─────────────────────────────────────────────────────
    if ts[0] > session_start_ms + _GAP_TOLERANCE_MS:
        gap_end = _ms_to_ist(ts[0] - _CANDLE_INTERVAL_MS)
        if session_start <= gap_end:
            gaps.append((session_start, gap_end))

    # ── middle gaps ───────────────────────────────────────────────────────
    for i in range(len(ts) - 1):
        diff = ts[i + 1] - ts[i]
        if diff > _CANDLE_INTERVAL_MS + _GAP_TOLERANCE_MS:
            gap_start = _ms_to_ist(ts[i]     + _CANDLE_INTERVAL_MS)
            gap_end   = _ms_to_ist(ts[i + 1] - _CANDLE_INTERVAL_MS)
            if gap_start <= gap_end:
                gaps.append((gap_start, gap_end))

    # ── trailing edge ─────────────────────────────────────────────────────
    if ts[-1] < session_end_ms - _GAP_TOLERANCE_MS:
        gap_start = _ms_to_ist(ts[-1] + _CANDLE_INTERVAL_MS)
        if gap_start <= session_end:
            gaps.append((gap_start, session_end))

    return gaps


def _find_symbol_gaps(
    db_paths: List[Path],
    symbol: str,
    required_days: List[date],
    pre_startup_ts: Optional[datetime],
    now: datetime,
) -> List[HistoryWindow]:
    """
    Return every (start, end) window missing for this symbol using a
    second-level gap scan — walks actual stored timestamps so partial
    coverage within a day is detected and filled precisely.

    Past days   — full session scanned second-by-second for holes.
    Today       — scanned from session open to latest_completed_candle
                  so every intra-day hole from mid-session restarts is found.
    """
    table = f"quote_{_safe_symbol(symbol)}"
    gaps: List[HistoryWindow] = []

    for day in required_days:
        if not is_trading_day(day):
            continue

        if day == now.date():
            # ── today ─────────────────────────────────────────────────────
            if now.time() < MARKET_OPEN:
                continue   # market not open yet — nothing to backfill

            session_end = _latest_completed_candle(now)
            if session_end is None:
                continue

            session_start = _session_open(day)
            timestamps    = _get_timestamps_for_day(db_paths, table, day)
            day_gaps      = _second_level_gaps(timestamps, session_start, session_end)

            for g in day_gaps:
                print(
                    f"[BACKFILL] {symbol} {day} TODAY gap: "
                    f"{g[0].strftime('%H:%M')}→{g[1].strftime('%H:%M')}",
                    flush=True,
                )
            gaps.extend(day_gaps)

        else:
            # ── past day ──────────────────────────────────────────────────
            session_start = _session_open(day)
            session_end   = _session_last_candle(day)
            timestamps    = _get_timestamps_for_day(db_paths, table, day)

            if not timestamps:
                print(
                    f"[BACKFILL] {symbol} {day} — no data, fetching full session",
                    flush=True,
                )
                gaps.append((session_start, session_end))
            else:
                day_gaps = _second_level_gaps(timestamps, session_start, session_end)
                if day_gaps:
                    for g in day_gaps:
                        print(
                            f"[BACKFILL] {symbol} {day} gap: "
                            f"{g[0].strftime('%H:%M')}→{g[1].strftime('%H:%M')}",
                            flush=True,
                        )
                    gaps.extend(day_gaps)
                else:
                    print(
                        f"[BACKFILL] {symbol} {day} — data complete, skipping",
                        flush=True,
                    )

    return gaps


# ---------------------------------------------------------------------------
# DB helpers kept for start_data.py compatibility
# ---------------------------------------------------------------------------

def _quote_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'quote_%'"
    )
    return [row[0] for row in cur.fetchall()]


def _latest_from_raw_rows(conn: sqlite3.Connection, table: str) -> Optional[int]:
    latest: Optional[int] = None
    cur = conn.execute(
        f"SELECT timestamp, raw_json FROM {table} ORDER BY rowid DESC LIMIT 1000"
    )
    for ts_value, raw_json in cur.fetchall():
        ts_ms = _to_ms(ts_value)
        if ts_ms is None and raw_json:
            try:
                payload = json.loads(raw_json)
                ts_ms = _to_ms(payload.get("timestamp") or payload.get("ltt"))
            except Exception:
                ts_ms = None
        if ts_ms is not None and (latest is None or ts_ms > latest):
            latest = ts_ms
    return latest


def _latest_timestamp_in_db(db_path: Path) -> Optional[int]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        latest: Optional[int] = None
        for table in _quote_tables(conn):
            try:
                cur = conn.execute(f"SELECT MAX(timestamp) FROM {table}")
                ts_ms = _to_ms(cur.fetchone()[0])
                if ts_ms is None:
                    ts_ms = _latest_from_raw_rows(conn, table)
                if ts_ms is not None and (latest is None or ts_ms > latest):
                    latest = ts_ms
            except Exception as exc:
                print(
                    f"[BACKFILL][WARN] failed scanning {db_path.name}:{table}: {exc}",
                    flush=True,
                )
        return latest
    finally:
        conn.close()


def latest_collected_timestamp(quote_output_dir: str) -> Optional[datetime]:
    """
    Scan all weekly SQLite DBs and return the most recent quote timestamp.
    Called from start_data.py BEFORE the websocket starts.
    """
    base_dir = Path(quote_output_dir)
    for db_path in _candidate_db_paths(base_dir):
        try:
            ts_ms = _latest_timestamp_in_db(db_path)
        except sqlite3.Error as exc:
            print(f"[BACKFILL][WARN] failed opening {db_path.name}: {exc}", flush=True)
            continue
        if ts_ms is not None:
            latest = _ms_to_ist(ts_ms)
            print(
                f"[BACKFILL] pre-startup latest quote: {latest.isoformat()} "
                f"from {db_path.name}",
                flush=True,
            )
            return latest
    return None


# ---------------------------------------------------------------------------
# Historical PostgreSQL helpers
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
) -> List[HistoryWindow]:
    table = f"quote_{_safe_symbol(symbol)}".lower()
    remaining: List[HistoryWindow] = []

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
            print(
                f"[BACKFILL] {exchange}:{symbol} "
                f"{window_start.strftime('%Y-%m-%d %H:%M')}→"
                f"{window_end.strftime('%H:%M')} "
                "already present in history DB, skipping",
                flush=True,
            )
            continue

        if missing_windows != [(window_start, window_end)]:
            print(
                f"[BACKFILL] {exchange}:{symbol} "
                f"{window_start.strftime('%Y-%m-%d %H:%M')}→"
                f"{window_end.strftime('%H:%M')} "
                f"partially present in history DB, fetching {len(missing_windows)} gap(s)",
                flush=True,
            )
        remaining.extend(missing_windows)

    return remaining


# ---------------------------------------------------------------------------
# OpenAlgo history API helpers
# ---------------------------------------------------------------------------

def _history_endpoint() -> str:
    endpoint = os.getenv("OPENALGO_HISTORY_URL", "").strip()
    if endpoint:
        return endpoint
    host = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000").rstrip("/")
    return f"{host}/api/v1/history"


def _extract_candle_rows(payload: Any) -> Sequence[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "candles", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_candle_rows(value)
            if nested:
                return nested
    if all(isinstance(v, list) for v in payload.values()):
        keys = list(payload.keys())
        row_count = min(len(payload[key]) for key in keys)
        return [{key: payload[key][idx] for key in keys} for idx in range(row_count)]
    return []


def _row_timestamp(row: Any) -> Optional[int]:
    if isinstance(row, dict):
        for key in ("timestamp", "time", "datetime", "date", "t"):
            ts_ms = _to_ms(row.get(key))
            if ts_ms is not None:
                return ts_ms
    elif isinstance(row, (list, tuple)) and row:
        return _to_ms(row[0])
    return None


def _normalize_candle(
    row: Any, symbol: str, exchange: str, interval: str
) -> Optional[dict]:
    ts_ms = _row_timestamp(row)
    if ts_ms is None:
        return None
    if isinstance(row, dict):
        payload = dict(row)
    elif isinstance(row, (list, tuple)):
        keys = ("timestamp", "open", "high", "low", "close", "volume", "oi")
        payload = {key: row[idx] for idx, key in enumerate(keys) if idx < len(row)}
    else:
        return None
    payload["timestamp"] = ts_ms
    payload.setdefault("symbol", symbol)
    payload.setdefault("exchange", exchange)
    payload.setdefault("interval", interval)
    payload.setdefault("source", "openalgo_history")
    return {
        "timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        .astimezone(tz_kolkata)
        .isoformat(),
        "ingest_ns": time.time_ns(),
        "exchange": exchange,
        "symbol": symbol,
        "raw_json": json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
    }


# ---------------------------------------------------------------------------
# BackfillManager
# ---------------------------------------------------------------------------

class BackfillManager:
    """
    Detects missing trading data and recovers it from the OpenAlgo history API.

    Gap detection:
        - Checks the last X9_BACKFILL_MIN_DAYS trading days per symbol
        - Past days: queries live SQLite — if no rows for that day, fetches full session
        - Today:     uses pre_startup_ts (captured before websocket starts)
                     so live ticks are never mistaken for pre-existing data

    Output:
        - Saves recovered candles to PostgreSQL (PG_HDBNAME database)
        - One table per symbol: quote_RELIANCE, quote_TCS ...
    """

    def __init__(
        self,
        symbols: Sequence[dict],
        quote_output_dir: str,
        api_key: str,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
        last_known_timestamp: Optional[datetime] = None,
    ):
        self.symbols            = list(symbols)
        self.quote_output_dir   = quote_output_dir
        self.api_key            = api_key
        self.interval           = os.getenv("OPENALGO_HISTORY_INTERVAL", "1m").strip() or "1m"
        self.endpoint           = _history_endpoint()
        self.history_dbname     = (
            os.getenv("PG_HDBNAME", "market_history").strip() or "market_history"
        )
        self.min_days           = max(
            3, int(os.getenv("X9_BACKFILL_MIN_DAYS", "3").strip() or "3")
        )
        self.flush_batch_size   = flush_batch_size
        self.flush_interval_sec = flush_interval_sec
        self._writer: Optional[PgWriter] = None
        # captured BEFORE websocket writes live ticks — used for today's gap
        self._last_known_timestamp: Optional[datetime] = last_known_timestamp

    async def run(self) -> None:
        try:
            await self._wait_for_completed_minute()
            # _run_once runs entirely in a thread pool thread via to_thread
            # — SQLite I/O, HTTP requests, PgWriter.shutdown() all stay off
            # the event loop so the websocket is never blocked
            await asyncio.to_thread(self._run_once)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[BACKFILL][ERROR] recovery failed: {exc}", flush=True)
        # no finally shutdown here — shutdown is called inside _run_once
        # in the thread to avoid blocking the event loop

    async def _wait_for_completed_minute(self) -> None:
        now = now_kolkata()
        delay = 60 - now.second - (now.microsecond / 1_000_000)
        if delay >= 60:
            delay = 0
        if delay > 0:
            target = now + timedelta(seconds=delay)
            print(
                f"[BACKFILL] waiting until {target.strftime('%H:%M:%S')} IST "
                f"before gap scan",
                flush=True,
            )
            await asyncio.sleep(delay)

    def _run_once(self) -> None:
        now           = now_kolkata()
        required_days = _last_n_trading_days(now, self.min_days)
        db_paths      = _candidate_db_paths(Path(self.quote_output_dir))

        print(
            f"[BACKFILL] scanning last {self.min_days} trading days "
            f"({required_days[0] if required_days else '?'} → "
            f"{required_days[-1] if required_days else '?'}) "
            f"across {len(db_paths)} DB file(s)",
            flush=True,
        )

        # ── per-symbol gap detection ──────────────────────────────────────
        all_gaps: Dict[str, Tuple[str, List[HistoryWindow]]] = {}
        history_conn = None

        try:
            history_conn = _history_connection(self.history_dbname)
        except Exception as exc:
            print(
                f"[BACKFILL][WARN] history DB check unavailable: {exc}",
                flush=True,
            )

        try:
            for symbol_row in self.symbols:
                symbol   = str(symbol_row["symbol"]).upper()
                exchange = str(symbol_row.get("exchange") or "NSE").upper()

                gaps = _find_symbol_gaps(
                    db_paths,
                    symbol,
                    required_days,
                    self._last_known_timestamp,
                    now,
                )

                if gaps and history_conn is not None:
                    try:
                        gaps = _filter_gaps_already_in_history(
                            history_conn,
                            symbol,
                            exchange,
                            gaps,
                        )
                    except Exception as exc:
                        print(
                            f"[BACKFILL][WARN] history DB check failed for "
                            f"{exchange}:{symbol}: {exc}",
                            flush=True,
                        )

                if gaps:
                    all_gaps[symbol] = (exchange, gaps)
        finally:
            if history_conn is not None:
                try:
                    history_conn.close()
                except Exception:
                    pass

        if not all_gaps:
            print("[BACKFILL] no missing data detected — nothing to fetch", flush=True)
            return

        total_windows = sum(len(v[1]) for v in all_gaps.values())
        print(
            f"[BACKFILL] {len(all_gaps)} symbol(s) need recovery "
            f"across {total_windows} window(s) → PG database '{self.history_dbname}'",
            flush=True,
        )

        # ── create PG writer ──────────────────────────────────────────────
        self._writer = PgWriter(
            table="quote",
            dbname=self.history_dbname,
            flush_batch_size=self.flush_batch_size,
            flush_interval_sec=self.flush_interval_sec,
            dedup_on_timestamp=True,
        )

        # ── fetch and enqueue ─────────────────────────────────────────────
        try:
            total_rows = 0
            for symbol, (exchange, gaps) in all_gaps.items():
                symbol_rows = 0
                for window_start, window_end in gaps:
                    candles = self._fetch_symbol_window(
                        symbol, exchange, window_start, window_end
                    )
                    for candle in candles:
                        self._writer.enqueue(symbol, candle)
                    symbol_rows += len(candles)
                    print(
                        f"[BACKFILL] {exchange}:{symbol} "
                        f"{window_start.strftime('%Y-%m-%d %H:%M')}→"
                        f"{window_end.strftime('%H:%M')} "
                        f"fetched {len(candles)} candle(s)",
                        flush=True,
                    )
                total_rows += symbol_rows

            print(f"[BACKFILL] total {total_rows} candle(s) queued", flush=True)

        finally:
            # shutdown inside the thread — thread.join() never reaches event loop
            # websocket is completely unaffected during drain + flush
            self._writer.shutdown()
            self._writer = None

    def _fetch_symbol_window(
        self,
        symbol: str,
        exchange: str,
        window_start: datetime,
        window_end: datetime,
    ) -> List[dict]:
        body = {
            "apikey":     self.api_key,
            "symbol":     symbol,
            "exchange":   exchange,
            "interval":   self.interval,
            "start_date": window_start.strftime("%Y-%m-%d"),
            "end_date":   window_end.strftime("%Y-%m-%d"),
            "source":     "api",
        }
        data         = json.dumps(body).encode("utf-8")
        http_request = request.Request(
            self.endpoint,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(http_request, timeout=30) as response:
                raw_response = response.read().decode("utf-8")
                payload = json.loads(raw_response)
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            print(
                f"[BACKFILL][WARN] fetch failed for {exchange}:{symbol}: {exc}",
                flush=True,
            )
            return []

        start_ms = int(window_start.astimezone(timezone.utc).timestamp() * 1000)
        end_ms   = int(window_end.astimezone(timezone.utc).timestamp() * 1000)
        candles: List[dict] = []

        for row in _extract_candle_rows(payload):
            ts_ms = _row_timestamp(row)
            if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                continue
            normalized = _normalize_candle(row, symbol, exchange, self.interval)
            if normalized:
                candles.append(normalized)

        if not candles:
            # log the raw response so we can diagnose API issues
            preview = raw_response[:300] if len(raw_response) > 300 else raw_response
            print(
                f"[BACKFILL][WARN] 0 candles from API for {exchange}:{symbol} "
                f"{window_start.strftime('%Y-%m-%d')} — response: {preview}",
                flush=True,
            )

        return candles
