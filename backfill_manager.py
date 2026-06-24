import asyncio
import json
import os
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple
from urllib import request
from urllib.error import URLError

from x9_data_fetcher.market_time import (
    MARKET_CLOSE,
    MARKET_OPEN,
    is_trading_day,
    now_kolkata,
    tz_kolkata,
)
from x9_data_fetcher.pg_writer import PgWriter


HistoryWindow = Tuple[datetime, datetime]


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


def _iter_trading_windows(start: datetime, end: datetime) -> Iterable[HistoryWindow]:
    cursor_day = start.date()
    end_day = end.date()

    while cursor_day <= end_day:
        if is_trading_day(cursor_day):
            window_start = max(start, _session_open(cursor_day))
            window_end = min(end, _session_last_candle(cursor_day))
            if window_start <= window_end:
                yield window_start, window_end
        cursor_day += timedelta(days=1)


def _candidate_db_paths(base_dir: Path) -> List[Path]:
    return sorted(base_dir.glob("market_*.db"), key=lambda p: p.name, reverse=True)


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
                f"[BACKFILL] latest live quote timestamp {latest.isoformat()} from {db_path.name}",
                flush=True,
            )
            return latest
    return None


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


class BackfillManager:
    def __init__(
        self,
        symbols: Sequence[dict],
        quote_output_dir: str,
        api_key: str,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
    ):
        self.symbols = list(symbols)
        self.quote_output_dir = quote_output_dir
        self.api_key = api_key
        self.interval = os.getenv("OPENALGO_HISTORY_INTERVAL", "1m").strip() or "1m"
        self.endpoint = _history_endpoint()
        self.history_dbname = (
            os.getenv("PG_HDBNAME", "market_history").strip() or "market_history"
        )
        self.flush_batch_size = flush_batch_size
        self.flush_interval_sec = flush_interval_sec
        self._writer: Optional[PgWriter] = None

    async def run(self) -> None:
        try:
            await self._wait_for_completed_minute()
            await asyncio.to_thread(self._run_once)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[BACKFILL][ERROR] recovery failed: {exc}", flush=True)
        finally:
            if self._writer is not None:
                self._writer.shutdown()

    async def _wait_for_completed_minute(self) -> None:
        now = now_kolkata()
        delay = 60 - now.second - (now.microsecond / 1_000_000)
        if delay >= 60:
            delay = 0
        if delay > 0:
            target = now + timedelta(seconds=delay)
            print(
                f"[BACKFILL] waiting until {target.strftime('%H:%M:%S')} IST before history scan",
                flush=True,
            )
            await asyncio.sleep(delay)

    def _run_once(self) -> None:
        latest_live = latest_collected_timestamp(self.quote_output_dir)
        if latest_live is None:
            print(
                "[BACKFILL] no live quote data found in weekly SQLite databases",
                flush=True,
            )
            return

        latest_completed = _latest_completed_candle(now_kolkata())
        if latest_completed is None:
            print("[BACKFILL] no completed market candle available yet", flush=True)
            return

        start = _floor_minute(latest_live) + timedelta(minutes=1)
        end = _floor_minute(latest_completed)
        if start > end:
            print("[BACKFILL] no missing 1-minute candles detected", flush=True)
            return

        windows = list(_iter_trading_windows(start, end))
        if not windows:
            print("[BACKFILL] missing range contains no trading sessions", flush=True)
            return

        print(
            f"[BACKFILL] recovering {len(windows)} trading window(s) "
            f"from {start.isoformat()} to {end.isoformat()} into PG database '{self.history_dbname}'",
            flush=True,
        )

        self._writer = PgWriter(
            table="quote",
            dbname=self.history_dbname,
            flush_batch_size=self.flush_batch_size,
            flush_interval_sec=self.flush_interval_sec,
        )

        total_rows = 0
        for symbol_row in self.symbols:
            symbol = str(symbol_row["symbol"]).upper()
            exchange = str(symbol_row.get("exchange") or "NSE").upper()
            symbol_rows = 0
            for window_start, window_end in windows:
                candles = self._fetch_symbol_window(
                    symbol, exchange, window_start, window_end
                )
                for candle in candles:
                    self._writer.enqueue(symbol, candle)
                symbol_rows += len(candles)
            total_rows += symbol_rows
            print(
                f"[BACKFILL] {exchange}:{symbol} queued {symbol_rows} candle(s)",
                flush=True,
            )

        print(f"[BACKFILL] queued {total_rows} recovered candle(s)", flush=True)

    def _fetch_symbol_window(
        self,
        symbol: str,
        exchange: str,
        window_start: datetime,
        window_end: datetime,
    ) -> List[dict]:
        body = {
            "apikey": self.api_key,
            "symbol": symbol,
            "exchange": exchange,
            "interval": self.interval,
            "start_date": window_start.strftime("%Y-%m-%d"),
            "end_date": window_end.strftime("%Y-%m-%d"),
        }
        data = json.dumps(body).encode("utf-8")
        http_request = request.Request(
            self.endpoint,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(http_request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            print(
                f"[BACKFILL][WARN] history fetch failed for {exchange}:{symbol}: {exc}",
                flush=True,
            )
            return []

        start_ms = int(window_start.astimezone(timezone.utc).timestamp() * 1000)
        end_ms = int(window_end.astimezone(timezone.utc).timestamp() * 1000)
        candles: List[dict] = []

        for row in _extract_candle_rows(payload):
            ts_ms = _row_timestamp(row)
            if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                continue
            normalized = _normalize_candle(row, symbol, exchange, self.interval)
            if normalized:
                candles.append(normalized)

        return candles
