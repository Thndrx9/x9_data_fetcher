import json
import time
from datetime import datetime, timezone
from typing import Any, Dict

from x9_data_fetcher.depth_parquet_writer import DepthParquetWriter
from x9_data_fetcher.event_bus import market_data_queue
from x9_data_fetcher.market_time import tz_kolkata
from x9_data_fetcher.ohlc_parquet_writer import OhlcParquetWriter

try:
    from x9_data_fetcher.pg_writer import PgWriter
except ImportError:  # pragma: no cover - optional dependency
    PgWriter = None


def _to_iso_ts(value: Any) -> str:
    if value is None:
        return datetime.now(tz_kolkata).isoformat()
    try:
        v = float(value)
        if v > 10_000_000_000:
            dt = datetime.fromtimestamp(v / 1000, tz=timezone.utc).astimezone(tz_kolkata)
        else:
            dt = datetime.fromtimestamp(v, tz=timezone.utc).astimezone(tz_kolkata)
        return dt.isoformat()
    except Exception:
        return datetime.now(tz_kolkata).isoformat()


def _packet_mode(message: Dict[str, Any]) -> str | None:
    mode = message.get("_subscription_mode", message.get("mode"))
    if isinstance(mode, str):
        return mode.strip().lower()
    if mode == 2:
        return "quote"
    if mode == 3:
        return "depth"
    return None


def _extract_depth_row(message: Dict[str, Any]) -> Dict[str, Any] | None:
    inner = message.get("data", {})
    if not isinstance(inner, dict):
        return None

    symbol = inner.get("symbol") or message.get("symbol")
    exchange = inner.get("exchange") or message.get("exchange")
    if not symbol or not exchange:
        return None

    return {
        "timestamp": _to_iso_ts(inner.get("timestamp")),
        "ingest_ns": time.time_ns(),
        "exchange": str(exchange).upper(),
        "symbol": str(symbol).upper(),
        "raw_json": json.dumps(inner, ensure_ascii=True),
    }


def _extract_quote_row(message: Dict[str, Any]) -> Dict[str, Any] | None:
    inner = message.get("data", {})
    if not isinstance(inner, dict):
        return None

    symbol = inner.get("symbol") or message.get("symbol")
    exchange = inner.get("exchange") or message.get("exchange")
    ltp = inner.get("ltp")
    if not symbol or not exchange or ltp is None:
        return None

    return {
        "timestamp": _to_iso_ts(inner.get("ltt") or inner.get("timestamp")),
        "ingest_ns": time.time_ns(),
        "exchange": str(exchange).upper(),
        "symbol": str(symbol).upper(),
        "raw_json": json.dumps(inner, ensure_ascii=True),
    }


class MarketDataFetcher:
    """
    Data fetch/transform only:
    consumes websocket packets from event_bus queue and writes SQLite,
    with an optional PostgreSQL mirror.
    """

    def __init__(
        self,
        depth_output_dir: str,
        quote_output_dir: str,
        pg_dsn: str | None = None,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
    ):
        self.depth_writer = DepthParquetWriter(
            base_dir=depth_output_dir,
            flush_batch_size=flush_batch_size,
            flush_interval_sec=flush_interval_sec,
        )
        self.ohlc_writer = OhlcParquetWriter(
            base_dir=quote_output_dir,
            flush_batch_size=flush_batch_size,
            flush_interval_sec=flush_interval_sec,
        )
        self.depth_pg_writer = None
        self.quote_pg_writer = None

        if pg_dsn:
            if PgWriter is None:
                raise RuntimeError(
                    "PG writer requested but psycopg2 is not available"
                )
            self.depth_pg_writer = PgWriter(
                table="depth",
                dsn=pg_dsn,
                flush_batch_size=flush_batch_size,
                flush_interval_sec=flush_interval_sec,
            )
            self.quote_pg_writer = PgWriter(
                table="quote",
                dsn=pg_dsn,
                flush_batch_size=flush_batch_size,
                flush_interval_sec=flush_interval_sec,
            )

    async def run(self):
        while True:
            packet = await market_data_queue.get()
            try:
                mode = _packet_mode(packet)
                if mode == "depth":
                    depth_row = _extract_depth_row(packet)
                    if depth_row:
                        symbol = depth_row["symbol"]
                        self.depth_writer.enqueue(symbol, depth_row)
                        if self.depth_pg_writer:
                            self.depth_pg_writer.enqueue(symbol, depth_row)
                elif mode == "quote":
                    quote_row = _extract_quote_row(packet)
                    if quote_row:
                        symbol = quote_row["symbol"]
                        self.ohlc_writer.enqueue(symbol, quote_row)
                        if self.quote_pg_writer:
                            self.quote_pg_writer.enqueue(symbol, quote_row)
            finally:
                market_data_queue.task_done()

    def shutdown(self):
        self.ohlc_writer.shutdown()
        self.depth_writer.shutdown()
        if self.quote_pg_writer:
            self.quote_pg_writer.shutdown()
        if self.depth_pg_writer:
            self.depth_pg_writer.shutdown()
