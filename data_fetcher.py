import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from x9_data_fetcher.depth_parquet_writer import DepthParquetWriter
from x9_data_fetcher.event_bus import market_data_queue
from x9_data_fetcher.market_time import tz_kolkata
from x9_data_fetcher.ohlc_parquet_writer import OhlcParquetWriter


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


def _flatten_depth_levels(
    row: Dict[str, Any], bids: List[dict], asks: List[dict], depth_levels: int
) -> Dict[str, Any]:
    for i in range(depth_levels):
        b = bids[i] if i < len(bids) else {}
        a = asks[i] if i < len(asks) else {}
        level = i + 1
        row[f"bid_{level}_price"] = b.get("price")
        row[f"bid_{level}_quantity"] = b.get("quantity")
        row[f"bid_{level}_orders"] = b.get("orders")
        row[f"ask_{level}_price"] = a.get("price")
        row[f"ask_{level}_quantity"] = a.get("quantity")
        row[f"ask_{level}_orders"] = a.get("orders")
    return row


def _extract_depth_row(message: Dict[str, Any], depth_levels: int) -> Dict[str, Any] | None:
    inner = message.get("data", {})
    if not isinstance(inner, dict):
        return None

    symbol = inner.get("symbol") or message.get("symbol")
    exchange = inner.get("exchange") or message.get("exchange")
    if not symbol or not exchange:
        return None

    bids = inner.get("bids") or inner.get("depth", {}).get("buy") or []
    asks = inner.get("asks") or inner.get("depth", {}).get("sell") or []

    now_iso = datetime.now(tz_kolkata).isoformat()
    row = {
        "timestamp": _to_iso_ts(inner.get("timestamp")),
        "ingest_time": now_iso,
        "ingest_ts_ns": time.time_ns(),
        "exchange": exchange,
        "symbol": symbol,
        "ltp": inner.get("ltp"),
        "ltq": inner.get("ltq") or inner.get("last_quantity"),
        "open": inner.get("open"),
        "high": inner.get("high"),
        "low": inner.get("low"),
        "close": inner.get("close"),
        "volume": inner.get("volume"),
        "totalbuyqty": inner.get("totalbuyqty") or inner.get("total_buy_quantity"),
        "totalsellqty": inner.get("totalsellqty") or inner.get("total_sell_quantity"),
        "raw_json": json.dumps(inner, ensure_ascii=True),
    }
    return _flatten_depth_levels(row, bids, asks, depth_levels)


def _extract_ohlc_tick_row(message: Dict[str, Any]) -> Dict[str, Any] | None:
    inner = message.get("data", {})
    if not isinstance(inner, dict):
        return None

    symbol = inner.get("symbol") or message.get("symbol")
    exchange = inner.get("exchange") or message.get("exchange")
    ltp = inner.get("ltp")
    if not symbol or not exchange or ltp is None:
        return None

    qty = inner.get("ltq") or inner.get("last_quantity") or inner.get("last_trade_quantity") or 0
    try:
        qty = int(qty)
    except Exception:
        qty = 0

    return {
        "timestamp": _to_iso_ts(inner.get("ltt") or inner.get("timestamp")),
        "ingest_time": datetime.now(tz_kolkata).isoformat(),
        "ingest_ts_ns": time.time_ns(),
        "exchange": str(exchange).upper(),
        "symbol": str(symbol).upper(),
        "ltp": ltp,
        "ltq": qty,
        "open": inner.get("open"),
        "high": inner.get("high"),
        "low": inner.get("low"),
        "close": inner.get("close"),
        "volume": inner.get("volume"),
        "raw_json": json.dumps(inner, ensure_ascii=True),
    }


class MarketDataFetcher:
    """
    Data fetch/transform only:
    consumes websocket packets from event_bus queue and writes parquet.
    """

    def __init__(
        self,
        depth_output_dir: str,
        ohlc_output_dir: str,
        depth_levels: int = 5,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
    ):
        self.depth_levels = depth_levels
        self.depth_writer = DepthParquetWriter(
            base_dir=depth_output_dir,
            flush_batch_size=flush_batch_size,
            flush_interval_sec=flush_interval_sec,
        )
        self.ohlc_writer = OhlcParquetWriter(
            base_dir=ohlc_output_dir,
            flush_batch_size=flush_batch_size,
            flush_interval_sec=flush_interval_sec,
        )

    async def run(self):
        while True:
            packet = await market_data_queue.get()
            try:
                depth_row = _extract_depth_row(packet, self.depth_levels)
                if depth_row:
                    self.depth_writer.enqueue(depth_row["symbol"], depth_row)

                ohlc_tick = _extract_ohlc_tick_row(packet)
                if ohlc_tick:
                    self.ohlc_writer.enqueue(ohlc_tick["symbol"], ohlc_tick)
            finally:
                market_data_queue.task_done()

    def shutdown(self):
        self.ohlc_writer.shutdown()
        self.depth_writer.shutdown()

