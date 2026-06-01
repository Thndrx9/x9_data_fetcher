import asyncio
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

# Allow running this file directly: `python x9_data_fetcher/start_data.py`
if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from x9_data_fetcher.venv_setup import create_and_activate_venv

create_and_activate_venv()

from x9_data_fetcher.data_fetcher import MarketDataFetcher
from x9_data_fetcher.symbols import load_symbols
from x9_data_fetcher.websocket_connect import websocket_client


async def run_engine():
    module_env = Path(__file__).resolve().parent / ".env"
    if module_env.exists():
        load_dotenv(dotenv_path=module_env)
    load_dotenv()

    api_key = os.getenv("API_KEY", "")
    if not api_key:
        raise RuntimeError("API_KEY missing in .env")

    ws_host = os.getenv("WEBSOCKET_HOST", "127.0.0.1")
    ws_port = int(os.getenv("WEBSOCKET_PORT", "8765"))
    ws_url = os.getenv("WEBSOCKET_URL", f"ws://{ws_host}:{ws_port}")

    symbols_csv = os.getenv("X9_FETCHER_SYMBOLS_CSV", "x9/symbols.csv")
    depth_output_dir = os.getenv("X9_DEPTH_OUTPUT_DIR", "data")
    ohlc_output_dir = os.getenv("X9_OHLC_OUTPUT_DIR", "data")
    depth_levels = int(os.getenv("X9_DEPTH_LEVELS", "5"))
    flush_batch = int(os.getenv("X9_DEPTH_FLUSH_BATCH", "200"))
    flush_interval = float(os.getenv("X9_DEPTH_FLUSH_INTERVAL_SEC", "1.0"))

    symbols = load_symbols(symbols_csv)
    if not symbols:
        raise RuntimeError(f"No symbols found in {symbols_csv}")

    instruments = [{"exchange": s["exchange"], "symbol": s["symbol"]} for s in symbols]
    fetcher = MarketDataFetcher(
        depth_output_dir=depth_output_dir,
        ohlc_output_dir=ohlc_output_dir,
        depth_levels=depth_levels,
        flush_batch_size=flush_batch,
        flush_interval_sec=flush_interval,
    )

    stop_event = asyncio.Event()

    def _request_stop():
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, _request_stop)
    loop.add_signal_handler(signal.SIGTERM, _request_stop)

    tasks = [
        asyncio.create_task(websocket_client(ws_url, api_key, instruments, depth_levels)),
        asyncio.create_task(fetcher.run()),
    ]

    print(
        f"[X9_FETCHER] Started | symbols={len(instruments)} | ws={ws_url} | depth={depth_levels}",
        flush=True,
    )

    await stop_event.wait()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    fetcher.shutdown()
    print("[X9_FETCHER] Shutdown complete", flush=True)


if __name__ == "__main__":
    asyncio.run(run_engine())
