import asyncio
import os
import signal
import sys
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv

# Allow running this file directly: `python x9_data_fetcher/start_data.py`
if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from x9_data_fetcher.venv_setup import create_and_activate_venv

create_and_activate_venv()

from x9_data_fetcher.backfill_manager import BackfillManager
from x9_data_fetcher.data_fetcher import MarketDataFetcher
from x9_data_fetcher.event_bus import market_data_queue
from x9_data_fetcher.market_time import (
    is_connectable,
    now_kolkata,
    seconds_until_close,
    seconds_until_pre_connect,
)
from x9_data_fetcher.symbols import load_symbols
from x9_data_fetcher.pg_writer import auto_setup as pg_auto_setup
from x9_data_fetcher.websocket_connect import websocket_client


def _drain_queue():
    """Drain leftover items from the shared async queue between sessions."""
    dropped = 0
    while not market_data_queue.empty():
        try:
            market_data_queue.get_nowait()
            market_data_queue.task_done()
            dropped += 1
        except asyncio.QueueEmpty:
            break
    if dropped:
        print(f"[X9_FETCHER] Drained {dropped} stale packets from queue", flush=True)


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
    quote_output_dir = os.getenv(
        "X9_QUOTE_OUTPUT_DIR", os.getenv("X9_OHLC_OUTPUT_DIR", "data")
    )
    pg_dsn = os.getenv("X9_PG_DSN", os.getenv("X9_POSTGRES_DSN", "")).strip() or None
    depth_levels = int(os.getenv("X9_DEPTH_LEVELS", "5"))
    flush_batch = int(os.getenv("X9_DEPTH_FLUSH_BATCH", "200"))
    flush_interval = float(os.getenv("X9_DEPTH_FLUSH_INTERVAL_SEC", "1.0"))

    symbols = load_symbols(symbols_csv)
    if not symbols:
        raise RuntimeError(f"No symbols found in {symbols_csv}")

    # run PG setup at startup so PostgreSQL is ready before market opens
    # skips instantly if already running, installs+configures if missing
    if os.getenv("PG_HOST", "").strip() or os.getenv("X9_PG_DSN", "").strip():
        pg_auto_setup()

    instruments = [{"exchange": s["exchange"], "symbol": s["symbol"]} for s in symbols]

    # ── Graceful exit on SIGINT / SIGTERM ──
    manual_stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, manual_stop.set)
    loop.add_signal_handler(signal.SIGTERM, manual_stop.set)

    print(
        f"[X9_FETCHER] Initialized | symbols={len(instruments)} | ws={ws_url}",
        flush=True,
    )

    # ── Daily loop ──────────────────────────────────────────────────────
    while not manual_stop.is_set():
        now = now_kolkata()

        # ── Wait for pre-connect window (9:14 IST) if market is closed ──
        if not is_connectable(now):
            wait_secs = seconds_until_pre_connect(now)
            resume_at = now + timedelta(seconds=wait_secs)
            print(
                f"[X9_FETCHER] Market closed. Next session: "
                f"{resume_at.strftime('%Y-%m-%d %H:%M:%S')} IST "
                f"(waiting {wait_secs / 3600:.1f}h)",
                flush=True,
            )
            try:
                await asyncio.wait_for(manual_stop.wait(), timeout=wait_secs)
            except asyncio.TimeoutError:
                pass  # Timer expired → time to connect
            continue  # Re-evaluate after waking

        # ── Start trading session ───────────────────────────────────────
        close_secs = seconds_until_close(now)
        if close_secs <= 0:
            continue  # Edge case: woke up exactly at 15:30

        _drain_queue()

        fetcher = MarketDataFetcher(
            depth_output_dir=depth_output_dir,
            quote_output_dir=quote_output_dir,
            pg_dsn=pg_dsn,
            flush_batch_size=flush_batch,
            flush_interval_sec=flush_interval,
        )

        # Auto-close event fires at market close (15:30)
        session_stop = asyncio.Event()

        async def _auto_close(secs: float):
            try:
                await asyncio.sleep(secs)
            except asyncio.CancelledError:
                return
            session_stop.set()

        close_task = asyncio.create_task(_auto_close(close_secs))

        tasks = [
            asyncio.create_task(
                websocket_client(ws_url, api_key, instruments, mode="Quote")
            ),
            asyncio.create_task(
                websocket_client(
                    ws_url, api_key, instruments, mode="Depth", depth_levels=depth_levels
                )
            ),
            asyncio.create_task(fetcher.run()),
        ]

        backfill_enabled = os.getenv("X9_BACKFILL_ENABLED", "1").strip().lower()
        backfill_enabled = backfill_enabled not in ("0", "false", "no", "off")
        pg_configured = bool(
            os.getenv("PG_HOST", "").strip()
            or os.getenv("PG_HDBNAME", "").strip()
            or pg_dsn
        )
        if backfill_enabled and pg_configured:
            tasks.append(
                asyncio.create_task(
                    BackfillManager(
                        symbols=symbols,
                        quote_output_dir=quote_output_dir,
                        api_key=api_key,
                        flush_batch_size=flush_batch,
                        flush_interval_sec=flush_interval,
                    ).run()
                )
            )
        elif backfill_enabled:
            print("[BACKFILL] skipped because PostgreSQL is not configured", flush=True)

        print(
            f"[X9_FETCHER] Session started at "
            f"{now.strftime('%H:%M:%S')} IST | "
            f"auto-close in {close_secs / 60:.0f} min",
            flush=True,
        )

        # ── Block until market close OR manual stop ─────────────────────
        wait_tasks = [
            asyncio.create_task(session_stop.wait()),
            asyncio.create_task(manual_stop.wait()),
        ]
        _done, pending = await asyncio.wait(
            wait_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()

        # ── Tear down session ───────────────────────────────────────────
        close_task.cancel()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, close_task, return_exceptions=True)
        fetcher.shutdown()

        if manual_stop.is_set():
            print("[X9_FETCHER] Manual shutdown complete", flush=True)
            break

        print(
            "[X9_FETCHER] Market closed at 15:30. Session ended, data flushed.",
            flush=True,
        )
        # Loop back → will calculate wait until next 9:14 (skipping weekends)


if __name__ == "__main__":
    asyncio.run(run_engine())
