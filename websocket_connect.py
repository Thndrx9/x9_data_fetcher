import asyncio
import json
from typing import List, Optional

import websockets

from x9_data_fetcher import connection_log
from x9_data_fetcher.event_bus import market_data_queue
from x9_data_fetcher.market_time import now_kolkata

DEFAULT_WS_URL = "ws://127.0.0.1:8765"


async def websocket_client(
    ws_url: str | None,
    api_key: str,
    instruments: List[dict],
    mode: str,
    depth_levels: int = 5,
    conn_log_dir: Optional[str] = None,
):
    """
    WebSocket connection only:
    - authenticate
    - subscribe one mode per connection
    - forward incoming market_data packets to event_bus queue

    conn_log_dir: if set, DAY_STARTED / RECONNECTED / DISCONNECTED events are
    written to the connection log for this connection. Pass this only for
    the "Quote" mode connection — that's the one BackfillManager cares about.
    """
    if not ws_url:
        ws_url = DEFAULT_WS_URL

    if not api_key:
        raise RuntimeError("API_KEY missing")

    mode_label = str(mode).strip().title()
    print(f"[WS] Connecting to {ws_url} | mode={mode_label}", flush=True)

    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                await ws.send(json.dumps({"action": "authenticate", "api_key": api_key}))
                print(f"[WS] Authentication sent | mode={mode_label}", flush=True)

                if conn_log_dir:
                    now = now_kolkata()
                    event = (
                        "RECONNECTED"
                        if connection_log.has_event_today(conn_log_dir, "DAY_STARTED", now)
                        else "DAY_STARTED"
                    )
                    connection_log.log_event(conn_log_dir, event, now, mode=mode_label)

                for inst in instruments:
                    payload = {
                        "action": "subscribe",
                        "exchange": inst["exchange"],
                        "symbol": inst["symbol"],
                        "mode": mode_label,
                    }
                    if mode_label == "Depth":
                        payload["depth"] = depth_levels
                    await ws.send(json.dumps(payload))

                # Log grouped subscription summary per exchange
                grouped = {}
                for inst in instruments:
                    ex = str(inst.get("exchange", "")).upper()
                    sym = str(inst.get("symbol", "")).upper()
                    if not ex or not sym:
                        continue
                    grouped.setdefault(ex, []).append(sym)

                for ex, symbols in grouped.items():
                    print(
                        f"[WS] Subscribed {mode_label} {ex}:{','.join(symbols)}",
                        flush=True,
                    )

                loop = asyncio.get_running_loop()
                last_rx_at = loop.time()

                async def heartbeat():
                    while True:
                        await asyncio.sleep(60)
                        idle_sec = int(loop.time() - last_rx_at)
                        print(
                            f"[WS][HEARTBEAT] Connected | mode={mode_label} | idle={idle_sec}s | queue={market_data_queue.qsize()}",
                            flush=True,
                        )

                hb_task = asyncio.create_task(heartbeat())
                try:
                    async for message in ws:
                        last_rx_at = loop.time()
                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            continue

                        if data.get("type") == "market_data":
                            data["_subscription_mode"] = mode_label
                            await market_data_queue.put(data)
                        elif data.get("status") == "error":
                            print(f"[WS][ERROR] mode={mode_label} {data}", flush=True)
                finally:
                    hb_task.cancel()
                    await asyncio.gather(hb_task, return_exceptions=True)
        except asyncio.CancelledError:
            if conn_log_dir:
                connection_log.log_event(
                    conn_log_dir, "DISCONNECTED", now_kolkata(),
                    mode=mode_label, note="task cancelled (shutdown/session end)",
                )
            raise
        except Exception as exc:
            if conn_log_dir:
                connection_log.log_event(
                    conn_log_dir, "DISCONNECTED", now_kolkata(),
                    mode=mode_label, note=str(exc),
                )
            print(f"[WS][ERROR] mode={mode_label} {exc}. Reconnecting in 2s...", flush=True)
            await asyncio.sleep(2)