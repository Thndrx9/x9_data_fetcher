import asyncio
import json
from typing import List

import websockets

from x9_data_fetcher.event_bus import market_data_queue

DEFAULT_WS_URL = "ws://127.0.0.1:8765"


async def websocket_client(
    ws_url: str | None,
    api_key: str,
    instruments: List[dict],
    mode: str,
    depth_levels: int = 5,
):
    """
    WebSocket connection only:
    - authenticate
    - subscribe one mode per connection
    - forward incoming market_data packets to event_bus queue
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
            raise
        except Exception as exc:
            print(f"[WS][ERROR] mode={mode_label} {exc}. Reconnecting in 2s...", flush=True)
            await asyncio.sleep(2)
