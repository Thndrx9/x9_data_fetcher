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
    depth_levels: int = 5,
):
    """
    WebSocket connection only:
    - authenticate
    - subscribe depth mode
    - forward incoming market_data packets to event_bus queue
    """
    if not ws_url:
        ws_url = DEFAULT_WS_URL

    if not api_key:
        raise RuntimeError("API_KEY missing")

    print(f"[WS] Connecting to {ws_url}", flush=True)

    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                await ws.send(json.dumps({"action": "authenticate", "api_key": api_key}))
                print("[WS] Authentication sent", flush=True)

                for inst in instruments:
                    payload = {
                        "action": "subscribe",
                        "exchange": inst["exchange"],
                        "symbol": inst["symbol"],
                        "mode": "Depth",
                        "depth": depth_levels,
                    }
                    await ws.send(json.dumps(payload))
                    print(f"[WS] Subscribed {inst['exchange']}:{inst['symbol']}", flush=True)

                async for message in ws:
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        continue

                    if data.get("type") == "market_data":
                        await market_data_queue.put(data)
                    elif data.get("status") == "error":
                        print(f"[WS][ERROR] {data}", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[WS][ERROR] {exc}. Reconnecting in 2s...", flush=True)
            await asyncio.sleep(2)

