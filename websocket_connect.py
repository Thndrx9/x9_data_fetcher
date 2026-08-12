import asyncio
import json
from typing import List, Optional

import websockets

from x9_data_fetcher import connection_log
from x9_data_fetcher.event_bus import (
    market_data_queue,
    mark_ws_connected,
    mark_ws_disconnected,
)
from x9_data_fetcher.market_time import now_kolkata
from x9_data_fetcher.console import colorize as _colorize

_builtin_print = print


def print(*args, **kwargs):  # noqa: A001 — shadow builtin so every existing
    # print() call in this file picks up the shared color scheme without
    # having to edit each call site individually.
    if args and isinstance(args[0], str):
        args = (_colorize(args[0]),) + args[1:]
    _builtin_print(*args, **kwargs)

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

                # ── A: don't trust "sent" — wait for the server's actual
                # auth response before treating this connection as live.
                # OpenAlgo's websocket proxy replies with either:
                #   success: {"type": "auth", "status": "success", ...}
                #   failure: {"status": "error", "code": "...", "message": "..."}
                # Previously we logged DAY_STARTED/RECONNECTED right after
                # sending the auth request, with no idea whether the server
                # actually accepted it — a stale daily token could get
                # rejected here while we'd already marked the day "connected".
                try:
                    raw_auth_reply = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        f"no auth response from server within 15s | mode={mode_label}"
                    )

                try:
                    auth_reply = json.loads(raw_auth_reply)
                except json.JSONDecodeError:
                    raise RuntimeError(
                        f"unparseable auth response | mode={mode_label} raw={raw_auth_reply!r}"
                    )

                if auth_reply.get("status") != "success":
                    # ── B: an explicit auth error is a real disconnect, not
                    # something to log and shrug off. Raising here routes us
                    # into the existing except-Exception block below, which
                    # logs DISCONNECTED and retries in 2s — so a bad/expired
                    # daily token shows up as a visible reconnect loop
                    # instead of a silently "successful" connection with zero
                    # data flowing.
                    err_code = auth_reply.get("code", "UNKNOWN")
                    err_msg  = auth_reply.get("message", "authentication failed")
                    raise RuntimeError(
                        f"auth rejected by server | mode={mode_label} "
                        f"code={err_code} message={err_msg}"
                    )

                print(
                    f"[WS] Authenticated | mode={mode_label} "
                    f"broker={auth_reply.get('broker')} user={auth_reply.get('user_id')}",
                    flush=True,
                )

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
                    suffix = (
                        " (same universe as Quote)" if mode_label == "Depth" else ""
                    )
                    print(
                        f"[WS] Subscribed {mode_label} {ex}:{len(symbols)} symbols{suffix}",
                        flush=True,
                    )

                # connection is authenticated + subscribed — safe to treat as "live"
                mark_ws_connected(mode_label)

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
                            # ── B (ongoing stream): a mid-session error — e.g.
                            # the broker session was invalidated after we'd
                            # already authenticated — is a real fault. Raising
                            # instead of just printing ensures it's logged as
                            # DISCONNECTED and triggers a reconnect/re-auth,
                            # rather than leaving a socket open that looks
                            # "connected" while receiving nothing useful.
                            raise RuntimeError(
                                f"server error message | mode={mode_label} data={data}"
                            )
                finally:
                    hb_task.cancel()
                    await asyncio.gather(hb_task, return_exceptions=True)
        except asyncio.CancelledError:
            mark_ws_disconnected(mode_label)
            if conn_log_dir:
                connection_log.log_event(
                    conn_log_dir, "DISCONNECTED", now_kolkata(),
                    mode=mode_label, note="task cancelled (shutdown/session end)",
                )
            raise
        except Exception as exc:
            mark_ws_disconnected(mode_label)
            if conn_log_dir:
                connection_log.log_event(
                    conn_log_dir, "DISCONNECTED", now_kolkata(),
                    mode=mode_label, note=str(exc),
                )
            print(f"[WS][ERROR] mode={mode_label} {exc}. Reconnecting in 2s...", flush=True)
            await asyncio.sleep(2)