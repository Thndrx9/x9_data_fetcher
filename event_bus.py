import asyncio

# Central async queue for websocket market data
market_data_queue = asyncio.Queue(maxsize=20000)

# ── WebSocket connection state ──────────────────────────────────────────
# Tracks which subscription modes ("Quote", "Depth", ...) currently have a
# live, authenticated websocket connection. Single-threaded asyncio access
# only, so a plain set is safe without extra locking.
_ws_connected_modes: set[str] = set()


def mark_ws_connected(mode: str) -> None:
    _ws_connected_modes.add(mode)


def mark_ws_disconnected(mode: str) -> None:
    _ws_connected_modes.discard(mode)


def is_ws_connected() -> bool:
    """True if at least one websocket mode is currently connected."""
    return bool(_ws_connected_modes)