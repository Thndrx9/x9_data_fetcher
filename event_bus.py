import asyncio

# Central async queue for websocket market data
market_data_queue = asyncio.Queue(maxsize=20000)

