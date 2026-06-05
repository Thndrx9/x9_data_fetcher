# x9_data_fetcher

Depth + Quote (websocket-only) data fetcher in x9-style split architecture.

It:
- creates/uses a local virtual environment
- uses a dedicated websocket connection file
- stores raw depth packets to Parquet per symbol
- stores raw quote tick packets for later candle generation

Structure:
- `websocket_connect.py`: websocket connect/auth/subscribe + queue publish only
- `event_bus.py`: shared async queue
- `data_fetcher.py`: consumes queue and writes depth + quote parquet
- `start_data.py`: runtime entrypoint

## Run

```bash
python3 -m x9_data_fetcher.start_data
```

## Output

- `data/<SYMBOL>/depth.parquet`
- `data/<SYMBOL>/quote.parquet`

Stored columns in both files:
- `timestamp`
- `ingest_ts_ns`
- `symbol`
- `exchange`
- `raw_json`

## Environment Variables

- `API_KEY` (required)
- `WEBSOCKET_HOST` (default: `127.0.0.1`)
- `WEBSOCKET_PORT` (default: `8765`)
- `X9_FETCHER_SYMBOLS_CSV` (default: `x9/symbols.csv`)
- `X9_DEPTH_OUTPUT_DIR` (default: `data`)
- `X9_QUOTE_OUTPUT_DIR` (default: `data`)
- `X9_DEPTH_LEVELS` (default: `5`)
- `X9_DEPTH_FLUSH_BATCH` (default: `200`)
- `X9_DEPTH_FLUSH_INTERVAL_SEC` (default: `1.0`)
- `X9_FETCHER_VENV_DIR` (default: `x9_data_fetcher/executor`)

## Requirements File

- `x9_data_fetcher/requirements.txt`
