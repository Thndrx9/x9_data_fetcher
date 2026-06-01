"""
Backward-compatible entrypoint.
Use `python3 -m x9_data_fetcher.start_data` for the x9-style split runtime.
"""

import asyncio
import sys
from pathlib import Path

# Allow running this file directly: `python x9_data_fetcher/depth_parquet_fetcher.py`
if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from x9_data_fetcher.start_data import run_engine


if __name__ == "__main__":
    asyncio.run(run_engine())
