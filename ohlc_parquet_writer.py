import os
import queue
import threading
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd


class OhlcParquetWriter:
    """
    Buffered single-writer for OHLC tick rows.
    Writes to: <base_dir>/<SYMBOL>/ohlc.parquet
    """

    def __init__(self, base_dir: str, flush_batch_size: int = 200, flush_interval_sec: float = 1.0):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.flush_batch_size = max(1, int(flush_batch_size))
        self.flush_interval_sec = max(0.2, float(flush_interval_sec))

        self._q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="ohlc-parquet-writer", daemon=True)
        self._thread.start()

    def enqueue(self, symbol: str, row: dict) -> None:
        self._q.put((symbol.upper(), dict(row)))

    def shutdown(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._thread.join(timeout=timeout)

    @staticmethod
    def _normalize_timestamp(series: pd.Series) -> pd.Series:
        # Normalize mixed naive/tz-aware values to UTC for parquet stability.
        return pd.to_datetime(series, errors="coerce", utc=True)

    def _flush_key(self, symbol: str, rows: List[dict]) -> None:
        if not rows:
            return

        path = self.base_dir / symbol / "ohlc.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df_new = pd.DataFrame(rows)
        if df_new.empty:
            return

        if path.exists():
            try:
                df_old = pd.read_parquet(path)
            except Exception:
                df_old = pd.DataFrame()
            df = pd.concat([df_old, df_new], ignore_index=True) if not df_old.empty else df_new
        else:
            df = df_new

        if "timestamp" in df.columns:
            df["timestamp"] = self._normalize_timestamp(df["timestamp"])
            df = df.dropna(subset=["timestamp"])

        if "timestamp" in df.columns:
            df = df.sort_values("timestamp")

        if "ingest_ts_ns" in df.columns:
            df = df.drop_duplicates(subset=["ingest_ts_ns"], keep="last")

        tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
        df.to_parquet(tmp, engine="pyarrow", index=False)
        os.replace(tmp, path)

    def _run(self) -> None:
        buffered: Dict[str, List[dict]] = {}
        last_flush = time.monotonic()

        while True:
            should_exit = self._stop.is_set() and self._q.empty()
            if should_exit:
                break

            try:
                symbol, row = self._q.get(timeout=0.25)
                key = symbol
                buffered.setdefault(key, []).append(row)
                self._q.task_done()
            except queue.Empty:
                pass

            now = time.monotonic()
            due_time = (now - last_flush) >= self.flush_interval_sec
            due_batch = any(len(rows) >= self.flush_batch_size for rows in buffered.values())
            if due_time or due_batch:
                for key, rows in list(buffered.items()):
                    if not rows:
                        continue
                    try:
                        self._flush_key(key, rows)
                    except Exception as exc:
                        print(f"[OHLC_WRITER][ERROR] flush failed for {key}: {exc}", flush=True)
                    buffered[key] = []
                last_flush = now

        for key, rows in buffered.items():
            if rows:
                try:
                    self._flush_key(key, rows)
                except Exception as exc:
                    print(f"[OHLC_WRITER][ERROR] final flush failed for {key}: {exc}", flush=True)
