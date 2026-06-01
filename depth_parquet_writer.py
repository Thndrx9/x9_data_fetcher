import os
import queue
import threading
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd


class DepthParquetWriter:
    """
    Buffered single-writer for depth snapshots.
    """

    def __init__(self, base_dir: str, flush_batch_size: int = 200, flush_interval_sec: float = 1.0):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.flush_batch_size = max(1, int(flush_batch_size))
        self.flush_interval_sec = max(0.2, float(flush_interval_sec))

        self._q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="depth-parquet-writer", daemon=True)
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

    def _flush_symbol(self, symbol: str, rows: List[dict]) -> None:
        if not rows:
            return

        path = self.base_dir / symbol / "depth.parquet"
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

        dedupe_col = "ingest_ts_ns" if "ingest_ts_ns" in df.columns else None
        if dedupe_col:
            df = df.drop_duplicates(subset=[dedupe_col], keep="last")

        if "timestamp" in df.columns:
            df = df.sort_values("timestamp")

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
                buffered.setdefault(symbol, []).append(row)
                self._q.task_done()
            except queue.Empty:
                pass

            now = time.monotonic()
            due_time = (now - last_flush) >= self.flush_interval_sec
            due_batch = any(len(rows) >= self.flush_batch_size for rows in buffered.values())
            if due_time or due_batch:
                for sym, rows in list(buffered.items()):
                    if rows:
                        try:
                            self._flush_symbol(sym, rows)
                        except Exception as exc:
                            print(f"[DEPTH_WRITER][ERROR] flush failed for {sym}: {exc}", flush=True)
                    buffered[sym] = []
                last_flush = now

        for sym, rows in buffered.items():
            if not rows:
                continue
            try:
                self._flush_symbol(sym, rows)
            except Exception as exc:
                print(f"[DEPTH_WRITER][ERROR] final flush failed for {sym}: {exc}", flush=True)
