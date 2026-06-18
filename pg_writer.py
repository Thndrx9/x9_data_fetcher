"""
PostgreSQL live mirror — runs on AWS alongside the SQLite writers.

Architecture
------------
    Tick arrives
        ├── DepthParquetWriter  → SQLite depth_SYMBOL tables  (primary)
        ├── OhlcParquetWriter   → SQLite quote_SYMBOL tables  (primary)
        ├── PgWriter('depth')   → PostgreSQL depth table      (live mirror)
        └── PgWriter('quote')   → PostgreSQL quote table      (live mirror)

Schema matches SQLite writers:
    timestamp INTEGER  → BIGINT  (exchange ms from raw_json)
    ingest_ns INTEGER  → BIGINT  (AWS box nanoseconds)
    raw_json  TEXT     → JSONB   (full tick — queryable in PG)

PG adds one extra column SQLite doesn't need:
    symbol TEXT  (SQLite encodes symbol in the table name, PG uses one table)

AWS Setup (run once)
--------------------
    sudo apt install postgresql postgresql-contrib -y

    sudo -u postgres psql << SQL
        CREATE USER collector WITH PASSWORD 'yourpassword';
        CREATE DATABASE market OWNER collector;
    SQL

    # tune postgresql.conf for low RAM (mirror only, not primary)
    # shared_buffers   = 64MB
    # work_mem         = 2MB
    # max_connections  = 5
    # wal_level        = minimal
    sudo systemctl restart postgresql

    # open port 5432 in AWS security group for your local IP only

Install dependency
------------------
    pip install psycopg2-binary

Wire into your fetcher
----------------------
    DSN = "host=<aws-ip> dbname=market user=collector password=yourpassword port=5432"

    depth_sqlite = DepthParquetWriter(base_dir)
    quote_sqlite = OhlcParquetWriter(base_dir)
    depth_pg     = PgWriter(table='depth', dsn=DSN)
    quote_pg     = PgWriter(table='quote', dsn=DSN)

    # on every depth tick
    depth_sqlite.enqueue(symbol, row)
    depth_pg.enqueue(symbol, row)

    # on every quote tick
    quote_sqlite.enqueue(symbol, row)
    quote_pg.enqueue(symbol, row)

    # on shutdown
    depth_sqlite.shutdown()
    quote_sqlite.shutdown()
    depth_pg.shutdown()
    quote_pg.shutdown()

Query from local PC
-------------------
    import psycopg2
    conn = psycopg2.connect("host=<aws-ip> dbname=market user=collector password=...")
    cur  = conn.cursor()

    # live last 100 ticks for a symbol
    cur.execute(
        "SELECT timestamp, raw_json FROM depth "
        "WHERE symbol=%s ORDER BY timestamp DESC LIMIT 100",
        ('RELIANCE',)
    )

    # fetch missed gap after internet dropout
    cur.execute(
        "SELECT timestamp, ingest_ns, symbol, raw_json FROM depth "
        "WHERE symbol=%s AND timestamp BETWEEN %s AND %s ORDER BY timestamp",
        ('RELIANCE', dropout_start_ms, reconnect_ms)
    )

    # query fields inside raw_json directly (JSONB advantage)
    cur.execute(
        "SELECT timestamp, raw_json->>'ltp' AS ltp FROM quote "
        "WHERE symbol=%s ORDER BY timestamp DESC LIMIT 100",
        ('RELIANCE',)
    )
"""

import json
import queue
import threading
import time
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import psycopg2.extensions


IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
# Single table per data type — symbol is a column not a table name.
# Matches SQLite column structure exactly:
#   timestamp INTEGER  →  BIGINT  (exchange ms)
#   ingest_ns INTEGER  →  BIGINT  (AWS box nanoseconds)
#   raw_json  TEXT     →  JSONB   (binary JSON — queryable by field)
#
# PG adds symbol TEXT since it uses one shared table unlike SQLite's
# per-symbol table approach.
# ---------------------------------------------------------------------------

_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS depth (
        timestamp  BIGINT NOT NULL,
        ingest_ns  BIGINT,
        symbol     TEXT   NOT NULL,
        raw_json   JSONB  NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_depth ON depth (symbol, timestamp);

    CREATE TABLE IF NOT EXISTS quote (
        timestamp  BIGINT NOT NULL,
        ingest_ns  BIGINT,
        symbol     TEXT   NOT NULL,
        raw_json   JSONB  NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_quote ON quote (symbol, timestamp);
"""


class PgWriter:
    """
    Mirrors one data stream (depth or quote) to PostgreSQL in real time.

    - Same queue-based threading model as the SQLite writers
    - Reconnects automatically if PostgreSQL goes down
    - On PG failure, ticks are NOT lost — SQLite writer has them
    - Uses execute_values for bulk inserts (much faster than executemany in PG)
    """

    def __init__(
        self,
        table: str,
        dsn: str,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
    ):
        if table not in ("depth", "quote"):
            raise ValueError("table must be 'depth' or 'quote'")

        self.table  = table
        self.dsn    = dsn
        self._tag   = f"[PG_{table.upper()}]"
        self.flush_batch_size   = max(1,   int(flush_batch_size))
        self.flush_interval_sec = max(0.2, float(flush_interval_sec))

        self._q      = queue.Queue()
        self._stop   = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"pg-{table}-writer", daemon=True
        )
        self._thread.start()

    # ------------------------------------------------------------------
    # Public API — identical interface to SQLite writers
    # ------------------------------------------------------------------

    def enqueue(self, symbol: str, row: dict) -> None:
        """Mirror a tick to PostgreSQL. Non-blocking."""
        self._q.put((symbol.upper(), dict(row)))

    def shutdown(self, timeout=None) -> None:
        """Flush remaining rows, close connection, stop thread."""
        self._stop.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            print(f"{self._tag}[ERROR] shutdown timed out", flush=True)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _connect(self) -> Optional[psycopg2.extensions.connection]:
        """
        Keep retrying until PostgreSQL is reachable.
        Runs in the writer thread — never blocks the main collector.
        """
        while not self._stop.is_set():
            try:
                conn = psycopg2.connect(self.dsn)
                conn.autocommit = False
                cur  = conn.cursor()
                cur.execute(_CREATE_SQL)
                conn.commit()
                print(f"{self._tag} connected to PostgreSQL", flush=True)
                return conn
            except Exception as exc:
                print(
                    f"{self._tag}[ERROR] connection failed: {exc} — retry in 5s",
                    flush=True,
                )
                time.sleep(5)
        return None

    @staticmethod
    def _parse(symbol: str, row: dict) -> tuple:
        """
        Return (timestamp, ingest_ns, symbol, raw_json) for INSERT.
        Matches SQLite column structure exactly.
        timestamp and ingest_ns stored as raw integers — no conversion.
        raw_json passed as string — PostgreSQL casts to JSONB automatically.
        """
        raw       = row.get("raw_json", "{}")
        d         = json.loads(raw) if isinstance(raw, str) else raw
        ts_ms     = d.get("timestamp", row.get("timestamp"))
        ingest_ns = row.get("ingest_ns", row.get("ingest_ts_ns"))

        return (
            ts_ms,
            ingest_ns,
            symbol,
            raw if isinstance(raw, str) else json.dumps(raw),
        )

    def _run(self) -> None:
        buffered: Dict[str, List[dict]] = {}
        last_flush = time.monotonic()

        conn = self._connect()
        if conn is None:
            return

        insert_sql = (
            f"INSERT INTO {self.table} "
            f"(timestamp, ingest_ns, symbol, raw_json) VALUES %s"
        )

        while True:
            if self._stop.is_set() and self._q.empty():
                break

            try:
                symbol, row = self._q.get(timeout=0.25)
                buffered.setdefault(symbol, []).append(row)
                self._q.task_done()
            except queue.Empty:
                pass

            now       = time.monotonic()
            due_time  = (now - last_flush) >= self.flush_interval_sec
            due_batch = any(len(r) >= self.flush_batch_size for r in buffered.values())

            if due_time or due_batch:
                conn       = self._flush(conn, buffered, insert_sql)
                last_flush = now

        # drain on exit
        self._flush(conn, buffered, insert_sql)
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    def _flush(
        self,
        conn: Optional[psycopg2.extensions.connection],
        buffered: Dict[str, List[dict]],
        insert_sql: str,
    ) -> Optional[psycopg2.extensions.connection]:
        """
        Bulk insert all buffered rows across all symbols in one execute_values call.
        execute_values sends everything as one multi-row SQL statement —
        much faster than executemany for PostgreSQL.
        On failure reconnects — next flush retries automatically.
        """
        parsed: List[tuple] = []

        for sym, rows in buffered.items():
            for row in rows:
                try:
                    parsed.append(self._parse(sym, row))
                except Exception as exc:
                    print(
                        f"{self._tag}[ERROR] parse failed for {sym}: {exc}",
                        flush=True,
                    )
            buffered[sym] = []

        if not parsed:
            return conn

        try:
            cur = conn.cursor()
            psycopg2.extras.execute_values(cur, insert_sql, parsed)
            conn.commit()
        except Exception as exc:
            print(
                f"{self._tag}[ERROR] insert failed: {exc} — reconnecting",
                flush=True,
            )
            try:
                conn.close()
            except Exception:
                pass
            conn = self._connect()

        return conn