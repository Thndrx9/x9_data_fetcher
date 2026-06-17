"""
PostgreSQL live mirror — runs on AWS alongside the SQLite writers.

Architecture
------------
    Tick arrives
        ├── DepthParquetWriter  → SQLite depth_SYMBOL tables  (primary)
        ├── OhlcParquetWriter   → SQLite quote_SYMBOL tables  (primary)
        ├── PgWriter('depth')   → PostgreSQL depth_SYMBOL tables (live mirror)
        └── PgWriter('quote')   → PostgreSQL quote_SYMBOL tables (live mirror)

Schema matches SQLite exactly — one table per symbol:
    depth_RELIANCE, depth_TCS, depth_WIPRO ...
    quote_RELIANCE, quote_TCS, quote_WIPRO ...

Each table has 3 columns:
    timestamp  BIGINT   (exchange ms from raw_json)
    ingest_ns  BIGINT   (AWS box nanoseconds)
    raw_json   JSONB    (full tick — queryable by field)

New symbols mid-day or mid-week:
    → CREATE TABLE IF NOT EXISTS runs once on first tick
    → no restart needed

AWS Setup (run once)
--------------------
    sudo apt install postgresql postgresql-contrib -y

    sudo -u postgres psql << SQL
        CREATE USER collector WITH PASSWORD 'yourpassword';
        CREATE DATABASE market OWNER collector;
    SQL

    sudo systemctl restart postgresql

Install dependency
------------------
    pip install psycopg2-binary

Wire into your fetcher
----------------------
    DSN = "host=<aws-ip> dbname=market user=collector password=yourpassword port=5432"

    depth_pg = PgWriter(table='depth', dsn=DSN)
    quote_pg = PgWriter(table='quote', dsn=DSN)

    depth_pg.enqueue(symbol, row)
    quote_pg.enqueue(symbol, row)

    depth_pg.shutdown()
    quote_pg.shutdown()

Query from local PC (DBeaver or psql)
--------------------------------------
    -- see all tables
    SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;

    -- last 100 ticks for one symbol
    SELECT timestamp, raw_json FROM depth_RELIANCE ORDER BY timestamp DESC LIMIT 100;

    -- query a field inside raw_json
    SELECT timestamp, raw_json->>'ltp' AS ltp FROM quote_RELIANCE ORDER BY timestamp DESC LIMIT 100;

    -- fetch missed gap after local internet dropout
    SELECT timestamp, ingest_ns, raw_json
    FROM depth_RELIANCE
    WHERE timestamp BETWEEN <dropout_ms> AND <reconnect_ms>
    ORDER BY timestamp;
"""

import json
import queue
import threading
import time
from functools import lru_cache
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import psycopg2.extensions


IST = ZoneInfo("Asia/Kolkata")


def _safe_symbol(symbol: str) -> str:
    """Strip non-alphanumeric chars — safe for use in table names."""
    return "".join(c for c in symbol if c.isalnum() or c == "_")


def _ensure_table(
    conn: psycopg2.extensions.connection,
    prefix: str,
    sym: str,
    known_tables: Set[str],
) -> str:
    """
    Create depth_SYMBOL or quote_SYMBOL table on first sight of a new symbol.
    Matches SQLite schema exactly — 3 columns: timestamp, ingest_ns, raw_json.
    raw_json stored as JSONB so fields are queryable from local PC.
    """
    table = f"{prefix}_{_safe_symbol(sym)}"
    if table in known_tables:
        return table

    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            timestamp  BIGINT NOT NULL,
            ingest_ns  BIGINT,
            raw_json   JSONB  NOT NULL
        )
    """)
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table} ON {table} (timestamp)"
    )
    conn.commit()
    known_tables.add(table)
    print(f"[PG_{prefix.upper()}] new table: {table}", flush=True)
    return table


def _load_existing_tables(
    conn: psycopg2.extensions.connection,
    prefix: str,
) -> Set[str]:
    """
    On startup or restart, find which tables already exist in PG.
    Prevents redundant CREATE TABLE calls for symbols seen this week.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname='public' AND tablename LIKE %s",
        (f"{prefix}_%",),
    )
    tables = {row[0] for row in cur.fetchall()}
    if tables:
        print(
            f"[PG_{prefix.upper()}] found {len(tables)} existing tables in DB",
            flush=True,
        )
    return tables


@lru_cache(maxsize=256)
def _insert_sql(table: str) -> str:
    """Cache INSERT SQL per table — built once per symbol."""
    return f"INSERT INTO {table} (timestamp, ingest_ns, raw_json) VALUES %s"


@staticmethod
def _parse(row: dict) -> tuple:
    """
    Return (timestamp, ingest_ns, raw_json) matching SQLite column order.
    timestamp and ingest_ns stored as raw integers — no conversion.
    raw_json passed as string — PostgreSQL casts to JSONB automatically.
    """
    raw       = row.get("raw_json", "{}")
    d         = json.loads(raw) if isinstance(raw, str) else raw
    ts_ms     = d.get("timestamp")
    ingest_ns = row.get("ingest_ns")
    return (
        ts_ms,
        ingest_ns,
        raw if isinstance(raw, str) else json.dumps(raw),
    )


class PgWriter:
    """
    Mirrors one data stream (depth or quote) to PostgreSQL in real time.
    Per-symbol table layout matches SQLite writers exactly.

    - Same queue-based threading model as SQLite writers
    - Reconnects automatically if PostgreSQL goes down
    - On PG failure ticks are NOT lost — SQLite writer has them
    - Uses execute_values for bulk inserts (faster than executemany in PG)
    - New symbols mid-day or mid-week handled automatically
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
        """Keep retrying until PostgreSQL is reachable."""
        while not self._stop.is_set():
            try:
                conn = psycopg2.connect(self.dsn)
                conn.autocommit = False
                print(f"{self._tag} connected to PostgreSQL", flush=True)
                return conn
            except Exception as exc:
                print(
                    f"{self._tag}[ERROR] connection failed: {exc} — retry in 5s",
                    flush=True,
                )
                time.sleep(5)
        return None

    def _run(self) -> None:
        buffered: Dict[str, List[dict]] = {}
        last_flush = time.monotonic()

        conn = self._connect()
        if conn is None:
            return

        known_tables = _load_existing_tables(conn, self.table)

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
                conn, known_tables = self._flush(conn, buffered, known_tables)
                last_flush = now

        self._flush(conn, buffered, known_tables)
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    def _flush(
        self,
        conn: Optional[psycopg2.extensions.connection],
        buffered: Dict[str, List[dict]],
        known_tables: Set[str],
    ):
        """
        For each symbol:
          1. ensure its table exists (CREATE once, skipped after that)
          2. parse all buffered rows
          3. execute_values into that symbol's table
        One commit at the end covers all symbols.
        """
        any_rows = False

        for sym, rows in buffered.items():
            if not rows:
                continue

            try:
                table = _ensure_table(conn, self.table, sym, known_tables)
            except Exception as exc:
                print(f"{self._tag}[ERROR] ensure table failed for {sym}: {exc}", flush=True)
                buffered[sym] = []
                continue

            sql    = _insert_sql(table)
            parsed: List[tuple] = []

            for row in rows:
                try:
                    parsed.append(_parse(row))
                except Exception as exc:
                    print(f"{self._tag}[ERROR] parse failed for {sym}: {exc}", flush=True)

            if parsed:
                try:
                    cur = conn.cursor()
                    psycopg2.extras.execute_values(cur, sql, parsed)
                    any_rows = True
                except Exception as exc:
                    print(f"{self._tag}[ERROR] insert failed for {sym}: {exc}", flush=True)

            buffered[sym] = []

        if any_rows:
            try:
                conn.commit()
            except Exception as exc:
                print(f"{self._tag}[ERROR] commit failed: {exc} — reconnecting", flush=True)
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self._connect()
                known_tables = _load_existing_tables(conn, self.table) if conn else set()

        return conn, known_tables
