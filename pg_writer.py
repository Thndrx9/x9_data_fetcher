"""
PostgreSQL live mirror with auto-setup.

On first run (localhost only):
  - installs PostgreSQL if missing
  - starts the service
  - detects version automatically (no hardcoded version)
  - configures listen_addresses and pg_hba.conf using sudo tee
  - creates user and database from .env credentials
  - verifies connection

After first successful setup, all setup steps are skipped instantly on restart.

.env keys read by this module
------------------------------
    PG_HOST      = localhost
    PG_PORT      = 5432
    PG_USER      = collector
    PG_PASSWORD  = yourpassword
    PG_DBNAME    = market

Table layout (matches SQLite writers exactly — one table per symbol)
---------------------------------------------------------------------
    depth_RELIANCE, depth_TCS, depth_WIPRO ...
    quote_RELIANCE, quote_TCS, quote_WIPRO ...

    Each table: timestamp BIGINT | ingest_ns BIGINT | raw_json JSONB

Query from local PC (DBeaver / psql)
--------------------------------------
    -- list all tables
    SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;

    -- last 100 ticks for one symbol
    SELECT timestamp, raw_json FROM depth_RELIANCE ORDER BY timestamp DESC LIMIT 100;

    -- query a field inside raw_json (JSONB advantage)
    SELECT timestamp, raw_json->>'ltp' AS ltp FROM quote_RELIANCE ORDER BY timestamp DESC LIMIT 100;

    -- fetch missed gap after local internet dropout
    SELECT timestamp, ingest_ns, raw_json
    FROM depth_RELIANCE
    WHERE timestamp BETWEEN <dropout_ms> AND <reconnect_ms>
    ORDER BY timestamp;
"""

import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import psycopg2.extensions


IST = ZoneInfo("Asia/Kolkata")


# ---------------------------------------------------------------------------
# Credentials — read from environment
# ---------------------------------------------------------------------------

def _conn_params() -> dict:
    """
    Return connection params as a dict — never as a DSN string.
    DSN strings treat # as a comment character which breaks passwords like Thnd@9#
    Using keyword args bypasses all DSN string parsing entirely.
    """
    return {
        "host":     os.getenv("PG_HOST",     "localhost"),
        "port":     int(os.getenv("PG_PORT", "5432")),
        "dbname":   os.getenv("PG_DBNAME",   "market"),
        "user":     os.getenv("PG_USER",     "collector"),
        "password": os.getenv("PG_PASSWORD", ""),
    }




# ---------------------------------------------------------------------------
# Auto setup helpers
# ---------------------------------------------------------------------------

def _run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, shell=True, capture_output=True, text=True, check=check
    )


def _sudo_read(path: Path) -> str:
    """Read file using sudo cat — needed for /etc/postgresql/ files."""
    result = subprocess.run(
        ["sudo", "cat", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to read {path}: {result.stderr}")
    return result.stdout


def _sudo_write(path: Path, content: str) -> None:
    """Write file using sudo tee — needed for /etc/postgresql/ files."""
    result = subprocess.run(
        ["sudo", "tee", str(path)],
        input=content,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to write {path}: {result.stderr}")


def _is_pg_installed() -> bool:
    return shutil.which("pg_lsclusters") is not None


def _install_pg() -> None:
    print("[PG_SETUP] PostgreSQL not found — installing...", flush=True)
    _run("sudo apt-get update -qq")
    _run("sudo apt-get install -y postgresql postgresql-contrib")
    print("[PG_SETUP] PostgreSQL installed", flush=True)


def _detect_pg_version() -> Optional[str]:
    """Detect installed PG version from pg_lsclusters output."""
    result = _run("pg_lsclusters", check=False)
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts and parts[0].isdigit():
            return parts[0]
    return None


def _ensure_service_running(version: str) -> None:
    result = _run(
        f"sudo systemctl is-active postgresql@{version}-main", check=False
    )
    if result.stdout.strip() != "active":
        print(f"[PG_SETUP] Starting PostgreSQL {version}...", flush=True)
        _run(f"sudo systemctl start postgresql@{version}-main")
        time.sleep(2)
        print("[PG_SETUP] Service started", flush=True)


def _configure_pg(version: str) -> None:
    """
    Set listen_addresses = * and tune for low RAM.
    Uses sudo tee to write — script runs as ubuntu user, not root.
    Only updates lines that need changing.
    """
    conf_path = Path(f"/etc/postgresql/{version}/main/postgresql.conf")
    hba_path  = Path(f"/etc/postgresql/{version}/main/pg_hba.conf")

    # --- postgresql.conf ---
    conf_text = _sudo_read(conf_path)
    changes   = False

    settings = {
        "listen_addresses": "'*'",
        "shared_buffers":   "64MB",
        "work_mem":         "2MB",
        "max_connections":  "10",
    }

    for key, val in settings.items():
        pattern  = rf"^#?\s*{key}\s*=.*$"
        new_line = f"{key} = {val}"
        if re.search(pattern, conf_text, re.MULTILINE):
            conf_text, n = re.subn(pattern, new_line, conf_text, flags=re.MULTILINE)
            if n:
                changes = True
        else:
            conf_text += f"\n{new_line}\n"
            changes = True

    if changes:
        _sudo_write(conf_path, conf_text)
        print("[PG_SETUP] postgresql.conf updated", flush=True)

    # --- pg_hba.conf ---
    user     = os.getenv("PG_USER",   "collector")
    dbname   = os.getenv("PG_DBNAME", "market")
    hba_line = f"host    {dbname}    {user}    0.0.0.0/0    scram-sha-256\n"

    hba_text = _sudo_read(hba_path)
    if hba_line.strip() not in hba_text:
        _sudo_write(hba_path, hba_text + "\n" + hba_line)
        print("[PG_SETUP] pg_hba.conf updated", flush=True)


def _create_user_and_db() -> None:
    """
    Create PG user and database using sudo -u postgres psql.
    Avoids peer auth issue — script runs as ubuntu, not postgres.
    """
    user     = os.getenv("PG_USER",     "collector")
    password = os.getenv("PG_PASSWORD", "")
    dbname   = os.getenv("PG_DBNAME",   "market")

    # escape single quotes in password for SQL safety
    safe_pw = password.replace("'", "''")

    def _psql(sql: str) -> subprocess.CompletedProcess:
        """Run SQL as postgres superuser via sudo."""
        return subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-c", sql],
            capture_output=True, text=True,
        )

    # create user or update password if already exists
    result = _psql(f"CREATE USER {user} WITH PASSWORD '{safe_pw}'")
    if "already exists" in result.stderr:
        _psql(f"ALTER USER {user} WITH PASSWORD '{safe_pw}'")
        print(f"[PG_SETUP] user '{user}' already exists — password updated", flush=True)
    else:
        print(f"[PG_SETUP] user '{user}' created", flush=True)

    # create database if not exists
    result = _psql(f"CREATE DATABASE {dbname} OWNER {user}")
    if "already exists" in result.stderr:
        print(f"[PG_SETUP] database '{dbname}' already exists", flush=True)
    else:
        print(f"[PG_SETUP] database '{dbname}' created", flush=True)

    _psql(f"GRANT ALL PRIVILEGES ON DATABASE {dbname} TO {user}")
    print(f"[PG_SETUP] privileges granted to '{user}'", flush=True)


def _restart_pg(version: str) -> None:
    print(f"[PG_SETUP] Restarting PostgreSQL {version}...", flush=True)
    result = subprocess.run(
        f"sudo systemctl restart postgresql@{version}-main",
        shell=True, capture_output=True, text=True,
    )
    if result.returncode != 0:
        # fetch last 20 lines of journal for diagnosis
        logs = subprocess.run(
            f"sudo journalctl -u postgresql@{version}-main -n 20 --no-pager",
            shell=True, capture_output=True, text=True,
        )
        raise RuntimeError(
            f"PostgreSQL restart failed.\n"
            f"stderr: {result.stderr.strip()}\n"
            f"logs:\n{logs.stdout.strip()}"
        )
    time.sleep(2)
    print("[PG_SETUP] Restarted", flush=True)


def auto_setup() -> dict:
    """
    Full auto-setup. Returns conn_params dict for psycopg2.connect(**params).
    Skips everything instantly if PG is already running and connectable.
    Only runs setup when PG_HOST is localhost or 127.0.0.1.
    """
    host   = os.getenv("PG_HOST", "localhost")
    params = _conn_params()

    # skip setup for remote hosts
    if host not in ("localhost", "127.0.0.1"):
        print("[PG_SETUP] Remote host — skipping auto-setup", flush=True)
        return params

    # fast path — already running and connectable
    try:
        conn = psycopg2.connect(**params)
        conn.close()
        print("[PG_SETUP] PostgreSQL already running and connectable", flush=True)
        return params
    except Exception:
        pass

    print("[PG_SETUP] Starting PostgreSQL auto-setup...", flush=True)

    if not _is_pg_installed():
        _install_pg()

    version = _detect_pg_version()
    if not version:
        raise RuntimeError("[PG_SETUP] Could not detect PostgreSQL version")
    print(f"[PG_SETUP] Detected PostgreSQL version: {version}", flush=True)

    _ensure_service_running(version)
    _configure_pg(version)
    _create_user_and_db()
    _restart_pg(version)

    for attempt in range(1, 6):
        try:
            conn = psycopg2.connect(**params)
            conn.close()
            print("[PG_SETUP] Setup complete — connection verified", flush=True)
            return params
        except Exception as exc:
            print(f"[PG_SETUP] Connection attempt {attempt}/5: {exc}", flush=True)
            time.sleep(3)

    raise RuntimeError("[PG_SETUP] Setup completed but connection still failing")


# ---------------------------------------------------------------------------
# Per-symbol table helpers
# ---------------------------------------------------------------------------

def _safe_symbol(symbol: str) -> str:
    return "".join(c for c in symbol if c.isalnum() or c == "_")


def _ensure_table(
    conn: psycopg2.extensions.connection,
    prefix: str,
    sym: str,
    known_tables: Set[str],
) -> str:
    """
    Create depth_SYMBOL or quote_SYMBOL on first tick for that symbol.
    Matches SQLite schema: timestamp BIGINT | ingest_ns BIGINT | raw_json JSONB.
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
    cur = conn.cursor()
    cur.execute(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname='public' AND tablename LIKE %s",
        (f"{prefix}_%",),
    )
    tables = {row[0] for row in cur.fetchall()}
    if tables:
        print(
            f"[PG_{prefix.upper()}] found {len(tables)} existing tables",
            flush=True,
        )
    return tables


@lru_cache(maxsize=256)
def _insert_sql(table: str) -> str:
    return f"INSERT INTO {table} (timestamp, ingest_ns, raw_json) VALUES %s"


def _parse(row: dict) -> tuple:
    raw       = row.get("raw_json", "{}")
    d         = json.loads(raw) if isinstance(raw, str) else raw
    ts_ms     = d.get("timestamp")
    ingest_ns = row.get("ingest_ns")
    return (
        ts_ms,
        ingest_ns,
        raw if isinstance(raw, str) else json.dumps(raw),
    )


# ---------------------------------------------------------------------------
# Writer class
# ---------------------------------------------------------------------------

class PgWriter:
    """
    Mirrors one data stream (depth or quote) to PostgreSQL in real time.

    On first run with PG_HOST=localhost:
      - installs PostgreSQL if missing
      - creates user and database from .env
      - configures and starts the service automatically

    Per-symbol table layout matches SQLite writers exactly:
      depth_RELIANCE, depth_TCS, quote_RELIANCE ...

    Public interface:
        writer = PgWriter(table='depth')
        writer.enqueue(symbol, row)
        writer.shutdown()
    """

    def __init__(
        self,
        table: str,
        dsn: Optional[str] = None,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
    ):
        if table not in ("depth", "quote"):
            raise ValueError("table must be 'depth' or 'quote'")

        self.table  = table
        self._tag   = f"[PG_{table.upper()}]"
        self.flush_batch_size   = max(1,   int(flush_batch_size))
        self.flush_interval_sec = max(0.2, float(flush_interval_sec))

        # use conn params dict — avoids DSN string parsing issues with
        # special characters like # in passwords being treated as comments
        if dsn:
            # legacy DSN string passed directly — convert to dict via libpq
            self._params = {"dsn": dsn}
        else:
            self._params = auto_setup()

        self._q      = queue.Queue()
        self._stop   = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"pg-{table}-writer", daemon=True
        )
        self._thread.start()

    def enqueue(self, symbol: str, row: dict) -> None:
        """Mirror a tick to PostgreSQL. Non-blocking."""
        self._q.put((symbol.upper(), dict(row)))

    def shutdown(self, timeout=None) -> None:
        """Flush remaining rows, close connection, stop thread."""
        self._stop.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            print(f"{self._tag}[ERROR] shutdown timed out", flush=True)

    def _connect(self) -> Optional[psycopg2.extensions.connection]:
        while not self._stop.is_set():
            try:
                conn = psycopg2.connect(**self._params)
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