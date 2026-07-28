"""
PostgreSQL live mirror with auto-setup.

On first run (localhost only):
  - installs PostgreSQL if missing
  - starts the service
  - detects version automatically (no hardcoded version)
  - configures listen_addresses and pg_hba.conf using sudo tee
  - creates user and live/history databases from .env credentials
  - verifies connection

After first successful setup, all setup steps are skipped instantly on restart.

.env keys read by this module
------------------------------
    PG_HOST      = localhost
    PG_PORT      = 5432
    PG_USER      = collector
    PG_PASSWORD  = yourpassword
    PG_DBNAME    = market
    PG_HDBNAME   = market_history

Table layout (one table per symbol; quote/depth use typed columns, NOT
JSONB — real-data measurements showed ~85%/~72.8% size reduction vs. a
JSONB blob. 'daily' candle-history tables are unchanged/legacy JSONB.)
---------------------------------------------------------------------
    depth_RELIANCE, depth_TCS, depth_WIPRO ...
        timestamp, ingest_ns, ltp, volume, last_quantity, oi,
        upper_circuit, lower_circuit,
        buy0_price, buy0_qty, buy0_orders, ... buy4_*,
        sell0_price, sell0_qty, sell0_orders, ... sell4_*

    quote_RELIANCE, quote_TCS, quote_WIPRO ...
        timestamp, ingest_ns, ltp, ltt, volume, open, high, low, close,
        last_quantity, oi, upper_circuit, lower_circuit

    (symbol/exchange/mode are NOT stored as columns — 100% redundant
    with the table name itself, since each table only ever holds one
    symbol's own ticks.)

Query from local PC (DBeaver / psql)
--------------------------------------
    -- list all tables
    SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;

    -- last 100 ticks for one symbol
    SELECT * FROM depth_RELIANCE ORDER BY timestamp DESC LIMIT 100;

    -- top-of-book only
    SELECT timestamp, ltp, buy0_price, buy0_qty, sell0_price, sell0_qty
    FROM depth_RELIANCE ORDER BY timestamp DESC LIMIT 100;

    -- fetch missed gap after local internet dropout
    SELECT *
    FROM depth_RELIANCE
    WHERE timestamp BETWEEN <dropout_ms> AND <reconnect_ms>
    ORDER BY timestamp;
"""

import json
import os
import queue
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from x9_data_fetcher.market_time import MARKET_OPEN, is_trading_day, now_kolkata, tz_kolkata


IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Console coordination — prevents background writer-thread prints from
# garbling a live progress line
# ---------------------------------------------------------------------------
#
# backfill_manager._progress_write draws a single, live-updating line using
# "\r...\033[K" with NO trailing newline. PgWriter's background flush
# thread (table creation, insert errors, etc.) runs on a completely
# separate thread and prints normally. If that print fires while a
# progress line is open, it lands mid-line with no newline in between —
# e.g. "...failed=0[PG_DAILY] new table: daily_reliance" all smashed onto
# one line, exactly as seen in production.
#
# _console_lock + _progress_open are shared module state: backfill_manager
# marks a progress line "open" while it's being drawn, and every print
# that could plausibly fire from a different thread goes through
# _safe_print(), which closes any open line with a newline first.

_console_lock = threading.Lock()
_progress_open = False   # True while a \r-based progress line has no trailing \n yet


def _safe_print(text: str) -> None:
    """Thread-safe print — closes any open \\r progress line first."""
    global _progress_open
    with _console_lock:
        if _progress_open:
            sys.stdout.write("\n")
            _progress_open = False
        print(text, flush=True)



# ---------------------------------------------------------------------------
# Credentials — read from environment
# ---------------------------------------------------------------------------

def _history_dbname() -> str:
    return os.getenv("PG_HDBNAME", "market_history").strip() or "market_history"


def _configured_dbnames() -> List[str]:
    dbnames = [
        os.getenv("PG_DBNAME", "market").strip() or "market",
        _history_dbname(),
    ]
    out: List[str] = []
    for dbname in dbnames:
        if dbname not in out:
            out.append(dbname)
    return out


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _conn_params(dbname: Optional[str] = None, statement_timeout_sec: Optional[int] = None) -> dict:
    """
    Return connection params as a dict — never as a DSN string.
    DSN strings treat # as a comment character which breaks passwords like Thnd@9#
    Using keyword args bypasses all DSN string parsing entirely.

    statement_timeout_sec lets a caller override the default tight timeout.
    The default (PG_STATEMENT_TIMEOUT_SEC, 10s) is sized for the live tick
    write path, where a normal INSERT/COMMIT takes milliseconds — 10s is
    already generous there, and the whole point is to bound how long a
    stuck writer thread can block shutdown. A retention purge is a
    different kind of operation: a DELETE spanning weeks of backlog across
    hundreds of thousands of rows can legitimately take well over 10s, so
    it needs its own, much longer budget rather than inheriting the live
    write path's tight limit.
    """
    timeout_sec = (
        statement_timeout_sec
        if statement_timeout_sec is not None
        else int(os.getenv("PG_STATEMENT_TIMEOUT_SEC", "10"))
    )
    return {
        "host":     os.getenv("PG_HOST",     "localhost"),
        "port":     int(os.getenv("PG_PORT", "5432")),
        "dbname":   dbname or os.getenv("PG_DBNAME", "market"),
        "user":     os.getenv("PG_USER",     "collector"),
        "password": os.getenv("PG_PASSWORD", ""),
        # Bound TCP connect time so a dead/unreachable host fails fast
        # instead of hanging on the OS-level connect() syscall.
        "connect_timeout": int(os.getenv("PG_CONNECT_TIMEOUT_SEC", "10")),
        # Bound how long any single query (INSERT/COMMIT/etc.) can run
        # server-side. Without this, a stalled network path or a wedged
        # server can leave the writer thread blocked inside execute()/
        # commit() with no way to notice _stop was set — which is what
        # makes shutdown() hang forever on thread.join().
        "options": f"-c statement_timeout={timeout_sec * 1000}",
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
    hba_text = _sudo_read(hba_path)
    hba_lines = [
        f"host    {dbname}    {user}    0.0.0.0/0    scram-sha-256\n"
        for dbname in _configured_dbnames()
    ]
    missing = [line for line in hba_lines if line.strip() not in hba_text]
    if missing:
        _sudo_write(hba_path, hba_text + "\n" + "".join(missing))
        print("[PG_SETUP] pg_hba.conf updated", flush=True)


def _create_user_and_db() -> None:
    """
    Create PG user and database using sudo -u postgres psql.
    Avoids peer auth issue — script runs as ubuntu, not postgres.
    """
    user     = os.getenv("PG_USER",     "collector")
    password = os.getenv("PG_PASSWORD", "")
    dbnames  = _configured_dbnames()

    # escape single quotes in password for SQL safety
    safe_pw = password.replace("'", "''")

    def _psql(sql: str) -> subprocess.CompletedProcess:
        """Run SQL as postgres superuser via sudo."""
        return subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-c", sql],
            capture_output=True, text=True,
        )

    # create user or update password if already exists
    safe_user = _quote_ident(user)

    result = _psql(f"CREATE USER {safe_user} WITH PASSWORD '{safe_pw}'")
    if "already exists" in result.stderr:
        _psql(f"ALTER USER {safe_user} WITH PASSWORD '{safe_pw}'")
        print(f"[PG_SETUP] user '{user}' already exists — password updated", flush=True)
    else:
        print(f"[PG_SETUP] user '{user}' created", flush=True)

    for dbname in dbnames:
        safe_dbname = _quote_ident(dbname)
        result = _psql(f"CREATE DATABASE {safe_dbname} OWNER {safe_user}")
        if "already exists" in result.stderr:
            print(f"[PG_SETUP] database '{dbname}' already exists", flush=True)
        else:
            print(f"[PG_SETUP] database '{dbname}' created", flush=True)

        _psql(f"GRANT ALL PRIVILEGES ON DATABASE {safe_dbname} TO {safe_user}")
        print(f"[PG_SETUP] privileges granted on '{dbname}' to '{user}'", flush=True)


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


def auto_setup(dbname: Optional[str] = None) -> dict:
    """
    Full auto-setup. Returns conn_params dict for psycopg2.connect(**params).
    Skips everything instantly if PG is already running and connectable.
    Only runs setup when PG_HOST is localhost or 127.0.0.1.
    """
    host   = os.getenv("PG_HOST", "localhost")
    params = _conn_params(dbname)

    # skip setup for remote hosts
    if host not in ("localhost", "127.0.0.1"):
        print("[PG_SETUP] Remote host — skipping auto-setup", flush=True)
        return params

    # fast path — already running and connectable
    try:
        for check_dbname in _configured_dbnames():
            conn = psycopg2.connect(**_conn_params(check_dbname))
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
            for check_dbname in _configured_dbnames():
                conn = psycopg2.connect(**_conn_params(check_dbname))
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
    dedup: bool = False,
) -> str:
    """
    Create depth_SYMBOL or quote_SYMBOL on first tick for that symbol.

    quote/depth use typed columns (not JSONB) — real-data measurements
    showed ~85% (quote) and ~72.8% (depth) size reduction vs. the JSONB
    blob approach. symbol/exchange/mode are deliberately NOT stored as
    columns: they're 100% redundant with the table name itself (this
    table only ever holds SYM's own ticks), so storing them per-row would
    be pure waste. oi/upper_circuit/lower_circuit ARE kept in both quote
    and depth tables on purpose — checked earlier and found NOT reliably
    duplicated between the two (~71.6% match, not the clean 100% we
    confirmed for the embedded depth object), so dropping them from
    either table risks losing real information for a few bytes' saving.

    'daily' (candle history) intentionally stays on the old JSONB schema —
    it wasn't part of the measured/agreed typed-columns scope, and touching
    it isn't worth the added risk for a table that isn't the size problem.
    """
    table = f"{prefix}_{_safe_symbol(sym)}".lower()
    if table in known_tables:
        return table

    cur = conn.cursor()

    if prefix == "quote":
        col_defs = ",\n                ".join(f"{name} {typ}" for name, typ in _QUOTE_COLUMN_DEFS)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n                {col_defs}\n            )")
    elif prefix == "depth":
        col_defs = ",\n                ".join(f"{name} {typ}" for name, typ in _DEPTH_COLUMN_DEFS)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n                {col_defs}\n            )")
    else:
        # 'daily' and anything else: unchanged legacy JSONB schema
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
    if dedup:
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uidx_{table}_ts "
            f"ON {table} (timestamp)"
        )
    conn.commit()
    known_tables.add(table)
    _safe_print(f"[PG_{prefix.upper()}] new table: {table}")
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

    # Self-heal any table whose schema predates the current column set
    # (e.g. created under an older code version) — see _reconcile_columns
    # for why this can't just happen lazily inside _ensure_table. Only
    # applies to the typed-column tables; 'daily' and anything else stays
    # on the legacy JSONB schema untouched.
    if tables and prefix in ("quote", "depth"):
        try:
            for table in tables:
                _reconcile_columns(conn, table, prefix)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _safe_print(
                f"[PG_{prefix.upper()}][ERROR] schema reconciliation failed: {exc} "
                f"— tables may still be missing columns from an older schema"
            )

    if tables:
        _safe_print(f"[PG_{prefix.upper()}] found {len(tables)} existing tables")
    return tables


# Single source of truth for quote/depth schema: (column_name, sql_type).
# CREATE TABLE, the column-repair ALTER statements below, AND the INSERT
# column list (_QUOTE_COLUMNS/_DEPTH_COLUMNS, derived) all come from these
# two tuples now, instead of three separately-hand-maintained copies that
# could silently drift out of sync with each other.
#
# Order matters and must exactly match _parse()'s positional tuple output
# for "quote"/"depth" above — execute_values() supplies values purely by
# position, with no column-name matching at all.
_QUOTE_COLUMN_DEFS = (
    ("timestamp", "BIGINT NOT NULL"),
    ("ingest_ns", "BIGINT"),
    ("ltp", "DOUBLE PRECISION"),
    ("ltt", "BIGINT"),
    ("volume", "BIGINT"),
    ("open", "DOUBLE PRECISION"),
    ("high", "DOUBLE PRECISION"),
    ("low", "DOUBLE PRECISION"),
    ("close", "DOUBLE PRECISION"),
    ("last_quantity", "BIGINT"),
    ("oi", "BIGINT"),
    ("upper_circuit", "DOUBLE PRECISION"),
    ("lower_circuit", "DOUBLE PRECISION"),
)

_DEPTH_COLUMN_DEFS = (
    ("timestamp", "BIGINT NOT NULL"),
    ("ingest_ns", "BIGINT"),
    ("ltp", "DOUBLE PRECISION"),
    ("volume", "BIGINT"),
    ("last_quantity", "BIGINT"),
    ("oi", "BIGINT"),
    ("upper_circuit", "DOUBLE PRECISION"),
    ("lower_circuit", "DOUBLE PRECISION"),
) + tuple(
    (f"{side}{lvl}_{field}", "DOUBLE PRECISION" if field == "price" else "BIGINT")
    for side in ("buy", "sell")
    for lvl in range(5)
    for field in ("price", "qty", "orders")
)

_QUOTE_COLUMNS = tuple(name for name, _ in _QUOTE_COLUMN_DEFS)
_DEPTH_COLUMNS = tuple(name for name, _ in _DEPTH_COLUMN_DEFS)


def _reconcile_columns(
    conn: psycopg2.extensions.connection, table: str, prefix: str
) -> None:
    """
    Self-heal a table's schema against a stale/partial CREATE. Needed
    because 'CREATE TABLE IF NOT EXISTS' is a no-op on a table that
    already exists — a table created under an older code version (fewer
    columns, or the pre-migration JSONB-only schema) would otherwise keep
    silently failing every insert forever ("column X does not exist"),
    since nothing ever goes back and patches it.

    ADD COLUMN IF NOT EXISTS is idempotent and cheap when nothing needs
    adding, so this is safe to call unconditionally. NOT NULL is
    deliberately stripped for the ALTER form — adding a NOT NULL column
    with no DEFAULT fails outright on a table that already has rows, and
    'timestamp' (the only NOT NULL column here) should already be present
    on any real table anyway; this is just a defensive guard against that
    one failure mode, not an expected case.

    Also handles a second, distinct legacy shape: a table created before
    the JSONB->typed-columns migration ever touched it at all, which still
    has the old raw_json JSONB NOT NULL column sitting alongside the newly
    added typed columns. The typed-column insert path never writes
    raw_json, so its NOT NULL constraint would block every insert forever
    the same way the missing typed columns did — confirmed live (two
    symbols hit this exact case after the missing-columns fix went out).
    Only the NOT NULL constraint is dropped, never the column itself or
    its data — any already-populated raw_json values for old rows are
    left completely alone.
    """
    defs = _QUOTE_COLUMN_DEFS if prefix == "quote" else _DEPTH_COLUMN_DEFS
    cur = conn.cursor()
    for name, sql_type in defs:
        alter_type = sql_type.replace(" NOT NULL", "")
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {alter_type}")

    cur.execute(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = 'raw_json'",
        (table,),
    )
    row = cur.fetchone()
    if row is not None and row[0] == "NO":
        cur.execute(f"ALTER TABLE {table} ALTER COLUMN raw_json DROP NOT NULL")


@lru_cache(maxsize=512)
def _insert_sql(table: str, prefix: str, dedup: bool = False) -> str:
    if prefix == "quote":
        columns = _QUOTE_COLUMNS
    elif prefix == "depth":
        columns = _DEPTH_COLUMNS
    else:
        columns = ("timestamp", "ingest_ns", "raw_json")

    col_list = ", ".join(columns)
    base = f"INSERT INTO {table} ({col_list}) VALUES %s"
    return base + " ON CONFLICT (timestamp) DO NOTHING" if dedup else base


# ---------------------------------------------------------------------------
# Retention — delete data older than the last N trading days
# ---------------------------------------------------------------------------
#
# Meant to be called once per trading day, right at market close, from
# start_data.py's daily loop (see purge_old_data()).
#
# Cutoff definition: "older than the previous 3 trading days" = keep
# everything from session-open (09:15 IST) of the 3rd-most-recent trading
# day (inclusive) onward; delete anything before that. Today counts as one
# of the 3 kept days once its session has started.
#
# Example, run at market close on a Wednesday with no holidays:
#     kept days = Mon, Tue, Wed (today) → cutoff = Monday 09:15 IST
#     deleted   = everything with timestamp < Monday 09:15 IST

def _last_n_trading_days(reference, n: int) -> List:
    result = []
    cursor = reference.date()
    for _ in range(n * 5):
        if is_trading_day(cursor):
            result.append(cursor)
            if len(result) == n:
                break
        cursor -= timedelta(days=1)
    return list(reversed(result))


LIVE_TICK_RETENTION_TRADING_DAYS = 3  # default for purge_old_data's keep_trading_days;
# also the ceiling backfill_manager.min_days must not exceed (see BackfillManager
# guard) — if backfill looks back further than PG actually retains, gap-detection
# would silently mistake purged history for missing history.


def _cutoff_ms(now=None, keep_trading_days: int = LIVE_TICK_RETENTION_TRADING_DAYS) -> int:
    now = now or now_kolkata()
    kept_days = _last_n_trading_days(now, keep_trading_days)
    oldest_kept_day = kept_days[0]
    cutoff_dt = datetime.combine(oldest_kept_day, MARKET_OPEN, tzinfo=tz_kolkata)
    return int(cutoff_dt.timestamp() * 1000)


def _purge_db(dbname: str, table_patterns: List[str], cutoff_ms: int, label: str) -> None:
    tag = f"[RETENTION:{dbname}]"
    # Purge-specific timeout: deletes/vacuums across a large backlog can
    # legitimately take minutes, especially on a table that's never been
    # purged before. PG_PURGE_STATEMENT_TIMEOUT_SEC defaults to 300s (5 min)
    # -- far longer than the 10s used for live tick writes, since a slow
    # purge just delays that day's cleanup, it doesn't risk hanging the
    # live process the way a stuck write-path connection would.
    purge_timeout_sec = int(os.getenv("PG_PURGE_STATEMENT_TIMEOUT_SEC", "300"))
    try:
        conn = psycopg2.connect(**_conn_params(dbname, statement_timeout_sec=purge_timeout_sec))
    except Exception as exc:
        _safe_print(f"{tag}[ERROR] connect failed: {exc}")
        return

    try:
        conn.autocommit = False
        cur = conn.cursor()
        where_clause = " OR ".join(["tablename LIKE %s"] * len(table_patterns))
        cur.execute(
            f"SELECT tablename FROM pg_tables "
            f"WHERE schemaname='public' AND ({where_clause})",
            table_patterns,
        )
        tables = [row[0] for row in cur.fetchall()]

        if not tables:
            _safe_print(f"{tag} no {label} tables found — nothing to purge")
            return

        total_deleted = 0
        tables_affected = 0
        for table in tables:
            try:
                cur.execute(f"DELETE FROM {table} WHERE timestamp < %s", (cutoff_ms,))
                deleted = cur.rowcount
                if deleted:
                    tables_affected += 1
                    total_deleted += deleted
            except Exception as exc:
                _safe_print(f"{tag}[ERROR] delete failed for {table}: {exc}")
                conn.rollback()
                cur = conn.cursor()  # cursor is dead after rollback — get a fresh one
                continue

        conn.commit()
        _safe_print(
            f"{tag} {label}: purged {total_deleted} row(s) across "
            f"{tables_affected}/{len(tables)} table(s) (cutoff={cutoff_ms})"
        )

        # VACUUM reclaims disk space after large deletes, but can't run
        # inside a transaction block — needs its own autocommit connection.
        if total_deleted:
            conn.autocommit = True
            vac_cur = conn.cursor()
            for table in tables:
                try:
                    vac_cur.execute(f"VACUUM {table}")
                except Exception as exc:
                    _safe_print(f"{tag}[WARN] vacuum failed for {table}: {exc}")
            _safe_print(f"{tag} {label}: vacuum complete")

    finally:
        try:
            conn.close()
        except Exception:
            pass


def purge_old_data(
    keep_trading_days: int = LIVE_TICK_RETENTION_TRADING_DAYS,
    keep_daily_trading_days: Optional[int] = None,
    now=None,
) -> None:
    """
    Delete old rows from every configured database (live + history):
      - quote_*/depth_* (tick data)  → keep last `keep_trading_days` trading days
      - daily_*         (EOD candle) → keep last `keep_daily_trading_days` trading days

    Two separate retention windows because daily_* holds one tiny row per
    symbol per day — cheap to keep much longer, and useful for historical
    close-price reference — while quote_/depth_ hold every tick and would
    grow unbounded if kept anywhere near that long.

    `keep_daily_trading_days`, if not given explicitly, reads the SAME
    X9_DAILY_BACKFILL_DAYS env var (default 30) that start_data.py's
    startup daily-candle backfill uses. One shared knob — so retention and
    the backfill catch-up window can't silently drift apart again (they
    did: retention kept 30 trading days while backfill only ever caught up
    the last 3, so any downtime beyond 3 days left a permanent hole).

    Safe to call even if PostgreSQL isn't configured/reachable — logs and
    returns rather than raising, so it can't take down the daily loop.
    """
    if keep_daily_trading_days is None:
        keep_daily_trading_days = max(
            1, int(os.getenv("X9_DAILY_BACKFILL_DAYS", "30").strip() or "30")
        )

    start = time.monotonic()
    now = now or now_kolkata()
    tick_cutoff_ms  = _cutoff_ms(now, keep_trading_days)
    daily_cutoff_ms = _cutoff_ms(now, keep_daily_trading_days)
    _safe_print(
        f"[RETENTION] starting purge — tick data: last {keep_trading_days} "
        f"trading day(s) (cutoff={tick_cutoff_ms}), daily candles: last "
        f"{keep_daily_trading_days} trading day(s) (cutoff={daily_cutoff_ms})"
    )
    for dbname in _configured_dbnames():
        _purge_db(dbname, ["quote_%", "depth_%"], tick_cutoff_ms, "tick data")
        _purge_db(dbname, ["daily_%"], daily_cutoff_ms, "daily candles")
    _safe_print(f"[RETENTION] done in {time.monotonic() - start:.1f}s")


# ---------------------------------------------------------------------------
# Live-DB read helpers — historical lookback against the LIVE tick database
# (PG_DBNAME, default 'market'), which mirrors what used to be scattered
# across local market_*.db SQLite files written by data_fetcher.py's
# PgWriter. These replace gap_detector.py's SQLite-file scanning now that
# hourly rollover deletes each local file shortly after archiving it —
# PG already retains the same multi-day window (LIVE_TICK_RETENTION_TRADING_DAYS)
# that backfill's lookback needs, so it's the new source of truth instead.
#
# Callers open one connection per run (see BackfillManager._run_once) and
# pass it in, rather than each helper opening its own — same convention as
# _history_connection/_filter_gaps_already_in_history's history-DB helpers.
# ---------------------------------------------------------------------------

def live_connection(dbname: Optional[str] = None):
    """Open a connection to the live tick database (default PG_DBNAME)."""
    conn = psycopg2.connect(**_conn_params(dbname))
    conn.autocommit = True
    return conn


def live_quote_tables(conn) -> List[str]:
    """Every quote_* table currently in the live DB."""
    cur = conn.cursor()
    cur.execute(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname='public' AND tablename LIKE 'quote_%'"
    )
    return [row[0] for row in cur.fetchall()]


def live_latest_quote_timestamp(conn) -> Optional[int]:
    """
    MAX(timestamp), in ms, across every quote_* table in the live DB.

    Replaces the old latest_collected_timestamp() SQLite scan — same
    "most recent tick collected, across all symbols" semantics used to
    find the pre-startup resume point.
    """
    latest: Optional[int] = None
    for table in live_quote_tables(conn):
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(timestamp) FROM {table}")
            row = cur.fetchone()
            ts_ms = row[0] if row else None
            if ts_ms is not None and (latest is None or ts_ms > latest):
                latest = ts_ms
        except Exception as exc:
            _safe_print(f"[LIVE_READ][WARN] failed reading {table}: {exc}")
    return latest


def live_timestamps_for_range(conn, table: str, start_ms: int, end_ms: int) -> List[int]:
    """
    Every raw tick timestamp (ms) in `table` within [start_ms, end_ms).

    Replaces gap_detector._get_timestamps_for_day's multi-file SQLite scan
    for one symbol/day. `table` is matched case-insensitively against the
    lowercase name PG actually stores (_ensure_table always lowercases).
    """
    table = table.lower()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s",
        (table,),
    )
    if cur.fetchone() is None:
        return []
    cur.execute(
        f"SELECT timestamp FROM {table} WHERE timestamp >= %s AND timestamp < %s",
        (start_ms, end_ms),
    )
    return sorted({row[0] for row in cur.fetchall() if row[0] is not None})


def live_any_row_in_range(conn, start_ms: int, end_ms: int) -> bool:
    """
    True if any quote_* table in the live DB has at least one row in
    [start_ms, end_ms). Replaces gap_detector._day_has_any_data's
    "confirm at least one row landed somewhere" sanity check.
    """
    for table in live_quote_tables(conn):
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT 1 FROM {table} WHERE timestamp >= %s AND timestamp < %s LIMIT 1",
                (start_ms, end_ms),
            )
            if cur.fetchone():
                return True
        except Exception as exc:
            _safe_print(f"[LIVE_READ][WARN] failed reading {table}: {exc}")
    return False


def _parse(row: dict, prefix: str) -> tuple:
    raw = row.get("raw_json", "{}")
    d = json.loads(raw) if isinstance(raw, str) else (raw or {})
    ingest_ns = row.get("ingest_ns")
    ts_ms = d.get("timestamp")

    if prefix == "quote":
        return (
            ts_ms, ingest_ns,
            d.get("ltp"), d.get("ltt"), d.get("volume"),
            d.get("open"), d.get("high"), d.get("low"), d.get("close"),
            d.get("last_quantity"), d.get("oi"),
            d.get("upper_circuit"), d.get("lower_circuit"),
        )

    if prefix == "depth":
        depth = d.get("depth") or {}
        buy = depth.get("buy") or []
        sell = depth.get("sell") or []

        def level(levels, i, field):
            # tolerate thin books with fewer than 5 real levels — store
            # NULL rather than erroring or fabricating a fake 0
            if i < len(levels) and isinstance(levels[i], dict):
                return levels[i].get(field)
            return None

        level_values = []
        for side_levels in (buy, sell):
            for i in range(5):
                level_values.append(level(side_levels, i, "price"))
                level_values.append(level(side_levels, i, "quantity"))
                level_values.append(level(side_levels, i, "orders"))

        return (
            ts_ms, ingest_ns,
            d.get("ltp"), d.get("volume"), d.get("last_quantity"), d.get("oi"),
            d.get("upper_circuit"), d.get("lower_circuit"),
            *level_values,
        )

    # legacy path ('daily' and anything else): unchanged JSONB blob
    return (
        ts_ms,
        ingest_ns,
        raw if isinstance(raw, str) else json.dumps(raw),
    )


# Retrying failed flushes is only safe if the retry buffer can't grow
# without bound — if PG is down for an extended period, ticks keep
# arriving from the websocket faster than they can be retried, and an
# uncapped buffer would eventually exhaust memory. Cap it per symbol and
# drop the OLDEST rows once the cap is hit (keep the freshest data,
# since for live market data recency matters more than completeness of
# an already-large gap — a big gap gets fully recovered later via
# BackfillManager/DailyCloseManager anyway).
_MAX_RETRY_ROWS_PER_SYMBOL = int(os.getenv("PG_MAX_RETRY_ROWS_PER_SYMBOL", "20000"))


def _cap_retry_rows(symbol: str, rows: List[dict], tag: str) -> List[dict]:
    if len(rows) <= _MAX_RETRY_ROWS_PER_SYMBOL:
        return rows
    dropped = len(rows) - _MAX_RETRY_ROWS_PER_SYMBOL
    _safe_print(
        f"{tag}[WARN] retry buffer for {symbol} exceeded "
        f"{_MAX_RETRY_ROWS_PER_SYMBOL} rows — dropping {dropped} oldest "
        f"row(s) (PG likely down for a while; full history still safe in "
        f"SQLite/Parquet and recoverable via backfill)"
    )
    return rows[-_MAX_RETRY_ROWS_PER_SYMBOL:]


# ---------------------------------------------------------------------------
# Shutdown spillover — a local SQLite file (same storage style already used
# by the depth/quote writers) that catches rows still sitting in memory if
# the process is killed before a final PG flush can succeed. Loaded back in
# and retried against PG the next time this writer starts up.
# ---------------------------------------------------------------------------

_SPILLOVER_DIR = os.getenv("X9_PG_SPILLOVER_DIR", ".")


def _spillover_path(table: str) -> Path:
    return Path(_SPILLOVER_DIR) / f"pg_{table}_spillover.db"


def _spillover_write(table: str, buffered: Dict[str, List[dict]], tag: str) -> None:
    pending = {sym: rows for sym, rows in buffered.items() if rows}
    if not pending:
        return
    path = _spillover_path(table)
    try:
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS spillover ("
            " symbol TEXT, timestamp TEXT, ingest_ns INTEGER, raw_json TEXT)"
        )
        rows_to_write = [
            (sym, row.get("timestamp"), row.get("ingest_ns"), row.get("raw_json"))
            for sym, rows in pending.items()
            for row in rows
        ]
        conn.executemany(
            "INSERT INTO spillover (symbol, timestamp, ingest_ns, raw_json) "
            "VALUES (?, ?, ?, ?)",
            rows_to_write,
        )
        conn.commit()
        conn.close()
        _safe_print(
            f"{tag}[WARN] shutdown with {len(rows_to_write)} unflushed row(s) "
            f"— saved to {path} for retry on next startup"
        )
    except Exception as exc:
        _safe_print(
            f"{tag}[ERROR] failed to save {sum(len(r) for r in pending.values())} "
            f"unflushed row(s) to spillover file: {exc} — these rows are lost"
        )


def _spillover_load(table: str, tag: str) -> Dict[str, List[dict]]:
    path = _spillover_path(table)
    if not path.exists():
        return {}
    recovered: Dict[str, List[dict]] = {}
    try:
        conn = sqlite3.connect(path)
        cur = conn.execute("SELECT symbol, timestamp, ingest_ns, raw_json FROM spillover")
        for symbol, timestamp, ingest_ns, raw_json in cur.fetchall():
            recovered.setdefault(symbol, []).append(
                {"timestamp": timestamp, "ingest_ns": ingest_ns, "raw_json": raw_json}
            )
        conn.close()
        # consumed — remove so we don't replay the same rows again on a
        # future startup if this run also fails to flush them
        path.unlink()
        if recovered:
            total = sum(len(v) for v in recovered.values())
            _safe_print(
                f"{tag} recovered {total} row(s) from previous shutdown "
                f"({path.name}) — retrying against PG"
            )
    except Exception as exc:
        _safe_print(f"{tag}[ERROR] failed to load spillover file {path}: {exc}")
    return recovered


# ---------------------------------------------------------------------------
# Writer class
# ---------------------------------------------------------------------------

class PgWriter:
    """
    Mirrors one data stream (depth or quote) to PostgreSQL in real time.

    On first run with PG_HOST=localhost:
      - installs PostgreSQL if missing
      - creates user and live/history databases from .env
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
        dbname: Optional[str] = None,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
        dedup_on_timestamp: bool = False,
    ):
        if table not in ("depth", "quote", "daily"):
            raise ValueError("table must be 'depth', 'quote', or 'daily'")

        self.table  = table
        self._tag   = f"[PG_{table.upper()}]"
        self._dedup = dedup_on_timestamp
        self.flush_batch_size   = max(1,   int(flush_batch_size))
        self.flush_interval_sec = max(0.2, float(flush_interval_sec))

        # use conn params dict — avoids DSN string parsing issues with
        # special characters like # in passwords being treated as comments
        if dsn:
            # legacy DSN string passed directly — convert to dict via libpq
            self._params = {"dsn": dsn}
        else:
            self._params = auto_setup(dbname=dbname)

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
            _safe_print(f"{self._tag}[ERROR] shutdown timed out")

    def _connect(self) -> Optional[psycopg2.extensions.connection]:
        while not self._stop.is_set():
            try:
                conn = psycopg2.connect(**self._params)
                conn.autocommit = False
                _safe_print(f"{self._tag} connected to PostgreSQL")
                return conn
            except Exception as exc:
                _safe_print(f"{self._tag}[ERROR] connection failed: {exc} — retry in 5s")
                time.sleep(5)
        return None

    def _run(self) -> None:
        buffered: Dict[str, List[dict]] = {}
        last_flush = time.monotonic()

        conn = self._connect()
        if conn is None:
            return

        known_tables = _load_existing_tables(conn, self.table)

        # recover anything left over from a previous run that got killed
        # before it could flush — merge in so the next flush cycle retries it
        for symbol, rows in _spillover_load(self.table, self._tag).items():
            buffered.setdefault(symbol, []).extend(rows)

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

        # final flush may itself have failed (e.g. PG unreachable at the
        # exact moment of shutdown) — anything still buffered would
        # otherwise be silently lost when this thread exits
        _spillover_write(self.table, buffered, self._tag)

    def _reconnect_after_failure(
        self,
        conn: Optional[psycopg2.extensions.connection],
        buffered: Dict[str, List[dict]],
        pending_commit: Dict[str, List[dict]],
    ):
        """Close a connection that's beyond recovery (e.g. a savepoint
        rollback itself failed, or the final commit failed) and reconnect.
        Anything in `pending_commit` was execute_values'd but never
        committed, so it goes back into `buffered` for the next flush.
        """
        try:
            conn.close()
        except Exception:
            pass
        conn = self._connect()
        known_tables = _load_existing_tables(conn, self.table) if conn else set()
        for sym, rows in pending_commit.items():
            buffered[sym] = _cap_retry_rows(sym, rows + buffered.get(sym, []), self._tag)
        return conn, known_tables

    def _flush(
        self,
        conn: Optional[psycopg2.extensions.connection],
        buffered: Dict[str, List[dict]],
        known_tables: Set[str],
    ):
        any_rows = False
        # rows that were execute_values'd this round but not yet committed —
        # if the commit fails, these get put back in `buffered` so the next
        # flush retries them instead of the data being silently lost.
        pending_commit: Dict[str, List[dict]] = {}

        for sym, rows in list(buffered.items()):
            if not rows:
                continue

            # Each symbol gets its own savepoint around ensure_table and
            # around the insert. Without this, a failure on one symbol
            # aborts the *entire* shared transaction in Postgres — every
            # symbol processed afterward in this flush cycle fails too
            # (cascading "current transaction is aborted" errors) — and the
            # final conn.commit() below does not raise when the transaction
            # is already aborted (Postgres treats COMMIT-while-aborted as a
            # ROLLBACK), so symbols that succeeded earlier in this same
            # flush get silently discarded with no error ever logged for
            # them. Rolling back to a per-symbol savepoint clears the
            # aborted state without touching anyone else's work.
            #
            # NOTE: _ensure_table() calls conn.commit() itself when it
            # creates a brand-new table, which ends the transaction and
            # destroys any savepoint taken before it runs. So the
            # ensure_table step and the insert step each need their own
            # fresh savepoint — they can't share one.
            try:
                conn.cursor().execute("SAVEPOINT sp_ensure_table")
                table = _ensure_table(conn, self.table, sym, known_tables, self._dedup)
            except Exception as exc:
                _safe_print(
                    f"{self._tag}[ERROR] ensure table failed for {sym}: {exc} "
                    f"— {len(rows)} row(s) kept for retry next flush"
                )
                buffered[sym] = _cap_retry_rows(sym, rows, self._tag)
                try:
                    conn.cursor().execute("ROLLBACK TO SAVEPOINT sp_ensure_table")
                except Exception as rb_exc:
                    # connection itself is unusable — abandon this flush
                    # cycle and reconnect, same as a commit failure below
                    _safe_print(
                        f"{self._tag}[ERROR] savepoint rollback failed: {rb_exc} "
                        f"— reconnecting"
                    )
                    return self._reconnect_after_failure(conn, buffered, pending_commit)
                continue

            sql = _insert_sql(table, self.table, self._dedup)

            # keep parsed rows paired with their original raw row so a failed
            # insert can put the *raw* row back for retry (parsing is cheap
            # and idempotent, no need to cache the parsed tuple across a retry)
            good_raw: List[dict] = []
            good_parsed: List[tuple] = []
            bad_count = 0
            for row in rows:
                try:
                    good_parsed.append(_parse(row, self.table))
                    good_raw.append(row)
                except Exception as exc:
                    bad_count += 1
                    _safe_print(f"{self._tag}[ERROR] parse failed for {sym}: {exc}")

            if bad_count:
                _safe_print(
                    f"{self._tag}[ERROR] {bad_count} row(s) for {sym} permanently "
                    f"dropped (malformed — would fail parsing again on retry)"
                )

            if good_parsed:
                try:
                    cur = conn.cursor()
                    cur.execute("SAVEPOINT sp_insert")
                    psycopg2.extras.execute_values(cur, sql, good_parsed)
                    any_rows = True
                    pending_commit[sym] = good_raw
                    buffered[sym] = []  # tentatively cleared; restored below if commit fails
                except Exception as exc:
                    _safe_print(
                        f"{self._tag}[ERROR] insert failed for {sym}: {exc} "
                        f"— {len(good_raw)} row(s) kept for retry next flush"
                    )
                    buffered[sym] = _cap_retry_rows(sym, good_raw, self._tag)
                    try:
                        conn.cursor().execute("ROLLBACK TO SAVEPOINT sp_insert")
                    except Exception as rb_exc:
                        _safe_print(
                            f"{self._tag}[ERROR] savepoint rollback failed: {rb_exc} "
                            f"— reconnecting"
                        )
                        return self._reconnect_after_failure(conn, buffered, pending_commit)
            else:
                buffered[sym] = []

        if any_rows:
            try:
                conn.commit()
            except Exception as exc:
                total_retry = sum(len(v) for v in pending_commit.values())
                _safe_print(
                    f"{self._tag}[ERROR] commit failed: {exc} — reconnecting, "
                    f"{total_retry} row(s) kept for retry next flush"
                )
                return self._reconnect_after_failure(conn, buffered, pending_commit)

        return conn, known_tables