import asyncio
import atexit
import fcntl
import os
import signal
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv

# Allow running this file directly: `python x9_data_fetcher/start_data.py`
if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from x9_data_fetcher.venv_setup import VENV_DIR, create_and_activate_venv

create_and_activate_venv()

from x9_data_fetcher.backfill_manager import (
    BackfillManager,
    DailyCloseManager,
    latest_collected_timestamp,
    _last_n_trading_days,
    _previous_trading_day,
)
from x9_data_fetcher import connection_log
from x9_data_fetcher.data_fetcher import MarketDataFetcher
from x9_data_fetcher.event_bus import market_data_queue
from x9_data_fetcher.market_time import (
    is_connectable,
    now_kolkata,
    refresh_trading_calendar,
    seconds_until_close,
    seconds_until_pre_connect,
)
from x9_data_fetcher.symbols import load_symbols
from x9_data_fetcher.pg_writer import auto_setup as pg_auto_setup, purge_old_data
from x9_data_fetcher import parquet_archiver
from x9_data_fetcher.websocket_connect import websocket_client


# Directory containing the x9_data_fetcher package — same one this file
# inserts into sys.path above when run directly. The archiver subprocess
# needs this as its cwd so `python -m x9_data_fetcher.parquet_archiver`
# resolves regardless of whatever cwd start_data.py itself happened to be
# launched from.
_PACKAGE_PARENT_DIR = Path(__file__).resolve().parent.parent

# Resolved eagerly, at import time — VENV_DIR is a relative path resolved
# against whatever the process's cwd was when create_and_activate_venv()
# ran above, which may differ from _PACKAGE_PARENT_DIR. Resolving the venv
# directory itself now (before anything changes directories) pins it to
# the right venv regardless of what cwd the archiver subprocess is later
# launched with.
#
# IMPORTANT: only the venv directory portion is resolved — the trailing
# bin/python is left as-is. venv's bin/python is a symlink to the base
# system interpreter, and Python's venv detection at startup depends on
# invoking it AS that symlink (it looks for a pyvenv.cfg next to
# sys.executable's own path). Fully resolving through the symlink would
# hand subprocess.Popen the bare system interpreter's real path instead —
# which looks identical for stdlib-only scripts, but silently loses access
# to every package actually installed in this venv (confirmed: psycopg2
# imports fine via the symlink path, ModuleNotFoundError via the resolved
# system path).
_ARCHIVER_PYTHON = (
    Path(VENV_DIR).resolve()
    / ("Scripts" if os.name == "nt" else "bin")
    / ("python.exe" if os.name == "nt" else "python")
)


_LOCK_FILE_PATH = os.getenv("X9_FETCHER_LOCK_FILE", "/tmp/x9_data_fetcher.lock")
_lock_file_handle = None  # kept open for process lifetime — closing releases the lock


def _acquire_singleton_lock() -> bool:
    """
    Ensure only one instance of this script runs at a time.

    Uses fcntl.flock on a lock file — unlike a PID file, the OS
    automatically releases this lock if the process dies or crashes,
    so it can never get stuck permanently locked.

    Returns True if the lock was acquired (safe to proceed), False if
    another instance already holds it.
    """
    global _lock_file_handle
    try:
        _lock_file_handle = open(_LOCK_FILE_PATH, "w")
        fcntl.flock(_lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_file_handle.write(str(os.getpid()))
        _lock_file_handle.flush()
        return True
    except BlockingIOError:
        return False
    except OSError as exc:
        print(f"[X9_FETCHER][WARN] lock file check failed ({exc}) — proceeding anyway", flush=True)
        return True


def _drain_queue():
    """Drain leftover items from the shared async queue between sessions."""
    dropped = 0
    while not market_data_queue.empty():
        try:
            market_data_queue.get_nowait()
            market_data_queue.task_done()
            dropped += 1
        except asyncio.QueueEmpty:
            break
    if dropped:
        print(f"[X9_FETCHER] Drained {dropped} stale packets from queue", flush=True)


def _start_archiver_process(depth_output_dir: str, quote_output_dir: str) -> "subprocess.Popen | None":
    """
    Launch parquet_archiver.py as its own OS process (never a thread in
    this process — see parquet_archiver.py's module docstring for why).
    Runs for the lifetime of this script, across sessions/days; only the
    end-of-day force_all sweep + daily/weekly merge (triggered separately,
    right after fetcher.shutdown()) needs synchronous handling — the
    hourly sweep this subprocess performs on its own schedule doesn't.
    """
    enabled = os.getenv("X9_ARCHIVER_ENABLED", "1").strip().lower()
    if enabled in ("0", "false", "no", "off"):
        print("[X9_FETCHER] Parquet archiver disabled (X9_ARCHIVER_ENABLED)", flush=True)
        return None
    if not _ARCHIVER_PYTHON.exists():
        print(
            f"[X9_FETCHER][WARN] archiver interpreter not found at "
            f"{_ARCHIVER_PYTHON} — skipping archiver process",
            flush=True,
        )
        return None
    try:
        # Pin these as ABSOLUTE paths, resolved against *this* process's
        # cwd (the one the live writer is actually using) — not the
        # archiver subprocess's cwd (_PACKAGE_PARENT_DIR, needed only for
        # `-m` module resolution). Without this, if start_data.py is
        # launched from anywhere other than _PACKAGE_PARENT_DIR, the
        # archiver silently looks in the wrong data/ dir all day and only
        # the end-of-day in-process force_all sweep (which shares this
        # process's cwd) ever finds anything.
        env = os.environ.copy()
        env["X9_DEPTH_OUTPUT_DIR"] = str(Path(depth_output_dir).resolve())
        env["X9_QUOTE_OUTPUT_DIR"] = str(Path(quote_output_dir).resolve())
        env["X9_ARCHIVE_DIR"] = str(Path(os.getenv("X9_ARCHIVE_DIR", "archive")).resolve())
        proc = subprocess.Popen(
            [str(_ARCHIVER_PYTHON), "-m", "x9_data_fetcher.parquet_archiver"],
            cwd=str(_PACKAGE_PARENT_DIR),
            env=env,
        )
        print(f"[X9_FETCHER] Parquet archiver process started (pid={proc.pid})", flush=True)
        return proc
    except Exception as exc:
        print(f"[X9_FETCHER][WARN] failed to start archiver process: {exc}", flush=True)
        return None


def _stop_archiver_process(proc: "subprocess.Popen | None") -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print("[X9_FETCHER][WARN] archiver process did not exit in time — killing", flush=True)
        proc.kill()
        proc.wait(timeout=5)
    print("[X9_FETCHER] Parquet archiver process stopped", flush=True)


async def run_engine():
    module_env = Path(__file__).resolve().parent / ".env"
    if module_env.exists():
        load_dotenv(dotenv_path=module_env)
    load_dotenv()

    api_key = os.getenv("API_KEY", "")
    if not api_key:
        raise RuntimeError("API_KEY missing in .env")

    ws_host = os.getenv("WEBSOCKET_HOST", "127.0.0.1")
    ws_port = int(os.getenv("WEBSOCKET_PORT", "8765"))
    ws_url = os.getenv("WEBSOCKET_URL", f"ws://{ws_host}:{ws_port}")

    symbols_csv = os.getenv("X9_FETCHER_SYMBOLS_CSV", "x9/symbols.csv")
    depth_output_dir = os.getenv("X9_DEPTH_OUTPUT_DIR", "data")
    quote_output_dir = os.getenv(
        "X9_QUOTE_OUTPUT_DIR", os.getenv("X9_OHLC_OUTPUT_DIR", "data")
    )
    pg_dsn = os.getenv("X9_PG_DSN", os.getenv("X9_POSTGRES_DSN", "")).strip() or None
    depth_levels = int(os.getenv("X9_DEPTH_LEVELS", "5"))
    flush_batch = int(os.getenv("X9_DEPTH_FLUSH_BATCH", "200"))
    flush_interval = float(os.getenv("X9_DEPTH_FLUSH_INTERVAL_SEC", "1.0"))

    symbols = load_symbols(symbols_csv)
    if not symbols:
        raise RuntimeError(f"No symbols found in {symbols_csv}")

    # Refresh trading calendar from NSE if stale (>7 days) or has no future holidays
    refresh_trading_calendar()

    # ── Connection log watchdog — if the previous trading day has zero
    #    connection events (process crashed before ever connecting, or the
    #    whole day was missed), mark it DAY_NOT_STARTED so BackfillManager
    #    treats it as a full-session gap instead of guessing via data scan ──
    _startup_now = now_kolkata()
    _prev_trading_day = _previous_trading_day(_startup_now.date())
    if _prev_trading_day is not None:
        connection_log.mark_day_not_started_if_missing(
            quote_output_dir, _prev_trading_day, _startup_now
        )

    # run PG setup at startup so PostgreSQL is ready before market opens
    # skips instantly if already running, installs+configures if missing
    if os.getenv("PG_HOST", "").strip() or os.getenv("X9_PG_DSN", "").strip():
        pg_auto_setup()

    # ── Parquet archiver — separate OS process (not a thread here), so it
    #    never contends with this process's event loop / GIL. Runs for the
    #    whole lifetime of this script, across sessions/days; it sweeps
    #    closed hourly SQLite files on its own schedule. The end-of-day
    #    force_all sweep + daily/weekly merge are triggered separately,
    #    synchronously, right after fetcher.shutdown() below — see there. ──
    archiver_proc = _start_archiver_process(depth_output_dir, quote_output_dir)
    # atexit is the safety net for any exit path that isn't the explicit
    # manual-shutdown stop below (e.g. an unhandled exception propagating
    # out of the daily loop) — terminate()/wait() on an already-stopped
    # process is a safe no-op, so it's fine if both this and the explicit
    # stop below end up running.
    atexit.register(_stop_archiver_process, archiver_proc)

    instruments = [{"exchange": s["exchange"], "symbol": s["symbol"]} for s in symbols]

    # ── Graceful exit on SIGINT / SIGTERM ──
    manual_stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, manual_stop.set)
    loop.add_signal_handler(signal.SIGTERM, manual_stop.set)

    print(
        f"[X9_FETCHER] Initialized | symbols={len(instruments)} | ws={ws_url}",
        flush=True,
    )

    # ── Evaluate backfill / PG config once (reused in both startup and session) ──
    backfill_enabled = os.getenv("X9_BACKFILL_ENABLED", "1").strip().lower()
    backfill_enabled = backfill_enabled not in ("0", "false", "no", "off")
    pg_configured = bool(
        os.getenv("PG_HOST", "").strip()
        or os.getenv("PG_HDBNAME", "").strip()
        or pg_dsn
    )

    # ── Startup backfill — fires immediately at script launch, independent of
    #    session schedule.  Recovers past days without waiting for 09:14. ──
    startup_backfill_task = None
    daily_backfill_task = None
    if backfill_enabled and pg_configured:
        pre_startup_ts = latest_collected_timestamp(quote_output_dir)
        startup_backfill_task = asyncio.create_task(
            BackfillManager(
                symbols=symbols,
                quote_output_dir=quote_output_dir,
                api_key=api_key,
                flush_batch_size=flush_batch,
                flush_interval_sec=flush_interval,
                last_known_timestamp=pre_startup_ts,
            ).run()
        )
        print("[X9_FETCHER] Startup backfill task launched", flush=True)

        # Same idea, EOD candle timeframe — daily_SYMBOL only gets written
        # at 15:30 close, so if the process was off/down at that moment
        # (weekend startup, downtime, restart), this catches it up. Works
        # regardless of market/weekend status since it only ever looks at
        # already-finalized past trading days.
        #
        # Own env var, own default — matches the 30-trading-day retention
        # window in pg_writer.purge_old_data (keep_daily_trading_days),
        # NOT the 3-day X9_BACKFILL_MIN_DAYS used for tick-level backfill.
        # Otherwise retention would happily hold 30 days of daily closes
        # while backfill only ever catches up the last 3 — days 4-30 of
        # any real downtime would silently never get filled in.
        _daily_min_days = max(
            1, int(os.getenv("X9_DAILY_BACKFILL_DAYS", "30").strip() or "30")
        )
        _daily_required_days = _last_n_trading_days(now_kolkata(), _daily_min_days)
        daily_backfill_task = asyncio.create_task(
            DailyCloseManager(symbols=symbols, api_key=api_key).run_backfill(
                _daily_required_days
            )
        )
        print("[X9_FETCHER] Startup daily-candle backfill task launched", flush=True)
    elif backfill_enabled:
        print("[BACKFILL] startup backfill skipped — PostgreSQL not configured", flush=True)

    # ── Daily loop ──────────────────────────────────────────────────────
    while not manual_stop.is_set():
        now = now_kolkata()

        # ── Wait for pre-connect window (9:14 IST) if market is closed ──
        if not is_connectable(now):
            wait_secs = seconds_until_pre_connect(now)
            resume_at = now + timedelta(seconds=wait_secs)
            print(
                f"[X9_FETCHER] Market closed. Next session: "
                f"{resume_at.strftime('%Y-%m-%d %H:%M:%S')} IST "
                f"(waiting {wait_secs / 3600:.1f}h)",
                flush=True,
            )
            try:
                await asyncio.wait_for(manual_stop.wait(), timeout=wait_secs)
            except asyncio.TimeoutError:
                pass  # Timer expired → time to connect
            continue  # Re-evaluate after waking

        # ── Start trading session ───────────────────────────────────────
        close_secs = seconds_until_close(now)
        if close_secs <= 0:
            continue  # Edge case: woke up exactly at 15:30

        # Re-check watchdog each session start (process may run for weeks)
        _prev_day = _previous_trading_day(now.date())
        if _prev_day is not None:
            connection_log.mark_day_not_started_if_missing(quote_output_dir, _prev_day, now)

        _drain_queue()

        # capture BEFORE websocket starts writing live ticks
        # backfill uses this to find today's gap without reading fresh live data
        # None on first startup — backfill_manager handles that case internally
        pre_startup_ts = latest_collected_timestamp(quote_output_dir)

        fetcher = MarketDataFetcher(
            depth_output_dir=depth_output_dir,
            quote_output_dir=quote_output_dir,
            symbols=symbols,
            pg_dsn=pg_dsn,
            flush_batch_size=flush_batch,
            flush_interval_sec=flush_interval,
        )

        # Auto-close event fires at market close (15:30)
        session_stop = asyncio.Event()

        async def _auto_close(secs: float):
            try:
                await asyncio.sleep(secs)
            except asyncio.CancelledError:
                return
            session_stop.set()

        close_task = asyncio.create_task(_auto_close(close_secs))

        tasks = [
            asyncio.create_task(
                websocket_client(
                    ws_url, api_key, instruments, mode="Quote",
                    conn_log_dir=quote_output_dir,
                )
            ),
            asyncio.create_task(
                websocket_client(
                    ws_url, api_key, instruments, mode="Depth", depth_levels=depth_levels
                )
            ),
            asyncio.create_task(fetcher.run()),
        ]

        if (
            backfill_enabled
            and pg_configured
            and startup_backfill_task
            and not startup_backfill_task.done()
        ):
            print(
                "[BACKFILL] session backfill skipped — startup backfill still running",
                flush=True,
            )
        elif backfill_enabled and pg_configured:
            tasks.append(
                asyncio.create_task(
                    BackfillManager(
                        symbols=symbols,
                        quote_output_dir=quote_output_dir,
                        api_key=api_key,
                        flush_batch_size=flush_batch,
                        flush_interval_sec=flush_interval,
                        last_known_timestamp=pre_startup_ts,
                    ).run()
                )
            )
        elif backfill_enabled:
            print("[BACKFILL] skipped because PostgreSQL is not configured", flush=True)

        print(
            f"[X9_FETCHER] Session started at "
            f"{now.strftime('%H:%M:%S')} IST | "
            f"auto-close in {close_secs / 60:.0f} min",
            flush=True,
        )

        # ── Block until market close OR manual stop ─────────────────────
        wait_tasks = [
            asyncio.create_task(session_stop.wait()),
            asyncio.create_task(manual_stop.wait()),
        ]
        _done, pending = await asyncio.wait(
            wait_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()

        # ── Tear down session ───────────────────────────────────────────
        close_task.cancel()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, close_task, return_exceptions=True)

        # ── Daily close price — must run BEFORE fetcher.shutdown() so it
        #    can enqueue through the still-running quote_writer (same
        #    quote_SYMBOL SQLite table live ticks go into). Natural close
        #    only — a manual stop mid-session has no real close price yet.
        if session_stop.is_set() and not manual_stop.is_set():
            try:
                await DailyCloseManager(
                    symbols=symbols,
                    api_key=api_key,
                ).run(fetcher.quote_writer, pg_configured)
            except Exception as exc:
                print(f"[DAILY_CLOSE][ERROR] {exc}", flush=True)

        fetcher.shutdown()

        # ── End-of-day archive sweep + daily/weekly merge — must run AFTER
        #    fetcher.shutdown() (not the DailyCloseManager call above),
        #    because that's the point the day's final hourly SQLite file is
        #    actually closed. force_all=True is safe here specifically
        #    because we've just confirmed the writer has stopped — this is
        #    the ONE place outside the standalone archiver loop that's
        #    allowed to pass it. Natural close only, same guard as the
        #    daily-close price above: a manual stop mid-session hasn't
        #    actually finished the trading day, so nothing should be merged.
        if session_stop.is_set() and not manual_stop.is_set():
            try:
                today = now_kolkata().date()
                await asyncio.to_thread(parquet_archiver.run_once, True)
                await asyncio.to_thread(parquet_archiver.merge_daily, today)
                if parquet_archiver.is_last_trading_day_of_week(today):
                    await asyncio.to_thread(parquet_archiver.merge_weekly, today)
            except Exception as exc:
                print(f"[ARCHIVER][ERROR] end-of-day archive/merge failed: {exc}", flush=True)

        if manual_stop.is_set():
            # Cancel startup backfill tasks if still running
            if startup_backfill_task and not startup_backfill_task.done():
                startup_backfill_task.cancel()
                await asyncio.gather(startup_backfill_task, return_exceptions=True)
            if daily_backfill_task and not daily_backfill_task.done():
                daily_backfill_task.cancel()
                await asyncio.gather(daily_backfill_task, return_exceptions=True)
            _stop_archiver_process(archiver_proc)
            print("[X9_FETCHER] Manual shutdown complete", flush=True)
            break

        print(
            "[X9_FETCHER] Market closed at 15:30. Session ended, data flushed.",
            flush=True,
        )

        # ── Daily retention purge — runs once per trading day at market
        #    close. Blocking psycopg2 calls, so offload to a thread rather
        #    than stalling the event loop / next session's pre-connect wait.
        if pg_configured:
            try:
                await asyncio.to_thread(purge_old_data)
            except Exception as exc:
                print(f"[RETENTION][ERROR] purge failed: {exc}", flush=True)
        # Loop back → will calculate wait until next 9:14 (skipping weekends)


if __name__ == "__main__":
    if not _acquire_singleton_lock():
        print(
            f"[X9_FETCHER][FATAL] another instance is already running "
            f"(lock file: {_LOCK_FILE_PATH}) — exiting to avoid duplicate "
            f"processes, duplicate websocket connections, and duplicate "
            f"backfill API calls",
            flush=True,
        )
        sys.exit(1)
    asyncio.run(run_engine())