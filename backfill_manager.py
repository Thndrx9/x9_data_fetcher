import asyncio
import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib import request
from urllib.error import URLError

from x9_data_fetcher.event_bus import is_ws_connected
from x9_data_fetcher.gap_detector import (
    HistoryWindow,
    _candidate_db_paths,
    _filter_gaps_already_in_history,
    _find_symbol_gaps,
    _history_connection,
    _last_n_trading_days,
    _missing_daily_days,
    _ms_to_ist,
    _previous_trading_day,  # noqa: F401 — re-exported for start_data.py
    _to_ms,
)
from x9_data_fetcher.market_time import (
    MARKET_CLOSE,
    is_market_open,
    now_kolkata,
    tz_kolkata,
)
from x9_data_fetcher import pg_writer as _pg_writer_module
from x9_data_fetcher.pg_writer import PgWriter


# Second-level gap detection constants are owned by gap_detector.py now;
# kept as aliases here only in case anything external still imports them
# from this module.
_CANDLE_INTERVAL_MS = 60_000


_IS_TTY = sys.stdout.isatty()
_NON_TTY_PROGRESS_INTERVAL = 20  # print a plain line every N updates when not a live terminal


def _progress_write(text: str, *, index: Optional[int] = None, total: Optional[int] = None) -> None:
    """
    Show live per-item progress without flooding the log.

    On a real interactive terminal (sys.stdout.isatty()), this overwrites
    the current line in place with "\\r" — clean, single line, no scrolling.

    "\\r" only means "overwrite" to an actual terminal emulator. When stdout
    is redirected — a log file, `nohup.out`, `docker logs`, systemd/
    journald, or piped through `tee`/`ssh ... | somewhere` — nothing ever
    interprets the "\\r", so every write just lands as more raw bytes with
    no line break, which is exactly the smashed-together, concatenated line
    you get if you cat/tail that file or paste it as plain text.

    In that case (not a tty), fall back to a plain newline-terminated print,
    but only periodically — every `_NON_TTY_PROGRESS_INTERVAL`-th update,
    plus always the first and last — so a redirected log still shows
    progress without one line per symbol for 180+ symbols.

    `index`/`total` are only needed for the non-tty fallback; omit them if
    you don't have a periodic count to give (the tty path ignores them).

    Long lines are truncated to the terminal's current column width. This
    matters because "\\r" only rewinds the cursor to the start of the
    *current visual row* — if the text is wider than the terminal and wraps
    onto a second row, "\\r" just rewinds that wrapped remainder, leaving
    the first row's tail behind on every update. That's what produces a
    stack of seemingly-separate lines even though this is a real tty and
    "\\r" is working exactly as designed.
    """
    if _IS_TTY:
        width = shutil.get_terminal_size(fallback=(80, 24)).columns
        # leave 1 col of headroom — writing into the very last column can
        # itself trigger an auto-wrap on some terminals
        max_len = max(width - 1, 1)
        if len(text) > max_len:
            text = text[: max_len - 1] + "…"
        # Shared with pg_writer._safe_print: any background writer-thread
        # print (table creation, insert errors, etc.) checks this same lock
        # + flag before printing, and closes this line with a newline first
        # if it's open — otherwise their print lands mid-line and garbles
        # the display (e.g. "...failed=0[PG_DAILY] new table: ..." smashed
        # onto one line, exactly as seen in production).
        with _pg_writer_module._console_lock:
            sys.stdout.write(f"\r{text}\033[K")
            sys.stdout.flush()
            _pg_writer_module._progress_open = True
        return

    if index is None or total is None:
        return

    if index == 1 or index == total or index % _NON_TTY_PROGRESS_INTERVAL == 0:
        with _pg_writer_module._console_lock:
            print(text, flush=True)


def _progress_done() -> None:
    """Finalize the current progress line so the next print starts fresh."""
    if _IS_TTY:
        with _pg_writer_module._console_lock:
            sys.stdout.write("\n")
            sys.stdout.flush()
            _pg_writer_module._progress_open = False
    # non-tty: nothing to finalize — periodic plain lines already have newlines


# ---------------------------------------------------------------------------
# DB helpers kept for start_data.py compatibility
# ---------------------------------------------------------------------------

def _quote_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'quote_%'"
    )
    return [row[0] for row in cur.fetchall()]


def _latest_from_raw_rows(conn: sqlite3.Connection, table: str) -> Optional[int]:
    latest: Optional[int] = None
    cur = conn.execute(
        f"SELECT timestamp, raw_json FROM {table} ORDER BY rowid DESC LIMIT 1000"
    )
    for ts_value, raw_json in cur.fetchall():
        ts_ms = _to_ms(ts_value)
        if ts_ms is None and raw_json:
            try:
                payload = json.loads(raw_json)
                ts_ms = _to_ms(payload.get("timestamp") or payload.get("ltt"))
            except Exception:
                ts_ms = None
        if ts_ms is not None and (latest is None or ts_ms > latest):
            latest = ts_ms
    return latest


def _latest_timestamp_in_db(db_path: Path) -> Optional[int]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        latest: Optional[int] = None
        for table in _quote_tables(conn):
            try:
                cur = conn.execute(f"SELECT MAX(timestamp) FROM {table}")
                ts_ms = _to_ms(cur.fetchone()[0])
                if ts_ms is None:
                    ts_ms = _latest_from_raw_rows(conn, table)
                if ts_ms is not None and (latest is None or ts_ms > latest):
                    latest = ts_ms
            except Exception as exc:
                print(
                    f"[BACKFILL][WARN] failed scanning {db_path.name}:{table}: {exc}",
                    flush=True,
                )
        return latest
    finally:
        conn.close()


def latest_collected_timestamp(quote_output_dir: str) -> Optional[datetime]:
    """
    Scan all weekly SQLite DBs and return the most recent quote timestamp.
    Called from start_data.py BEFORE the websocket starts.
    """
    base_dir = Path(quote_output_dir)
    for db_path in _candidate_db_paths(base_dir):
        try:
            ts_ms = _latest_timestamp_in_db(db_path)
        except sqlite3.Error as exc:
            print(f"[BACKFILL][WARN] failed opening {db_path.name}: {exc}", flush=True)
            continue
        if ts_ms is not None:
            latest = _ms_to_ist(ts_ms)
            print(
                f"[BACKFILL] pre-startup latest quote: {latest.isoformat()} "
                f"from {db_path.name}",
                flush=True,
            )
            return latest
    return None


# ---------------------------------------------------------------------------
# OpenAlgo history API helpers
# ---------------------------------------------------------------------------

def _history_endpoint() -> str:
    endpoint = os.getenv("OPENALGO_HISTORY_URL", "").strip()
    if endpoint:
        return endpoint
    host = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000").rstrip("/")
    return f"{host}/api/v1/history"


def _extract_candle_rows(payload: Any) -> Sequence[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "candles", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_candle_rows(value)
            if nested:
                return nested
    if all(isinstance(v, list) for v in payload.values()):
        keys = list(payload.keys())
        row_count = min(len(payload[key]) for key in keys)
        return [{key: payload[key][idx] for key in keys} for idx in range(row_count)]
    return []


def _row_timestamp(row: Any) -> Optional[int]:
    if isinstance(row, dict):
        for key in ("timestamp", "time", "datetime", "date", "t"):
            ts_ms = _to_ms(row.get(key))
            if ts_ms is not None:
                return ts_ms
    elif isinstance(row, (list, tuple)) and row:
        return _to_ms(row[0])
    return None


def _normalize_candle(
    row: Any, symbol: str, exchange: str, interval: str
) -> Optional[dict]:
    ts_ms = _row_timestamp(row)
    if ts_ms is None:
        return None
    if isinstance(row, dict):
        payload = dict(row)
    elif isinstance(row, (list, tuple)):
        keys = ("timestamp", "open", "high", "low", "close", "volume", "oi")
        payload = {key: row[idx] for idx, key in enumerate(keys) if idx < len(row)}
    else:
        return None
    payload["timestamp"] = ts_ms
    payload.setdefault("symbol", symbol)
    payload.setdefault("exchange", exchange)
    payload.setdefault("interval", interval)
    payload.setdefault("source", "openalgo_history")
    return {
        "timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        .astimezone(tz_kolkata)
        .isoformat(),
        "ingest_ns": time.time_ns(),
        "exchange": exchange,
        "symbol": symbol,
        "raw_json": json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
    }


# ---------------------------------------------------------------------------
# BackfillManager
# ---------------------------------------------------------------------------

class BackfillManager:
    """
    Detects missing trading data and recovers it from the OpenAlgo history API.

    Gap detection:
        - Checks the last X9_BACKFILL_MIN_DAYS trading days per symbol
        - Past days: queries live SQLite — if no rows for that day, fetches full session
        - Today:     uses pre_startup_ts (captured before websocket starts)
                     so live ticks are never mistaken for pre-existing data

    Output:
        - Saves recovered candles to PostgreSQL (PG_HDBNAME database)
        - One table per symbol: quote_RELIANCE, quote_TCS ...
    """

    def __init__(
        self,
        symbols: Sequence[dict],
        quote_output_dir: str,
        api_key: str,
        flush_batch_size: int = 200,
        flush_interval_sec: float = 1.0,
        last_known_timestamp: Optional[datetime] = None,
    ):
        self.symbols            = list(symbols)
        self.quote_output_dir   = quote_output_dir
        self.api_key            = api_key
        self.interval           = os.getenv("OPENALGO_HISTORY_INTERVAL", "1m").strip() or "1m"
        self.endpoint           = _history_endpoint()
        self.history_dbname     = (
            os.getenv("PG_HDBNAME", "market_history").strip() or "market_history"
        )
        self.min_days           = max(
            3, int(os.getenv("X9_BACKFILL_MIN_DAYS", "3").strip() or "3")
        )
        self.flush_batch_size   = flush_batch_size
        self.flush_interval_sec = flush_interval_sec
        self._writer: Optional[PgWriter] = None
        # captured BEFORE websocket writes live ticks — used for today's gap
        self._last_known_timestamp: Optional[datetime] = last_known_timestamp

    async def run(self) -> None:
        try:
            await self._wait_for_completed_minute()
            # _run_once runs entirely in a thread pool thread via to_thread
            # — SQLite I/O, HTTP requests, PgWriter.shutdown() all stay off
            # the event loop so the websocket is never blocked
            await asyncio.to_thread(self._run_once)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[BACKFILL][ERROR] recovery failed: {exc}", flush=True)
        # no finally shutdown here — shutdown is called inside _run_once
        # in the thread to avoid blocking the event loop

    async def _wait_for_completed_minute(self) -> None:
        now = now_kolkata()

        # Only worth waiting for a clean minute boundary if ticks could
        # actually be arriving right now (market open or a websocket is
        # live). Otherwise there's no in-progress candle to protect
        # against — scan immediately.
        if not is_market_open(now) and not is_ws_connected():
            print(
                "[BACKFILL] market closed and no websocket connected "
                "— skipping wait, scanning now",
                flush=True,
            )
            return

        delay = 60 - now.second - (now.microsecond / 1_000_000)
        if delay >= 60:
            delay = 0
        if delay > 0:
            target = now + timedelta(seconds=delay)
            print(
                f"[BACKFILL] waiting until {target.strftime('%H:%M:%S')} IST "
                f"before gap scan",
                flush=True,
            )
            await asyncio.sleep(delay)

    def _run_once(self) -> None:
        now           = now_kolkata()
        required_days = _last_n_trading_days(now, self.min_days)
        db_paths      = _candidate_db_paths(Path(self.quote_output_dir))

        print(
            f"[BACKFILL] scanning last {self.min_days} trading days "
            f"({required_days[0] if required_days else '?'} → "
            f"{required_days[-1] if required_days else '?'}) "
            f"across {len(db_paths)} DB file(s)",
            flush=True,
        )

        # ── per-symbol gap detection ──────────────────────────────────────
        all_gaps: Dict[str, Tuple[str, List[HistoryWindow]]] = {}
        history_conn = None
        # computed once per day, shared across all symbols — log-derived
        # windows are system-wide (same outage applies to every symbol)
        log_windows_cache: Dict[date, Optional[List[HistoryWindow]]] = {}

        try:
            history_conn = _history_connection(self.history_dbname)
        except Exception as exc:
            print(
                f"[BACKFILL][WARN] history DB check unavailable: {exc}",
                flush=True,
            )

        try:
            total_symbols = len(self.symbols)
            checked = 0
            fully_present_total = 0
            partial_total = 0

            for symbol_row in self.symbols:
                symbol   = str(symbol_row["symbol"]).upper()
                exchange = str(symbol_row.get("exchange") or "NSE").upper()

                gaps = _find_symbol_gaps(
                    db_paths,
                    symbol,
                    required_days,
                    self._last_known_timestamp,
                    now,
                    quote_output_dir=self.quote_output_dir,
                    log_windows_cache=log_windows_cache,
                )

                if gaps and history_conn is not None:
                    try:
                        gaps, fully_present, partial = _filter_gaps_already_in_history(
                            history_conn,
                            symbol,
                            exchange,
                            gaps,
                        )
                        fully_present_total += fully_present
                        partial_total += partial
                    except Exception as exc:
                        _progress_done()
                        print(
                            f"[BACKFILL][WARN] history DB check failed for "
                            f"{exchange}:{symbol}: {exc}",
                            flush=True,
                        )

                if gaps:
                    all_gaps[symbol] = (exchange, gaps)

                checked += 1
                _progress_write(
                    f"[BACKFILL] {checked}/{total_symbols} | {exchange}:{symbol} "
                    f"| present={fully_present_total} partial={partial_total} "
                    f"need={len(all_gaps)}",
                    index=checked,
                    total=total_symbols,
                )

            _progress_done()
        finally:
            if history_conn is not None:
                try:
                    history_conn.close()
                except Exception:
                    pass

        if not all_gaps:
            print("[BACKFILL] no missing data detected — nothing to fetch", flush=True)
            return

        total_windows = sum(len(v[1]) for v in all_gaps.values())
        print(
            f"[BACKFILL] {len(all_gaps)} symbol(s) need recovery "
            f"across {total_windows} window(s) → PG database '{self.history_dbname}' "
            f"({fully_present_total} window(s) skipped, already in history DB)",
            flush=True,
        )

        # ── create PG writer ──────────────────────────────────────────────
        self._writer = PgWriter(
            table="quote",
            dbname=self.history_dbname,
            flush_batch_size=self.flush_batch_size,
            flush_interval_sec=self.flush_interval_sec,
            dedup_on_timestamp=True,
        )

        # ── fetch and enqueue ─────────────────────────────────────────────
        try:
            total_rows = 0
            windows_done = 0

            for symbol, (exchange, gaps) in all_gaps.items():
                symbol_rows = 0
                for window_start, window_end in gaps:
                    candles = self._fetch_symbol_window(
                        symbol, exchange, window_start, window_end
                    )
                    for candle in candles:
                        self._writer.enqueue(symbol, candle)
                    symbol_rows += len(candles)
                    windows_done += 1
                    _progress_write(
                        f"[BACKFILL] fetching {windows_done}/{total_windows} window(s) "
                        f"| {exchange}:{symbol} "
                        f"{window_start.strftime('%Y-%m-%d %H:%M')}→"
                        f"{window_end.strftime('%H:%M')} "
                        f"| {total_rows + symbol_rows} candle(s) queued so far",
                        index=windows_done,
                        total=total_windows,
                    )
                total_rows += symbol_rows

            _progress_done()
            print(f"[BACKFILL] total {total_rows} candle(s) queued", flush=True)

        finally:
            # shutdown inside the thread — thread.join() never reaches event loop
            # websocket is completely unaffected during drain + flush
            self._writer.shutdown()
            self._writer = None

    def _fetch_symbol_window(
        self,
        symbol: str,
        exchange: str,
        window_start: datetime,
        window_end: datetime,
    ) -> List[dict]:
        now = now_kolkata()

        # When fetching today's data during market hours the OpenAlgo history
        # API requires end_date = tomorrow to return the live/partial session.
        # For past dates end_date == the date itself is correct.
        if window_start.date() == now.date() and is_market_open(now):
            from datetime import timedelta as _td
            api_end_date = (window_end.date() + _td(days=1)).strftime("%Y-%m-%d")
        else:
            api_end_date = window_end.strftime("%Y-%m-%d")

        body = {
            "apikey":     self.api_key,
            "symbol":     symbol,
            "exchange":   exchange,
            "interval":   self.interval,
            "start_date": window_start.strftime("%Y-%m-%d"),
            "end_date":   api_end_date,
            "source":     "api",
        }
        data     = json.dumps(body).encode("utf-8")
        start_ms = int(window_start.astimezone(timezone.utc).timestamp() * 1000)
        end_ms   = int(window_end.astimezone(timezone.utc).timestamp() * 1000)
        # Tolerate APIs that stamp the candle close time instead of open time
        end_ms_tolerant = end_ms + _CANDLE_INTERVAL_MS
        max_attempts = 3
        last_response = ""

        for attempt in range(1, max_attempts + 1):
            http_request = request.Request(
                self.endpoint,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            candles: List[dict] = []

            try:
                with request.urlopen(http_request, timeout=30) as response:
                    raw_response = response.read().decode("utf-8")
                    last_response = raw_response
                    payload = json.loads(raw_response)
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt < max_attempts:
                    print(
                        f"[BACKFILL][WARN] fetch failed for {exchange}:{symbol} "
                        f"(attempt {attempt}/{max_attempts}): {exc}; retrying in 1s",
                        flush=True,
                    )
                    time.sleep(1)
                    continue
                print(
                    f"[BACKFILL][WARN] fetch failed for {exchange}:{symbol}: {exc}",
                    flush=True,
                )
                return []

            rows = _extract_candle_rows(payload)
            if not rows:
                if attempt < max_attempts:
                    print(
                        f"[BACKFILL][WARN] API returned 0 rows for {exchange}:{symbol} "
                        f"{window_start.strftime('%Y-%m-%d')} "
                        f"(attempt {attempt}/{max_attempts}); retrying in 1s",
                        flush=True,
                    )
                    time.sleep(1)
                    continue
                break

            parsed_timestamps: List[int] = []
            for row in rows:
                ts_ms = _row_timestamp(row)
                if ts_ms is None:
                    continue
                parsed_timestamps.append(ts_ms)
                if ts_ms < start_ms or ts_ms > end_ms_tolerant:
                    continue
                normalized = _normalize_candle(row, symbol, exchange, self.interval)
                if normalized:
                    candles.append(normalized)

            if candles:
                return candles

            if parsed_timestamps:
                first     = _ms_to_ist(min(parsed_timestamps))
                last      = _ms_to_ist(max(parsed_timestamps))
                first_utc = datetime.fromtimestamp(min(parsed_timestamps) / 1000, tz=timezone.utc)
                last_utc  = datetime.fromtimestamp(max(parsed_timestamps) / 1000, tz=timezone.utc)
                print(
                    f"[BACKFILL][WARN] {exchange}:{symbol} API returned {len(rows)} "
                    f"row(s) but none matched gap "
                    f"{window_start.strftime('%Y-%m-%d %H:%M')}→"
                    f"{window_end.strftime('%H:%M')} IST. "
                    f"Response range: "
                    f"{first.strftime('%Y-%m-%d %H:%M')}→{last.strftime('%H:%M')} IST "
                    f"({first_utc.strftime('%Y-%m-%d %H:%M')}→"
                    f"{last_utc.strftime('%H:%M')} UTC). "
                    f"Broker may not have captured this period.",
                    flush=True,
                )
                return []

            print(
                f"[BACKFILL][WARN] API returned {len(rows)} row(s) for "
                f"{exchange}:{symbol}, but none had a readable timestamp",
                flush=True,
            )
            return []

        preview = last_response[:300] if len(last_response) > 300 else last_response
        print(
            f"[BACKFILL][WARN] API returned 0 rows for {exchange}:{symbol} "
            f"{window_start.strftime('%Y-%m-%d')} after {max_attempts} attempts "
            f"— response: {preview}",
            flush=True,
        )
        return []


# ---------------------------------------------------------------------------
# DailyCloseManager
# ---------------------------------------------------------------------------

class DailyCloseManager:
    """
    Fetches the authoritative daily (1D / EOD) candle for each symbol right
    after market close. The websocket only ever provides a live VWAP-based
    price, never the broker's real, official close — this is the only way
    to get the actual close price.

    Writes the SAME candle to two places:
      - PostgreSQL table daily_SYMBOL — one row per symbol per day, via a
        dedicated PgWriter(table="daily", ...). Retained separately from
        quote_/depth_ (see pg_writer.purge_old_data — 30 trading days by
        default vs. 3 for tick data).
      - The SAME SQLite quote_SYMBOL table live ticks already go into, as
        ONE extra synthetic row per symbol per day. Tagged
        "source": "daily_close" inside raw_json so it's unambiguous from a
        real tick — anything reading that table can trivially tell them
        apart, and nothing that assumes "every row is a live tick" (candle
        generation, gap detection, etc.) is fooled into treating it as one.

    Must be called BEFORE the live SQLite writer (OhlcParquetWriter) is
    shut down for the session — pass its still-running instance in via
    `ohlc_writer` so this can enqueue through it normally rather than
    opening a second, competing connection to the same DB file.
    """

    def __init__(
        self,
        symbols: Sequence[dict],
        api_key: str,
        history_dbname: Optional[str] = None,
        settle_delay_sec: float = 60.0,
        flush_batch_size: int = 50,
        flush_interval_sec: float = 1.0,
    ):
        self.symbols          = list(symbols)
        self.api_key          = api_key
        self.endpoint         = _history_endpoint()
        self.history_dbname   = (
            history_dbname
            or os.getenv("PG_HDBNAME", "market_history").strip()
            or "market_history"
        )
        # The broker may take a short while after 15:30 to finalize the
        # day's daily candle — give it a head start before the first
        # fetch attempt rather than hammering it with immediate retries.
        self.settle_delay_sec = max(0.0, settle_delay_sec)
        self.flush_batch_size = flush_batch_size
        self.flush_interval_sec = flush_interval_sec

    async def run(self, ohlc_writer, pg_configured: bool) -> None:
        try:
            if self.settle_delay_sec > 0:
                print(
                    f"[DAILY_CLOSE] waiting {self.settle_delay_sec:.0f}s for "
                    f"broker to finalize today's daily candle",
                    flush=True,
                )
                await asyncio.sleep(self.settle_delay_sec)
            # blocking HTTP + SQLite/PG I/O — off the event loop, same as
            # BackfillManager, so nothing else in the shutdown sequence stalls
            await asyncio.to_thread(self._run_once, ohlc_writer, pg_configured)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[DAILY_CLOSE][ERROR] {exc}", flush=True)

    def _run_once(self, ohlc_writer, pg_configured: bool) -> None:
        pg_writer: Optional[PgWriter] = None
        if pg_configured:
            pg_writer = PgWriter(
                table="daily",
                dbname=self.history_dbname,
                flush_batch_size=self.flush_batch_size,
                flush_interval_sec=self.flush_interval_sec,
                dedup_on_timestamp=True,
            )

        written, failed = 0, 0
        total_symbols = len(self.symbols)
        checked = 0
        try:
            for symbol_row in self.symbols:
                symbol   = str(symbol_row["symbol"]).upper()
                exchange = str(symbol_row.get("exchange") or "NSE").upper()
                row = self._fetch_daily_close_row(symbol, exchange)
                if row is None:
                    failed += 1
                else:
                    if pg_writer is not None:
                        pg_writer.enqueue(symbol, row)
                    ohlc_writer.enqueue(symbol, row)
                    written += 1

                checked += 1
                _progress_write(
                    f"[DAILY_CLOSE] {checked}/{total_symbols} | {exchange}:{symbol} "
                    f"| written={written} failed={failed}",
                    index=checked,
                    total=total_symbols,
                )

            _progress_done()
            print(
                f"[DAILY_CLOSE] wrote {written}/{len(self.symbols)} daily "
                f"close(s){f', {failed} failed' if failed else ''}",
                flush=True,
            )
        finally:
            # flush inside this thread — join() never reaches the event loop,
            # websocket/live writers are unaffected during this drain
            if pg_writer is not None:
                pg_writer.shutdown()

    async def run_backfill(self, required_days: List[date]) -> None:
        """
        Catch-up path for the daily_SYMBOL tables — same idea as
        BackfillManager's minute-level backfill, but for EOD candles.

        Runs at process startup, independent of whether the market is open,
        closed, or it's a weekend — these are always PAST trading days, so
        the data is already finalized at the broker and fetchable any time,
        unlike the live _run_once path above which only makes sense right
        at today's close.
        """
        if not required_days:
            return
        try:
            await asyncio.to_thread(self.backfill_missing_days, required_days)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[DAILY_CLOSE][BACKFILL][ERROR] {exc}", flush=True)

    def backfill_missing_days(self, required_days: List[date]) -> None:
        try:
            conn = _history_connection(self.history_dbname)
        except Exception as exc:
            print(f"[DAILY_CLOSE][BACKFILL][ERROR] history db connect failed: {exc}", flush=True)
            return

        pg_writer = PgWriter(
            table="daily",
            dbname=self.history_dbname,
            flush_batch_size=self.flush_batch_size,
            flush_interval_sec=self.flush_interval_sec,
            dedup_on_timestamp=True,
        )

        total_symbols = len(self.symbols)
        checked = 0
        written, present, failed = 0, 0, 0

        try:
            for symbol_row in self.symbols:
                symbol   = str(symbol_row["symbol"]).upper()
                exchange = str(symbol_row.get("exchange") or "NSE").upper()

                missing_days = _missing_daily_days(conn, symbol, required_days)
                present += len(required_days) - len(missing_days)

                for day in missing_days:
                    row = self._fetch_daily_close_row(
                        symbol, exchange, day=day, source_tag="daily_close_backfill"
                    )
                    if row is None:
                        failed += 1
                        continue
                    pg_writer.enqueue(symbol, row)
                    written += 1

                checked += 1
                _progress_write(
                    f"[DAILY_CLOSE][BACKFILL] {checked}/{total_symbols} | "
                    f"{exchange}:{symbol} | written={written} present={present} "
                    f"failed={failed}",
                    index=checked,
                    total=total_symbols,
                )

            _progress_done()
            print(
                f"[DAILY_CLOSE][BACKFILL] done — {written} written, {present} "
                f"already present, {failed} failed "
                f"({total_symbols} symbol(s) x {len(required_days)} day(s))",
                flush=True,
            )
        finally:
            pg_writer.shutdown()
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_daily_close_row(
        self,
        symbol: str,
        exchange: str,
        day: Optional[date] = None,
        source_tag: str = "daily_close",
    ) -> Optional[dict]:
        day = day or now_kolkata().date()
        body = {
            "apikey":     self.api_key,
            "symbol":     symbol,
            "exchange":   exchange,
            "interval":   "D",
            "start_date": day.strftime("%Y-%m-%d"),
            "end_date":   day.strftime("%Y-%m-%d"),
            "source":     "api",
        }
        data = json.dumps(body).encode("utf-8")
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            http_request = request.Request(
                self.endpoint,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with request.urlopen(http_request, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt < max_attempts:
                    print(
                        f"[DAILY_CLOSE][WARN] fetch failed for {exchange}:{symbol} "
                        f"{day} (attempt {attempt}/{max_attempts}): {exc}; "
                        f"retrying in 2s",
                        flush=True,
                    )
                    time.sleep(2)
                    continue
                print(
                    f"[DAILY_CLOSE][WARN] fetch failed for {exchange}:{symbol} "
                    f"{day}: {exc}",
                    flush=True,
                )
                return None

            rows = _extract_candle_rows(payload)
            if not rows:
                if attempt < max_attempts:
                    print(
                        f"[DAILY_CLOSE][WARN] no daily candle yet for "
                        f"{exchange}:{symbol} {day} (attempt {attempt}/{max_attempts}); "
                        f"retrying in 2s",
                        flush=True,
                    )
                    time.sleep(2)
                    continue
                print(
                    f"[DAILY_CLOSE][WARN] no daily candle returned for "
                    f"{exchange}:{symbol} {day} after {max_attempts} attempts",
                    flush=True,
                )
                return None

            # Single-day request → normally exactly one row; if the API
            # ever returns more, the last one is the authoritative EOD row.
            candle = rows[-1]
            normalized = _normalize_candle(candle, symbol, exchange, "D")
            if normalized is None:
                print(
                    f"[DAILY_CLOSE][WARN] daily candle for {exchange}:{symbol} "
                    f"{day} had no readable timestamp — skipping",
                    flush=True,
                )
                return None

            # The broker's own daily-candle timestamp convention is
            # ambiguous (could be session open, midnight, etc.) — pin it
            # explicitly to market close so it always sorts after every
            # real tick that day and is unambiguous to any reader.
            close_dt = datetime.combine(day, MARKET_CLOSE, tzinfo=tz_kolkata)
            close_ms = int(close_dt.astimezone(timezone.utc).timestamp() * 1000)

            payload_dict = json.loads(normalized["raw_json"])
            payload_dict["timestamp"] = close_ms
            payload_dict["source"] = source_tag
            normalized["timestamp"] = close_dt.isoformat()
            normalized["raw_json"] = json.dumps(
                payload_dict, ensure_ascii=True, separators=(",", ":")
            )
            return normalized

        return None