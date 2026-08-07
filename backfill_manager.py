import asyncio
import json
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib import request
from urllib.error import URLError

from x9_data_fetcher.event_bus import is_ws_connected
from x9_data_fetcher.gap_detector import (
    HistoryWindow,
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
from x9_data_fetcher.pg_writer import (
    LIVE_TICK_RETENTION_TRADING_DAYS,
    PgWriter,
    live_connection,
    live_latest_quote_timestamp,
)


# Second-level gap detection constants are owned by gap_detector.py now;
# kept as aliases here only in case anything external still imports them
# from this module.
_CANDLE_INTERVAL_MS = 60_000

# Round-based backfill retry tuning (see BackfillManager._run_once):
# up to this many full passes over whatever's still pending, with this
# pacing between every individual fetch call within a pass.
MAX_BACKFILL_ROUNDS = 9

# A symbol's gap windows get grouped and fetched with ONE broker call per
# group instead of one call per window, whenever a group's overall span
# (earliest window's start date to latest window's end date) fits within
# this many days. The broker's history endpoint is already day-granular
# regardless of how narrow a window is (start_date/end_date, not minute
# timestamps) — two windows on the same day, or a few days apart, cost the
# exact same one call either way, so there is no reason to fetch that span
# twice. Kept as a threshold rather than always merging everything for one
# symbol: a stray gap weeks away from the rest would otherwise drag a huge,
# mostly-unwanted date range into a single request just to patch one
# unrelated window.
WINDOW_MERGE_MAX_SPAN_DAYS = 5
FETCH_PACING_SEC = 0.5


def _group_windows_for_merge(gaps: List[HistoryWindow]) -> List[List[HistoryWindow]]:
    """
    Cluster one symbol's gap windows into groups that can each be fetched
    with a single broker call — see WINDOW_MERGE_MAX_SPAN_DAYS. Greedy:
    windows are sorted by start time, and each is added to the current
    group as long as doing so keeps that group's overall span (earliest
    start date to latest end date) within the threshold; otherwise it
    starts a new group.
    """
    if not gaps:
        return []
    ordered = sorted(gaps, key=lambda w: w[0])
    groups: List[List[HistoryWindow]] = [[ordered[0]]]
    group_start_date = ordered[0][0].date()
    for window in ordered[1:]:
        candidate_end_date = max(group_start_date, window[1].date())
        span_days = (candidate_end_date - group_start_date).days
        if span_days <= WINDOW_MERGE_MAX_SPAN_DAYS:
            groups[-1].append(window)
        else:
            groups.append([window])
            group_start_date = window[0].date()
    return groups


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

# NOTE: this used to scan weekly SQLite DBs (_quote_tables, _latest_from_raw_rows,
# _latest_timestamp_in_db) — removed now that hourly rollover deletes each local
# file shortly after archiving it. Old version is in git history if ever needed.

def latest_collected_timestamp(quote_output_dir: str) -> Optional[datetime]:
    """
    Return the most recent quote timestamp across all symbols, read from the
    live PostgreSQL DB. Called from start_data.py BEFORE the websocket starts.

    quote_output_dir is accepted for call-site compatibility with
    start_data.py but is no longer used — the live DB replaces the local
    SQLite scan as the source of truth.
    """
    try:
        conn = live_connection()
    except Exception as exc:
        print(f"[BACKFILL][WARN] could not reach live DB for resume point: {exc}", flush=True)
        return None
    try:
        ts_ms = live_latest_quote_timestamp(conn)
    finally:
        conn.close()

    if ts_ms is None:
        return None
    latest = _ms_to_ist(ts_ms)
    print(f"[BACKFILL] pre-startup latest quote: {latest.isoformat()}", flush=True)
    return latest


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
        if self.min_days > LIVE_TICK_RETENTION_TRADING_DAYS:
            print(
                f"[BACKFILL][WARN] X9_BACKFILL_MIN_DAYS={self.min_days} exceeds "
                f"the live DB's actual retention window "
                f"({LIVE_TICK_RETENTION_TRADING_DAYS} trading days) — gap "
                f"detection can only see what PG still has. Days beyond "
                f"{LIVE_TICK_RETENTION_TRADING_DAYS} back will silently look "
                f"'missing' even if they were once collected and simply purged. "
                f"Lower X9_BACKFILL_MIN_DAYS to match, or raise PG's "
                f"keep_trading_days if you actually need a longer lookback.",
                flush=True,
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

        print(
            f"[BACKFILL] scanning last {self.min_days} trading days "
            f"({required_days[0] if required_days else '?'} → "
            f"{required_days[-1] if required_days else '?'}) "
            f"against the live PostgreSQL DB",
            flush=True,
        )

        # ── per-symbol gap detection ──────────────────────────────────────
        all_gaps: Dict[str, Tuple[str, List[HistoryWindow]]] = {}
        history_conn = None
        live_conn = None
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
            live_conn = live_connection()
        except Exception as exc:
            print(
                f"[BACKFILL][WARN] live DB unavailable for gap scan — falling back "
                f"to log-only detection, days with no log data will be treated as "
                f"full gaps: {exc}",
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
                    live_conn,
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
            if live_conn is not None:
                try:
                    live_conn.close()
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
        # Round-based retry: instead of hammering one symbol 3x back-to-back
        # (bursty, noisy — every failing symbol dumped 3 stacked WARN lines
        # before moving on), each round makes exactly ONE pass over every
        # still-pending symbol, pacing calls FETCH_PACING_SEC apart. Symbols
        # whose windows all returned candles are done and never retried;
        # anything that had at least one empty/failed window that round goes
        # into the next round's pending set — up to MAX_BACKFILL_ROUNDS
        # total. If a round clears the whole pending set, remaining rounds
        # are skipped. A symbol is retried as a whole (all its gap windows
        # together) — already-successful windows for a still-pending symbol
        # simply get queued again next round, which is harmless since
        # PgWriter dedupes on timestamp.
        try:
            total_rows = 0
            pending: Dict[str, Tuple[str, List[HistoryWindow]]] = dict(all_gaps)
            last_failure_reason: Dict[str, str] = {}
            round_num = 0

            while pending and round_num < MAX_BACKFILL_ROUNDS:
                round_num += 1
                round_total = len(pending)
                if round_num == 1:
                    print(
                        f"[BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} — "
                        f"sweeping {round_total} symbol(s)",
                        flush=True,
                    )
                else:
                    print(
                        f"[BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} — "
                        f"retrying {round_total} symbol(s) that failed round "
                        f"{round_num - 1}",
                        flush=True,
                    )

                still_pending: Dict[str, Tuple[str, List[HistoryWindow]]] = {}
                round_success = 0
                checked = 0

                for symbol, (exchange, gaps) in pending.items():
                    checked += 1
                    symbol_failed = False
                    symbol_rows = 0

                    # Windows close enough together (see
                    # WINDOW_MERGE_MAX_SPAN_DAYS) share ONE broker call
                    # instead of one call each — the broker's response for
                    # a wider date range already covers every window inside
                    # it, so a second call for a nearby window would just
                    # be re-fetching data the first call already got.
                    for group in _group_windows_for_merge(gaps):
                        if len(group) == 1:
                            window_start, window_end = group[0]
                            window_results = {
                                group[0]: self._fetch_symbol_window(
                                    symbol, exchange, window_start, window_end
                                )
                            }
                        else:
                            window_results = self._fetch_symbol_windows_merged(
                                symbol, exchange, group
                            )

                        for (window_start, window_end), (candles, failure_reason) in window_results.items():
                            if candles:
                                for candle in candles:
                                    self._writer.enqueue(symbol, candle)
                                symbol_rows += len(candles)
                                total_rows += len(candles)
                            else:
                                symbol_failed = True
                                last_failure_reason[symbol] = failure_reason or "0 rows returned"

                            _progress_write(
                                f"[BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} | "
                                f"{checked}/{round_total} | {exchange}:{symbol} "
                                f"{window_start.strftime('%Y-%m-%d %H:%M')}→"
                                f"{window_end.strftime('%H:%M')} "
                                f"| {total_rows} candle(s) queued so far",
                                index=checked,
                                total=round_total,
                            )

                        # pace once per broker call (one per group, not one
                        # per window inside it)
                        time.sleep(FETCH_PACING_SEC)

                    if symbol_failed:
                        still_pending[symbol] = (exchange, gaps)
                    else:
                        last_failure_reason.pop(symbol, None)
                        round_success += 1

                _progress_done()
                pending = still_pending

                if pending:
                    print(
                        f"[BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                        f"success={round_success} pending={len(pending)} "
                        f"→ retrying pending in round {round_num + 1}"
                        if round_num < MAX_BACKFILL_ROUNDS
                        else f"[BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                             f"success={round_success} pending={len(pending)}",
                        flush=True,
                    )
                else:
                    print(
                        f"[BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                        f"success={round_success} pending=0 → all recovered, stopping early",
                        flush=True,
                    )

            print(f"[BACKFILL] total {total_rows} candle(s) queued", flush=True)

            if pending:
                names = ", ".join(
                    f"{exchange}:{symbol} ({last_failure_reason.get(symbol, 'unknown')})"
                    for symbol, (exchange, _gaps) in pending.items()
                )
                print(
                    f"[BACKFILL][WARN] {len(pending)} symbol(s) still failing after "
                    f"{round_num} round(s): {names}",
                    flush=True,
                )

        finally:
            # shutdown inside the thread — thread.join() never reaches event loop
            # websocket is completely unaffected during drain + flush
            self._writer.shutdown()
            self._writer = None

    def _fetch_raw_rows(
        self,
        symbol: str,
        exchange: str,
        range_start_date: date,
        range_end_date: date,
    ) -> Tuple[List[dict], Optional[str]]:
        """
        Single HTTP call spanning [range_start_date, range_end_date]
        (inclusive), unfiltered — returns whatever raw candle rows the
        broker sends back for that whole span. Shared by both the
        single-window path and the merged-multi-window path below; the
        only difference between fetching one narrow window and fetching
        several nearby ones is how many windows get sliced out of this
        same response afterward.

        Returns (rows, failure_reason). failure_reason is None only when
        rows is non-empty.
        """
        now = now_kolkata()

        # When fetching today's data during market hours the OpenAlgo history
        # API requires end_date = tomorrow to return the live/partial session.
        # For past dates end_date == the date itself is correct.
        if range_end_date == now.date() and is_market_open(now):
            api_end_date = (range_end_date + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            api_end_date = range_end_date.strftime("%Y-%m-%d")

        body = {
            "apikey":     self.api_key,
            "symbol":     symbol,
            "exchange":   exchange,
            "interval":   self.interval,
            "start_date": range_start_date.strftime("%Y-%m-%d"),
            "end_date":   api_end_date,
            "source":     "api",
        }
        data = json.dumps(body).encode("utf-8")

        http_request = request.Request(
            self.endpoint,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(http_request, timeout=30) as response:
                raw_response = response.read().decode("utf-8")
                payload = json.loads(raw_response)
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            return [], f"fetch error: {exc}"

        rows = _extract_candle_rows(payload)
        if not rows:
            return [], "0 rows returned"
        return rows, None

    def _filter_window(
        self,
        rows: List[dict],
        symbol: str,
        exchange: str,
        window_start: datetime,
        window_end: datetime,
    ) -> Tuple[List[dict], Optional[str]]:
        """Slice/normalize already-fetched raw rows down to one window."""
        start_ms = int(window_start.astimezone(timezone.utc).timestamp() * 1000)
        end_ms   = int(window_end.astimezone(timezone.utc).timestamp() * 1000)
        # Tolerate APIs that stamp the candle close time instead of open time
        end_ms_tolerant = end_ms + _CANDLE_INTERVAL_MS

        candles: List[dict] = []
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
            return candles, None

        if parsed_timestamps:
            first = _ms_to_ist(min(parsed_timestamps))
            last  = _ms_to_ist(max(parsed_timestamps))
            return [], (
                f"{len(rows)} row(s) returned but none matched gap window "
                f"(response covered {first.strftime('%Y-%m-%d %H:%M')}→"
                f"{last.strftime('%H:%M')} IST — broker may not have captured "
                f"this period)"
            )

        return [], f"{len(rows)} row(s) returned but none had a readable timestamp"

    def _fetch_symbol_window(
        self,
        symbol: str,
        exchange: str,
        window_start: datetime,
        window_end: datetime,
    ) -> Tuple[List[dict], Optional[str]]:
        """
        Single attempt, no internal retry — retrying across attempts is now
        the outer round-based loop's job (see run()), so this just makes
        one call and reports what happened.

        Returns (candles, failure_reason). failure_reason is None on
        success; on failure it's a short human-readable string the caller
        stashes for the final consolidated WARN if the symbol is still
        failing after all rounds. Nothing is printed here — printing per
        attempt is exactly the noise this refactor removes.
        """
        rows, err = self._fetch_raw_rows(symbol, exchange, window_start.date(), window_end.date())
        if not rows:
            return [], err
        return self._filter_window(rows, symbol, exchange, window_start, window_end)

    def _fetch_symbol_windows_merged(
        self,
        symbol: str,
        exchange: str,
        windows: List[HistoryWindow],
    ) -> Dict[HistoryWindow, Tuple[List[dict], Optional[str]]]:
        """
        One HTTP call spanning every window's combined date range, then
        sliced back out per window — see _group_windows_for_merge for when
        this gets used instead of one _fetch_symbol_window call per window.
        """
        range_start_date = min(w[0].date() for w in windows)
        range_end_date   = max(w[1].date() for w in windows)
        rows, err = self._fetch_raw_rows(symbol, exchange, range_start_date, range_end_date)

        results: Dict[HistoryWindow, Tuple[List[dict], Optional[str]]] = {}
        if not rows:
            for w in windows:
                results[w] = ([], err)
            return results

        for w in windows:
            window_start, window_end = w
            results[w] = self._filter_window(rows, symbol, exchange, window_start, window_end)
        return results


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

    Must be called BEFORE the live SQLite writer (TickWriter, prefix="quote")
    is shut down for the session — pass its still-running instance in via
    `quote_writer` so this can enqueue through it normally rather than
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

    async def run(self, quote_writer, pg_configured: bool) -> None:
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
            await asyncio.to_thread(self._run_once, quote_writer, pg_configured)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[DAILY_CLOSE][ERROR] {exc}", flush=True)

    def _run_once(self, quote_writer, pg_configured: bool) -> None:
        pg_writer: Optional[PgWriter] = None
        if pg_configured:
            pg_writer = PgWriter(
                table="daily",
                dbname=self.history_dbname,
                flush_batch_size=self.flush_batch_size,
                flush_interval_sec=self.flush_interval_sec,
                dedup_on_timestamp=True,
            )

        # Round-based retry — same shape as BackfillManager.run(): each
        # round makes one pass over whatever's still pending, pacing calls
        # FETCH_PACING_SEC apart. A symbol that succeeds is done for good;
        # one that fails goes into the next round's pending set, up to
        # MAX_BACKFILL_ROUNDS total.
        try:
            total_rows = 0
            pending: Dict[str, str] = {
                str(s["symbol"]).upper(): str(s.get("exchange") or "NSE").upper()
                for s in self.symbols
            }
            last_failure_reason: Dict[str, str] = {}
            round_num = 0

            while pending and round_num < MAX_BACKFILL_ROUNDS:
                round_num += 1
                round_total = len(pending)
                if round_num == 1:
                    print(
                        f"[DAILY_CLOSE] round {round_num}/{MAX_BACKFILL_ROUNDS} — "
                        f"sweeping {round_total} symbol(s)",
                        flush=True,
                    )
                else:
                    print(
                        f"[DAILY_CLOSE] round {round_num}/{MAX_BACKFILL_ROUNDS} — "
                        f"retrying {round_total} symbol(s) that failed round "
                        f"{round_num - 1}",
                        flush=True,
                    )

                still_pending: Dict[str, str] = {}
                round_success = 0
                checked = 0

                for symbol, exchange in pending.items():
                    checked += 1
                    row, failure_reason = self._fetch_daily_close_row(symbol, exchange)
                    if row is not None:
                        if pg_writer is not None:
                            pg_writer.enqueue(symbol, row)
                        quote_writer.enqueue(symbol, row)
                        total_rows += 1
                        last_failure_reason.pop(symbol, None)
                        round_success += 1
                    else:
                        still_pending[symbol] = exchange
                        last_failure_reason[symbol] = failure_reason or "0 rows returned"

                    _progress_write(
                        f"[DAILY_CLOSE] round {round_num}/{MAX_BACKFILL_ROUNDS} | "
                        f"{checked}/{round_total} | {exchange}:{symbol} "
                        f"| {total_rows} written so far",
                        index=checked,
                        total=round_total,
                    )
                    time.sleep(FETCH_PACING_SEC)

                _progress_done()
                pending = still_pending

                if pending:
                    print(
                        f"[DAILY_CLOSE] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                        f"success={round_success} pending={len(pending)} "
                        f"→ retrying pending in round {round_num + 1}"
                        if round_num < MAX_BACKFILL_ROUNDS
                        else f"[DAILY_CLOSE] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                             f"success={round_success} pending={len(pending)}",
                        flush=True,
                    )
                else:
                    print(
                        f"[DAILY_CLOSE] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                        f"success={round_success} pending=0 → all recovered, stopping early",
                        flush=True,
                    )

            print(
                f"[DAILY_CLOSE] wrote {total_rows}/{len(self.symbols)} daily close(s)",
                flush=True,
            )

            if pending:
                names = ", ".join(
                    f"{exchange}:{symbol} ({last_failure_reason.get(symbol, 'unknown')})"
                    for symbol, exchange in pending.items()
                )
                print(
                    f"[DAILY_CLOSE][WARN] {len(pending)} symbol(s) still failing "
                    f"after {round_num} round(s): {names}",
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
        # This class's whole contract is "these are past, finalized trading
        # days" (see run_backfill's docstring) — the broker has no daily
        # candle for today until after 15:30 IST close, so fetching it
        # earlier just fails every time and retries forever. Rather than
        # trust every caller to always filter today out correctly
        # (start_data.py's startup call passes _last_n_trading_days(now, n),
        # which — correctly, for the tick-level BackfillManager it's also
        # used for — includes today the moment it's a trading day, closed
        # or not), enforce the invariant here, once, for every caller.
        now = now_kolkata()
        today_closed = now.time() >= MARKET_CLOSE
        if not today_closed:
            before = len(required_days)
            required_days = [d for d in required_days if d != now.date()]
            if len(required_days) != before:
                print(
                    f"[DAILY_CLOSE][BACKFILL] excluding {now.date()} — "
                    f"market hasn't closed yet today, no daily candle exists to fetch",
                    flush=True,
                )
        if not required_days:
            return

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
        present = 0

        # Which days each symbol is actually missing, computed once up
        # front — the same "skip what's already there" step as before,
        # just building the round loop's initial pending set instead of
        # feeding straight into a single flat pass.
        pending: Dict[str, Tuple[str, List[date]]] = {}
        try:
            for symbol_row in self.symbols:
                symbol   = str(symbol_row["symbol"]).upper()
                exchange = str(symbol_row.get("exchange") or "NSE").upper()
                missing_days = _missing_daily_days(conn, symbol, required_days)
                present += len(required_days) - len(missing_days)
                if missing_days:
                    pending[symbol] = (exchange, missing_days)
        except Exception as exc:
            print(f"[DAILY_CLOSE][BACKFILL][ERROR] {exc}", flush=True)
            pg_writer.shutdown()
            conn.close()
            return

        # Round-based retry — same shape as BackfillManager.run(). A
        # symbol is retried as a whole (all its missing days together) if
        # ANY of them failed that round — already-written days simply get
        # queued again next round, harmless since PgWriter dedupes on
        # timestamp, same rationale as the tick-level backfill.
        try:
            total_rows = 0
            last_failure_reason: Dict[str, str] = {}
            round_num = 0

            while pending and round_num < MAX_BACKFILL_ROUNDS:
                round_num += 1
                round_total = len(pending)
                if round_num == 1:
                    print(
                        f"[DAILY_CLOSE][BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} — "
                        f"sweeping {round_total} symbol(s)",
                        flush=True,
                    )
                else:
                    print(
                        f"[DAILY_CLOSE][BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} — "
                        f"retrying {round_total} symbol(s) that failed round "
                        f"{round_num - 1}",
                        flush=True,
                    )

                still_pending: Dict[str, Tuple[str, List[date]]] = {}
                round_success = 0
                checked = 0

                for symbol, (exchange, missing_days) in pending.items():
                    checked += 1
                    symbol_failed = False

                    for day in missing_days:
                        row, failure_reason = self._fetch_daily_close_row(
                            symbol, exchange, day=day, source_tag="daily_close_backfill"
                        )
                        if row is not None:
                            pg_writer.enqueue(symbol, row)
                            total_rows += 1
                        else:
                            symbol_failed = True
                            last_failure_reason[symbol] = failure_reason or "0 rows returned"
                        time.sleep(FETCH_PACING_SEC)

                    if symbol_failed:
                        still_pending[symbol] = (exchange, missing_days)
                    else:
                        last_failure_reason.pop(symbol, None)
                        round_success += 1

                    _progress_write(
                        f"[DAILY_CLOSE][BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} | "
                        f"{checked}/{round_total} | {exchange}:{symbol} "
                        f"| {total_rows} written so far",
                        index=checked,
                        total=round_total,
                    )

                _progress_done()
                pending = still_pending

                if pending:
                    print(
                        f"[DAILY_CLOSE][BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                        f"success={round_success} pending={len(pending)} "
                        f"→ retrying pending in round {round_num + 1}"
                        if round_num < MAX_BACKFILL_ROUNDS
                        else f"[DAILY_CLOSE][BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                             f"success={round_success} pending={len(pending)}",
                        flush=True,
                    )
                else:
                    print(
                        f"[DAILY_CLOSE][BACKFILL] round {round_num}/{MAX_BACKFILL_ROUNDS} done — "
                        f"success={round_success} pending=0 → all recovered, stopping early",
                        flush=True,
                    )

            print(
                f"[DAILY_CLOSE][BACKFILL] done — {total_rows} written, {present} "
                f"already present ({total_symbols} symbol(s) x {len(required_days)} day(s))",
                flush=True,
            )

            if pending:
                names = ", ".join(
                    f"{exchange}:{symbol} ({last_failure_reason.get(symbol, 'unknown')})"
                    for symbol, (exchange, _days) in pending.items()
                )
                print(
                    f"[DAILY_CLOSE][BACKFILL][WARN] {len(pending)} symbol(s) still "
                    f"failing after {round_num} round(s): {names}",
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
    ) -> Tuple[Optional[dict], Optional[str]]:
        """
        Single attempt, no internal retry — retrying across attempts is now
        the outer round-based loop's job (see backfill_missing_days /
        _run_once), matching BackfillManager._fetch_symbol_window. Nothing
        is printed here; printing per attempt is exactly the noise that
        refactor removed for the tick-level backfill, and the same applies
        here.

        Returns (row, failure_reason). failure_reason is None only when
        row is not None.
        """
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
            return None, f"fetch error: {exc}"

        rows = _extract_candle_rows(payload)
        if not rows:
            return None, "0 rows returned"

        # Single-day request → normally exactly one row; if the API
        # ever returns more, the last one is the authoritative EOD row.
        candle = rows[-1]
        normalized = _normalize_candle(candle, symbol, exchange, "D")
        if normalized is None:
            return None, "no readable timestamp on returned candle"

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
        return normalized, None