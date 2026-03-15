"""Historical price backfill via Yahoo Finance.

Ported from:
  _legacy_v1/custom_components/pp_reader/prices/history_queue.py
  _legacy_v1/custom_components/pp_reader/prices/history_ingest.py

Uses asyncpg (not sqlite3). All prices stored as BIGINT * 10^8.

Entry points:
  plan_history_jobs(conn)   — enqueue jobs for securities missing coverage
  process_pending_jobs(conn, max_jobs) — fetch Yahoo data and persist candles
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from typing import Any

from app.db.queries.prices import (
    complete_job,
    enqueue_history_job,
    get_latest_price_date,
    get_pending_jobs,
    get_securities_for_backfill,
    mark_job_running,
    pending_job_exists,
    upsert_historical_price,
)

__all__ = [
    "HistoryCandle",
    "HistoryJob",
    "plan_history_jobs",
    "process_pending_jobs",
]

_LOGGER = logging.getLogger(__name__)

_DEFAULT_LOOKBACK_DAYS = 365
_REFRESH_OVERLAP_DAYS = 30
DEFAULT_HISTORY_INTERVAL = "1d"
DEFAULT_MAX_CONCURRENCY = 8

_YAHOOQUERY_IMPORT_ERROR = False
_YAHOO_DNS_ERROR_TOKENS = (
    "Could not resolve host: guce.yahoo.com",
    "Could not resolve host: consent.yahoo.com",
    "Could not resolve host: query2.finance.yahoo.com",
    "Could not resolve host: finance.yahoo.com",
)
_YAHOOQUERY_DNS_WARNED: set[str] = set()
_DNS_RETRY_DELAY = 0.4


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HistoryJob:
    """A single historical price fetch request."""

    symbol: str
    start: date
    end: date
    interval: str = DEFAULT_HISTORY_INTERVAL


@dataclass(frozen=True)
class HistoryCandle:
    """Normalized OHLCV candle from Yahoo Finance."""

    symbol: str
    candle_date: date
    close: float
    high: float | None = None
    low: float | None = None
    open: float | None = None
    volume: float | None = None
    data_source: str = "yahoo"


# ---------------------------------------------------------------------------
# Symbol resolution
# ---------------------------------------------------------------------------


def _normalize_symbol_token(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    token = value.strip()
    if not token:
        return None
    if "#" in token:
        token = token.split("#", 1)[1]
    elif ":" in token:
        token = token.split(":", 1)[-1]
    token = token.strip()
    return token.upper() if token else None


def _symbol_from_properties(properties: Mapping[str, Any]) -> tuple[str | None, str]:
    for key, value in properties.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        lowered = key.lower()
        if "symbol" in lowered or "ticker" in lowered or "yahoo" in lowered:
            normalized = _normalize_symbol_token(value)
            if normalized:
                return normalized, f"property:{key}"
    return None, ""


def _resolve_symbol(
    ticker_symbol: str | None,
    online_id: str | None = None,
    properties: Mapping[str, Any] | None = None,
) -> tuple[str | None, str]:
    """4-priority fallback: properties → online_id → ticker_symbol → None."""
    if properties:
        sym, src = _symbol_from_properties(properties)
        if sym:
            return sym, src

    sym = _normalize_symbol_token(online_id)
    if sym:
        return sym, "online_id"

    sym = _normalize_symbol_token(ticker_symbol)
    if sym:
        return sym, "ticker_symbol"

    return None, ""


# ---------------------------------------------------------------------------
# Price scaling
# ---------------------------------------------------------------------------


def _scale_price(value: float | None) -> int | None:
    if value is None or value == 0:
        return None
    return round(float(value) * 1_000_000_00)


# ---------------------------------------------------------------------------
# Yahoo history fetcher (blocking, runs in executor)
# ---------------------------------------------------------------------------


def _handle_yahoo_dns_error(exc: Exception) -> bool:
    message = str(exc)
    for token in _YAHOO_DNS_ERROR_TOKENS:
        if token in message:
            if token not in _YAHOOQUERY_DNS_WARNED:
                _LOGGER.warning(
                    "YahooQuery history DNS error (%s). Retrying once.", token
                )
                _YAHOOQUERY_DNS_WARNED.add(token)
            return True
    return False


def _coerce_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        cast = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(cast):
        return None
    return cast


def _coerce_date(value: object) -> date | None:
    """Convert various yahooquery timestamp types to a Python date."""
    if value is None:
        return None

    # pandas Timestamp
    if hasattr(value, "to_pydatetime"):
        try:
            dt = value.to_pydatetime()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return None
        return dt.date() if hasattr(dt, "date") else None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC).date()
        except (OverflowError, ValueError):
            return None

    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except (ValueError, TypeError):
            return None

    return None


def _normalize_history(symbol: str, history: object) -> list[HistoryCandle]:
    """Convert yahooquery history return (DataFrame or dict) to HistoryCandle list."""
    if history is None:
        return []

    # DataFrame path (yahooquery usually returns pandas DataFrame)
    if hasattr(history, "empty"):
        try:
            if history.empty:  # type: ignore[attr-defined]
                return []
            frame = history.reset_index()  # type: ignore[call-arg]
        except Exception:  # noqa: BLE001
            _LOGGER.debug("DataFrame normalization failed", exc_info=True)
            return []

        columns = frame.columns  # type: ignore[attr-defined]

        def _get_col(name: str):  # type: ignore[return]
            if name in columns:
                return frame[name]  # type: ignore[index]
            return (None for _ in range(len(frame)))

        col_sym = frame["symbol"] if "symbol" in columns else (symbol for _ in range(len(frame)))  # type: ignore[union-attr]
        col_date = _get_col("date")
        col_close = _get_col("close")
        col_high = _get_col("high")
        col_low = _get_col("low")
        col_open = _get_col("open")
        col_volume = _get_col("volume")

        records: list[HistoryCandle] = []
        for s_val, d_val, c_val, h_val, l_val, o_val, v_val in zip(
            col_sym, col_date, col_close, col_high, col_low, col_open, col_volume,
            strict=False,
        ):
            if c_val in (None, 0):
                continue
            candle_date = _coerce_date(d_val)
            if candle_date is None:
                continue
            records.append(
                HistoryCandle(
                    symbol=str(s_val or symbol).upper(),
                    candle_date=candle_date,
                    close=float(c_val),
                    high=_coerce_float(h_val),
                    low=_coerce_float(l_val),
                    open=_coerce_float(o_val),
                    volume=_coerce_float(v_val),
                )
            )
        return records

    # Dict path
    if isinstance(history, dict):
        records = []
        quotes = history.get(symbol) if symbol in history else history
        if isinstance(quotes, dict) and "close" in quotes:
            d = _coerce_date(quotes.get("date"))
            c = quotes.get("close")
            if c not in (None, 0) and d is not None:
                records.append(
                    HistoryCandle(
                        symbol=symbol,
                        candle_date=d,
                        close=float(c),  # type: ignore[arg-type]
                        high=_coerce_float(quotes.get("high")),
                        low=_coerce_float(quotes.get("low")),
                    )
                )
            return records

        if isinstance(quotes, dict):
            for key, payload in quotes.items():
                if not isinstance(payload, dict):
                    continue
                row_sym = symbol
                if isinstance(key, tuple) and key:
                    row_sym = str(key[0]).upper()
                    ts_value = key[-1]
                else:
                    ts_value = payload.get("date")
                d = _coerce_date(ts_value)
                c = payload.get("close")
                if c in (None, 0) or d is None:
                    continue
                records.append(
                    HistoryCandle(
                        symbol=row_sym,
                        candle_date=d,
                        close=float(c),  # type: ignore[arg-type]
                        high=_coerce_float(payload.get("high")),
                        low=_coerce_float(payload.get("low")),
                    )
                )
        return records

    _LOGGER.debug("Unknown history result type: %s", type(history).__name__)
    return []


def _fetch_history_blocking(job: HistoryJob) -> object:
    """Blocking portion: import yahooquery, call ticker.history(). Runs in executor."""
    global _YAHOOQUERY_IMPORT_ERROR  # noqa: PLW0603

    try:
        yahoo_module = import_module("yahooquery")
    except ImportError as exc:
        if not _YAHOOQUERY_IMPORT_ERROR:
            _LOGGER.debug("yahooquery import failed: %s", exc)
            _YAHOOQUERY_IMPORT_ERROR = True
        return []

    ticker_factory = getattr(yahoo_module, "Ticker", None)
    if ticker_factory is None:
        if not _YAHOOQUERY_IMPORT_ERROR:
            _LOGGER.debug("yahooquery.Ticker not available")
            _YAHOOQUERY_IMPORT_ERROR = True
        return []

    start_str = job.start.strftime("%Y-%m-%d")
    # yahooquery end is exclusive — extend by 1 day to include the end date
    end_str = (job.end + timedelta(days=1)).strftime("%Y-%m-%d")
    ticker = None

    def _call_history() -> object:
        nonlocal ticker
        ticker = ticker or ticker_factory(job.symbol, asynchronous=False)
        return ticker.history(interval=job.interval, start=start_str, end=end_str)

    try:
        return _call_history()
    except Exception as exc:  # noqa: BLE001
        if _handle_yahoo_dns_error(exc):
            time.sleep(_DNS_RETRY_DELAY)
            try:
                return _call_history()
            except Exception as retry_exc:  # noqa: BLE001
                if not _handle_yahoo_dns_error(retry_exc):
                    _LOGGER.warning(
                        "yahooquery history fetch error for %s (%s-%s): %s",
                        job.symbol, start_str, end_str, retry_exc,
                    )
                return []
        _LOGGER.warning(
            "yahooquery history fetch error for %s (%s-%s): %s",
            job.symbol, start_str, end_str, exc,
        )
        return []


async def _fetch_history(job: HistoryJob) -> list[HistoryCandle]:
    """Async wrapper: run blocking fetch in executor, normalize result."""
    loop = asyncio.get_running_loop()
    raw = await loop.run_in_executor(None, _fetch_history_blocking, job)
    candles = _normalize_history(job.symbol, raw)
    _LOGGER.debug(
        "history fetch: %s candles for %s (%s → %s)",
        len(candles), job.symbol, job.start, job.end,
    )
    return candles


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------


async def plan_history_jobs(
    conn: Any,
    *,
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS,
    interval: str = DEFAULT_HISTORY_INTERVAL,
) -> int:
    """Enqueue history jobs for securities that lack full coverage.

    Returns the count of newly enqueued jobs.
    """
    today = datetime.now(UTC).date()
    history_end = today - timedelta(days=1)
    start_floor = history_end - timedelta(days=lookback_days - 1)

    securities = await get_securities_for_backfill(conn)
    if not securities:
        return 0

    enqueued = 0
    for sec in securities:
        uuid = sec["uuid"]
        ticker = sec.get("ticker_symbol")
        symbol, symbol_source = _resolve_symbol(ticker)
        if not symbol:
            continue

        # Skip if a pending/running job already exists
        if await pending_job_exists(conn, uuid):
            continue

        latest_date = await get_latest_price_date(conn, uuid)
        if latest_date is None:
            job_start = start_floor
        else:
            overlap = min(_REFRESH_OVERLAP_DAYS, lookback_days)
            job_start = max(start_floor, latest_date - timedelta(days=overlap - 1))

        if job_start > history_end:
            continue

        provenance = json.dumps(
            {
                "symbol": symbol,
                "start": job_start.isoformat(),
                "end": history_end.isoformat(),
                "interval": interval,
                "symbol_source": symbol_source or "ticker_symbol",
                "security_uuid": uuid,
            }
        )

        await enqueue_history_job(
            conn,
            uuid,
            history_end,
            provenance,
            data_source="yahoo",
        )
        enqueued += 1
        _LOGGER.debug("enqueued history job for %s (symbol=%s)", uuid, symbol)

    _LOGGER.info("plan_history_jobs: enqueued %s new jobs", enqueued)
    return enqueued


async def process_pending_jobs(conn: Any, max_jobs: int = 10) -> int:
    """Fetch and persist historical price data for pending queue entries.

    Returns the count of successfully processed jobs (jobs with ≥1 candles saved).
    """
    entries = await get_pending_jobs(conn, max_jobs)
    if not entries:
        return 0

    # Parse provenance and build HistoryJob objects
    jobs: dict[int, HistoryJob] = {}
    unresolved: dict[int, str] = {}

    for entry in entries:
        job_id = entry["id"]
        prov_raw = entry.get("provenance") or {}
        if isinstance(prov_raw, str):
            try:
                prov = json.loads(prov_raw)
            except json.JSONDecodeError:
                unresolved[job_id] = "invalid provenance JSON"
                continue
        else:
            prov = dict(prov_raw)

        symbol = prov.get("symbol")
        start_iso = prov.get("start")
        end_iso = prov.get("end")
        interval = prov.get("interval", DEFAULT_HISTORY_INTERVAL)

        if not symbol or not start_iso or not end_iso:
            unresolved[job_id] = "incomplete provenance"
            continue

        try:
            job_start = date.fromisoformat(start_iso)
            job_end = date.fromisoformat(end_iso)
        except (ValueError, TypeError) as exc:
            unresolved[job_id] = str(exc)
            continue

        if job_end < job_start:
            unresolved[job_id] = "end before start"
            continue

        await mark_job_running(conn, job_id)
        jobs[job_id] = HistoryJob(
            symbol=symbol, start=job_start, end=job_end, interval=interval
        )

    # Fail unresolvable jobs
    for job_id, error in unresolved.items():
        await complete_job(conn, job_id, status="failed", error=error)

    if not jobs:
        return 0

    # Fetch with bounded concurrency
    semaphore = asyncio.Semaphore(DEFAULT_MAX_CONCURRENCY)
    fetch_results: dict[int, list[HistoryCandle]] = {}

    async def _run(job_id: int, job: HistoryJob) -> None:
        async with semaphore:
            try:
                candles = await _fetch_history(job)
            except Exception:  # noqa: BLE001
                _LOGGER.warning(
                    "history fetch failed for job %s (%s)", job_id, job.symbol, exc_info=True
                )
                candles = []
            fetch_results[job_id] = candles

    await asyncio.gather(*(_run(jid, j) for jid, j in jobs.items()))

    # Persist candles and mark jobs complete
    success_count = 0
    for job_id, candles in fetch_results.items():
        job = jobs[job_id]
        # Find the security_uuid from the original entries list
        security_uuid = next(
            (e["security_uuid"] for e in entries if e["id"] == job_id), None
        )

        if candles and security_uuid:
            for candle in candles:
                close_scaled = _scale_price(candle.close)
                if close_scaled is None:
                    continue
                await upsert_historical_price(
                    conn,
                    security_uuid,
                    candle.candle_date,
                    close_scaled,
                    high_scaled=_scale_price(candle.high),
                    low_scaled=_scale_price(candle.low),
                    volume=int(candle.volume) if candle.volume is not None else None,
                    data_source="yahoo",
                    provider="yahooquery",
                    provenance=json.dumps({"symbol": candle.symbol}),
                )
            success_count += 1
            _LOGGER.debug(
                "persisted %s candles for job %s (%s)", len(candles), job_id, job.symbol
            )

        result_json = json.dumps({"candles": len(candles), "symbol": job.symbol})
        await complete_job(
            conn, job_id, status="done", result_json=result_json
        )

    _LOGGER.info(
        "process_pending_jobs: processed %s/%s jobs with data", success_count, len(jobs)
    )
    return success_count
