"""Yahoo Finance live price fetcher.

Ported from _legacy_v1/custom_components/pp_reader/prices/yahooquery_provider.py.

Key design:
- yahooquery is a blocking library — ALL calls run in loop.run_in_executor().
- Chunk size is capped at CHUNK_SIZE (50) symbols per call.
- DNS errors are retried once with a short delay and logged once per host.
- Returns prices as float; callers convert to BIGINT (10^8 scaled) before storage.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from importlib import import_module
from typing import Any

__all__ = [
    "QuoteResult",
    "fetch_live_quotes",
    "has_import_error",
    "scale_price",
]

_LOGGER = logging.getLogger(__name__)

CHUNK_SIZE = 50

_YAHOOQUERY_IMPORT_ERROR = False
_YAHOO_DNS_ERROR_TOKENS = (
    "Could not resolve host: guce.yahoo.com",
    "Could not resolve host: consent.yahoo.com",
    "Could not resolve host: query2.finance.yahoo.com",
    "Could not resolve host: finance.yahoo.com",
)
_YAHOOQUERY_DNS_WARNED: set[str] = set()
_DNS_RETRY_DELAY = 0.4


@dataclass
class QuoteResult:
    """Normalized quote returned from Yahoo Finance."""

    symbol: str
    price: float
    previous_close: float | None
    currency: str | None
    volume: int | None
    market_cap: float | None
    high_52w: float | None
    low_52w: float | None
    dividend_yield: float | None
    ts: float
    source: str = "yahoo"


def has_import_error() -> bool:
    """Return True if yahooquery failed to import (lets callers disable the feature)."""
    return _YAHOOQUERY_IMPORT_ERROR


def scale_price(value: float | None) -> int | None:
    """Convert a float price to BIGINT 10^8-scaled integer, or None."""
    if value is None or value == 0:
        return None
    return round(float(value) * 1_000_000_00)


def _select_quote_timestamp(raw_quote: dict[str, Any], fallback_ts: float) -> float:
    for candidate in (raw_quote.get("regularMarketTime"), raw_quote.get("postMarketTime")):
        try:
            ts_val = float(candidate)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if ts_val > 0:
            return ts_val
    return fallback_ts


def _handle_yahoo_dns_error(exc: Exception) -> bool:
    message = str(exc)
    for token in _YAHOO_DNS_ERROR_TOKENS:
        if token in message:
            if token not in _YAHOOQUERY_DNS_WARNED:
                _LOGGER.warning(
                    "YahooQuery DNS error detected (%s). "
                    "Check network/DNS config; retrying once.",
                    token,
                )
                _YAHOOQUERY_DNS_WARNED.add(token)
            return True
    return False


def _fetch_quotes_blocking(symbols: list[str]) -> dict:
    """Blocking helper executed in executor thread.

    Returns raw .quotes dict from yahooquery or {} on any error.
    """
    global _YAHOOQUERY_IMPORT_ERROR  # noqa: PLW0603

    try:
        yahoo_module = import_module("yahooquery")
    except ImportError as exc:
        if not _YAHOOQUERY_IMPORT_ERROR:
            _LOGGER.debug("yahooquery import failed (feature disabled): %s", exc)
            _YAHOOQUERY_IMPORT_ERROR = True
        return {}

    ticker_factory = getattr(yahoo_module, "Ticker", None)
    if ticker_factory is None:
        if not _YAHOOQUERY_IMPORT_ERROR:
            _LOGGER.debug("yahooquery.Ticker not available (feature disabled)")
            _YAHOOQUERY_IMPORT_ERROR = True
        return {}

    # Defensive cap (caller should already chunk, this is a safety guardrail)
    if len(symbols) > CHUNK_SIZE:
        symbols = symbols[:CHUNK_SIZE]

    def _call_once() -> dict:
        tk = ticker_factory(symbols, asynchronous=False)
        return getattr(tk, "quotes", {}) or {}

    try:
        return _call_once()
    except Exception as exc:  # noqa: BLE001
        if _handle_yahoo_dns_error(exc):
            time.sleep(_DNS_RETRY_DELAY)
            try:
                return _call_once()
            except Exception as retry_exc:  # noqa: BLE001
                if not _handle_yahoo_dns_error(retry_exc):
                    _LOGGER.warning("yahooquery chunk fetch error on retry: %s", retry_exc)
                return {}
        _LOGGER.warning("yahooquery chunk fetch error: %s", exc)
        return {}


async def fetch_live_quotes(symbols: list[str]) -> dict[str, QuoteResult]:
    """Fetch live quotes for the given symbols from Yahoo Finance.

    Symbols are chunked at CHUNK_SIZE=50.  Returns only symbols with price > 0.
    Returns empty dict on import failure or network error (non-fatal).
    """
    if not symbols:
        return {}

    result: dict[str, QuoteResult] = {}

    # Process in chunks to stay within Yahoo rate limits
    chunks = [symbols[i : i + CHUNK_SIZE] for i in range(0, len(symbols), CHUNK_SIZE)]
    loop = asyncio.get_running_loop()

    for chunk in chunks:
        try:
            raw_quotes: dict = await loop.run_in_executor(None, _fetch_quotes_blocking, chunk)
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Unexpected error in yahooquery executor call", exc_info=True)
            continue

        if not raw_quotes:
            continue

        fetch_ts = time.time()
        for sym in chunk:
            data = raw_quotes.get(sym)
            if not data:
                _LOGGER.debug("yahooquery: no data for symbol=%s", sym)
                continue

            price = data.get("regularMarketPrice")
            if price is None or price <= 0:
                _LOGGER.debug("yahooquery: skip symbol=%s invalid price=%s", sym, price)
                continue

            result[sym] = QuoteResult(
                symbol=sym,
                price=float(price),
                previous_close=data.get("regularMarketPreviousClose"),
                currency=data.get("currency"),
                volume=data.get("regularMarketVolume"),
                market_cap=data.get("marketCap"),
                high_52w=data.get("fiftyTwoWeekHigh"),
                low_52w=data.get("fiftyTwoWeekLow"),
                dividend_yield=data.get("trailingAnnualDividendYield"),
                ts=_select_quote_timestamp(data, fetch_ts),
            )
            _LOGGER.debug(
                "yahooquery: accept symbol=%s price=%s currency=%s",
                sym,
                price,
                result[sym].currency,
            )

        if _LOGGER.isEnabledFor(logging.DEBUG):
            skipped = [s for s in chunk if s not in result]
            if skipped:
                _LOGGER.debug(
                    "yahooquery chunk summary: skipped=%s accepted=%s/%s",
                    skipped,
                    len(result),
                    len(chunk),
                )

    return result
