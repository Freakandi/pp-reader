"""Tests for Phase 6 — Enrichment: Prices.

All Yahoo Finance calls are mocked — no network access.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.enrichment.history import (
    HistoryCandle,
    _coerce_date,
    _coerce_float,
    _normalize_history,
    _resolve_symbol,
    _scale_price,
    plan_history_jobs,
    process_pending_jobs,
)
from app.enrichment.prices import (
    QuoteResult,
    _handle_yahoo_dns_error,
    _select_quote_timestamp,
    fetch_live_quotes,
    scale_price,
)

# ---------------------------------------------------------------------------
# scale_price helpers
# ---------------------------------------------------------------------------


class TestScalePrice:
    def test_normal_price(self):
        # €45.67 → 4567000000
        assert scale_price(45.67) == 4_567_000_000

    def test_none_returns_none(self):
        assert scale_price(None) is None

    def test_zero_returns_none(self):
        assert scale_price(0) is None

    def test_small_price(self):
        # 0.01 → 1_000_000
        assert scale_price(0.01) == 1_000_000

    def test_history_scale_price(self):
        assert _scale_price(45.67) == 4_567_000_000

    def test_history_scale_price_none(self):
        assert _scale_price(None) is None


# ---------------------------------------------------------------------------
# _select_quote_timestamp
# ---------------------------------------------------------------------------


class TestSelectQuoteTimestamp:
    def test_regular_market_time_used(self):
        raw = {"regularMarketTime": 1700000000.0}
        result = _select_quote_timestamp(raw, 9999.0)
        assert result == 1700000000.0

    def test_fallback_when_missing(self):
        result = _select_quote_timestamp({}, 1234.0)
        assert result == 1234.0

    def test_post_market_time_fallback(self):
        raw = {"regularMarketTime": None, "postMarketTime": 1700000001.0}
        result = _select_quote_timestamp(raw, 9999.0)
        assert result == 1700000001.0

    def test_zero_ts_uses_fallback(self):
        raw = {"regularMarketTime": 0}
        result = _select_quote_timestamp(raw, 5000.0)
        assert result == 5000.0


# ---------------------------------------------------------------------------
# _handle_yahoo_dns_error
# ---------------------------------------------------------------------------


class TestHandleYahooDnsError:
    def test_known_dns_token(self):
        exc = Exception("Could not resolve host: query2.finance.yahoo.com")
        assert _handle_yahoo_dns_error(exc) is True

    def test_unknown_error(self):
        exc = Exception("Connection refused")
        assert _handle_yahoo_dns_error(exc) is False


# ---------------------------------------------------------------------------
# fetch_live_quotes — mocked
# ---------------------------------------------------------------------------


def _make_raw_quotes(symbol: str, price: float = 100.0) -> dict:
    return {
        symbol: {
            "regularMarketPrice": price,
            "regularMarketPreviousClose": price - 1.0,
            "currency": "USD",
            "regularMarketVolume": 1_000_000,
            "marketCap": None,
            "fiftyTwoWeekHigh": price + 10.0,
            "fiftyTwoWeekLow": price - 10.0,
            "trailingAnnualDividendYield": None,
            "regularMarketTime": 1700000000.0,
        }
    }


class TestFetchLiveQuotes:
    @pytest.mark.asyncio
    async def test_returns_quote_for_valid_symbol(self):
        with patch(
            "app.enrichment.prices._fetch_quotes_blocking",
            return_value=_make_raw_quotes("AAPL", 175.0),
        ):
            result = await fetch_live_quotes(["AAPL"])

        assert "AAPL" in result
        q = result["AAPL"]
        assert isinstance(q, QuoteResult)
        assert q.price == 175.0
        assert q.currency == "USD"
        assert q.ts == 1700000000.0

    @pytest.mark.asyncio
    async def test_empty_symbols_returns_empty(self):
        result = await fetch_live_quotes([])
        assert result == {}

    @pytest.mark.asyncio
    async def test_zero_price_skipped(self):
        raw = {"AAPL": {"regularMarketPrice": 0}}
        with patch("app.enrichment.prices._fetch_quotes_blocking", return_value=raw):
            result = await fetch_live_quotes(["AAPL"])
        assert result == {}

    @pytest.mark.asyncio
    async def test_negative_price_skipped(self):
        raw = {"AAPL": {"regularMarketPrice": -1.5}}
        with patch("app.enrichment.prices._fetch_quotes_blocking", return_value=raw):
            result = await fetch_live_quotes(["AAPL"])
        assert result == {}

    @pytest.mark.asyncio
    async def test_executor_exception_returns_empty(self):
        async def _raise(*_a, **_kw):
            raise RuntimeError("executor error")

        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=RuntimeError)
            result = await fetch_live_quotes(["AAPL"])
        assert result == {}

    @pytest.mark.asyncio
    async def test_multiple_symbols(self):
        raw = {
            **_make_raw_quotes("AAPL", 175.0),
            **_make_raw_quotes("MSFT", 300.0),
        }
        with patch("app.enrichment.prices._fetch_quotes_blocking", return_value=raw):
            result = await fetch_live_quotes(["AAPL", "MSFT"])

        assert set(result.keys()) == {"AAPL", "MSFT"}
        assert result["MSFT"].price == 300.0

    @pytest.mark.asyncio
    async def test_chunking_at_50(self):
        """More than 50 symbols → multiple executor calls."""
        symbols = [f"S{i:03d}" for i in range(55)]
        call_count = 0

        def _blocking(chunk):
            nonlocal call_count
            call_count += 1
            return _make_raw_quotes(chunk[0], 10.0)

        with patch("app.enrichment.prices._fetch_quotes_blocking", side_effect=_blocking):
            result = await fetch_live_quotes(symbols)

        assert call_count == 2  # 50 + 5
        assert len(result) == 2  # only first symbol of each chunk gets a price


# ---------------------------------------------------------------------------
# _resolve_symbol
# ---------------------------------------------------------------------------


class TestResolveSymbol:
    def test_ticker_symbol(self):
        sym, src = _resolve_symbol("AAPL")
        assert sym == "AAPL"
        assert src == "ticker_symbol"

    def test_online_id_preferred_over_ticker(self):
        sym, src = _resolve_symbol("AAPL", online_id="MSFT")
        assert sym == "MSFT"
        assert src == "online_id"

    def test_properties_preferred_over_online_id(self):
        sym, src = _resolve_symbol("AAPL", online_id="MSFT", properties={"Yahoo Symbol": "GOOG"})
        assert sym == "GOOG"

    def test_hash_in_symbol_strips_prefix(self):
        sym, _ = _resolve_symbol("XETRA#BMW.DE")
        assert sym == "BMW.DE"

    def test_colon_in_symbol_strips_exchange(self):
        sym, _ = _resolve_symbol("XETRA:BMW.DE")
        assert sym == "BMW.DE"

    def test_empty_ticker_returns_none(self):
        sym, _ = _resolve_symbol("")
        assert sym is None

    def test_none_ticker_returns_none(self):
        sym, _ = _resolve_symbol(None)
        assert sym is None


# ---------------------------------------------------------------------------
# _coerce_date / _coerce_float
# ---------------------------------------------------------------------------


class TestCoerceHelpers:
    def test_coerce_date_from_date(self):
        d = date(2024, 1, 15)
        assert _coerce_date(d) == d

    def test_coerce_date_from_datetime(self):
        dt = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        assert _coerce_date(dt) == date(2024, 1, 15)

    def test_coerce_date_from_iso_string(self):
        assert _coerce_date("2024-01-15") == date(2024, 1, 15)

    def test_coerce_date_from_timestamp(self):
        # 2024-01-15 = 1705276800
        ts = 1705276800.0
        result = _coerce_date(ts)
        assert result == date(2024, 1, 15)

    def test_coerce_date_none(self):
        assert _coerce_date(None) is None

    def test_coerce_float_normal(self):
        assert _coerce_float(3.14) == 3.14

    def test_coerce_float_none(self):
        assert _coerce_float(None) is None

    def test_coerce_float_nan(self):
        assert _coerce_float(float("nan")) is None

    def test_coerce_float_string(self):
        assert _coerce_float("1.5") == 1.5


# ---------------------------------------------------------------------------
# _normalize_history
# ---------------------------------------------------------------------------


class TestNormalizeHistory:
    def test_none_returns_empty(self):
        assert _normalize_history("AAPL", None) == []

    def test_unknown_type_returns_empty(self):
        assert _normalize_history("AAPL", object()) == []

    def test_dict_single_candle(self):
        history = {
            "close": 150.0,
            "high": 155.0,
            "low": 145.0,
            "date": "2024-01-15",
        }
        candles = _normalize_history("AAPL", history)
        assert len(candles) == 1
        assert candles[0].close == 150.0
        assert candles[0].candle_date == date(2024, 1, 15)

    def test_dict_symbol_keyed(self):
        history = {
            "AAPL": {
                "close": 150.0,
                "date": "2024-01-15",
            }
        }
        candles = _normalize_history("AAPL", history)
        assert len(candles) == 1

    def test_zero_close_skipped(self):
        history = {"close": 0, "date": "2024-01-15"}
        candles = _normalize_history("AAPL", history)
        assert candles == []


# ---------------------------------------------------------------------------
# plan_history_jobs — mocked asyncpg conn
# ---------------------------------------------------------------------------


def _make_mock_conn(
    securities: list[dict] | None = None,
    latest_date: date | None = None,
    has_pending: bool = False,
) -> MagicMock:
    conn = MagicMock()
    conn.fetch = AsyncMock(
        side_effect=lambda q, *a: (
            asyncio.coroutine(lambda: securities or [])()
            if "securities" in q.lower() or "ticker_symbol" in q.lower()
            else asyncio.coroutine(lambda: [])()
        )
    )
    conn.fetchrow = AsyncMock(
        side_effect=lambda q, *a: (
            asyncio.coroutine(lambda: {"1": 1} if has_pending else None)()
            if "pending" in q.lower() or "running" in q.lower()
            else asyncio.coroutine(lambda: {0: latest_date} if latest_date else None)()
        )
    )
    conn.execute = AsyncMock(return_value=None)
    return conn


class TestPlanHistoryJobs:
    @pytest.mark.asyncio
    async def test_no_securities_returns_zero(self):
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        result = await plan_history_jobs(conn)
        assert result == 0

    @pytest.mark.asyncio
    async def test_enqueues_job_for_security_without_history(self):
        securities = [
            {"uuid": "sec-001", "ticker_symbol": "AAPL", "feed": None, "retired": False}
        ]
        conn = MagicMock()
        # get_securities_for_backfill returns our list
        conn.fetch = AsyncMock(return_value=securities)
        # pending_job_exists → None (no pending job)
        # get_latest_price_date → None (no history)
        # enqueue_history_job → returns id
        call_count = {"fetchrow": 0}

        async def _fetchrow(q, *args):
            call_count["fetchrow"] += 1
            if "pending" in q.lower() or "running" in q.lower():
                return None  # no pending job
            return None  # no latest date

        conn.fetchrow = AsyncMock(side_effect=_fetchrow)
        conn.execute = AsyncMock(return_value=None)

        with patch(
            "app.enrichment.history.enqueue_history_job",
            new_callable=AsyncMock,
            return_value=1,
        ):
            count = await plan_history_jobs(conn)

        assert count == 1

    @pytest.mark.asyncio
    async def test_skips_security_with_pending_job(self):
        securities = [
            {"uuid": "sec-001", "ticker_symbol": "AAPL", "feed": None, "retired": False}
        ]
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=securities)

        async def _fetchrow(q, *args):
            if "pending" in q.lower() or "running" in q.lower():
                return {"1": 1}  # pending job exists
            return None

        conn.fetchrow = AsyncMock(side_effect=_fetchrow)

        enqueue_mock = AsyncMock(return_value=1)
        with patch("app.enrichment.history.enqueue_history_job", enqueue_mock):
            count = await plan_history_jobs(conn)

        enqueue_mock.assert_not_called()
        assert count == 0

    @pytest.mark.asyncio
    async def test_skips_security_without_ticker(self):
        securities = [
            {"uuid": "sec-002", "ticker_symbol": None, "feed": None, "retired": False}
        ]
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=securities)
        conn.fetchrow = AsyncMock(return_value=None)

        enqueue_mock = AsyncMock(return_value=1)
        with patch("app.enrichment.history.enqueue_history_job", enqueue_mock):
            count = await plan_history_jobs(conn)

        enqueue_mock.assert_not_called()
        assert count == 0


# ---------------------------------------------------------------------------
# process_pending_jobs — mocked asyncpg conn
# ---------------------------------------------------------------------------


def _make_pending_job(job_id: int, symbol: str, uuid: str = "sec-001") -> dict:
    return {
        "id": job_id,
        "security_uuid": uuid,
        "status": "pending",
        "data_source": "yahoo",
        "provenance": {
            "symbol": symbol,
            "start": "2024-01-01",
            "end": "2024-01-31",
            "interval": "1d",
            "symbol_source": "ticker_symbol",
        },
    }


class TestProcessPendingJobs:
    @pytest.mark.asyncio
    async def test_no_pending_jobs_returns_zero(self):
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        result = await process_pending_jobs(conn)
        assert result == 0

    @pytest.mark.asyncio
    async def test_processes_job_with_candles(self):
        jobs = [_make_pending_job(1, "AAPL")]
        conn = MagicMock()
        conn.execute = AsyncMock(return_value=None)

        mock_candles = [
            HistoryCandle(
                symbol="AAPL",
                candle_date=date(2024, 1, 15),
                close=175.0,
                high=178.0,
                low=172.0,
            )
        ]

        with (
            patch("app.enrichment.history.get_pending_jobs", new_callable=AsyncMock, return_value=jobs),
            patch("app.enrichment.history.mark_job_running", new_callable=AsyncMock),
            patch("app.enrichment.history._fetch_history", new_callable=AsyncMock, return_value=mock_candles),
            patch("app.enrichment.history.upsert_historical_price", new_callable=AsyncMock),
            patch("app.enrichment.history.complete_job", new_callable=AsyncMock),
        ):
            result = await process_pending_jobs(conn)

        assert result == 1

    @pytest.mark.asyncio
    async def test_job_with_invalid_provenance_fails(self):
        jobs = [
            {
                "id": 99,
                "security_uuid": "sec-001",
                "status": "pending",
                "data_source": "yahoo",
                "provenance": "not-valid-json{{{",
            }
        ]

        complete_mock = AsyncMock()
        with (
            patch("app.enrichment.history.get_pending_jobs", new_callable=AsyncMock, return_value=jobs),
            patch("app.enrichment.history.mark_job_running", new_callable=AsyncMock),
            patch("app.enrichment.history.complete_job", complete_mock),
        ):
            result = await process_pending_jobs(MagicMock())

        complete_mock.assert_called_once()
        call_kwargs = complete_mock.call_args.kwargs
        assert call_kwargs["status"] == "failed"
        assert result == 0

    @pytest.mark.asyncio
    async def test_job_with_missing_symbol_fails(self):
        jobs = [
            {
                "id": 98,
                "security_uuid": "sec-001",
                "status": "pending",
                "data_source": "yahoo",
                "provenance": {"start": "2024-01-01", "end": "2024-01-31"},
            }
        ]

        complete_mock = AsyncMock()
        with (
            patch("app.enrichment.history.get_pending_jobs", new_callable=AsyncMock, return_value=jobs),
            patch("app.enrichment.history.complete_job", complete_mock),
        ):
            result = await process_pending_jobs(MagicMock())

        complete_mock.assert_called_once()
        assert complete_mock.call_args.kwargs["status"] == "failed"
        assert result == 0

    @pytest.mark.asyncio
    async def test_fetch_with_no_candles_marks_done_with_zero_success(self):
        jobs = [_make_pending_job(2, "UNKNOWN")]

        complete_mock = AsyncMock()
        with (
            patch("app.enrichment.history.get_pending_jobs", new_callable=AsyncMock, return_value=jobs),
            patch("app.enrichment.history.mark_job_running", new_callable=AsyncMock),
            patch("app.enrichment.history._fetch_history", new_callable=AsyncMock, return_value=[]),
            patch("app.enrichment.history.upsert_historical_price", new_callable=AsyncMock),
            patch("app.enrichment.history.complete_job", complete_mock),
        ):
            result = await process_pending_jobs(MagicMock())

        # Job is marked done (no error), but no candles → success_count = 0
        complete_mock.assert_called_once()
        assert complete_mock.call_args.kwargs["status"] == "done"
        assert result == 0


# ---------------------------------------------------------------------------
# DB query helpers — price scaling round-trip
# ---------------------------------------------------------------------------


class TestPriceScalingRoundTrip:
    """Verify 10^8 scaling is consistent between prices.py and history.py."""

    def test_scale_round_trip(self):
        price_float = 123.456789
        scaled = scale_price(price_float)
        assert scaled is not None
        restored = scaled / 1e8
        assert abs(restored - price_float) < 1e-4

    def test_1_euro_equals_100_million(self):
        assert scale_price(1.0) == 100_000_000

    def test_history_and_prices_agree(self):
        price = 45.0
        assert scale_price(price) == _scale_price(price)
