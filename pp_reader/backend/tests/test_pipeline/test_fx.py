"""Tests for Phase 5 — Enrichment: FX Rates.

Uses mocked asyncpg connections and mocked httpx clients — no network
calls or live database required.  All tests are deterministic.

Coverage:
  - _parse_ecb_sdmx_response: happy path, bad input, missing dimensions
  - fetch_ecb_rates: success, 404, empty body, transport retry
  - discover_active_currencies: normal, empty result
  - ensure_fx_rates: skips EUR, skips empty sets, skips present rates,
                     fetches and upserts missing rates
  - db/queries/fx helpers: upsert_fx_rate, get_fx_rate,
                            get_fx_rates_in_range, get_latest_fx_rate
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.db.queries.fx import (
    get_fx_rate,
    get_fx_rates_in_range,
    get_latest_fx_rate,
    upsert_fx_rate,
)
from app.enrichment.fx import (
    _parse_ecb_sdmx_response,
    discover_active_currencies,
    ensure_fx_rates,
    fetch_ecb_rates,
)
from app.models.constants import EIGHT_DECIMAL_SCALE

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USD_RATE_1 = 1.0941
_USD_RATE_2 = 1.0927
_DATE_1 = date(2024, 1, 2)
_DATE_2 = date(2024, 1, 3)


def _make_sdmx(
    currency: str = "USD",
    date_rates: dict[str, float] | None = None,
) -> dict:
    """Build a minimal but structurally correct ECB SDMX-JSON fixture."""
    if date_rates is None:
        date_rates = {"2024-01-02": _USD_RATE_1, "2024-01-03": _USD_RATE_2}
    dates_list = list(date_rates.keys())
    rates_list = list(date_rates.values())
    return {
        "structure": {
            "dimensions": {
                "series": [
                    {"id": "FREQ", "values": [{"id": "D"}]},
                    {"id": "CURRENCY", "values": [{"id": currency}]},
                    {"id": "CURRENCY_DENOM", "values": [{"id": "EUR"}]},
                    {"id": "EXR_TYPE", "values": [{"id": "SP00"}]},
                    {"id": "EXR_SUFFIX", "values": [{"id": "A"}]},
                ],
                "observation": [
                    {
                        "id": "TIME_PERIOD",
                        "values": [{"id": d} for d in dates_list],
                    }
                ],
            }
        },
        "dataSets": [
            {
                "series": {
                    "0:0:0:0:0": {
                        "observations": {
                            str(i): [rate, 0] for i, rate in enumerate(rates_list)
                        }
                    }
                }
            }
        ],
    }


def _make_conn(fetchrow_return=None, fetch_return=None) -> MagicMock:
    """Return a mock asyncpg connection."""
    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value=fetchrow_return)
    conn.fetch = AsyncMock(return_value=fetch_return or [])
    conn.execute = AsyncMock(return_value=None)
    return conn


def _make_http_resp(status: int = 200, body: dict | None = None) -> MagicMock:
    """Return a mock httpx Response."""
    resp = MagicMock()
    resp.status_code = status
    resp.content = b"..." if body is not None else b""
    resp.json = MagicMock(return_value=body or {})
    return resp


# ---------------------------------------------------------------------------
# _parse_ecb_sdmx_response
# ---------------------------------------------------------------------------


def test_parse_ecb_sdmx_happy_path() -> None:
    data = _make_sdmx("USD", {"2024-01-02": _USD_RATE_1, "2024-01-03": _USD_RATE_2})
    result = _parse_ecb_sdmx_response(data)

    assert "2024-01-02" in result
    assert "USD" in result["2024-01-02"]
    assert result["2024-01-02"]["USD"] == round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE)
    assert result["2024-01-03"]["USD"] == round(_USD_RATE_2 * EIGHT_DECIMAL_SCALE)


def test_parse_ecb_sdmx_not_dict() -> None:
    assert _parse_ecb_sdmx_response([]) == {}
    assert _parse_ecb_sdmx_response(None) == {}
    assert _parse_ecb_sdmx_response("bad") == {}


def test_parse_ecb_sdmx_empty_datasets() -> None:
    data = _make_sdmx("USD", {"2024-01-02": _USD_RATE_1})
    data["dataSets"] = []
    assert _parse_ecb_sdmx_response(data) == {}


def test_parse_ecb_sdmx_missing_currency_dim() -> None:
    data = _make_sdmx("USD", {"2024-01-02": _USD_RATE_1})
    # Remove CURRENCY from series dimensions
    data["structure"]["dimensions"]["series"] = [
        {"id": "FREQ", "values": [{"id": "D"}]}
    ]
    assert _parse_ecb_sdmx_response(data) == {}


def test_parse_ecb_sdmx_missing_time_period() -> None:
    data = _make_sdmx("USD", {"2024-01-02": _USD_RATE_1})
    data["structure"]["dimensions"]["observation"] = []
    assert _parse_ecb_sdmx_response(data) == {}


def test_parse_ecb_sdmx_multiple_currencies() -> None:
    data = {
        "structure": {
            "dimensions": {
                "series": [
                    {"id": "FREQ", "values": [{"id": "D"}]},
                    {"id": "CURRENCY", "values": [{"id": "USD"}, {"id": "GBP"}]},
                    {"id": "CURRENCY_DENOM", "values": [{"id": "EUR"}]},
                    {"id": "EXR_TYPE", "values": [{"id": "SP00"}]},
                    {"id": "EXR_SUFFIX", "values": [{"id": "A"}]},
                ],
                "observation": [
                    {"id": "TIME_PERIOD", "values": [{"id": "2024-01-02"}]},
                ],
            }
        },
        "dataSets": [
            {
                "series": {
                    "0:0:0:0:0": {"observations": {"0": [1.0941, 0]}},  # USD idx 0
                    "0:1:0:0:0": {"observations": {"0": [0.8573, 0]}},  # GBP idx 1
                }
            }
        ],
    }
    result = _parse_ecb_sdmx_response(data)
    assert result["2024-01-02"]["USD"] == round(1.0941 * EIGHT_DECIMAL_SCALE)
    assert result["2024-01-02"]["GBP"] == round(0.8573 * EIGHT_DECIMAL_SCALE)


def test_parse_ecb_sdmx_invalid_obs_value_skipped() -> None:
    data = _make_sdmx("USD", {"2024-01-02": _USD_RATE_1})
    # Corrupt the observation value
    data["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]["0"] = ["not-a-float", 0]
    result = _parse_ecb_sdmx_response(data)
    # Bad value skipped — date may be absent or currency absent
    assert result.get("2024-01-02", {}).get("USD") is None


# ---------------------------------------------------------------------------
# fetch_ecb_rates
# ---------------------------------------------------------------------------


async def test_fetch_ecb_rates_success() -> None:
    sdmx = _make_sdmx("USD", {"2024-01-02": _USD_RATE_1, "2024-01-03": _USD_RATE_2})
    resp = _make_http_resp(200, sdmx)

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=resp)

    result = await fetch_ecb_rates("USD", _DATE_1, _DATE_2, client=mock_client)

    assert _DATE_1 in result
    assert result[_DATE_1] == round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE)
    assert _DATE_2 in result
    assert result[_DATE_2] == round(_USD_RATE_2 * EIGHT_DECIMAL_SCALE)


async def test_fetch_ecb_rates_404_returns_empty() -> None:
    resp = _make_http_resp(404)
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=resp)

    result = await fetch_ecb_rates("USD", _DATE_1, _DATE_2, client=mock_client)
    assert result == {}


async def test_fetch_ecb_rates_empty_body_returns_empty() -> None:
    resp = MagicMock()
    resp.status_code = 200
    resp.content = b""

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=resp)

    result = await fetch_ecb_rates("USD", _DATE_1, _DATE_2, client=mock_client)
    assert result == {}


async def test_fetch_ecb_rates_retries_on_transport_error() -> None:
    import httpx

    sdmx = _make_sdmx("USD", {"2024-01-02": _USD_RATE_1})
    good_resp = _make_http_resp(200, sdmx)

    call_count = 0

    async def _get(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise httpx.TransportError("connection refused")
        return good_resp

    mock_client = AsyncMock()
    mock_client.get = _get

    with patch("app.enrichment.fx.asyncio.sleep", new_callable=AsyncMock):
        result = await fetch_ecb_rates("USD", _DATE_1, _DATE_1, client=mock_client)

    assert _DATE_1 in result
    assert call_count == 2  # failed once, succeeded on second attempt


async def test_fetch_ecb_rates_exhausts_retries_returns_empty() -> None:
    import httpx

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(
        side_effect=httpx.TransportError("unreachable")
    )

    with patch("app.enrichment.fx.asyncio.sleep", new_callable=AsyncMock):
        result = await fetch_ecb_rates("USD", _DATE_1, _DATE_1, client=mock_client)

    assert result == {}


async def test_fetch_ecb_rates_non_200_exhausts_retries() -> None:
    resp = _make_http_resp(503)

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=resp)

    with patch("app.enrichment.fx.asyncio.sleep", new_callable=AsyncMock):
        result = await fetch_ecb_rates("USD", _DATE_1, _DATE_1, client=mock_client)

    assert result == {}


# ---------------------------------------------------------------------------
# discover_active_currencies
# ---------------------------------------------------------------------------


async def test_discover_active_currencies_returns_set() -> None:
    conn = _make_conn(
        fetch_return=[{"currency_code": "USD"}, {"currency_code": "GBP"}]
    )
    result = await discover_active_currencies(conn)
    assert result == {"USD", "GBP"}


async def test_discover_active_currencies_empty_db() -> None:
    conn = _make_conn(fetch_return=[])
    result = await discover_active_currencies(conn)
    assert result == set()


async def test_discover_active_currencies_filters_none() -> None:
    conn = _make_conn(fetch_return=[{"currency_code": None}, {"currency_code": "USD"}])
    result = await discover_active_currencies(conn)
    assert result == {"USD"}


# ---------------------------------------------------------------------------
# ensure_fx_rates
# ---------------------------------------------------------------------------


async def test_ensure_fx_rates_skips_eur_only() -> None:
    conn = _make_conn()
    with patch("app.enrichment.fx.fetch_ecb_rates") as mock_fetch:
        await ensure_fx_rates(conn, {"EUR"}, {_DATE_1})
        mock_fetch.assert_not_called()


async def test_ensure_fx_rates_skips_empty_currencies() -> None:
    conn = _make_conn()
    with patch("app.enrichment.fx.fetch_ecb_rates") as mock_fetch:
        await ensure_fx_rates(conn, set(), {_DATE_1})
        mock_fetch.assert_not_called()


async def test_ensure_fx_rates_skips_empty_dates() -> None:
    conn = _make_conn()
    with patch("app.enrichment.fx.fetch_ecb_rates") as mock_fetch:
        await ensure_fx_rates(conn, {"USD"}, set())
        mock_fetch.assert_not_called()


async def test_ensure_fx_rates_skips_already_present() -> None:
    conn = _make_conn(fetchrow_return={"rate": round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE)})
    with patch("app.enrichment.fx.fetch_ecb_rates") as mock_fetch:
        with patch("app.enrichment.fx.httpx.AsyncClient") as mock_cls:
            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=AsyncMock())
            ctx.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = ctx

            await ensure_fx_rates(conn, {"USD"}, {_DATE_1})
        mock_fetch.assert_not_called()


async def test_ensure_fx_rates_fetches_and_upserts_missing() -> None:
    # All fetchrow calls return None → rate is missing.
    conn = _make_conn(fetchrow_return=None)

    fetched = {_DATE_1: round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE)}

    with patch("app.enrichment.fx.fetch_ecb_rates", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = fetched
        with patch("app.enrichment.fx.httpx.AsyncClient") as mock_cls:
            mock_instance = AsyncMock()
            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=mock_instance)
            ctx.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = ctx

            await ensure_fx_rates(conn, {"USD"}, {_DATE_1})

    # upsert_fx_rate delegates to conn.execute — verify it was called.
    conn.execute.assert_called_once()
    call_args = conn.execute.call_args[0]
    assert _DATE_1 in call_args
    assert round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE) in call_args


async def test_ensure_fx_rates_no_fetch_result_warns_but_does_not_raise() -> None:
    conn = _make_conn(fetchrow_return=None)

    with patch("app.enrichment.fx.fetch_ecb_rates", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = {}  # ECB returned nothing
        with patch("app.enrichment.fx.httpx.AsyncClient") as mock_cls:
            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=AsyncMock())
            ctx.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = ctx

            # Must not raise even when no rates are returned.
            await ensure_fx_rates(conn, {"USD"}, {_DATE_1})

    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# db/queries/fx — unit tests
# ---------------------------------------------------------------------------


async def test_upsert_fx_rate_calls_execute() -> None:
    conn = _make_conn()
    await upsert_fx_rate(
        conn,
        "USD",
        _DATE_1,
        round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE),
        fetched_at="2024-01-02T00:00:00",
        data_source="ecb",
        provider="ecb.europa.eu",
        provenance='{"source":"ecb"}',
    )
    conn.execute.assert_called_once()
    # Verify key parameters are forwarded to the SQL call.
    args = conn.execute.call_args[0]
    assert _DATE_1 in args
    assert "USD" in args
    assert round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE) in args


async def test_get_fx_rate_returns_scaled_int() -> None:
    expected = round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE)
    conn = _make_conn(fetchrow_return={"rate": expected})
    result = await get_fx_rate(conn, "USD", _DATE_1)
    assert result == expected


async def test_get_fx_rate_returns_none_when_missing() -> None:
    conn = _make_conn(fetchrow_return=None)
    result = await get_fx_rate(conn, "USD", _DATE_1)
    assert result is None


async def test_get_fx_rates_in_range_returns_tuples() -> None:
    rows = [
        {"date": _DATE_1, "currency": "USD", "rate": round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE)},
        {"date": _DATE_2, "currency": "USD", "rate": round(_USD_RATE_2 * EIGHT_DECIMAL_SCALE)},
    ]
    conn = _make_conn(fetch_return=rows)
    result = await get_fx_rates_in_range(conn, "USD", _DATE_1, _DATE_2)

    assert len(result) == 2
    assert result[0] == (_DATE_1, "USD", round(_USD_RATE_1 * EIGHT_DECIMAL_SCALE))
    assert result[1] == (_DATE_2, "USD", round(_USD_RATE_2 * EIGHT_DECIMAL_SCALE))


async def test_get_fx_rates_in_range_empty() -> None:
    conn = _make_conn(fetch_return=[])
    result = await get_fx_rates_in_range(conn, "USD", _DATE_1, _DATE_2)
    assert result == []


async def test_get_latest_fx_rate_found() -> None:
    conn = _make_conn(fetchrow_return={"date": _DATE_2, "rate": round(_USD_RATE_2 * EIGHT_DECIMAL_SCALE)})
    result = await get_latest_fx_rate(conn, "USD")
    assert result == (_DATE_2, round(_USD_RATE_2 * EIGHT_DECIMAL_SCALE))


async def test_get_latest_fx_rate_not_found() -> None:
    conn = _make_conn(fetchrow_return=None)
    result = await get_latest_fx_rate(conn, "USD")
    assert result is None
