"""Fetch ECB exchange rates and persist to the fx_rates table.

Uses the ECB SDMX-JSON Data API (EXR endpoint) to fetch historical
exchange rates against EUR for non-EUR currencies present in the
canonical securities and accounts tables.

All rates are stored as BIGINT scaled by EIGHT_DECIMAL_SCALE (10^8)
per Architecture Decision 3 — never as REAL/FLOAT.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import httpx

from app.db.queries.fx import get_fx_rate, upsert_fx_rate
from app.models.constants import EIGHT_DECIMAL_SCALE

__all__ = [
    "discover_active_currencies",
    "ensure_fx_rates",
    "fetch_ecb_rates",
]

_LOGGER = logging.getLogger(__name__)

ECB_API_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
_ECB_ACCEPT = "application/vnd.sdmx.data+json;version=1.0.0-wd"
_ECB_SOURCE = "ecb"
_ECB_PROVIDER = "ecb.europa.eu"
_FETCH_RETRIES = 3
_FETCH_BACKOFF = 1.0  # seconds; doubled on each retry

_DISCOVER_CURRENCIES = """
    SELECT DISTINCT currency_code
    FROM (
        SELECT currency_code FROM securities
        WHERE currency_code IS NOT NULL
          AND currency_code != ''
          AND currency_code != 'EUR'
        UNION
        SELECT currency_code FROM accounts
        WHERE currency_code IS NOT NULL
          AND currency_code != ''
          AND currency_code != 'EUR'
    ) AS combined
"""


# ---------------------------------------------------------------------------
# SDMX-JSON parser
# ---------------------------------------------------------------------------


def _parse_ecb_sdmx_response(data: Any) -> dict[str, dict[str, int]]:
    """Parse an ECB SDMX-JSON response into {date_str: {currency: rate_scaled}}.

    ECB rates are expressed as units of foreign currency per 1 EUR.
    Each rate float is multiplied by EIGHT_DECIMAL_SCALE and rounded to
    produce the BIGINT value stored in the database.

    Weekends and holidays are absent from the feed — gaps are normal.
    Returns an empty dict on any parse failure.
    """
    result: dict[str, dict[str, int]] = {}
    if not isinstance(data, dict):
        return result

    try:
        dims = data.get("structure", {}).get("dimensions", {})
        series_dims = dims.get("series", [])
        obs_dims = dims.get("observation", [])

        # Locate the CURRENCY dimension among series dimensions.
        curr_idx = next(
            (i for i, d in enumerate(series_dims) if d["id"] == "CURRENCY"),
            None,
        )
        if curr_idx is None:
            _LOGGER.debug("ECB SDMX response missing CURRENCY dimension")
            return result

        currencies_map: dict[int, str] = {
            i: v["id"] for i, v in enumerate(series_dims[curr_idx]["values"])
        }

        # Locate the TIME_PERIOD dimension among observation dimensions.
        time_dim = next((d for d in obs_dims if d["id"] == "TIME_PERIOD"), None)
        if time_dim is None:
            _LOGGER.debug("ECB SDMX response missing TIME_PERIOD dimension")
            return result

        dates_map: dict[str, str] = {
            str(i): v["id"] for i, v in enumerate(time_dim["values"])
        }

        data_sets = data.get("dataSets", [])
        if not data_sets:
            return result

        for key, series_data in data_sets[0].get("series", {}).items():
            parts = key.split(":")
            if len(parts) <= curr_idx:
                continue
            currency = currencies_map.get(int(parts[curr_idx]))
            if not currency:
                continue

            for obs_idx, obs_vals in series_data.get("observations", {}).items():
                date_str = dates_map.get(str(obs_idx))
                if date_str and obs_vals:
                    try:
                        rate_scaled = round(float(obs_vals[0]) * EIGHT_DECIMAL_SCALE)
                        result.setdefault(date_str, {})[currency] = rate_scaled
                    except (ValueError, TypeError):
                        pass

    except Exception:
        _LOGGER.exception("Unexpected error parsing ECB SDMX-JSON response")

    return result


# ---------------------------------------------------------------------------
# ECB API fetch
# ---------------------------------------------------------------------------


async def fetch_ecb_rates(
    currency: str,
    start: date,
    end: date,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[date, int]:
    """Fetch ECB exchange rates for one currency over a date range.

    Returns {date: rate_scaled} for trading days that have data.
    Weekend and holiday gaps are normal and are not treated as errors.

    Retries up to _FETCH_RETRIES times with exponential backoff on
    network errors or non-200 HTTP status codes.

    Parameters
    ----------
    currency:
        ISO 4217 currency code (e.g. "USD", "GBP").
    start, end:
        Inclusive date range.
    client:
        Optional pre-existing httpx.AsyncClient; if None, one is created
        and closed within this call.
    """
    url = f"{ECB_API_URL}/D.{currency}.EUR.SP00.A"
    params = {
        "startPeriod": start.isoformat(),
        "endPeriod": end.isoformat(),
        "detail": "dataonly",
    }
    headers = {"Accept": _ECB_ACCEPT}

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=15.0)

    result: dict[date, int] = {}
    try:
        for attempt in range(1, _FETCH_RETRIES + 1):
            try:
                resp = await client.get(url, params=params, headers=headers)

                if resp.status_code == 404:
                    # No data for this currency/range — normal for niche currencies.
                    return result

                if resp.status_code != 200:
                    _LOGGER.warning(
                        "ECB API returned HTTP %d for %s %s..%s (attempt %d/%d)",
                        resp.status_code,
                        currency,
                        start,
                        end,
                        attempt,
                        _FETCH_RETRIES,
                    )
                    if attempt < _FETCH_RETRIES:
                        await asyncio.sleep(_FETCH_BACKOFF * (2 ** (attempt - 1)))
                    continue

                if not resp.content:
                    # ECB returns 200 with empty body for some holiday periods.
                    return result

                parsed = _parse_ecb_sdmx_response(resp.json())
                for date_str, rates_by_currency in parsed.items():
                    if currency in rates_by_currency:
                        try:
                            result[date.fromisoformat(date_str)] = rates_by_currency[currency]
                        except ValueError:
                            pass
                return result

            except httpx.TransportError as exc:
                _LOGGER.warning(
                    "Network error fetching ECB rates for %s (attempt %d/%d): %s",
                    currency,
                    attempt,
                    _FETCH_RETRIES,
                    exc,
                )
                if attempt < _FETCH_RETRIES:
                    await asyncio.sleep(_FETCH_BACKOFF * (2 ** (attempt - 1)))

    finally:
        if own_client:
            await client.aclose()

    return result


# ---------------------------------------------------------------------------
# Currency discovery
# ---------------------------------------------------------------------------


async def discover_active_currencies(conn: Any) -> set[str]:
    """Return the set of non-EUR currencies referenced in canonical tables.

    Queries both ``securities.currency_code`` and
    ``accounts.currency_code``, excluding NULL, empty, and EUR values.
    """
    rows = await conn.fetch(_DISCOVER_CURRENCIES)
    return {row["currency_code"] for row in rows if row["currency_code"]}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def ensure_fx_rates(
    conn: Any,
    currencies: set[str],
    dates: set[date],
) -> None:
    """Ensure FX rates exist for every (currency, date) combination.

    For each non-EUR currency, identifies dates without a stored rate,
    fetches the required range from the ECB API, and upserts the results.
    Weekend and public-holiday gaps in ECB data are tolerated.

    Parameters
    ----------
    conn:
        asyncpg connection (or compatible mock).
    currencies:
        ISO 4217 currency codes to cover.  EUR is skipped automatically.
    dates:
        Calendar dates that must be covered.  Future dates are skipped.
    """
    if not currencies or not dates:
        return

    non_eur = {c.upper() for c in currencies if c and c.upper() != "EUR"}
    if not non_eur:
        return

    today = datetime.now(UTC).date()
    sorted_dates = sorted(d for d in dates if d <= today)
    if not sorted_dates:
        return

    fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat()

    async with httpx.AsyncClient(timeout=15.0) as client:
        for currency in sorted(non_eur):
            # Identify which dates are missing for this currency.
            missing = [
                d for d in sorted_dates
                if await get_fx_rate(conn, currency, d) is None
            ]
            if not missing:
                _LOGGER.debug("All FX rates already present for %s", currency)
                continue

            start, end = missing[0], missing[-1]
            _LOGGER.info(
                "Fetching ECB rates for %s %s..%s (%d missing dates)",
                currency,
                start,
                end,
                len(missing),
            )

            fetched = await fetch_ecb_rates(currency, start, end, client=client)
            if not fetched:
                _LOGGER.warning(
                    "No ECB rates returned for %s %s..%s", currency, start, end
                )
                continue

            provenance = json.dumps(
                {
                    "source": _ECB_SOURCE,
                    "range_start": start.isoformat(),
                    "range_end": end.isoformat(),
                },
                sort_keys=True,
            )
            for day, rate_scaled in fetched.items():
                await upsert_fx_rate(
                    conn,
                    currency,
                    day,
                    rate_scaled,
                    fetched_at=fetched_at,
                    data_source=_ECB_SOURCE,
                    provider=_ECB_PROVIDER,
                    provenance=provenance,
                )

            _LOGGER.info("Upserted %d FX rates for %s", len(fetched), currency)
