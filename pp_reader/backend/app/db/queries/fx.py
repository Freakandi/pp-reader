"""Async database helpers for the fx_rates table.

All functions accept an asyncpg connection (or compatible mock) and use
positional parameters ($1, $2, …) as required by asyncpg.

Rates are stored as BIGINT scaled by EIGHT_DECIMAL_SCALE (10^8) per
Architecture Decision 3 — never as REAL/FLOAT.
"""

from __future__ import annotations

from datetime import date
from typing import Any

# ---------------------------------------------------------------------------
# SQL statements
# ---------------------------------------------------------------------------

_UPSERT = """
    INSERT INTO fx_rates (
        date, currency, rate, fetched_at, data_source, provider, provenance
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (date, currency) DO UPDATE
        SET rate        = EXCLUDED.rate,
            fetched_at  = EXCLUDED.fetched_at,
            data_source = EXCLUDED.data_source,
            provider    = EXCLUDED.provider,
            provenance  = EXCLUDED.provenance
"""

_GET_RATE = """
    SELECT rate
    FROM fx_rates
    WHERE currency = $1 AND date = $2
"""

_GET_RANGE = """
    SELECT date, currency, rate
    FROM fx_rates
    WHERE currency = $1 AND date >= $2 AND date <= $3
    ORDER BY date
"""

_GET_LATEST = """
    SELECT date, rate
    FROM fx_rates
    WHERE currency = $1
    ORDER BY date DESC
    LIMIT 1
"""

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


async def upsert_fx_rate(
    conn: Any,
    currency: str,
    date_val: date,
    rate_scaled: int,
    *,
    fetched_at: str,
    data_source: str,
    provider: str,
    provenance: str,
) -> None:
    """Insert or update a single FX rate row (ON CONFLICT DO UPDATE)."""
    await conn.execute(
        _UPSERT,
        date_val,
        currency,
        rate_scaled,
        fetched_at,
        data_source,
        provider,
        provenance,
    )


async def get_fx_rate(conn: Any, currency: str, date_val: date) -> int | None:
    """Return the scaled rate for (currency, date), or None if absent."""
    row = await conn.fetchrow(_GET_RATE, currency, date_val)
    return row["rate"] if row else None


async def get_fx_rates_in_range(
    conn: Any,
    currency: str,
    start: date,
    end: date,
) -> list[tuple[date, str, int]]:
    """Return list of (date, currency, rate_scaled) tuples for the range."""
    rows = await conn.fetch(_GET_RANGE, currency, start, end)
    return [(row["date"], row["currency"], row["rate"]) for row in rows]


async def get_latest_fx_rate(conn: Any, currency: str) -> tuple[date, int] | None:
    """Return (date, rate_scaled) for the most recent stored rate, or None."""
    row = await conn.fetchrow(_GET_LATEST, currency)
    return (row["date"], row["rate"]) if row else None
