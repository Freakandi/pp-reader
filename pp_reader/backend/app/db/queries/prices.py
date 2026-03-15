"""SQL statements for historical price and price queue operations.

Prices are stored as BIGINT scaled by 10^8 throughout (Architecture Decision 3).
Conversion to float happens only at the API boundary.
"""

from __future__ import annotations

from datetime import date
from typing import Any

# ---------------------------------------------------------------------------
# Historical prices — upsert / query
# ---------------------------------------------------------------------------

UPSERT_HISTORICAL_PRICE = """
    INSERT INTO historical_prices (
        security_uuid, date, close, high, low, volume,
        fetched_at, data_source, provider, provenance
    ) VALUES ($1, $2, $3, $4, $5, $6,
              NOW(), $7, $8, $9)
    ON CONFLICT (security_uuid, date)
    DO UPDATE SET
        close       = EXCLUDED.close,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        volume      = EXCLUDED.volume,
        fetched_at  = EXCLUDED.fetched_at,
        data_source = EXCLUDED.data_source,
        provider    = EXCLUDED.provider,
        provenance  = EXCLUDED.provenance
"""

GET_LATEST_PRICE = """
    SELECT date, close
    FROM historical_prices
    WHERE security_uuid = $1
    ORDER BY date DESC
    LIMIT 1
"""

GET_PRICE_HISTORY = """
    SELECT date, close, high, low, volume
    FROM historical_prices
    WHERE security_uuid = $1
      AND date BETWEEN $2 AND $3
    ORDER BY date ASC
"""

# ---------------------------------------------------------------------------
# Price history queue — management
# ---------------------------------------------------------------------------

ENQUEUE_HISTORY_JOB = """
    INSERT INTO price_history_queue (
        security_uuid, requested_date, status, priority,
        scheduled_at, data_source, provenance
    ) VALUES ($1, $2, 'pending', $3,
              NOW(), $4, $5)
    RETURNING id
"""

PENDING_JOB_EXISTS = """
    SELECT 1
    FROM price_history_queue
    WHERE security_uuid = $1
      AND status IN ('pending', 'running')
    LIMIT 1
"""

GET_PENDING_JOBS = """
    SELECT id, security_uuid, requested_date, status, provenance, data_source
    FROM price_history_queue
    WHERE status = 'pending'
    ORDER BY priority DESC, scheduled_at ASC
    LIMIT $1
"""

MARK_JOB_RUNNING = """
    UPDATE price_history_queue
    SET status     = 'running',
        started_at = NOW(),
        attempts   = attempts + 1,
        updated_at = NOW()
    WHERE id = $1
"""

COMPLETE_JOB = """
    UPDATE price_history_queue
    SET status      = $2,
        finished_at = NOW(),
        updated_at  = NOW(),
        last_error  = $3,
        provenance  = COALESCE(provenance, '{}'::jsonb) || $4::jsonb
    WHERE id = $1
"""

# ---------------------------------------------------------------------------
# Securities needing backfill
# ---------------------------------------------------------------------------

GET_SECURITIES_FOR_BACKFILL = """
    SELECT s.uuid, s.ticker_symbol, s.feed, s.retired
    FROM securities s
    WHERE s.retired IS NOT TRUE
      AND s.ticker_symbol IS NOT NULL
      AND s.ticker_symbol != ''
"""

GET_LATEST_PRICE_DATE = """
    SELECT MAX(date)
    FROM historical_prices
    WHERE security_uuid = $1
"""


# ---------------------------------------------------------------------------
# asyncpg helper functions
# ---------------------------------------------------------------------------

async def upsert_historical_price(
    conn: Any,
    security_uuid: str,
    price_date: date,
    close_scaled: int,
    *,
    high_scaled: int | None = None,
    low_scaled: int | None = None,
    volume: int | None = None,
    data_source: str = "yahoo",
    provider: str = "yahoo",
    provenance: str | None = None,
) -> None:
    """Upsert a single historical price row (ON CONFLICT DO UPDATE)."""
    await conn.execute(
        UPSERT_HISTORICAL_PRICE,
        security_uuid,
        price_date,
        close_scaled,
        high_scaled,
        low_scaled,
        volume,
        data_source,
        provider,
        provenance,
    )


async def get_latest_price(
    conn: Any,
    security_uuid: str,
) -> tuple[date, int] | None:
    """Return (date, close_scaled) of the most recent historical price, or None."""
    row = await conn.fetchrow(GET_LATEST_PRICE, security_uuid)
    if row is None:
        return None
    return row["date"], row["close"]


async def get_price_history(
    conn: Any,
    security_uuid: str,
    start: date,
    end: date,
) -> list[dict]:
    """Return list of price rows (date, close, high, low, volume) in ascending order."""
    rows = await conn.fetch(GET_PRICE_HISTORY, security_uuid, start, end)
    return [dict(r) for r in rows]


async def pending_job_exists(conn: Any, security_uuid: str) -> bool:
    """Return True if a pending or running job already exists for the security."""
    row = await conn.fetchrow(PENDING_JOB_EXISTS, security_uuid)
    return row is not None


async def enqueue_history_job(
    conn: Any,
    security_uuid: str,
    requested_date: date,
    provenance_json: str,
    *,
    priority: int = 0,
    data_source: str = "yahoo",
) -> int:
    """Insert a pending history job and return its id."""
    row = await conn.fetchrow(
        ENQUEUE_HISTORY_JOB,
        security_uuid,
        requested_date,
        priority,
        data_source,
        provenance_json,
    )
    return row["id"]


async def get_pending_jobs(conn: Any, limit: int = 10) -> list[dict]:
    """Fetch up to `limit` pending queue entries ordered by priority/schedule."""
    rows = await conn.fetch(GET_PENDING_JOBS, limit)
    return [dict(r) for r in rows]


async def mark_job_running(conn: Any, job_id: int) -> None:
    await conn.execute(MARK_JOB_RUNNING, job_id)


async def complete_job(
    conn: Any,
    job_id: int,
    *,
    status: str,
    error: str | None = None,
    result_json: str = "{}",
) -> None:
    await conn.execute(COMPLETE_JOB, job_id, status, error, result_json)


async def get_securities_for_backfill(conn: Any) -> list[dict]:
    """Return all non-retired securities that have a ticker_symbol."""
    rows = await conn.fetch(GET_SECURITIES_FOR_BACKFILL)
    return [dict(r) for r in rows]


async def get_latest_price_date(conn: Any, security_uuid: str) -> date | None:
    """Return the most recent date in historical_prices for the security, or None."""
    row = await conn.fetchrow(GET_LATEST_PRICE_DATE, security_uuid)
    if row is None or row[0] is None:
        return None
    return row[0]
