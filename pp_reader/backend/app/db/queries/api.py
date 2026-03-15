"""Read-only SQL queries for the REST API layer.

All monetary values are returned in raw BIGINT form (cents or 10^8-scaled).
Conversion to float happens in the route handlers at the API boundary
(Architecture Decision 3).
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "get_latest_completed_run",
    "get_dashboard_data",
    "get_accounts_for_run",
    "get_portfolios_for_run",
    "get_positions_for_portfolio",
    "get_security_snapshot",
    "get_security_history",
    "get_daily_wealth",
    "get_trades",
    "get_status",
]

# ── Latest metric run ─────────────────────────────────────────────────────

_GET_LATEST_COMPLETED_RUN = """
    SELECT run_uuid, finished_at, status, trigger, started_at
    FROM metric_runs
    WHERE status = 'completed'
    ORDER BY finished_at DESC NULLS LAST
    LIMIT 1
"""

# ── Dashboard ─────────────────────────────────────────────────────────────

_GET_DASHBOARD = """
    SELECT
        COALESCE(
            (SELECT total_wealth_cents
             FROM daily_wealth
             WHERE scope_uuid = 'all' AND scope_type = 'global'
             ORDER BY date DESC
             LIMIT 1),
            0
        ) AS total_wealth_cents,
        (SELECT COUNT(*) FROM portfolios WHERE is_retired IS NOT TRUE) AS portfolio_count,
        (SELECT COUNT(*) FROM accounts   WHERE is_retired IS NOT TRUE) AS account_count,
        (SELECT finished_at
         FROM metric_runs
         WHERE status = 'completed'
         ORDER BY finished_at DESC NULLS LAST
         LIMIT 1
        ) AS last_updated
"""

# ── Accounts ──────────────────────────────────────────────────────────────

_GET_ACCOUNTS_FOR_RUN = """
    SELECT
        s.account_uuid                                  AS uuid,
        s.name,
        s.currency_code                                 AS currency,
        s.balance                                       AS balance,
        s.fx_unavailable,
        NOT EXISTS (
            SELECT 1 FROM portfolios p
            WHERE p.reference_account = s.account_uuid
              AND p.is_retired IS NOT TRUE
        ) AS is_deposit
    FROM account_snapshots s
    WHERE s.metric_run_uuid = $1
    ORDER BY s.name
"""

# ── Portfolios ────────────────────────────────────────────────────────────

_GET_PORTFOLIOS_FOR_RUN = """
    SELECT
        s.portfolio_uuid    AS uuid,
        s.name,
        s.currency_code     AS currency,
        s.current_value,
        s.purchase_sum      AS purchase_value,
        s.gain_abs,
        s.gain_pct
    FROM portfolio_snapshots s
    WHERE s.metric_run_uuid = $1
    ORDER BY s.name
"""

# ── Positions ─────────────────────────────────────────────────────────────

_GET_POSITIONS_FOR_PORTFOLIO = """
    SELECT
        sm.security_uuid,
        s.name                          AS security_name,
        s.isin,
        s.ticker_symbol                 AS ticker,
        sm.security_currency_code       AS currency,
        sm.holdings_raw,
        sm.current_value_cents,
        sm.purchase_value_cents,
        sm.purchase_security_value_raw,
        sm.gain_abs_cents,
        sm.gain_pct,
        sm.day_change_eur,
        sm.day_change_pct,
        sm.coverage_ratio,
        (sm.coverage_ratio IS NOT NULL AND sm.coverage_ratio < 1.0) AS fx_unavailable
    FROM security_metrics sm
    JOIN securities s ON s.uuid = sm.security_uuid
    WHERE sm.metric_run_uuid = $1
      AND sm.portfolio_uuid  = $2
    ORDER BY sm.current_value_cents DESC NULLS LAST
"""

# ── Security snapshot ─────────────────────────────────────────────────────

_GET_SECURITY_SNAPSHOT = """
    SELECT
        sec.uuid,
        sec.name,
        sec.isin,
        sec.ticker_symbol               AS ticker,
        sec.currency_code               AS currency,
        sec.last_price,
        sec.last_price_date,
        sm.holdings_raw,
        sm.current_value_cents,
        sm.purchase_value_cents,
        sm.purchase_security_value_raw,
        sm.gain_abs_cents,
        sm.gain_pct,
        sm.day_change_eur,
        sm.day_change_pct,
        sm.coverage_ratio,
        (sm.coverage_ratio IS NOT NULL AND sm.coverage_ratio < 1.0) AS fx_unavailable
    FROM securities sec
    LEFT JOIN security_metrics sm
           ON sm.security_uuid    = sec.uuid
          AND sm.metric_run_uuid  = $2
    WHERE sec.uuid = $1
"""

# ── Security price history ────────────────────────────────────────────────

_GET_SECURITY_HISTORY = """
    SELECT date::text AS date, close
    FROM historical_prices
    WHERE security_uuid = $1
      AND date >= $2::date
    ORDER BY date ASC
"""

# ── Daily wealth series ───────────────────────────────────────────────────

_GET_DAILY_WEALTH = """
    SELECT date, total_wealth_cents
    FROM daily_wealth
    WHERE scope_uuid  = 'all'
      AND scope_type  = 'global'
      AND date >= $1
      AND date <= $2
    ORDER BY date ASC
"""

# ── Trades ────────────────────────────────────────────────────────────────

# Type filter: BUY=0, SELL=1, INBOUND_DELIVERY=2, OUTBOUND_DELIVERY=3,
#              DIVIDEND=8, INTEREST=9
_GET_TRADES = """
    SELECT
        t.uuid,
        t.portfolio             AS portfolio_uuid,
        COALESCE(p.name, '')    AS portfolio_name,
        t.security              AS security_uuid,
        COALESCE(s.name, '')    AS security_name,
        t.type,
        t.date::text            AS date,
        t.shares,
        t.amount,
        t.currency_code         AS currency,
        COALESCE(
            (SELECT tu.amount
             FROM transaction_units tu
             WHERE tu.transaction_uuid = t.uuid
               AND tu.type = 2
             LIMIT 1),
            0
        ) AS fees
    FROM transactions t
    LEFT JOIN portfolios p ON p.uuid = t.portfolio
    LEFT JOIN securities  s ON s.uuid = t.security
    WHERE t.security IS NOT NULL
      AND t.type IN (0, 1, 2, 3, 8, 9)
    ORDER BY t.date DESC, t.updated_at DESC
"""

# ── App status ────────────────────────────────────────────────────────────

_GET_STATUS = """
    SELECT
        (SELECT finished_at
         FROM metric_runs
         WHERE status = 'completed'
         ORDER BY finished_at DESC NULLS LAST
         LIMIT 1
        ) AS last_file_update,
        (SELECT status
         FROM metric_runs
         ORDER BY started_at DESC NULLS LAST
         LIMIT 1
        ) AS pipeline_status
"""


# ── Helper functions ──────────────────────────────────────────────────────

async def get_latest_completed_run(conn: Any) -> dict | None:
    """Return the most recent completed metric_run row, or None."""
    row = await conn.fetchrow(_GET_LATEST_COMPLETED_RUN)
    return dict(row) if row else None


async def get_dashboard_data(conn: Any) -> dict:
    """Return aggregated dashboard counts and total wealth."""
    row = await conn.fetchrow(_GET_DASHBOARD)
    return dict(row) if row else {}


async def get_accounts_for_run(conn: Any, run_uuid: str) -> list[dict]:
    """Return account snapshot rows for a given metric run."""
    rows = await conn.fetch(_GET_ACCOUNTS_FOR_RUN, run_uuid)
    return [dict(r) for r in rows]


async def get_portfolios_for_run(conn: Any, run_uuid: str) -> list[dict]:
    """Return portfolio snapshot rows for a given metric run."""
    rows = await conn.fetch(_GET_PORTFOLIOS_FOR_RUN, run_uuid)
    return [dict(r) for r in rows]


async def get_positions_for_portfolio(
    conn: Any,
    run_uuid: str,
    portfolio_uuid: str,
) -> list[dict]:
    """Return security_metrics rows for a single portfolio in a metric run."""
    rows = await conn.fetch(_GET_POSITIONS_FOR_PORTFOLIO, run_uuid, portfolio_uuid)
    return [dict(r) for r in rows]


async def get_security_snapshot(
    conn: Any,
    security_uuid: str,
    run_uuid: str,
) -> dict | None:
    """Return securities + security_metrics for a single security."""
    row = await conn.fetchrow(_GET_SECURITY_SNAPSHOT, security_uuid, run_uuid)
    return dict(row) if row else None


async def get_security_history(
    conn: Any,
    security_uuid: str,
    from_date: str,
) -> list[dict]:
    """Return historical price rows for a security from from_date onward."""
    rows = await conn.fetch(_GET_SECURITY_HISTORY, security_uuid, from_date)
    return [dict(r) for r in rows]


async def get_daily_wealth(
    conn: Any,
    from_date: str,
    to_date: str,
) -> list[dict]:
    """Return daily_wealth rows for the global scope in the given date range."""
    rows = await conn.fetch(_GET_DAILY_WEALTH, from_date, to_date)
    return [dict(r) for r in rows]


async def get_trades(conn: Any) -> list[dict]:
    """Return investment transactions suitable for the trades list."""
    rows = await conn.fetch(_GET_TRADES)
    return [dict(r) for r in rows]


async def get_status(conn: Any) -> dict:
    """Return pipeline status info."""
    row = await conn.fetchrow(_GET_STATUS)
    return dict(row) if row else {}
