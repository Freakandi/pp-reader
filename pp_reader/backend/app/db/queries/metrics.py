"""SQL statements and helpers for the metrics engine.

Handles metric_runs lifecycle, security/portfolio/account metric upserts,
and data retrieval for the calculation pipeline.

All monetary values are BIGINT cents (10^-2), shares/prices 10^-8 scaled.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

__all__ = [
    "create_metric_run",
    "finish_metric_run",
    "upsert_security_metrics",
    "upsert_portfolio_metrics",
    "upsert_account_metrics",
    "get_security_transactions",
    "get_portfolio_holdings",
    "get_portfolios",
    "get_accounts",
    "get_securities_for_portfolio",
]

# ── Metric runs ───────────────────────────────────────────────────────

CREATE_METRIC_RUN = """
    INSERT INTO metric_runs (
        run_uuid, status, trigger, started_at, provenance
    ) VALUES ($1, 'running', $2, NOW(), $3)
"""

FINISH_METRIC_RUN = """
    UPDATE metric_runs
    SET status = $2,
        finished_at = NOW(),
        duration_ms = $3,
        total_entities = $4,
        processed_portfolios = $5,
        processed_accounts = $6,
        processed_securities = $7,
        error_message = $8,
        updated_at = NOW()
    WHERE run_uuid = $1
"""

# ── Security metrics ──────────────────────────────────────────────────

UPSERT_SECURITY_METRICS = """
    INSERT INTO security_metrics (
        metric_run_uuid, portfolio_uuid, security_uuid,
        valuation_currency, security_currency_code,
        holdings_raw, current_value_cents, purchase_value_cents,
        purchase_security_value_raw, purchase_account_value_cents,
        gain_abs_cents, gain_pct,
        total_change_eur_cents, total_change_pct,
        source, coverage_ratio, provenance
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
    ON CONFLICT (metric_run_uuid, portfolio_uuid, security_uuid) DO UPDATE SET
        valuation_currency = EXCLUDED.valuation_currency,
        security_currency_code = EXCLUDED.security_currency_code,
        holdings_raw = EXCLUDED.holdings_raw,
        current_value_cents = EXCLUDED.current_value_cents,
        purchase_value_cents = EXCLUDED.purchase_value_cents,
        purchase_security_value_raw = EXCLUDED.purchase_security_value_raw,
        purchase_account_value_cents = EXCLUDED.purchase_account_value_cents,
        gain_abs_cents = EXCLUDED.gain_abs_cents,
        gain_pct = EXCLUDED.gain_pct,
        total_change_eur_cents = EXCLUDED.total_change_eur_cents,
        total_change_pct = EXCLUDED.total_change_pct,
        source = EXCLUDED.source,
        coverage_ratio = EXCLUDED.coverage_ratio,
        provenance = EXCLUDED.provenance,
        updated_at = NOW()
"""

# ── Portfolio metrics ─────────────────────────────────────────────────

UPSERT_PORTFOLIO_METRICS = """
    INSERT INTO portfolio_metrics (
        metric_run_uuid, portfolio_uuid,
        valuation_currency, current_value_cents, purchase_value_cents,
        gain_abs_cents, gain_pct,
        total_change_eur_cents, total_change_pct,
        source, coverage_ratio,
        position_count, missing_value_positions,
        provenance
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT (metric_run_uuid, portfolio_uuid) DO UPDATE SET
        valuation_currency = EXCLUDED.valuation_currency,
        current_value_cents = EXCLUDED.current_value_cents,
        purchase_value_cents = EXCLUDED.purchase_value_cents,
        gain_abs_cents = EXCLUDED.gain_abs_cents,
        gain_pct = EXCLUDED.gain_pct,
        total_change_eur_cents = EXCLUDED.total_change_eur_cents,
        total_change_pct = EXCLUDED.total_change_pct,
        source = EXCLUDED.source,
        coverage_ratio = EXCLUDED.coverage_ratio,
        position_count = EXCLUDED.position_count,
        missing_value_positions = EXCLUDED.missing_value_positions,
        provenance = EXCLUDED.provenance,
        updated_at = NOW()
"""

# ── Account metrics ───────────────────────────────────────────────────

UPSERT_ACCOUNT_METRICS = """
    INSERT INTO account_metrics (
        metric_run_uuid, account_uuid,
        currency_code, valuation_currency,
        balance_native_cents, balance_eur_cents,
        fx_rate, fx_rate_source, fx_rate_timestamp,
        coverage_ratio, provenance
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (metric_run_uuid, account_uuid) DO UPDATE SET
        currency_code = EXCLUDED.currency_code,
        valuation_currency = EXCLUDED.valuation_currency,
        balance_native_cents = EXCLUDED.balance_native_cents,
        balance_eur_cents = EXCLUDED.balance_eur_cents,
        fx_rate = EXCLUDED.fx_rate,
        fx_rate_source = EXCLUDED.fx_rate_source,
        fx_rate_timestamp = EXCLUDED.fx_rate_timestamp,
        coverage_ratio = EXCLUDED.coverage_ratio,
        provenance = EXCLUDED.provenance,
        updated_at = NOW()
"""

# ── Data retrieval for calculation ────────────────────────────────────

GET_PORTFOLIOS = """
    SELECT uuid, name, note, reference_account, is_retired
    FROM portfolios
    WHERE is_retired IS NOT TRUE
"""

GET_ACCOUNTS = """
    SELECT uuid, name, currency_code, is_retired, balance
    FROM accounts
    WHERE is_retired IS NOT TRUE
"""

GET_SECURITIES_FOR_PORTFOLIO = """
    SELECT ps.security_uuid, ps.current_holdings, ps.purchase_value,
           s.name, s.currency_code, s.isin, s.ticker_symbol,
           s.last_price, s.last_price_date
    FROM portfolio_securities ps
    JOIN securities s ON s.uuid = ps.security_uuid
    WHERE ps.portfolio_uuid = $1
"""

GET_SECURITY_TRANSACTIONS = """
    SELECT t.uuid, t.type, t.account, t.portfolio,
           t.date, t.currency_code,
           t.amount, t.amount_eur_cents, t.fx_rate_used,
           t.shares, t.note, t.security, t.source, t.updated_at,
           t.other_account, t.other_portfolio
    FROM transactions t
    WHERE t.security = $1
    ORDER BY t.date ASC, t.updated_at ASC
"""

GET_TRANSACTION_UNITS = """
    SELECT tu.transaction_uuid, tu.type, tu.amount, tu.amount_eur_cents,
           tu.fx_rate_used, tu.currency_code,
           tu.fx_amount, tu.fx_currency_code, tu.fx_rate_to_base
    FROM transaction_units tu
    WHERE tu.transaction_uuid = ANY($1)
    ORDER BY tu.transaction_uuid
"""

GET_SECURITY_PRICE_ON_DATE = """
    SELECT close
    FROM historical_prices
    WHERE security_uuid = $1 AND date <= $2
    ORDER BY date DESC
    LIMIT 1
"""

GET_DAILY_PRICES_IN_RANGE = """
    SELECT date, close
    FROM historical_prices
    WHERE security_uuid = $1
      AND date BETWEEN $2 AND $3
    ORDER BY date ASC
"""

DELETE_METRICS_FOR_RUN = """
    DELETE FROM security_metrics WHERE metric_run_uuid = $1;
    DELETE FROM portfolio_metrics WHERE metric_run_uuid = $1;
    DELETE FROM account_metrics WHERE metric_run_uuid = $1;
"""


# ── Helper functions ──────────────────────────────────────────────────

async def create_metric_run(
    conn: Any,
    run_uuid: str,
    *,
    trigger: str = "manual",
    provenance: str = "{}",
) -> None:
    await conn.execute(CREATE_METRIC_RUN, run_uuid, trigger, provenance)


async def finish_metric_run(
    conn: Any,
    run_uuid: str,
    *,
    status: str = "completed",
    duration_ms: int = 0,
    total_entities: int = 0,
    processed_portfolios: int = 0,
    processed_accounts: int = 0,
    processed_securities: int = 0,
    error_message: str | None = None,
) -> None:
    await conn.execute(
        FINISH_METRIC_RUN,
        run_uuid, status, duration_ms, total_entities,
        processed_portfolios, processed_accounts, processed_securities,
        error_message,
    )


async def upsert_security_metrics(
    conn: Any,
    run_uuid: str,
    portfolio_uuid: str,
    security_uuid: str,
    *,
    valuation_currency: str = "EUR",
    security_currency_code: str = "EUR",
    holdings_raw: int = 0,
    current_value_cents: int = 0,
    purchase_value_cents: int = 0,
    purchase_security_value_raw: int | None = None,
    purchase_account_value_cents: int | None = None,
    gain_abs_cents: int = 0,
    gain_pct: float | None = None,
    total_change_eur_cents: int = 0,
    total_change_pct: float | None = None,
    source: str = "metrics_engine",
    coverage_ratio: float = 1.0,
    provenance: str = "{}",
) -> None:
    await conn.execute(
        UPSERT_SECURITY_METRICS,
        run_uuid, portfolio_uuid, security_uuid,
        valuation_currency, security_currency_code,
        holdings_raw, current_value_cents, purchase_value_cents,
        purchase_security_value_raw, purchase_account_value_cents,
        gain_abs_cents, gain_pct,
        total_change_eur_cents, total_change_pct,
        source, coverage_ratio, provenance,
    )


async def upsert_portfolio_metrics(
    conn: Any,
    run_uuid: str,
    portfolio_uuid: str,
    *,
    valuation_currency: str = "EUR",
    current_value_cents: int = 0,
    purchase_value_cents: int = 0,
    gain_abs_cents: int = 0,
    gain_pct: float | None = None,
    total_change_eur_cents: int = 0,
    total_change_pct: float | None = None,
    source: str = "metrics_engine",
    coverage_ratio: float = 1.0,
    position_count: int = 0,
    missing_value_positions: int = 0,
    provenance: str = "{}",
) -> None:
    await conn.execute(
        UPSERT_PORTFOLIO_METRICS,
        run_uuid, portfolio_uuid,
        valuation_currency, current_value_cents, purchase_value_cents,
        gain_abs_cents, gain_pct,
        total_change_eur_cents, total_change_pct,
        source, coverage_ratio,
        position_count, missing_value_positions,
        provenance,
    )


async def upsert_account_metrics(
    conn: Any,
    run_uuid: str,
    account_uuid: str,
    *,
    currency_code: str = "EUR",
    valuation_currency: str = "EUR",
    balance_native_cents: int = 0,
    balance_eur_cents: int | None = None,
    fx_rate: int | None = None,
    fx_rate_source: str | None = None,
    fx_rate_timestamp: datetime | None = None,
    coverage_ratio: float = 1.0,
    provenance: str = "{}",
) -> None:
    await conn.execute(
        UPSERT_ACCOUNT_METRICS,
        run_uuid, account_uuid,
        currency_code, valuation_currency,
        balance_native_cents, balance_eur_cents,
        fx_rate, fx_rate_source, fx_rate_timestamp,
        coverage_ratio, provenance,
    )


async def get_portfolios(conn: Any) -> list[dict]:
    rows = await conn.fetch(GET_PORTFOLIOS)
    return [dict(r) for r in rows]


async def get_accounts(conn: Any) -> list[dict]:
    rows = await conn.fetch(GET_ACCOUNTS)
    return [dict(r) for r in rows]


async def get_securities_for_portfolio(conn: Any, portfolio_uuid: str) -> list[dict]:
    rows = await conn.fetch(GET_SECURITIES_FOR_PORTFOLIO, portfolio_uuid)
    return [dict(r) for r in rows]


async def get_security_transactions(conn: Any, security_uuid: str) -> list[dict]:
    rows = await conn.fetch(GET_SECURITY_TRANSACTIONS, security_uuid)
    return [dict(r) for r in rows]


async def get_transaction_units(conn: Any, transaction_uuids: list[str]) -> list[dict]:
    rows = await conn.fetch(GET_TRANSACTION_UNITS, transaction_uuids)
    return [dict(r) for r in rows]


async def get_security_price_on_date(
    conn: Any, security_uuid: str, target_date: date,
) -> int | None:
    row = await conn.fetchrow(GET_SECURITY_PRICE_ON_DATE, security_uuid, target_date)
    return row["close"] if row else None


async def get_daily_prices_in_range(
    conn: Any, security_uuid: str, start: date, end: date,
) -> list[dict]:
    rows = await conn.fetch(GET_DAILY_PRICES_IN_RANGE, security_uuid, start, end)
    return [dict(r) for r in rows]
