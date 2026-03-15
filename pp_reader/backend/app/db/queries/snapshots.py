"""SQL statements and helpers for snapshot persistence.

Handles UPSERTs into portfolio_snapshots, account_snapshots, and daily_wealth
tables after a metric run is complete.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

__all__ = [
    "upsert_portfolio_snapshot",
    "upsert_account_snapshot",
    "upsert_daily_wealth",
    "get_portfolio_metrics_for_run",
    "get_account_metrics_for_run",
    "get_security_metrics_for_run",
]

# ── Portfolio snapshots ───────────────────────────────────────────────

UPSERT_PORTFOLIO_SNAPSHOT = """
    INSERT INTO portfolio_snapshots (
        metric_run_uuid, portfolio_uuid, snapshot_at,
        name, currency_code,
        current_value, purchase_sum, gain_abs, gain_pct,
        total_change_eur, total_change_pct,
        position_count, missing_value_positions,
        has_current_value, coverage_ratio,
        performance_source, provenance, payload
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    ON CONFLICT (metric_run_uuid, portfolio_uuid) DO UPDATE SET
        snapshot_at = EXCLUDED.snapshot_at,
        name = EXCLUDED.name,
        currency_code = EXCLUDED.currency_code,
        current_value = EXCLUDED.current_value,
        purchase_sum = EXCLUDED.purchase_sum,
        gain_abs = EXCLUDED.gain_abs,
        gain_pct = EXCLUDED.gain_pct,
        total_change_eur = EXCLUDED.total_change_eur,
        total_change_pct = EXCLUDED.total_change_pct,
        position_count = EXCLUDED.position_count,
        missing_value_positions = EXCLUDED.missing_value_positions,
        has_current_value = EXCLUDED.has_current_value,
        coverage_ratio = EXCLUDED.coverage_ratio,
        performance_source = EXCLUDED.performance_source,
        provenance = EXCLUDED.provenance,
        payload = EXCLUDED.payload,
        updated_at = NOW()
"""

# ── Account snapshots ─────────────────────────────────────────────────

UPSERT_ACCOUNT_SNAPSHOT = """
    INSERT INTO account_snapshots (
        metric_run_uuid, account_uuid, snapshot_at,
        name, currency_code,
        orig_balance, balance,
        fx_unavailable, fx_rate, fx_rate_source, fx_rate_timestamp,
        coverage_ratio, provenance, payload
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT (metric_run_uuid, account_uuid) DO UPDATE SET
        snapshot_at = EXCLUDED.snapshot_at,
        name = EXCLUDED.name,
        currency_code = EXCLUDED.currency_code,
        orig_balance = EXCLUDED.orig_balance,
        balance = EXCLUDED.balance,
        fx_unavailable = EXCLUDED.fx_unavailable,
        fx_rate = EXCLUDED.fx_rate,
        fx_rate_source = EXCLUDED.fx_rate_source,
        fx_rate_timestamp = EXCLUDED.fx_rate_timestamp,
        coverage_ratio = EXCLUDED.coverage_ratio,
        provenance = EXCLUDED.provenance,
        payload = EXCLUDED.payload,
        updated_at = NOW()
"""

# ── Daily wealth ──────────────────────────────────────────────────────

UPSERT_DAILY_WEALTH = """
    INSERT INTO daily_wealth (
        date, scope_uuid, scope_type,
        total_wealth_cents, total_invested_cents
    ) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (date, scope_uuid, scope_type) DO UPDATE SET
        total_wealth_cents = EXCLUDED.total_wealth_cents,
        total_invested_cents = EXCLUDED.total_invested_cents
"""

# ── Data retrieval ────────────────────────────────────────────────────

GET_PORTFOLIO_METRICS_FOR_RUN = """
    SELECT
        pm.portfolio_uuid,
        p.name,
        pm.valuation_currency,
        pm.current_value_cents,
        pm.purchase_value_cents,
        pm.gain_abs_cents,
        pm.gain_pct,
        pm.total_change_eur_cents,
        pm.total_change_pct,
        pm.position_count,
        pm.missing_value_positions,
        pm.source,
        pm.coverage_ratio,
        pm.provenance
    FROM portfolio_metrics pm
    JOIN portfolios p ON p.uuid = pm.portfolio_uuid
    WHERE pm.metric_run_uuid = $1
"""

GET_ACCOUNT_METRICS_FOR_RUN = """
    SELECT
        am.account_uuid,
        a.name,
        am.currency_code,
        am.valuation_currency,
        am.balance_native_cents,
        am.balance_eur_cents,
        am.fx_rate,
        am.fx_rate_source,
        am.fx_rate_timestamp,
        am.coverage_ratio,
        am.provenance
    FROM account_metrics am
    JOIN accounts a ON a.uuid = am.account_uuid
    WHERE am.metric_run_uuid = $1
"""

GET_SECURITY_METRICS_FOR_RUN = """
    SELECT
        sm.portfolio_uuid,
        sm.security_uuid,
        s.name,
        s.isin,
        s.ticker_symbol,
        s.currency_code AS security_currency_code,
        sm.valuation_currency,
        sm.holdings_raw,
        sm.current_value_cents,
        sm.purchase_value_cents,
        sm.gain_abs_cents,
        sm.gain_pct,
        sm.total_change_eur_cents,
        sm.total_change_pct,
        sm.coverage_ratio,
        sm.day_change_native,
        sm.day_change_eur,
        sm.day_change_pct,
        sm.provenance
    FROM security_metrics sm
    JOIN securities s ON s.uuid = sm.security_uuid
    WHERE sm.metric_run_uuid = $1
    ORDER BY sm.portfolio_uuid, sm.current_value_cents DESC
"""


# ── Helper functions ──────────────────────────────────────────────────

async def upsert_portfolio_snapshot(
    conn: Any,
    run_uuid: str,
    portfolio_uuid: str,
    *,
    snapshot_at: datetime,
    name: str,
    currency_code: str = "EUR",
    current_value: float = 0.0,
    purchase_sum: float = 0.0,
    gain_abs: float = 0.0,
    gain_pct: float | None = None,
    total_change_eur: float | None = None,
    total_change_pct: float | None = None,
    position_count: int = 0,
    missing_value_positions: int = 0,
    has_current_value: bool = True,
    coverage_ratio: float | None = None,
    performance_source: str | None = None,
    provenance: dict | None = None,
    payload: dict | None = None,
) -> None:
    await conn.execute(
        UPSERT_PORTFOLIO_SNAPSHOT,
        run_uuid, portfolio_uuid, snapshot_at,
        name, currency_code,
        current_value, purchase_sum, gain_abs, gain_pct,
        total_change_eur, total_change_pct,
        position_count, missing_value_positions,
        has_current_value, coverage_ratio,
        performance_source,
        json.dumps(provenance) if provenance is not None else None,
        json.dumps(payload) if payload is not None else None,
    )


async def upsert_account_snapshot(
    conn: Any,
    run_uuid: str,
    account_uuid: str,
    *,
    snapshot_at: datetime,
    name: str,
    currency_code: str,
    orig_balance: float = 0.0,
    balance: float | None = None,
    fx_unavailable: bool = False,
    fx_rate: int | None = None,
    fx_rate_source: str | None = None,
    fx_rate_timestamp: datetime | None = None,
    coverage_ratio: float | None = None,
    provenance: dict | None = None,
    payload: dict | None = None,
) -> None:
    await conn.execute(
        UPSERT_ACCOUNT_SNAPSHOT,
        run_uuid, account_uuid, snapshot_at,
        name, currency_code,
        orig_balance, balance,
        fx_unavailable, fx_rate, fx_rate_source, fx_rate_timestamp,
        coverage_ratio,
        json.dumps(provenance) if provenance is not None else None,
        json.dumps(payload) if payload is not None else None,
    )


async def upsert_daily_wealth(
    conn: Any,
    *,
    date: date,
    scope_uuid: str,
    scope_type: str,
    total_wealth_cents: int,
    total_invested_cents: int,
) -> None:
    await conn.execute(
        UPSERT_DAILY_WEALTH,
        str(date), scope_uuid, scope_type,
        total_wealth_cents, total_invested_cents,
    )


async def get_portfolio_metrics_for_run(conn: Any, run_uuid: str) -> list[dict]:
    rows = await conn.fetch(GET_PORTFOLIO_METRICS_FOR_RUN, run_uuid)
    return [dict(r) for r in rows]


async def get_account_metrics_for_run(conn: Any, run_uuid: str) -> list[dict]:
    rows = await conn.fetch(GET_ACCOUNT_METRICS_FOR_RUN, run_uuid)
    return [dict(r) for r in rows]


async def get_security_metrics_for_run(conn: Any, run_uuid: str) -> list[dict]:
    rows = await conn.fetch(GET_SECURITY_METRICS_FOR_RUN, run_uuid)
    return [dict(r) for r in rows]
