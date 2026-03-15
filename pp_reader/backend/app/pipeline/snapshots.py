"""Snapshot builder — assembles denormalized snapshots from metric results.

Reads metric_runs, portfolio_metrics, account_metrics, and security_metrics
then writes denormalized rows to portfolio_snapshots, account_snapshots, and
daily_wealth tables for fast API serving.

Scaling conventions (Architecture Decision 3):
  - BIGINT cents (10^-2) → divide by 100 → float EUR
  - BIGINT 10^8 shares   → divide by 10^8 → float shares
  - BIGINT 10^8 FX rate  → kept as-is in snapshot (raw, for payload display)
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from app.db.queries.snapshots import (
    get_account_metrics_for_run,
    get_portfolio_metrics_for_run,
    get_security_metrics_for_run,
    upsert_account_snapshot,
    upsert_daily_wealth,
    upsert_portfolio_snapshot,
)
from app.models.constants import EIGHT_DECIMAL_SCALE, MONETARY_SCALE

__all__ = [
    "build_portfolio_snapshots",
    "build_account_snapshots",
    "build_daily_wealth",
    "build_snapshots",
]

logger = logging.getLogger(__name__)

_CENTS = MONETARY_SCALE  # 100


def _cents_to_float(cents: int | None) -> float:
    """Convert BIGINT cents to a float EUR value."""
    if cents is None:
        return 0.0
    return cents / _CENTS


def _shares_to_float(raw: int | None) -> float:
    """Convert BIGINT 10^8-scaled shares to a float."""
    if raw is None:
        return 0.0
    return raw / EIGHT_DECIMAL_SCALE


async def build_portfolio_snapshots(
    conn: Any,
    run_uuid: str,
    *,
    snapshot_at: datetime | None = None,
) -> list[dict]:
    """Build and persist portfolio snapshots for a completed metric run.

    Args:
        conn: asyncpg connection.
        run_uuid: UUID of the completed metric run.
        snapshot_at: Timestamp to stamp snapshots with (defaults to now UTC).

    Returns:
        List of snapshot dicts that were written (for logging / downstream use).
    """
    if snapshot_at is None:
        snapshot_at = datetime.now(tz=timezone.utc)

    portfolio_metrics = await get_portfolio_metrics_for_run(conn, run_uuid)
    security_metrics = await get_security_metrics_for_run(conn, run_uuid)

    # Index security metrics by portfolio for fast lookup
    positions_by_portfolio: dict[str, list[dict]] = {}
    for sm in security_metrics:
        p_uuid = sm["portfolio_uuid"]
        positions_by_portfolio.setdefault(p_uuid, []).append(sm)

    snapshots: list[dict] = []

    for pm in portfolio_metrics:
        p_uuid = pm["portfolio_uuid"]
        currency_code = pm.get("valuation_currency") or "EUR"

        current_value = _cents_to_float(pm.get("current_value_cents"))
        purchase_sum = _cents_to_float(pm.get("purchase_value_cents"))
        gain_abs = _cents_to_float(pm.get("gain_abs_cents"))
        gain_pct = pm.get("gain_pct")
        total_change_eur = _cents_to_float(pm.get("total_change_eur_cents"))
        total_change_pct = pm.get("total_change_pct")
        position_count = pm.get("position_count") or 0
        missing_value_positions = pm.get("missing_value_positions") or 0
        coverage_ratio = pm.get("coverage_ratio")

        has_current_value = missing_value_positions == 0 or current_value > 0

        # Build position-level payload
        positions = positions_by_portfolio.get(p_uuid, [])
        position_payloads = []
        for pos in positions:
            prov_raw = pos.get("provenance")
            prov = {}
            if isinstance(prov_raw, str):
                try:
                    prov = json.loads(prov_raw)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(prov_raw, dict):
                prov = prov_raw

            position_payloads.append({
                "security_uuid": pos["security_uuid"],
                "name": pos.get("name"),
                "isin": pos.get("isin"),
                "ticker": pos.get("ticker_symbol"),
                "currency": pos.get("security_currency_code"),
                "holdings": _shares_to_float(pos.get("holdings_raw")),
                "current_value": _cents_to_float(pos.get("current_value_cents")),
                "purchase_value": _cents_to_float(pos.get("purchase_value_cents")),
                "gain_abs": _cents_to_float(pos.get("gain_abs_cents")),
                "gain_pct": pos.get("gain_pct"),
                "day_change_pct": pos.get("day_change_pct"),
                "day_change_eur": pos.get("day_change_eur"),
                "coverage": pos.get("coverage_ratio"),
                "irr": prov.get("irr"),
                "fifo_cost": _cents_to_float(prov.get("fifo_cost")),
                "realized_gains": _cents_to_float(prov.get("realized_gains_fifo")),
                "dividend_total": _cents_to_float(prov.get("dividend_total")),
            })

        payload = {
            "positions": position_payloads,
            "run_uuid": run_uuid,
        }

        # Parse portfolio-level provenance
        pm_prov_raw = pm.get("provenance")
        pm_prov: dict = {}
        if isinstance(pm_prov_raw, str):
            try:
                pm_prov = json.loads(pm_prov_raw)
            except (json.JSONDecodeError, TypeError):
                pass
        elif isinstance(pm_prov_raw, dict):
            pm_prov = pm_prov_raw

        await upsert_portfolio_snapshot(
            conn,
            run_uuid,
            p_uuid,
            snapshot_at=snapshot_at,
            name=pm["name"],
            currency_code=currency_code,
            current_value=current_value,
            purchase_sum=purchase_sum,
            gain_abs=gain_abs,
            gain_pct=gain_pct,
            total_change_eur=total_change_eur,
            total_change_pct=total_change_pct,
            position_count=position_count,
            missing_value_positions=missing_value_positions,
            has_current_value=has_current_value,
            coverage_ratio=coverage_ratio,
            performance_source=pm.get("source"),
            provenance=pm_prov,
            payload=payload,
        )

        snap = {
            "portfolio_uuid": p_uuid,
            "name": pm["name"],
            "currency_code": currency_code,
            "current_value": current_value,
            "purchase_sum": purchase_sum,
            "gain_abs": gain_abs,
            "gain_pct": gain_pct,
            "position_count": position_count,
            "coverage_ratio": coverage_ratio,
        }
        snapshots.append(snap)
        logger.debug(
            "Portfolio snapshot: %s  value=%.2f  gain=%.2f  positions=%d",
            pm["name"], current_value, gain_abs, position_count,
        )

    logger.info("Built %d portfolio snapshots for run %s", len(snapshots), run_uuid)
    return snapshots


async def build_account_snapshots(
    conn: Any,
    run_uuid: str,
    *,
    snapshot_at: datetime | None = None,
) -> list[dict]:
    """Build and persist account snapshots for a completed metric run.

    Args:
        conn: asyncpg connection.
        run_uuid: UUID of the completed metric run.
        snapshot_at: Timestamp to stamp snapshots with (defaults to now UTC).

    Returns:
        List of snapshot dicts that were written.
    """
    if snapshot_at is None:
        snapshot_at = datetime.now(tz=timezone.utc)

    account_metrics = await get_account_metrics_for_run(conn, run_uuid)
    snapshots: list[dict] = []

    for am in account_metrics:
        a_uuid = am["account_uuid"]
        currency_code = am.get("currency_code") or "EUR"
        valuation_currency = am.get("valuation_currency") or "EUR"

        orig_balance = _cents_to_float(am.get("balance_native_cents"))
        balance_eur_cents = am.get("balance_eur_cents")
        fx_rate = am.get("fx_rate")
        fx_rate_source = am.get("fx_rate_source")
        fx_rate_timestamp = am.get("fx_rate_timestamp")

        # Determine EUR-denominated balance
        if currency_code == valuation_currency:
            balance = orig_balance
            fx_unavailable = False
        elif balance_eur_cents is not None:
            balance = _cents_to_float(balance_eur_cents)
            fx_unavailable = False
        else:
            balance = None
            fx_unavailable = True

        coverage_ratio = am.get("coverage_ratio")

        am_prov_raw = am.get("provenance")
        am_prov: dict = {}
        if isinstance(am_prov_raw, str):
            try:
                am_prov = json.loads(am_prov_raw)
            except (json.JSONDecodeError, TypeError):
                pass
        elif isinstance(am_prov_raw, dict):
            am_prov = am_prov_raw

        payload = {
            "valuation_currency": valuation_currency,
            "run_uuid": run_uuid,
        }

        await upsert_account_snapshot(
            conn,
            run_uuid,
            a_uuid,
            snapshot_at=snapshot_at,
            name=am["name"],
            currency_code=currency_code,
            orig_balance=orig_balance,
            balance=balance,
            fx_unavailable=fx_unavailable,
            fx_rate=fx_rate,
            fx_rate_source=fx_rate_source,
            fx_rate_timestamp=fx_rate_timestamp,
            coverage_ratio=coverage_ratio,
            provenance=am_prov,
            payload=payload,
        )

        snap = {
            "account_uuid": a_uuid,
            "name": am["name"],
            "currency_code": currency_code,
            "orig_balance": orig_balance,
            "balance": balance,
            "fx_unavailable": fx_unavailable,
            "coverage_ratio": coverage_ratio,
        }
        snapshots.append(snap)
        logger.debug(
            "Account snapshot: %s  balance=%.2f %s  fx_unavailable=%s",
            am["name"], orig_balance, currency_code, fx_unavailable,
        )

    logger.info("Built %d account snapshots for run %s", len(snapshots), run_uuid)
    return snapshots


async def build_daily_wealth(
    conn: Any,
    run_uuid: str,
    *,
    snapshot_date: date | None = None,
) -> list[dict]:
    """Build and persist daily wealth rows for a completed metric run.

    Computes total portfolio wealth (current values) and total invested
    (purchase sums) for each portfolio and for the aggregate "all" scope.

    Args:
        conn: asyncpg connection.
        run_uuid: UUID of the completed metric run.
        snapshot_date: Date to record wealth for (defaults to today).

    Returns:
        List of daily wealth dicts that were written.
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    portfolio_metrics = await get_portfolio_metrics_for_run(conn, run_uuid)
    account_metrics = await get_account_metrics_for_run(conn, run_uuid)

    rows: list[dict] = []

    # Per-portfolio wealth rows
    total_wealth_cents = 0
    total_invested_cents = 0

    for pm in portfolio_metrics:
        p_uuid = pm["portfolio_uuid"]
        current = pm.get("current_value_cents") or 0
        invested = pm.get("purchase_value_cents") or 0

        await upsert_daily_wealth(
            conn,
            date=snapshot_date,
            scope_uuid=p_uuid,
            scope_type="portfolio",
            total_wealth_cents=current,
            total_invested_cents=invested,
        )
        rows.append({
            "date": str(snapshot_date),
            "scope_uuid": p_uuid,
            "scope_type": "portfolio",
            "total_wealth_cents": current,
            "total_invested_cents": invested,
        })
        total_wealth_cents += current
        total_invested_cents += invested

    # Add account cash balances to aggregate wealth
    for am in account_metrics:
        balance_eur = am.get("balance_eur_cents") or 0
        total_wealth_cents += balance_eur

    # Aggregate "all" scope row
    await upsert_daily_wealth(
        conn,
        date=snapshot_date,
        scope_uuid="all",
        scope_type="global",
        total_wealth_cents=total_wealth_cents,
        total_invested_cents=total_invested_cents,
    )
    rows.append({
        "date": str(snapshot_date),
        "scope_uuid": "all",
        "scope_type": "global",
        "total_wealth_cents": total_wealth_cents,
        "total_invested_cents": total_invested_cents,
    })

    logger.info(
        "Built %d daily wealth rows for %s (total=%.2f EUR invested=%.2f EUR)",
        len(rows), snapshot_date,
        total_wealth_cents / _CENTS,
        total_invested_cents / _CENTS,
    )
    return rows


async def build_snapshots(
    conn: Any,
    run_uuid: str,
    *,
    snapshot_at: datetime | None = None,
    snapshot_date: date | None = None,
) -> dict[str, list[dict]]:
    """Run all snapshot builders for a completed metric run.

    This is the top-level entry point called by the pipeline orchestrator
    (Phase 9) after `run_metrics()` completes.

    Args:
        conn: asyncpg connection.
        run_uuid: UUID of the completed metric run.
        snapshot_at: Timestamp for snapshot records (defaults to now UTC).
        snapshot_date: Date for daily_wealth records (defaults to today).

    Returns:
        Dict with keys "portfolios", "accounts", "daily_wealth" containing
        the written snapshot lists.
    """
    if snapshot_at is None:
        snapshot_at = datetime.now(tz=timezone.utc)
    if snapshot_date is None:
        snapshot_date = snapshot_at.date()

    portfolio_snaps = await build_portfolio_snapshots(
        conn, run_uuid, snapshot_at=snapshot_at,
    )
    account_snaps = await build_account_snapshots(
        conn, run_uuid, snapshot_at=snapshot_at,
    )
    wealth_rows = await build_daily_wealth(
        conn, run_uuid, snapshot_date=snapshot_date,
    )

    logger.info(
        "Snapshot build complete for run %s: %d portfolios, %d accounts, %d wealth rows",
        run_uuid, len(portfolio_snaps), len(account_snaps), len(wealth_rows),
    )
    return {
        "portfolios": portfolio_snaps,
        "accounts": account_snaps,
        "daily_wealth": wealth_rows,
    }
