"""Metrics engine orchestrator.

Coordinates the full metrics calculation pipeline:
1. Load portfolios, accounts, securities from canonical tables
2. For each security in each portfolio:
   - Build sorted LineItem timeline from transactions
   - Run all calculation modules (cost, gains, dividends, IRR, delta)
3. Aggregate to portfolio-level metrics
4. Persist results to metric_runs, security_metrics, portfolio_metrics,
   account_metrics tables

All operations run within a single REPEATABLE READ transaction
(Architecture Decision 3).
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import date, datetime
from typing import Any

from app.db.queries import metrics as mq
from app.metrics.capital_gains import (
    calculate_capital_gains_fifo,
    calculate_capital_gains_moving_avg,
)
from app.metrics.cost_fifo import calculate_cost
from app.metrics.delta import calculate_delta
from app.metrics.dividends import calculate_dividends
from app.metrics.irr_calculation import calculate_security_irr
from app.metrics.shares import calculate_shares_held
from app.metrics.types import (
    LineItem,
    LineItemType,
    SecurityMetricsResult,
)
from app.models.constants import EIGHT_DECIMAL_SCALE, MONETARY_SCALE, TransactionType

__all__ = ["run_metrics", "build_line_items"]

logger = logging.getLogger(__name__)

# Map DB transaction type integers to LineItemType
_TX_TYPE_MAP: dict[int, LineItemType] = {
    TransactionType.BUY: LineItemType.BUY,
    TransactionType.SELL: LineItemType.SELL,
    TransactionType.INBOUND_DELIVERY: LineItemType.INBOUND_DELIVERY,
    TransactionType.OUTBOUND_DELIVERY: LineItemType.OUTBOUND_DELIVERY,
    TransactionType.SECURITY_TRANSFER: LineItemType.TRANSFER_IN,  # resolved per direction
    TransactionType.DIVIDEND: LineItemType.DIVIDEND,
    TransactionType.TAX: LineItemType.TAX,
    TransactionType.TAX_REFUND: LineItemType.TAX_REFUND,
    TransactionType.FEE: LineItemType.FEE,
    TransactionType.FEE_REFUND: LineItemType.FEE_REFUND,
}


def build_line_items(
    transactions: list[dict],
    units_by_tx: dict[str, list[dict]],
    *,
    portfolio_uuid: str,
    start_valuation: int = 0,
    start_shares: int = 0,
    start_date: date | None = None,
    end_valuation: int = 0,
    end_date: date | None = None,
) -> list[LineItem]:
    """Build a sorted timeline of LineItems from raw transaction data.

    Args:
        transactions: Transaction rows from DB.
        units_by_tx: Transaction units grouped by transaction_uuid.
        portfolio_uuid: Portfolio for filtering owner context.
        start_valuation: Valuation at start of period (term-currency cents).
        start_shares: Shares held at start (10^8 scaled).
        start_date: Start of reporting period.
        end_valuation: Valuation at end of period (term-currency cents).
        end_date: End of reporting period.

    Returns:
        List of LineItem sorted by (date, ordering_hint).
    """
    items: list[LineItem] = []

    # Add valuation at start
    if start_valuation != 0 and start_shares != 0 and start_date is not None:
        items.append(LineItem(
            date=start_date,
            item_type=LineItemType.VALUATION_START,
            shares=start_shares,
            amount=start_valuation,
            net_amount=start_valuation,
            owner=portfolio_uuid,
            ordering_hint=0,
        ))

    # Process transactions
    for tx in transactions:
        tx_type_int = tx["type"]
        tx_type = _TX_TYPE_MAP.get(tx_type_int)
        if tx_type is None:
            continue  # Skip unsupported transaction types (DEPOSIT, REMOVAL, etc.)

        # Resolve security transfers to TRANSFER_IN / TRANSFER_OUT
        if tx_type_int == TransactionType.SECURITY_TRANSFER:
            if tx.get("portfolio") == portfolio_uuid:
                tx_type = LineItemType.TRANSFER_IN
            else:
                tx_type = LineItemType.TRANSFER_OUT

        # Get transaction units
        tx_uuid = tx["uuid"]
        units = units_by_tx.get(tx_uuid, [])

        fee = 0
        tax = 0
        for u in units:
            if u["type"] == 2:  # UnitType.FEE
                fee += u.get("amount_eur_cents") or u.get("amount", 0)
            elif u["type"] == 1:  # UnitType.TAX
                tax += u.get("amount_eur_cents") or u.get("amount", 0)

        amount = tx.get("amount_eur_cents") or tx.get("amount", 0)
        net_amount = amount - fee - tax if tx_type in (LineItemType.BUY, LineItemType.INBOUND_DELIVERY) else amount

        # Ordering hint from updated_at
        updated_at = tx.get("updated_at")
        if isinstance(updated_at, datetime):
            hint = int(updated_at.timestamp())
        elif isinstance(updated_at, str):
            hint = int(datetime.fromisoformat(updated_at).timestamp())
        else:
            hint = 1  # between VALUATION_START(0) and VALUATION_END(MAX)

        source_owner = ""
        if tx_type == LineItemType.TRANSFER_IN:
            source_owner = tx.get("other_portfolio", "") or ""

        items.append(LineItem(
            date=tx["date"],
            item_type=tx_type,
            shares=tx.get("shares", 0) or 0,
            amount=abs(amount),
            net_amount=abs(net_amount),
            tax=abs(tax),
            fee=abs(fee),
            owner=tx.get("portfolio", "") or portfolio_uuid,
            source_owner=source_owner,
            ordering_hint=hint,
            security_currency_amount=abs(tx.get("amount", 0) or 0),
            fx_rate=tx.get("fx_rate_used", 0) or 0,
        ))

    # Add valuation at end
    if end_valuation != 0 and end_date is not None:
        items.append(LineItem(
            date=end_date,
            item_type=LineItemType.VALUATION_END,
            shares=0,  # not needed
            amount=end_valuation,
            net_amount=end_valuation,
            owner=portfolio_uuid,
            ordering_hint=2**63 - 1,  # Long.MAX_VALUE equivalent
        ))

    # Sort by (date, ordering_hint)
    items.sort(key=lambda it: (it.date, it.ordering_hint))

    return items


def _calculate_security_metrics(
    items: list[LineItem],
    *,
    security_uuid: str,
    portfolio_uuid: str,
    is_forex: bool = False,
    current_value: int = 0,
) -> SecurityMetricsResult:
    """Run all calculations on a single security's line items."""
    cost = calculate_cost(items)
    gains_fifo = calculate_capital_gains_fifo(items, is_forex=is_forex)
    gains_ma = calculate_capital_gains_moving_avg(items, is_forex=is_forex)
    dividends = calculate_dividends(items)
    delta_result = calculate_delta(items)
    irr = calculate_security_irr(items)
    shares = calculate_shares_held(items)

    return SecurityMetricsResult(
        security_uuid=security_uuid,
        portfolio_uuid=portfolio_uuid,
        shares_held=shares,
        cost=cost,
        capital_gains_fifo=gains_fifo,
        capital_gains_moving_avg=gains_ma,
        dividends=dividends,
        delta=delta_result,
        irr=irr,
        current_value=current_value,
        purchase_value=cost.fifo_cost,
    )


async def run_metrics(
    conn: Any,
    *,
    trigger: str = "manual",
    valuation_currency: str = "EUR",
    report_date: date | None = None,
) -> str:
    """Execute the full metrics calculation pipeline.

    Args:
        conn: asyncpg connection (should be in a transaction).
        trigger: What initiated this run ("manual", "file_change", "schedule").
        valuation_currency: Target currency for valuations.
        report_date: Date for end-of-period valuation (defaults to today).

    Returns:
        The run_uuid of the completed metric run.
    """
    if report_date is None:
        report_date = date.today()

    run_uuid = str(uuid.uuid4())
    start_time = time.monotonic()

    await mq.create_metric_run(
        conn, run_uuid,
        trigger=trigger,
        provenance=json.dumps({"valuation_currency": valuation_currency}),
    )

    processed_portfolios = 0
    processed_accounts = 0
    processed_securities = 0
    error_message: str | None = None

    try:
        # Load all portfolios and accounts
        portfolios = await mq.get_portfolios(conn)
        accounts = await mq.get_accounts(conn)

        # Process each portfolio
        for portfolio in portfolios:
            p_uuid = portfolio["uuid"]
            securities = await mq.get_securities_for_portfolio(conn, p_uuid)

            portfolio_current_value = 0
            portfolio_purchase_value = 0
            position_count = 0
            missing_value_positions = 0

            for sec in securities:
                s_uuid = sec["security_uuid"]
                holdings = sec.get("current_holdings", 0) or 0
                purchase_value = sec.get("purchase_value", 0) or 0
                sec_currency = sec.get("currency_code", valuation_currency)
                is_forex = sec_currency != valuation_currency

                # Get transactions for this security
                transactions = await mq.get_security_transactions(conn, s_uuid)
                tx_uuids = [tx["uuid"] for tx in transactions]
                units_list = await mq.get_transaction_units(conn, tx_uuids) if tx_uuids else []

                # Group units by transaction
                units_by_tx: dict[str, list[dict]] = {}
                for u in units_list:
                    key = u["transaction_uuid"]
                    units_by_tx.setdefault(key, []).append(u)

                # Calculate current value from latest price
                last_price = sec.get("last_price")
                if last_price and holdings:
                    # price is 10^8 scaled, holdings is 10^8 scaled
                    # current_value = (holdings * price) / 10^8 / 10^8 * 10^2
                    # = holdings * price / 10^14
                    current_value = round(holdings * last_price / (EIGHT_DECIMAL_SCALE * EIGHT_DECIMAL_SCALE) * MONETARY_SCALE)
                else:
                    current_value = 0
                    if holdings > 0:
                        missing_value_positions += 1

                # Build line items
                items = build_line_items(
                    transactions,
                    units_by_tx,
                    portfolio_uuid=p_uuid,
                    end_valuation=current_value,
                    end_date=report_date,
                )

                # Run calculations
                result = _calculate_security_metrics(
                    items,
                    security_uuid=s_uuid,
                    portfolio_uuid=p_uuid,
                    is_forex=is_forex,
                    current_value=current_value,
                )

                # Persist security metrics
                gain_abs = result.delta.delta
                gain_pct = result.delta.delta_percent if result.delta.cost != 0 else None

                await mq.upsert_security_metrics(
                    conn, run_uuid, p_uuid, s_uuid,
                    valuation_currency=valuation_currency,
                    security_currency_code=sec_currency,
                    holdings_raw=result.shares_held,
                    current_value_cents=current_value,
                    purchase_value_cents=result.purchase_value,
                    gain_abs_cents=gain_abs,
                    gain_pct=gain_pct,
                    total_change_eur_cents=gain_abs,
                    total_change_pct=gain_pct,
                    provenance=json.dumps({
                        "irr": result.irr if result.irr == result.irr else None,
                        "fifo_cost": result.cost.fifo_cost,
                        "moving_avg_cost": result.cost.moving_avg_cost,
                        "realized_gains_fifo": result.capital_gains_fifo.realized_gains,
                        "unrealized_gains_fifo": result.capital_gains_fifo.unrealized_gains,
                        "dividend_total": result.dividends.total_amount,
                        "dividend_periodicity": result.dividends.periodicity.value,
                    }),
                )

                portfolio_current_value += current_value
                portfolio_purchase_value += result.purchase_value
                position_count += 1
                processed_securities += 1

            # Persist portfolio metrics
            portfolio_gain = portfolio_current_value - portfolio_purchase_value
            portfolio_gain_pct = (
                portfolio_gain / portfolio_purchase_value
                if portfolio_purchase_value != 0 else None
            )

            await mq.upsert_portfolio_metrics(
                conn, run_uuid, p_uuid,
                valuation_currency=valuation_currency,
                current_value_cents=portfolio_current_value,
                purchase_value_cents=portfolio_purchase_value,
                gain_abs_cents=portfolio_gain,
                gain_pct=portfolio_gain_pct,
                total_change_eur_cents=portfolio_gain,
                total_change_pct=portfolio_gain_pct,
                position_count=position_count,
                missing_value_positions=missing_value_positions,
            )
            processed_portfolios += 1

        # Process accounts
        for account in accounts:
            a_uuid = account["uuid"]
            currency = account.get("currency_code", "EUR")
            balance = account.get("balance", 0) or 0

            balance_eur: int | None = None
            fx_rate: int | None = None
            fx_rate_source: str | None = None

            if currency == valuation_currency:
                balance_eur = balance
            else:
                # Look up FX rate
                from app.db.queries.fx import get_latest_fx_rate
                rate_info = await get_latest_fx_rate(conn, currency)
                if rate_info:
                    _, rate_scaled = rate_info
                    fx_rate = rate_scaled
                    fx_rate_source = "ecb"
                    # Convert: balance_eur = balance * rate / 10^8
                    balance_eur = round(balance * rate_scaled / EIGHT_DECIMAL_SCALE)

            await mq.upsert_account_metrics(
                conn, run_uuid, a_uuid,
                currency_code=currency,
                valuation_currency=valuation_currency,
                balance_native_cents=balance,
                balance_eur_cents=balance_eur,
                fx_rate=fx_rate,
                fx_rate_source=fx_rate_source,
            )
            processed_accounts += 1

    except Exception as exc:
        error_message = str(exc)
        logger.exception("Metrics engine failed: %s", exc)
        raise
    finally:
        duration_ms = int((time.monotonic() - start_time) * 1000)
        total = processed_portfolios + processed_accounts + processed_securities
        status = "failed" if error_message else "completed"

        await mq.finish_metric_run(
            conn, run_uuid,
            status=status,
            duration_ms=duration_ms,
            total_entities=total,
            processed_portfolios=processed_portfolios,
            processed_accounts=processed_accounts,
            processed_securities=processed_securities,
            error_message=error_message,
        )

    logger.info(
        "Metrics run %s completed in %dms: %d portfolios, %d accounts, %d securities",
        run_uuid, duration_ms, processed_portfolios, processed_accounts, processed_securities,
    )

    return run_uuid
