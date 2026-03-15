"""Canonical sync pipeline stage: promotes staging data to canonical PostgreSQL tables.

Entry point: ``sync_to_canonical(conn) -> None``

Design notes:
- Single REPEATABLE READ transaction wraps all canonical writes.
- Transfer protocol: zero-sum enforcement for paired cash/security transfers.
  Applied to stg_transactions BEFORE promoting to canonical.
  Only EUR transfers are processed in Phase 4 (cross-currency requires Phase 5 FX data).
- Account balances computed in Python from staged transactions (pure integer arithmetic).
- Portfolio securities holdings and purchase values computed via FIFO in Python.
- Historical prices: upsert to preserve Phase 5+ enrichment data.
- All other canonical tables: upsert (accounts, portfolios, securities) or
  delete-replace (transactions, portfolio_securities).
- All numeric values remain in BIGINT wire format; no floating-point conversions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

from app.db.queries.sync import (
    DELETE_PORTFOLIO_SECURITIES,
    DELETE_TRANSACTIONS,
    FILL_EUR_AMOUNT_EUR_CENTS,
    INSERT_PORTFOLIO_SECURITY,
    INSERT_TRANSACTION,
    INSERT_TRANSACTION_UNIT,
    LOAD_STG_ACCOUNTS,
    LOAD_STG_HISTORICAL_PRICES,
    LOAD_STG_PORTFOLIOS,
    LOAD_STG_SECURITIES,
    LOAD_STG_TRANSACTION_UNITS,
    LOAD_STG_TRANSACTIONS,
    SELECT_TRANSFER_PAIRS,
    UPDATE_TRANSFER_LEG,
    UPSERT_ACCOUNT,
    UPSERT_HISTORICAL_PRICE,
    UPSERT_PORTFOLIO,
    UPSERT_SECURITY,
)
from app.models.constants import EIGHT_DECIMAL_SCALE, TransactionType

_LOGGER = logging.getLogger(__name__)

# Transaction types that credit an account balance.
_CREDIT_TYPES: frozenset[int] = frozenset(
    {
        TransactionType.DEPOSIT,
        TransactionType.INTEREST,
        TransactionType.DIVIDEND,
        TransactionType.TAX_REFUND,
        TransactionType.SELL,
        TransactionType.FEE_REFUND,
    }
)

# Transaction types that debit an account balance.
_DEBIT_TYPES: frozenset[int] = frozenset(
    {
        TransactionType.REMOVAL,
        TransactionType.FEE,
        TransactionType.INTEREST_CHARGE,
        TransactionType.TAX,
        TransactionType.BUY,
    }
)

# Transaction types that add shares to a portfolio position.
_PURCHASE_TYPES: frozenset[int] = frozenset(
    {TransactionType.BUY, TransactionType.INBOUND_DELIVERY}
)

# Transaction types that remove shares from a portfolio position.
_SALE_TYPES: frozenset[int] = frozenset(
    {TransactionType.SELL, TransactionType.OUTBOUND_DELIVERY}
)


# ---------------------------------------------------------------------------
# Internal dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _HoldingLot:
    """A FIFO lot of shares with associated acquisition cost."""

    shares: int  # 10^8-scaled share units
    amount: int  # acquisition cost in account currency minor units (e.g. cents)
    fx_amount: int | None = None  # acquisition cost in security native currency minor units


@dataclass(slots=True)
class _PurchaseResult:
    """Aggregated purchase metrics for a portfolio × security pair."""

    purchase_value: int = 0  # account currency, minor units
    security_currency_total: int = 0  # native security currency, minor units
    account_currency_total: int = 0  # account currency, minor units
    current_holdings: int = 0  # 10^8-scaled share units


# ---------------------------------------------------------------------------
# Helper: build lookup maps from unit rows
# ---------------------------------------------------------------------------


def _build_units_map(unit_rows: list[Any]) -> dict[str, dict[str, Any]]:
    """Aggregate fx_amount and fx_currency_code per transaction UUID.

    Returns a dict keyed by transaction_uuid → {fx_amount: int, fx_currency_code: str|None}.
    Used by account balance computation for cross-currency CASH_TRANSFER credits.
    """
    units_map: dict[str, dict[str, Any]] = {}
    for row in unit_rows:
        tx_uuid = row["transaction_uuid"]
        fx_amount = row["fx_amount"]
        fx_currency = row["fx_currency_code"]
        if fx_amount is None:
            continue
        entry = units_map.setdefault(
            tx_uuid, {"fx_amount": 0, "fx_currency_code": None}
        )
        try:
            entry["fx_amount"] += int(fx_amount)
        except (TypeError, ValueError):
            pass
        if not entry["fx_currency_code"] and fx_currency:
            entry["fx_currency_code"] = fx_currency
    return units_map


def _build_full_units_map(unit_rows: list[Any]) -> dict[str, list[dict[str, Any]]]:
    """Group all unit records by transaction_uuid.

    Returns {transaction_uuid: [row_dict, ...]} for purchase value computation.
    """
    result: dict[str, list[dict[str, Any]]] = {}
    for row in unit_rows:
        tx_uuid = row["transaction_uuid"]
        result.setdefault(tx_uuid, []).append(dict(row))
    return result


# ---------------------------------------------------------------------------
# Transfer protocol
# ---------------------------------------------------------------------------


async def _apply_transfer_protocol(conn: asyncpg.Connection) -> None:
    """Enforce zero-sum invariant for paired transfer legs in stg_transactions.

    Three cases (mirrors legacy canonical_sync.py):
    - EUR outbound / Foreign inbound: inbound EUR = −outbound EUR
    - Foreign outbound / EUR inbound: outbound EUR = −inbound EUR
    - Foreign / Foreign: each leg EUR = ± average of magnitudes

    Only processes pairs where both legs have amount_eur_cents set.
    Cross-currency pairs without FX data remain unchanged (Phase 5 will fix them).

    FX rates stored as BIGINT at EIGHT_DECIMAL_SCALE:
        fx_rate_used = abs(new_eur_cents) * 10^8 / abs(native_amount_cents)
    """
    rows = await conn.fetch(SELECT_TRANSFER_PAIRS)
    if not rows:
        return

    updates: list[tuple[int, int | None, str]] = []  # (new_eur_cents, new_rate, uuid)

    for row in rows:
        eur1 = row["eur1"]
        eur2 = row["eur2"]
        if eur1 is None or eur2 is None:
            continue

        eur1 = int(eur1)
        eur2 = int(eur2)
        currency1 = (row["currency1"] or "").strip().upper()
        currency2 = (row["currency2"] or "").strip().upper()
        is_c1_eur = currency1 in ("EUR", "")
        is_c2_eur = currency2 in ("EUR", "")
        amount1 = int(row["amount1"] or 0)
        amount2 = int(row["amount2"] or 0)

        if is_c1_eur and not is_c2_eur:
            # EUR outbound → Foreign inbound: correct inbound EUR to −outbound.
            new_eur2 = -eur1
            if new_eur2 != eur2:
                new_rate2 = (
                    round(abs(new_eur2) * EIGHT_DECIMAL_SCALE / abs(amount2))
                    if amount2
                    else None
                )
                updates.append((new_eur2, new_rate2, row["uuid2"]))

        elif not is_c1_eur and is_c2_eur:
            # Foreign outbound → EUR inbound: correct outbound EUR to −inbound.
            new_eur1 = -eur2
            if new_eur1 != eur1:
                new_rate1 = (
                    round(abs(new_eur1) * EIGHT_DECIMAL_SCALE / abs(amount1))
                    if amount1
                    else None
                )
                updates.append((new_eur1, new_rate1, row["uuid1"]))

        elif not is_c1_eur and not is_c2_eur:
            # Both foreign: average magnitudes; outbound negative, inbound positive.
            avg = round((abs(eur1) + abs(eur2)) / 2)
            new_eur1 = -avg
            new_eur2 = avg
            if new_eur1 != eur1:
                new_rate1 = (
                    round(abs(new_eur1) * EIGHT_DECIMAL_SCALE / abs(amount1))
                    if amount1
                    else None
                )
                updates.append((new_eur1, new_rate1, row["uuid1"]))
            if new_eur2 != eur2:
                new_rate2 = (
                    round(abs(new_eur2) * EIGHT_DECIMAL_SCALE / abs(amount2))
                    if amount2
                    else None
                )
                updates.append((new_eur2, new_rate2, row["uuid2"]))
        # EUR+EUR: both legs already correct (PP generates zero-sum EUR transfers).

    for new_eur, new_rate, tx_uuid in updates:
        await conn.execute(UPDATE_TRANSFER_LEG, new_eur, new_rate, tx_uuid)

    if updates:
        _LOGGER.info("Transfer protocol: adjusted %d transfer leg(s).", len(updates))


# ---------------------------------------------------------------------------
# Account balance computation
# ---------------------------------------------------------------------------


def _compute_account_balance(
    account_uuid: str,
    account_currency: str,
    transactions: list[Any],
    units_map: dict[str, dict[str, Any]],
) -> int:
    """Compute account balance in minor currency units (pure integer arithmetic).

    Mirrors legacy ``db_calc_account_balance`` without float conversions.

    CASH_TRANSFER (type 5):
    - Source account: subtract amount (debit in account currency).
    - Destination account: credit amount, or fx_amount if the unit carries
      the correct target currency (cross-currency transfer support).

    All other transaction types apply to the primary account only.
    """
    saldo: int = 0
    for tx in transactions:
        acc = tx["account"]
        other_acc = tx["other_account"]
        if account_uuid not in (acc, other_acc):
            continue

        tx_type = int(tx["type"])
        amount = int(tx["amount"] or 0)

        if tx_type == TransactionType.CASH_TRANSFER:
            if acc == account_uuid:
                saldo -= amount
            elif other_acc == account_uuid:
                credit = amount
                unit = units_map.get(tx["uuid"])
                if (
                    unit
                    and unit.get("fx_amount") is not None
                    and unit.get("fx_currency_code") == account_currency
                ):
                    credit = int(unit["fx_amount"])
                saldo += credit
            continue

        # All other types: only the primary account is affected.
        if acc != account_uuid:
            continue

        if tx_type in _CREDIT_TYPES:
            saldo += amount
        elif tx_type in _DEBIT_TYPES:
            saldo -= amount

    return saldo


# ---------------------------------------------------------------------------
# Holdings and purchase value computation (FIFO)
# ---------------------------------------------------------------------------


def _compute_holdings(transactions: list[Any]) -> dict[tuple[str, str], int]:
    """Compute current share holdings (10^8-scaled BIGINT) per portfolio × security."""
    holdings: dict[tuple[str, str], int] = {}
    for tx in transactions:
        sec = tx["security"]
        port = tx["portfolio"]
        if not sec or not port:
            continue
        shares = int(tx["shares"] or 0)
        key = (port, sec)
        tx_type = int(tx["type"])
        if tx_type in _PURCHASE_TYPES:
            holdings[key] = holdings.get(key, 0) + shares
        elif tx_type in _SALE_TYPES:
            holdings[key] = holdings.get(key, 0) - shares
    return holdings


def _apply_fifo_sale(lots: list[_HoldingLot], shares_to_sell: int) -> list[_HoldingLot]:
    """Reduce FIFO lots by ``shares_to_sell``; return remaining lots.

    Partial lot consumption reduces cost proportionally (integer arithmetic).
    """
    remaining = shares_to_sell
    updated: list[_HoldingLot] = []
    for lot in lots:
        if remaining <= 0:
            updated.append(lot)
            continue
        if lot.shares > remaining:
            # Partial consumption: keep the unsold fraction.
            kept = lot.shares - remaining
            ratio_num = kept  # ratio = kept / lot.shares (avoid float)
            new_amount = round(lot.amount * ratio_num / lot.shares)
            new_fx = (
                round(lot.fx_amount * ratio_num / lot.shares)
                if lot.fx_amount is not None
                else None
            )
            updated.append(_HoldingLot(shares=kept, amount=new_amount, fx_amount=new_fx))
            remaining = 0
        else:
            remaining -= lot.shares
            # Lot fully consumed; do not add to updated.
    return updated


def _compute_purchase_values(
    transactions: list[Any],
    full_units_map: dict[str, list[dict[str, Any]]],
) -> dict[tuple[str, str], _PurchaseResult]:
    """Compute FIFO purchase values per portfolio × security pair.

    All amounts remain as BIGINT minor units (cents for EUR).
    Native (security-currency) amount is sourced from transaction units of
    type GROSS_VALUE (0) when available.

    Returns a dict keyed by (portfolio_uuid, security_uuid) → _PurchaseResult.
    """
    lots: dict[tuple[str, str], list[_HoldingLot]] = {}

    for tx in transactions:
        sec = tx["security"]
        port = tx["portfolio"]
        if not sec or not port:
            continue
        key = (port, sec)
        shares = int(tx["shares"] or 0)
        amount = int(tx["amount"] or 0)
        tx_type = int(tx["type"])

        if tx_type in _PURCHASE_TYPES:
            if shares <= 0:
                continue
            # Look for native fx_amount from a GROSS_VALUE unit.
            fx_amount: int | None = None
            for unit in full_units_map.get(tx["uuid"], []):
                if int(unit.get("type", -1)) == 0 and unit.get("fx_amount") is not None:
                    fx_amount = int(unit["fx_amount"])
                    break
            lots.setdefault(key, []).append(
                _HoldingLot(shares=shares, amount=amount, fx_amount=fx_amount)
            )

        elif tx_type in _SALE_TYPES:
            shares_to_sell = abs(shares)
            if shares_to_sell <= 0:
                continue
            if key in lots:
                lots[key] = _apply_fifo_sale(lots[key], shares_to_sell)

    results: dict[tuple[str, str], _PurchaseResult] = {}
    for key, key_lots in lots.items():
        total_amount = sum(lot.amount for lot in key_lots)
        has_fx = any(lot.fx_amount is not None for lot in key_lots)
        total_fx = (
            sum(lot.fx_amount for lot in key_lots if lot.fx_amount is not None)
            if has_fx
            else None
        )
        total_shares = sum(lot.shares for lot in key_lots)
        results[key] = _PurchaseResult(
            purchase_value=total_amount,
            # Use native fx_amount when available; fall back to account amount.
            security_currency_total=total_fx if total_fx is not None else total_amount,
            account_currency_total=total_amount,
            current_holdings=total_shares,
        )
    return results


# ---------------------------------------------------------------------------
# Sync helpers
# ---------------------------------------------------------------------------


async def _sync_accounts(
    conn: asyncpg.Connection,
    rows: list[Any],
    transactions: list[Any],
    units_map: dict[str, dict[str, Any]],
) -> None:
    """Upsert accounts into canonical table with computed balances."""
    account_rows = []
    for row in rows:
        uuid = row["uuid"]
        if not uuid:
            continue
        currency = (row["currency_code"] or "EUR").strip().upper()
        balance = _compute_account_balance(uuid, currency, transactions, units_map)
        account_rows.append(
            (
                uuid,
                row["name"],
                row["currency_code"] or "",
                row["note"],
                row["is_retired"],
                row["updated_at"],
                balance,
            )
        )
    if account_rows:
        await conn.executemany(UPSERT_ACCOUNT, account_rows)


async def _sync_portfolios(conn: asyncpg.Connection, rows: list[Any]) -> None:
    """Upsert portfolios into canonical table."""
    portfolio_rows = [
        (
            row["uuid"],
            row["name"],
            row["note"],
            row["reference_account"],
            row["is_retired"],
            row["updated_at"],
        )
        for row in rows
        if row["uuid"]
    ]
    if portfolio_rows:
        await conn.executemany(UPSERT_PORTFOLIO, portfolio_rows)


async def _sync_securities(conn: asyncpg.Connection, rows: list[Any]) -> None:
    """Upsert securities into canonical table."""
    security_rows = [
        (
            row["uuid"],
            row["name"],
            row["isin"],
            row["wkn"],
            row["ticker_symbol"],
            row["feed"],
            None,  # type — not present in staging; set by Phase 6 enrichment
            row["currency_code"],
            row["is_retired"],
            row["updated_at"],
            row["latest_close"],  # last_price (BIGINT, 10^8 scaled)
            row["latest_date"],  # last_price_date
            row["latest_feed"],  # last_price_source
            row["updated_at"],  # last_price_fetched_at
        )
        for row in rows
        if row["uuid"]
    ]
    if security_rows:
        await conn.executemany(UPSERT_SECURITY, security_rows)


async def _sync_transactions(conn: asyncpg.Connection, rows: list[Any]) -> None:
    """Delete-replace transactions; CASCADE removes orphaned transaction_units."""
    await conn.execute(DELETE_TRANSACTIONS)
    txn_rows = [
        (
            row["uuid"],
            row["type"],
            row["account"],
            row["portfolio"],
            row["other_account"],
            row["other_portfolio"],
            row["other_uuid"],
            row["other_updated_at"],
            row["date"],
            row["currency_code"],
            row["amount"],
            row["amount_eur_cents"],
            row["fx_rate_used"],
            row["shares"],
            row["note"],
            row["security"],
            row["source"],
            row["updated_at"],
        )
        for row in rows
        if row["uuid"]
    ]
    if txn_rows:
        await conn.executemany(INSERT_TRANSACTION, txn_rows)


async def _sync_transaction_units(conn: asyncpg.Connection, rows: list[Any]) -> None:
    """Insert transaction units (clean slate after transactions delete)."""
    unit_rows = [
        (
            row["transaction_uuid"],
            row["type"],
            row["amount"],
            row["amount_eur_cents"],
            row["fx_rate_used"],
            row["currency_code"],
            row["fx_amount"],
            row["fx_currency_code"],
            row["fx_rate_to_base"],
        )
        for row in rows
        if row["transaction_uuid"]
    ]
    if unit_rows:
        await conn.executemany(INSERT_TRANSACTION_UNIT, unit_rows)


async def _sync_historical_prices(conn: asyncpg.Connection, rows: list[Any]) -> None:
    """Upsert historical prices; preserves Phase 5+ enriched fields."""
    price_rows = [
        (
            row["security_uuid"],
            row["date"],
            row["close"],
            row["high"],
            row["low"],
            row["volume"],
        )
        for row in rows
        if row["security_uuid"] and row["date"]
    ]
    if price_rows:
        await conn.executemany(UPSERT_HISTORICAL_PRICE, price_rows)


async def _sync_portfolio_securities(
    conn: asyncpg.Connection,
    transactions: list[Any],
    unit_rows: list[Any],
) -> None:
    """Delete-replace portfolio_securities with FIFO-computed holdings/values.

    avg_price columns are derived as:
        avg_price = purchase_value * EIGHT_DECIMAL_SCALE // current_holdings

    This gives the price per whole share in minor currency units (e.g., cents/share).
    current_value is left at 0 — populated by Phase 7 (metrics engine).
    """
    await conn.execute(DELETE_PORTFOLIO_SECURITIES)

    full_units_map = _build_full_units_map(unit_rows)
    purchase_vals = _compute_purchase_values(transactions, full_units_map)
    holdings = _compute_holdings(transactions)

    all_keys = set(holdings.keys()) | set(purchase_vals.keys())
    insert_rows = []
    for portfolio_uuid, security_uuid in all_keys:
        key = (portfolio_uuid, security_uuid)
        current_holdings = holdings.get(key, 0)
        pv = purchase_vals.get(key)

        purchase_value = pv.purchase_value if pv else 0
        sec_total = pv.security_currency_total if pv else 0
        acc_total = pv.account_currency_total if pv else 0

        avg_price: int | None = None
        if current_holdings > 0 and purchase_value:
            avg_price = round(purchase_value * EIGHT_DECIMAL_SCALE / current_holdings)

        insert_rows.append(
            (
                portfolio_uuid,
                security_uuid,
                current_holdings,
                purchase_value,
                avg_price,  # avg_price_native
                avg_price,  # avg_price_security (same as native in Phase 4; refined in Phase 7)
                avg_price,  # avg_price_account
                sec_total,
                acc_total,
                0,  # current_value — Phase 7
            )
        )

    if insert_rows:
        await conn.executemany(INSERT_PORTFOLIO_SECURITY, insert_rows)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def sync_to_canonical(conn: asyncpg.Connection) -> None:
    """Promote staging tables to canonical tables within a REPEATABLE READ transaction.

    Must be called after ``pipeline.ingestion.ingest()`` has populated stg_* tables.

    Sync order:
        1. Fill EUR amount_eur_cents in staging
        2. Apply transfer protocol (zero-sum enforcement for paired transfers)
        3. Load all staging data into memory
        4. Upsert accounts (with computed balances)
        5. Upsert portfolios
        6. Upsert securities
        7. Delete-replace transactions (CASCADE removes transaction_units)
        8. Insert transaction units
        9. Upsert historical prices
        10. Delete-replace portfolio_securities (FIFO holdings + purchase values)
    """
    async with conn.transaction(isolation="repeatable_read"):
        # Prepare staging data for transfer protocol.
        await conn.execute(FILL_EUR_AMOUNT_EUR_CENTS)
        await _apply_transfer_protocol(conn)

        # Load all staging data.
        rows_accounts = await conn.fetch(LOAD_STG_ACCOUNTS)
        rows_portfolios = await conn.fetch(LOAD_STG_PORTFOLIOS)
        rows_securities = await conn.fetch(LOAD_STG_SECURITIES)
        rows_txns = await conn.fetch(LOAD_STG_TRANSACTIONS)
        rows_units = await conn.fetch(LOAD_STG_TRANSACTION_UNITS)
        rows_prices = await conn.fetch(LOAD_STG_HISTORICAL_PRICES)

        # Build helper maps.
        units_map = _build_units_map(rows_units)

        # Sync canonical tables in dependency order.
        await _sync_accounts(conn, rows_accounts, rows_txns, units_map)
        await _sync_portfolios(conn, rows_portfolios)
        await _sync_securities(conn, rows_securities)
        await _sync_transactions(conn, rows_txns)
        await _sync_transaction_units(conn, rows_units)
        await _sync_historical_prices(conn, rows_prices)
        await _sync_portfolio_securities(conn, rows_txns, rows_units)

    _LOGGER.info(
        "Canonical sync complete: %d accounts, %d portfolios, %d securities, "
        "%d transactions, %d units, %d prices",
        len(rows_accounts),
        len(rows_portfolios),
        len(rows_securities),
        len(rows_txns),
        len(rows_units),
        len(rows_prices),
    )
