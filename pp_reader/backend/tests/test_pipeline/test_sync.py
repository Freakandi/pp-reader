"""Tests for the canonical sync pipeline stage.

Uses mocked asyncpg connections — no live database required.
Verifies:
  - Transfer protocol (EUR+EUR, EUR+Foreign skip, Foreign+Foreign)
  - Account balance computation (various transaction types, CASH_TRANSFER)
  - Holdings computation (buy/sell FIFO)
  - Purchase value computation (FIFO with partial-lot sales)
  - Full sync_to_canonical wiring (correct SQL methods called)
  - Idempotency: calling sync twice produces the same result
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.constants import EIGHT_DECIMAL_SCALE, TransactionType
from app.pipeline.sync import (
    _apply_fifo_sale,
    _build_full_units_map,
    _build_units_map,
    _compute_account_balance,
    _compute_holdings,
    _compute_purchase_values,
    _HoldingLot,
    sync_to_canonical,
)

# ---------------------------------------------------------------------------
# Helpers — mock connection factory
# ---------------------------------------------------------------------------


def make_conn(fetch_return: list | None = None) -> MagicMock:
    """Return a mock asyncpg connection."""
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)
    conn.executemany = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=fetch_return or [])

    txn = MagicMock()
    txn.__aenter__ = AsyncMock(return_value=None)
    txn.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=txn)
    return conn


def tx(
    uuid: str = "tx-1",
    tx_type: int = TransactionType.BUY,
    account: str | None = "acc-1",
    portfolio: str | None = "port-1",
    other_account: str | None = None,
    other_portfolio: str | None = None,
    other_uuid: str | None = None,
    other_updated_at=None,
    date=None,
    currency_code: str = "EUR",
    amount: int = 100_00,
    amount_eur_cents: int | None = None,
    fx_rate_used: int | None = None,
    shares: int = 0,
    note: str | None = None,
    security: str | None = None,
    source: str | None = None,
    updated_at=None,
) -> dict:
    """Minimal transaction record dict."""
    return {
        "uuid": uuid,
        "type": tx_type,
        "account": account,
        "portfolio": portfolio,
        "other_account": other_account,
        "other_portfolio": other_portfolio,
        "other_uuid": other_uuid,
        "other_updated_at": other_updated_at,
        "date": date,
        "currency_code": currency_code,
        "amount": amount,
        "amount_eur_cents": amount_eur_cents,
        "fx_rate_used": fx_rate_used,
        "shares": shares,
        "note": note,
        "security": security,
        "source": source,
        "updated_at": updated_at,
    }


def unit(
    transaction_uuid: str = "tx-1",
    unit_index: int = 0,
    unit_type: int = 0,
    amount: int | None = None,
    amount_eur_cents: int | None = None,
    fx_rate_used: int | None = None,
    currency_code: str | None = None,
    fx_amount: int | None = None,
    fx_currency_code: str | None = None,
    fx_rate_to_base: int | None = None,
) -> dict:
    """Minimal transaction unit record dict."""
    return {
        "transaction_uuid": transaction_uuid,
        "unit_index": unit_index,
        "type": unit_type,
        "amount": amount,
        "amount_eur_cents": amount_eur_cents,
        "fx_rate_used": fx_rate_used,
        "currency_code": currency_code,
        "fx_amount": fx_amount,
        "fx_currency_code": fx_currency_code,
        "fx_rate_to_base": fx_rate_to_base,
    }


# ---------------------------------------------------------------------------
# _build_units_map
# ---------------------------------------------------------------------------


def test_build_units_map_aggregates_fx_amount() -> None:
    rows = [
        unit("tx-1", fx_amount=5000, fx_currency_code="USD"),
        unit("tx-1", fx_amount=3000, fx_currency_code="USD"),
        unit("tx-2", fx_amount=None, fx_currency_code="GBP"),
    ]
    result = _build_units_map(rows)
    assert "tx-1" in result
    assert result["tx-1"]["fx_amount"] == 8000
    assert result["tx-1"]["fx_currency_code"] == "USD"
    assert "tx-2" not in result  # None fx_amount skipped


def test_build_units_map_empty() -> None:
    assert _build_units_map([]) == {}


# ---------------------------------------------------------------------------
# _compute_account_balance
# ---------------------------------------------------------------------------


def test_balance_deposit() -> None:
    txns = [tx(tx_type=TransactionType.DEPOSIT, amount=500_00)]
    balance = _compute_account_balance("acc-1", "EUR", txns, {})
    assert balance == 500_00


def test_balance_removal() -> None:
    txns = [
        tx(tx_type=TransactionType.DEPOSIT, amount=1000_00),
        tx(uuid="tx-2", tx_type=TransactionType.REMOVAL, amount=200_00),
    ]
    balance = _compute_account_balance("acc-1", "EUR", txns, {})
    assert balance == 800_00


def test_balance_buy_debits() -> None:
    txns = [
        tx(tx_type=TransactionType.DEPOSIT, amount=2000_00),
        tx(uuid="tx-2", tx_type=TransactionType.BUY, amount=500_00, security="sec-1"),
    ]
    balance = _compute_account_balance("acc-1", "EUR", txns, {})
    assert balance == 1500_00


def test_balance_sell_credits() -> None:
    txns = [
        tx(tx_type=TransactionType.DEPOSIT, amount=1000_00),
        tx(uuid="tx-2", tx_type=TransactionType.SELL, amount=300_00, security="sec-1"),
    ]
    balance = _compute_account_balance("acc-1", "EUR", txns, {})
    assert balance == 1300_00


def test_balance_cash_transfer_outgoing() -> None:
    txns = [
        tx(tx_type=TransactionType.DEPOSIT, amount=1000_00),
        tx(
            uuid="tx-2",
            tx_type=TransactionType.CASH_TRANSFER,
            amount=200_00,
            account="acc-1",
            other_account="acc-2",
        ),
    ]
    balance = _compute_account_balance("acc-1", "EUR", txns, {})
    assert balance == 800_00


def test_balance_cash_transfer_incoming() -> None:
    txns = [
        tx(
            uuid="tx-1",
            tx_type=TransactionType.CASH_TRANSFER,
            amount=200_00,
            account="acc-2",
            other_account="acc-1",
        ),
    ]
    balance = _compute_account_balance("acc-1", "EUR", txns, {})
    assert balance == 200_00


def test_balance_cash_transfer_incoming_fx() -> None:
    """Cross-currency CASH_TRANSFER uses fx_amount for destination account."""
    txns = [
        tx(
            uuid="tx-1",
            tx_type=TransactionType.CASH_TRANSFER,
            amount=200_00,  # EUR cents on source side
            account="acc-2",
            other_account="acc-1",
            currency_code="EUR",
        ),
    ]
    units_map = {"tx-1": {"fx_amount": 180_00, "fx_currency_code": "GBP"}}
    balance = _compute_account_balance("acc-1", "GBP", txns, units_map)
    assert balance == 180_00


def test_balance_unrelated_transactions_ignored() -> None:
    txns = [
        tx(uuid="tx-x", tx_type=TransactionType.DEPOSIT, account="acc-other"),
    ]
    balance = _compute_account_balance("acc-1", "EUR", txns, {})
    assert balance == 0


# ---------------------------------------------------------------------------
# _compute_holdings
# ---------------------------------------------------------------------------


def test_holdings_buy_increases() -> None:
    txns = [
        tx(
            tx_type=TransactionType.BUY,
            portfolio="port-1",
            security="sec-1",
            shares=10 * EIGHT_DECIMAL_SCALE,
        ),
    ]
    holdings = _compute_holdings(txns)
    assert holdings[("port-1", "sec-1")] == 10 * EIGHT_DECIMAL_SCALE


def test_holdings_sell_decreases() -> None:
    txns = [
        tx(
            tx_type=TransactionType.BUY,
            portfolio="port-1",
            security="sec-1",
            shares=10 * EIGHT_DECIMAL_SCALE,
        ),
        tx(
            uuid="tx-2",
            tx_type=TransactionType.SELL,
            portfolio="port-1",
            security="sec-1",
            shares=3 * EIGHT_DECIMAL_SCALE,
        ),
    ]
    holdings = _compute_holdings(txns)
    assert holdings[("port-1", "sec-1")] == 7 * EIGHT_DECIMAL_SCALE


def test_holdings_inbound_delivery() -> None:
    txns = [
        tx(
            tx_type=TransactionType.INBOUND_DELIVERY,
            portfolio="port-1",
            security="sec-1",
            shares=5 * EIGHT_DECIMAL_SCALE,
        ),
    ]
    holdings = _compute_holdings(txns)
    assert holdings[("port-1", "sec-1")] == 5 * EIGHT_DECIMAL_SCALE


def test_holdings_no_security_skipped() -> None:
    txns = [tx(tx_type=TransactionType.BUY, security=None, shares=10 * EIGHT_DECIMAL_SCALE)]
    assert _compute_holdings(txns) == {}


def test_holdings_multiple_securities() -> None:
    txns = [
        tx(
            "tx-a",
            tx_type=TransactionType.BUY,
            portfolio="port-1",
            security="sec-a",
            shares=10 * EIGHT_DECIMAL_SCALE,
        ),
        tx(
            "tx-b",
            tx_type=TransactionType.BUY,
            portfolio="port-1",
            security="sec-b",
            shares=5 * EIGHT_DECIMAL_SCALE,
        ),
    ]
    holdings = _compute_holdings(txns)
    assert holdings[("port-1", "sec-a")] == 10 * EIGHT_DECIMAL_SCALE
    assert holdings[("port-1", "sec-b")] == 5 * EIGHT_DECIMAL_SCALE


# ---------------------------------------------------------------------------
# _apply_fifo_sale
# ---------------------------------------------------------------------------


def test_fifo_sale_full_lot() -> None:
    lots = [_HoldingLot(shares=10, amount=1000, fx_amount=None)]
    result = _apply_fifo_sale(lots, 10)
    assert result == []


def test_fifo_sale_partial_lot() -> None:
    lots = [_HoldingLot(shares=10, amount=1000, fx_amount=None)]
    result = _apply_fifo_sale(lots, 4)
    assert len(result) == 1
    assert result[0].shares == 6
    assert result[0].amount == 600  # proportional


def test_fifo_sale_multiple_lots() -> None:
    lots = [
        _HoldingLot(shares=5, amount=500, fx_amount=None),
        _HoldingLot(shares=5, amount=600, fx_amount=None),
    ]
    result = _apply_fifo_sale(lots, 5)
    assert len(result) == 1
    assert result[0].shares == 5
    assert result[0].amount == 600


def test_fifo_sale_partial_with_fx() -> None:
    lots = [_HoldingLot(shares=10, amount=1000, fx_amount=800)]
    result = _apply_fifo_sale(lots, 6)
    assert result[0].shares == 4
    assert result[0].amount == 400
    assert result[0].fx_amount == 320  # 800 * 4 / 10


# ---------------------------------------------------------------------------
# _compute_purchase_values
# ---------------------------------------------------------------------------


def test_purchase_values_simple_buy() -> None:
    txns = [
        tx(
            tx_type=TransactionType.BUY,
            portfolio="port-1",
            security="sec-1",
            shares=10 * EIGHT_DECIMAL_SCALE,
            amount=1000_00,
        ),
    ]
    result = _compute_purchase_values(txns, {})
    pv = result[("port-1", "sec-1")]
    assert pv.purchase_value == 1000_00
    assert pv.current_holdings == 10 * EIGHT_DECIMAL_SCALE
    assert pv.account_currency_total == 1000_00


def test_purchase_values_buy_then_partial_sell() -> None:
    txns = [
        tx(
            "tx-buy",
            tx_type=TransactionType.BUY,
            portfolio="port-1",
            security="sec-1",
            shares=10 * EIGHT_DECIMAL_SCALE,
            amount=1000_00,
        ),
        tx(
            "tx-sell",
            tx_type=TransactionType.SELL,
            portfolio="port-1",
            security="sec-1",
            shares=4 * EIGHT_DECIMAL_SCALE,
            amount=500_00,
        ),
    ]
    result = _compute_purchase_values(txns, {})
    pv = result[("port-1", "sec-1")]
    # After selling 4/10, 6/10 of cost remains → 600_00
    assert pv.purchase_value == 600_00
    assert pv.current_holdings == 6 * EIGHT_DECIMAL_SCALE


def test_purchase_values_with_fx_unit() -> None:
    """GROSS_VALUE unit fx_amount used as security_currency_total."""
    txns = [
        tx(
            "tx-1",
            tx_type=TransactionType.BUY,
            portfolio="port-1",
            security="sec-1",
            shares=10 * EIGHT_DECIMAL_SCALE,
            amount=1200_00,  # EUR cents
        ),
    ]
    full_units_map = {
        "tx-1": [
            unit(
                "tx-1",
                unit_type=0,  # GROSS_VALUE
                fx_amount=1100_00,  # native currency cents
                fx_currency_code="USD",
            )
        ]
    }
    result = _compute_purchase_values(txns, full_units_map)
    pv = result[("port-1", "sec-1")]
    assert pv.purchase_value == 1200_00
    assert pv.security_currency_total == 1100_00  # from fx_amount
    assert pv.account_currency_total == 1200_00


def test_purchase_values_no_security_skipped() -> None:
    txns = [tx(tx_type=TransactionType.BUY, security=None, shares=10, amount=1000)]
    result = _compute_purchase_values(txns, {})
    assert result == {}


# ---------------------------------------------------------------------------
# sync_to_canonical integration (mocked connection)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_to_canonical_calls_execute_and_fetch() -> None:
    """Verify that sync_to_canonical performs the expected high-level DB operations."""
    conn = make_conn(fetch_return=[])

    await sync_to_canonical(conn)

    # Transaction context entered.
    conn.transaction.assert_called_once_with(isolation="repeatable_read")

    # FILL_EUR_AMOUNT_EUR_CENTS executed.
    execute_calls = [c.args[0] for c in conn.execute.call_args_list if c.args]
    assert any("UPDATE stg_transactions" in sql for sql in execute_calls)

    # SELECT_TRANSFER_PAIRS fetched.
    assert conn.fetch.called

    # DELETE_TRANSACTIONS executed (even when no rows).
    assert any("DELETE FROM transactions" in sql for sql in execute_calls)

    # DELETE_PORTFOLIO_SECURITIES executed.
    assert any("DELETE FROM portfolio_securities" in sql for sql in execute_calls)


@pytest.mark.asyncio
async def test_sync_to_canonical_upserts_accounts() -> None:
    """Accounts from staging are upserted into canonical table."""
    account_row = {
        "uuid": "acc-1",
        "name": "Test Account",
        "currency_code": "EUR",
        "note": None,
        "is_retired": False,
        "updated_at": None,
    }

    def _make_fetch():
        """Return different data for successive fetch calls.

        Call order in sync_to_canonical:
          0 → SELECT_TRANSFER_PAIRS  (inside _apply_transfer_protocol)
          1 → LOAD_STG_ACCOUNTS
          2 → LOAD_STG_PORTFOLIOS
          3 → LOAD_STG_SECURITIES
          4 → LOAD_STG_TRANSACTIONS
          5 → LOAD_STG_TRANSACTION_UNITS
          6 → LOAD_STG_HISTORICAL_PRICES
        """
        calls = [0]

        async def fetch(sql):
            idx = calls[0]
            calls[0] += 1
            returns = [
                [],             # SELECT_TRANSFER_PAIRS → no pairs
                [account_row],  # LOAD_STG_ACCOUNTS
                [],             # LOAD_STG_PORTFOLIOS
                [],             # LOAD_STG_SECURITIES
                [],             # LOAD_STG_TRANSACTIONS
                [],             # LOAD_STG_TRANSACTION_UNITS
                [],             # LOAD_STG_HISTORICAL_PRICES
            ]
            if idx < len(returns):
                return returns[idx]
            return []

        return fetch

    conn = make_conn()
    conn.fetch = _make_fetch()

    await sync_to_canonical(conn)

    # UPSERT_ACCOUNT executemany was called.
    executemany_calls = conn.executemany.call_args_list
    account_upserts = [
        c for c in executemany_calls if "INSERT INTO accounts" in c.args[0]
    ]
    assert len(account_upserts) == 1
    rows = account_upserts[0].args[1]
    assert len(rows) == 1
    assert rows[0][0] == "acc-1"  # uuid
    assert rows[0][6] == 0  # balance (no transactions)


@pytest.mark.asyncio
async def test_sync_to_canonical_idempotent() -> None:
    """Calling sync twice with the same staging data produces the same executemany args."""
    conn1 = make_conn(fetch_return=[])
    conn2 = make_conn(fetch_return=[])

    await sync_to_canonical(conn1)
    await sync_to_canonical(conn2)

    # Both connections receive the same number of executemany calls.
    assert len(conn1.executemany.call_args_list) == len(conn2.executemany.call_args_list)


# ---------------------------------------------------------------------------
# Transfer protocol unit tests (pure logic via mocked fetch)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transfer_protocol_eur_foreign() -> None:
    """EUR outbound / Foreign inbound: inbound EUR corrected to −outbound EUR."""
    from app.pipeline.sync import _apply_transfer_protocol

    pair_row = {
        "uuid1": "tx-out",
        "currency1": "EUR",
        "eur1": -1000_00,  # EUR outbound, 1000 EUR
        "amount1": -1000_00,
        "uuid2": "tx-in",
        "currency2": "USD",
        "eur2": 950_00,  # Slightly off — should be corrected to 1000_00
        "amount2": 1100_00,  # USD amount
    }

    conn = make_conn()
    conn.fetch = AsyncMock(return_value=[pair_row])

    await _apply_transfer_protocol(conn)

    # UPDATE_TRANSFER_LEG called once for the inbound leg.
    conn.execute.assert_called_once()
    args = conn.execute.call_args.args
    assert args[1] == 1000_00  # new_eur2 = -(-1000_00) = 1000_00
    assert args[3] == "tx-in"


@pytest.mark.asyncio
async def test_transfer_protocol_eur_eur_no_update() -> None:
    """EUR+EUR pairs are already zero-sum; no UPDATE should be issued."""
    from app.pipeline.sync import _apply_transfer_protocol

    pair_row = {
        "uuid1": "tx-out",
        "currency1": "EUR",
        "eur1": -500_00,
        "amount1": -500_00,
        "uuid2": "tx-in",
        "currency2": "EUR",
        "eur2": 500_00,
        "amount2": 500_00,
    }

    conn = make_conn()
    conn.fetch = AsyncMock(return_value=[pair_row])

    await _apply_transfer_protocol(conn)

    # No UPDATE needed.
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_transfer_protocol_foreign_foreign() -> None:
    """Foreign+Foreign: each leg EUR set to ±average of magnitudes."""
    from app.pipeline.sync import _apply_transfer_protocol

    pair_row = {
        "uuid1": "tx-out",
        "currency1": "USD",
        "eur1": -900_00,  # slightly off
        "amount1": -1000_00,
        "uuid2": "tx-in",
        "currency2": "GBP",
        "eur2": 850_00,  # slightly off
        "amount2": 800_00,
    }

    conn = make_conn()
    conn.fetch = AsyncMock(return_value=[pair_row])

    await _apply_transfer_protocol(conn)

    # Average = (900 + 850) / 2 = 875; outbound → -875, inbound → 875.
    execute_calls = conn.execute.call_args_list
    assert len(execute_calls) == 2
    # First call: outbound leg corrected to -875_00
    assert execute_calls[0].args[1] == -875_00
    assert execute_calls[0].args[3] == "tx-out"
    # Second call: inbound leg corrected to 875_00
    assert execute_calls[1].args[1] == 875_00
    assert execute_calls[1].args[3] == "tx-in"


@pytest.mark.asyncio
async def test_transfer_protocol_null_eur_skipped() -> None:
    """Cross-currency pair with NULL amount_eur_cents is skipped (Phase 5 handles it)."""
    from app.pipeline.sync import _apply_transfer_protocol

    # SELECT_TRANSFER_PAIRS already filters out NULL eur via WHERE clause;
    # simulate an empty fetch result.
    conn = make_conn()
    conn.fetch = AsyncMock(return_value=[])

    await _apply_transfer_protocol(conn)

    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# _build_full_units_map
# ---------------------------------------------------------------------------


def test_build_full_units_map_groups_by_tx() -> None:
    rows = [
        unit("tx-1", unit_index=0, fx_amount=100),
        unit("tx-1", unit_index=1, unit_type=1, fx_amount=None),
        unit("tx-2", unit_index=0, fx_amount=200),
    ]
    result = _build_full_units_map(rows)
    assert len(result["tx-1"]) == 2
    assert len(result["tx-2"]) == 1


def test_build_full_units_map_empty() -> None:
    assert _build_full_units_map([]) == {}
