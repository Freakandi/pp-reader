"""Tests for FIFO cost basis calculation."""

from __future__ import annotations

from datetime import date

import pytest

from app.metrics.cost_fifo import calculate_cost
from app.metrics.types import LineItem, LineItemType


def _li(
    d: date,
    typ: LineItemType,
    shares: int = 0,
    amount: int = 0,
    net_amount: int = 0,
    tax: int = 0,
    fee: int = 0,
    owner: str = "P1",
    source_owner: str = "",
) -> LineItem:
    return LineItem(
        date=d,
        item_type=typ,
        shares=shares,
        amount=amount,
        net_amount=net_amount or amount,
        tax=tax,
        fee=fee,
        owner=owner,
        source_owner=source_owner,
        ordering_hint=1,
    )


class TestCostBasic:
    def test_single_buy(self):
        items = [_li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000)]
        result = calculate_cost(items)
        assert result.shares_held == 100
        assert result.fifo_cost == 10000
        assert result.moving_avg_cost == 10000

    def test_two_buys(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.BUY, shares=50, amount=6000),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 150
        assert result.fifo_cost == 16000
        assert result.moving_avg_cost == 16000

    def test_buy_then_sell_all(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=100, amount=12000),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 0
        assert result.fifo_cost == 0
        assert result.moving_avg_cost == 0

    def test_buy_then_partial_sell(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=40, amount=5000),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 60
        assert result.fifo_cost == 6000
        assert result.moving_avg_cost == 6000

    def test_empty_items(self):
        result = calculate_cost([])
        assert result.shares_held == 0
        assert result.fifo_cost == 0

    def test_only_valuation(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.VALUATION_START, shares=100, amount=10000),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 100
        assert result.fifo_cost == 10000


class TestFIFOOrdering:
    def test_fifo_sells_oldest_first(self):
        """Two buys at different prices, sell should consume the first."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),  # $100/share
            _li(date(2024, 2, 1), LineItemType.BUY, shares=100, amount=20000),  # $200/share
            _li(date(2024, 3, 1), LineItemType.SELL, shares=100, amount=15000),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 100
        # After selling the first 100 shares, only the $200/share lot remains
        assert result.fifo_cost == 20000

    def test_fifo_partial_lot_consumption(self):
        """Sell consumes part of first lot, then part of second."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.BUY, shares=100, amount=20000),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=150, amount=22500),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 50
        # 50 shares remain from second lot (10000 of 20000 remaining)
        assert result.fifo_cost == 10000

    def test_fifo_multiple_sells(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=30, amount=3600),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=30, amount=3600),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 40
        assert result.fifo_cost == 4000


class TestMovingAverage:
    def test_moving_avg_after_sell(self):
        """Moving average maintains proportional cost."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.BUY, shares=100, amount=20000),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=100, amount=15000),
        ]
        result = calculate_cost(items)
        # Moving avg: 30000 total for 200 shares = 150/share
        # After selling 100: 100 shares remain → 15000
        assert result.moving_avg_cost == 15000

    def test_moving_avg_sell_all(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=100, amount=12000),
        ]
        result = calculate_cost(items)
        assert result.moving_avg_cost == 0

    def test_moving_avg_oversell(self):
        """Sell more than held → cost resets to 0."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=150, amount=15000),
        ]
        result = calculate_cost(items)
        assert result.moving_avg_cost == 0
        assert result.shares_held == 0


class TestFeesAndTaxes:
    def test_fees_tracked(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, fee=50),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=100, amount=12000, fee=50),
        ]
        result = calculate_cost(items)
        assert result.total_fees == 100

    def test_taxes_tracked(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, tax=200),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=100, amount=12000, tax=300),
        ]
        result = calculate_cost(items)
        assert result.total_taxes == 500

    def test_net_cost_excludes_fees_taxes(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10500, net_amount=10000, fee=300, tax=200),
        ]
        result = calculate_cost(items)
        assert result.fifo_cost == 10500
        assert result.net_fifo_cost == 10000

    def test_dividend_taxes(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 6, 1), LineItemType.DIVIDEND, shares=100, amount=200, tax=50),
        ]
        result = calculate_cost(items)
        assert result.total_taxes == 50

    def test_tax_refund_subtracts(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.TAX, amount=100),
            _li(date(2024, 2, 1), LineItemType.TAX_REFUND, amount=30),
        ]
        result = calculate_cost(items)
        assert result.total_taxes == 70

    def test_fee_refund_subtracts(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.FEE, amount=100),
            _li(date(2024, 2, 1), LineItemType.FEE_REFUND, amount=30),
        ]
        result = calculate_cost(items)
        assert result.total_fees == 70


class TestTransfers:
    def test_transfer_in_reassigns_lot(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, owner="P1"),
            _li(date(2024, 2, 1), LineItemType.TRANSFER_IN, shares=100, owner="P2", source_owner="P1"),
        ]
        result = calculate_cost(items)
        # Total shares unchanged
        assert result.shares_held == 100
        assert result.fifo_cost == 10000

    def test_partial_transfer(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, owner="P1"),
            _li(date(2024, 2, 1), LineItemType.TRANSFER_IN, shares=40, owner="P2", source_owner="P1"),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 100
        assert result.fifo_cost == 10000

    def test_transfer_out_ignored(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, owner="P1"),
            _li(date(2024, 2, 1), LineItemType.TRANSFER_OUT, shares=50, owner="P1"),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 100


class TestInboundDelivery:
    def test_inbound_delivery_adds_like_buy(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.INBOUND_DELIVERY, shares=100, amount=10000),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 100
        assert result.fifo_cost == 10000

    def test_outbound_delivery_removes_like_sell(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000),
            _li(date(2024, 2, 1), LineItemType.OUTBOUND_DELIVERY, shares=50, amount=6000),
        ]
        result = calculate_cost(items)
        assert result.shares_held == 50
        assert result.fifo_cost == 5000
