"""Tests for capital gains calculations (FIFO and moving average)."""

from __future__ import annotations

from datetime import date

import pytest

from app.metrics.capital_gains import (
    calculate_capital_gains_fifo,
    calculate_capital_gains_moving_avg,
)
from app.metrics.types import LineItem, LineItemType


def _li(
    d: date,
    typ: LineItemType,
    shares: int = 0,
    amount: int = 0,
    net_amount: int = 0,
    owner: str = "P1",
    source_owner: str = "",
    sec_amount: int = 0,
    fx_rate: int = 0,
) -> LineItem:
    return LineItem(
        date=d,
        item_type=typ,
        shares=shares,
        amount=amount,
        net_amount=net_amount or amount,
        owner=owner,
        source_owner=source_owner,
        ordering_hint=1,
        security_currency_amount=sec_amount,
        fx_rate=fx_rate,
    )


# ── FIFO Capital Gains ───────────────────────────────────────────────

class TestFifoGainsBasic:
    def test_no_items(self):
        result = calculate_capital_gains_fifo([])
        assert result.realized_gains == 0
        assert result.unrealized_gains == 0

    def test_buy_and_sell_gain(self):
        """Buy at 100, sell at 120 → realized gain = 20."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 6, 1), LineItemType.SELL, shares=100, amount=12000, net_amount=12000),
        ]
        result = calculate_capital_gains_fifo(items)
        assert result.realized_gains == 2000

    def test_buy_and_sell_loss(self):
        """Buy at 100, sell at 80 → realized loss = -20."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 6, 1), LineItemType.SELL, shares=100, amount=8000, net_amount=8000),
        ]
        result = calculate_capital_gains_fifo(items)
        assert result.realized_gains == -2000

    def test_unrealized_gain(self):
        """Buy at 100, valuation at 120 → unrealized gain = 20."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, amount=12000, net_amount=12000),
        ]
        result = calculate_capital_gains_fifo(items)
        assert result.unrealized_gains == 2000
        assert result.realized_gains == 0

    def test_unrealized_loss(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, amount=8000, net_amount=8000),
        ]
        result = calculate_capital_gains_fifo(items)
        assert result.unrealized_gains == -2000


class TestFifoGainsFIFOOrder:
    def test_sells_oldest_lot_first(self):
        """Two lots at different prices; sell should match oldest first."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),  # $100
            _li(date(2024, 2, 1), LineItemType.BUY, shares=100, amount=15000, net_amount=15000),  # $150
            _li(date(2024, 3, 1), LineItemType.SELL, shares=100, amount=12000, net_amount=12000),  # sell at $120
        ]
        result = calculate_capital_gains_fifo(items)
        # Sold oldest lot (cost 10000) for 12000 → gain = 2000
        assert result.realized_gains == 2000

    def test_partial_lot_sell(self):
        """Sell 50 of 100 share lot."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=50, amount=6000, net_amount=6000),
        ]
        result = calculate_capital_gains_fifo(items)
        # Cost of 50 shares = 5000, sold for 6000 → gain = 1000
        assert result.realized_gains == 1000

    def test_sell_across_lots(self):
        """Sell spans two lots."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=50, amount=5000, net_amount=5000),
            _li(date(2024, 2, 1), LineItemType.BUY, shares=50, amount=7500, net_amount=7500),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=75, amount=9000, net_amount=9000),
        ]
        result = calculate_capital_gains_fifo(items)
        # First lot: 50 shares cost 5000, sold portion = 50/75*9000 = 6000 → gain 1000
        # Second lot: 25 shares cost 3750, sold portion = 25/75*9000 = 3000 → loss -750
        assert result.realized_gains == 250

    def test_mixed_realized_and_unrealized(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=50, amount=6000, net_amount=6000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, amount=7000, net_amount=7000),
        ]
        result = calculate_capital_gains_fifo(items)
        # Realized: sold 50@5000 cost for 6000 → +1000
        assert result.realized_gains == 1000
        # Unrealized: 50 remaining @5000 cost, valued at 7000 → +2000
        assert result.unrealized_gains == 2000


class TestFifoTransfers:
    def test_transfer_preserves_cost_basis(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000, owner="P1"),
            _li(date(2024, 2, 1), LineItemType.TRANSFER_IN, shares=100, owner="P2", source_owner="P1"),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=100, amount=12000, net_amount=12000, owner="P2"),
        ]
        result = calculate_capital_gains_fifo(items)
        assert result.realized_gains == 2000

    def test_partial_transfer(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000, owner="P1"),
            _li(date(2024, 2, 1), LineItemType.TRANSFER_IN, shares=50, owner="P2", source_owner="P1"),
            _li(date(2024, 3, 1), LineItemType.SELL, shares=50, amount=6000, net_amount=6000, owner="P2"),
        ]
        result = calculate_capital_gains_fifo(items)
        assert result.realized_gains == 1000


# ── Moving Average Capital Gains ──────────────────────────────────────

class TestMovingAvgGainsBasic:
    def test_no_items(self):
        result = calculate_capital_gains_moving_avg([])
        assert result.realized_gains == 0
        assert result.unrealized_gains == 0

    def test_buy_and_sell_gain(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 6, 1), LineItemType.SELL, shares=100, amount=12000, net_amount=12000),
        ]
        result = calculate_capital_gains_moving_avg(items)
        assert result.realized_gains == 2000

    def test_buy_and_sell_loss(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 6, 1), LineItemType.SELL, shares=100, amount=8000, net_amount=8000),
        ]
        result = calculate_capital_gains_moving_avg(items)
        assert result.realized_gains == -2000

    def test_unrealized_gain(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, amount=12000, net_amount=12000),
        ]
        result = calculate_capital_gains_moving_avg(items)
        assert result.unrealized_gains == 2000

    def test_moving_avg_uses_average_cost(self):
        """Two buys at different prices, sell uses average."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),  # $100
            _li(date(2024, 2, 1), LineItemType.BUY, shares=100, amount=20000, net_amount=20000),  # $200
            _li(date(2024, 3, 1), LineItemType.SELL, shares=100, amount=18000, net_amount=18000),  # sell at $180
        ]
        result = calculate_capital_gains_moving_avg(items)
        # Avg cost = 30000/200 = 150/share, sold 100 at 180 → gain = 3000
        assert result.realized_gains == 3000

    def test_oversell_resets(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 2, 1), LineItemType.SELL, shares=150, amount=15000, net_amount=15000),
        ]
        result = calculate_capital_gains_moving_avg(items)
        # Oversell: no gain calculated, cost resets
        assert result.realized_gains == 0


class TestMovingAvgWithValuation:
    def test_start_valuation_plus_buy(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.VALUATION_START, shares=100, net_amount=10000),
            _li(date(2024, 2, 1), LineItemType.BUY, shares=100, amount=12000, net_amount=12000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, amount=25000, net_amount=25000),
        ]
        result = calculate_capital_gains_moving_avg(items)
        # Cost = 10000 + 12000 = 22000, valued at 25000 → unrealized = 3000
        assert result.unrealized_gains == 3000

    def test_delivery_inbound(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.INBOUND_DELIVERY, shares=100, amount=10000, net_amount=10000),
            _li(date(2024, 6, 1), LineItemType.SELL, shares=100, amount=12000, net_amount=12000),
        ]
        result = calculate_capital_gains_moving_avg(items)
        assert result.realized_gains == 2000
