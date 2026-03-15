"""Tests for simple delta (profit/loss) calculation."""

from __future__ import annotations

from datetime import date

import pytest

from app.metrics.delta import calculate_delta
from app.metrics.types import LineItem, LineItemType


def _li(d: date, typ: LineItemType, amount: int = 0) -> LineItem:
    return LineItem(date=d, item_type=typ, amount=amount, ordering_hint=1)


class TestDeltaBasic:
    def test_empty(self):
        result = calculate_delta([])
        assert result.delta == 0
        assert result.cost == 0
        assert result.delta_percent == 0.0

    def test_buy_and_valuation(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 12000),
        ]
        result = calculate_delta(items)
        assert result.delta == 2000
        assert result.cost == 10000
        assert result.delta_percent == pytest.approx(0.20)

    def test_buy_sell_profit(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.SELL, 12000),
        ]
        result = calculate_delta(items)
        assert result.delta == 2000

    def test_buy_sell_loss(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.SELL, 8000),
        ]
        result = calculate_delta(items)
        assert result.delta == -2000

    def test_with_dividend(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.DIVIDEND, 500),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 10000),
        ]
        result = calculate_delta(items)
        assert result.delta == 500  # Dividend adds to return

    def test_with_fees_and_taxes(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.TAX, 200),
            _li(date(2024, 6, 1), LineItemType.FEE, 50),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 10000),
        ]
        result = calculate_delta(items)
        assert result.delta == -250

    def test_tax_refund(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.TAX, 200),
            _li(date(2024, 9, 1), LineItemType.TAX_REFUND, 50),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 10000),
        ]
        result = calculate_delta(items)
        assert result.delta == -150

    def test_fee_refund(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 3, 1), LineItemType.FEE, 100),
            _li(date(2024, 6, 1), LineItemType.FEE_REFUND, 40),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 10000),
        ]
        result = calculate_delta(items)
        assert result.delta == -60

    def test_transfers_neutral(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 3, 1), LineItemType.TRANSFER_IN, 5000),
            _li(date(2024, 6, 1), LineItemType.TRANSFER_OUT, 5000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 10000),
        ]
        result = calculate_delta(items)
        assert result.delta == 0

    def test_valuation_start_is_cost(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.VALUATION_START, 10000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 11000),
        ]
        result = calculate_delta(items)
        assert result.cost == 10000
        assert result.delta == 1000

    def test_delivery_inbound_adds_cost(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.INBOUND_DELIVERY, 10000),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 12000),
        ]
        result = calculate_delta(items)
        assert result.cost == 10000
        assert result.delta == 2000

    def test_delivery_outbound_is_inflow(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.OUTBOUND_DELIVERY, 12000),
        ]
        result = calculate_delta(items)
        assert result.delta == 2000

    def test_delta_percent_zero_cost(self):
        result = calculate_delta([])
        assert result.delta_percent == 0.0

    def test_complex_scenario(self):
        """Multiple buys, dividends, fees, and end valuation."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 3, 1), LineItemType.BUY, 5000),
            _li(date(2024, 6, 1), LineItemType.DIVIDEND, 300),
            _li(date(2024, 6, 1), LineItemType.TAX, 75),
            _li(date(2024, 9, 1), LineItemType.DIVIDEND, 300),
            _li(date(2024, 9, 1), LineItemType.TAX, 75),
            _li(date(2024, 12, 31), LineItemType.VALUATION_END, 16000),
        ]
        result = calculate_delta(items)
        # delta = -10000 - 5000 + 300 - 75 + 300 - 75 + 16000 = 1450
        assert result.delta == 1450
        assert result.cost == 15000
