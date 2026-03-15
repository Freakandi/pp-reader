"""Tests for security-level IRR calculation from line items."""

from __future__ import annotations

import math
from datetime import date

import pytest

from app.metrics.irr_calculation import calculate_security_irr
from app.metrics.types import LineItem, LineItemType


def _li(
    d: date,
    typ: LineItemType,
    amount: int = 0,
    shares: int = 0,
    tax: int = 0,
) -> LineItem:
    return LineItem(
        date=d,
        item_type=typ,
        amount=amount,
        net_amount=amount,
        shares=shares,
        tax=tax,
        ordering_hint=1,
    )


class TestSecurityIRR:
    def test_empty_items(self):
        assert math.isnan(calculate_security_irr([]))

    def test_simple_buy_sell(self):
        """Buy for 10000, sell for 11000 after 1 year → ~10% IRR."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2025, 1, 1), LineItemType.SELL, amount=11000),
        ]
        irr = calculate_security_irr(items)
        assert irr == pytest.approx(0.10, abs=0.02)

    def test_buy_with_valuation(self):
        """Buy for 10000, valued at 11000 → ~10%."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=11000),
        ]
        irr = calculate_security_irr(items)
        assert irr == pytest.approx(0.10, abs=0.02)

    def test_valuation_start_and_end(self):
        """Existing position valued at start and end."""
        items = [
            _li(date(2024, 1, 1), LineItemType.VALUATION_START, amount=10000),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=11000),
        ]
        irr = calculate_security_irr(items)
        assert irr == pytest.approx(0.10, abs=0.02)

    def test_with_dividend(self):
        """Buy, receive dividends, valuation at end."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2024, 7, 1), LineItemType.DIVIDEND, amount=200, tax=50),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=10500),
        ]
        irr = calculate_security_irr(items)
        assert math.isfinite(irr)
        # Should be positive
        assert irr > 0

    def test_with_fees(self):
        """Fees are treated as outflows."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2024, 6, 1), LineItemType.FEE, amount=100),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=10500),
        ]
        irr = calculate_security_irr(items)
        assert math.isfinite(irr)

    def test_fee_refund(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2024, 6, 1), LineItemType.FEE_REFUND, amount=50),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=10500),
        ]
        irr = calculate_security_irr(items)
        assert math.isfinite(irr)

    def test_delivery_inbound(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.INBOUND_DELIVERY, amount=10000),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=11000),
        ]
        irr = calculate_security_irr(items)
        assert irr == pytest.approx(0.10, abs=0.02)

    def test_delivery_outbound(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2025, 1, 1), LineItemType.OUTBOUND_DELIVERY, amount=11000),
        ]
        irr = calculate_security_irr(items)
        assert irr == pytest.approx(0.10, abs=0.02)

    def test_transfer_in_out(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2024, 6, 1), LineItemType.TRANSFER_IN, amount=5000),
            _li(date(2024, 9, 1), LineItemType.TRANSFER_OUT, amount=5000),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=11000),
        ]
        irr = calculate_security_irr(items)
        assert math.isfinite(irr)

    def test_tax_events_ignored(self):
        """TAX and TAX_REFUND should not affect IRR for individual security."""
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2024, 6, 1), LineItemType.TAX, amount=100),
            _li(date(2024, 9, 1), LineItemType.TAX_REFUND, amount=50),
            _li(date(2025, 1, 1), LineItemType.VALUATION_END, amount=11000),
        ]
        irr = calculate_security_irr(items)
        assert irr == pytest.approx(0.10, abs=0.02)

    def test_negative_return(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, amount=10000),
            _li(date(2025, 1, 1), LineItemType.SELL, amount=9000),
        ]
        irr = calculate_security_irr(items)
        assert irr == pytest.approx(-0.10, abs=0.02)

    def test_only_dividends_no_buy(self):
        """Only dividend events → should still compute (or NaN if insufficient)."""
        items = [
            _li(date(2024, 3, 15), LineItemType.DIVIDEND, amount=200),
        ]
        irr = calculate_security_irr(items)
        # Single point → NaN
        assert math.isnan(irr)
