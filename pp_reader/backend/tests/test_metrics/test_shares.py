"""Tests for shares held calculation."""

from __future__ import annotations

from datetime import date

from app.metrics.shares import calculate_shares_held
from app.metrics.types import LineItem, LineItemType


def _li(d: date, typ: LineItemType, shares: int = 0) -> LineItem:
    return LineItem(date=d, item_type=typ, shares=shares, ordering_hint=1)


class TestSharesHeld:
    def test_empty(self):
        assert calculate_shares_held([]) == 0

    def test_single_buy(self):
        items = [_li(date(2024, 1, 1), LineItemType.BUY, 10000)]
        assert calculate_shares_held(items) == 10000

    def test_buy_and_sell(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.SELL, 4000),
        ]
        assert calculate_shares_held(items) == 6000

    def test_buy_sell_all(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.SELL, 10000),
        ]
        assert calculate_shares_held(items) == 0

    def test_multiple_buys(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 5000),
            _li(date(2024, 2, 1), LineItemType.BUY, 3000),
            _li(date(2024, 3, 1), LineItemType.BUY, 2000),
        ]
        assert calculate_shares_held(items) == 10000

    def test_valuation_start_adds(self):
        items = [_li(date(2024, 1, 1), LineItemType.VALUATION_START, 5000)]
        assert calculate_shares_held(items) == 5000

    def test_delivery_inbound(self):
        items = [_li(date(2024, 1, 1), LineItemType.INBOUND_DELIVERY, 5000)]
        assert calculate_shares_held(items) == 5000

    def test_delivery_outbound(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 6, 1), LineItemType.OUTBOUND_DELIVERY, 3000),
        ]
        assert calculate_shares_held(items) == 7000

    def test_transfers_neutral(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.BUY, 10000),
            _li(date(2024, 2, 1), LineItemType.TRANSFER_IN, 5000),
            _li(date(2024, 3, 1), LineItemType.TRANSFER_OUT, 5000),
        ]
        assert calculate_shares_held(items) == 10000

    def test_complex_scenario(self):
        items = [
            _li(date(2024, 1, 1), LineItemType.VALUATION_START, 5000),
            _li(date(2024, 2, 1), LineItemType.BUY, 3000),
            _li(date(2024, 3, 1), LineItemType.SELL, 2000),
            _li(date(2024, 4, 1), LineItemType.INBOUND_DELIVERY, 1000),
            _li(date(2024, 5, 1), LineItemType.OUTBOUND_DELIVERY, 500),
        ]
        # 5000 + 3000 - 2000 + 1000 - 500 = 6500
        assert calculate_shares_held(items) == 6500
