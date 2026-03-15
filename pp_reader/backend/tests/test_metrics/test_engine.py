"""Tests for the metrics engine orchestrator.

Tests build_line_items and _calculate_security_metrics using in-memory
data (no database required).
"""

from __future__ import annotations

import math
from datetime import date, datetime

import pytest

from app.metrics.engine import build_line_items, _calculate_security_metrics
from app.metrics.types import LineItem, LineItemType


# ── build_line_items tests ────────────────────────────────────────────

class TestBuildLineItems:
    def test_empty_transactions(self):
        items = build_line_items([], {}, portfolio_uuid="P1")
        assert items == []

    def test_buy_transaction(self):
        txns = [{
            "uuid": "TX1",
            "type": 0,  # BUY
            "account": "A1",
            "portfolio": "P1",
            "date": date(2024, 1, 15),
            "currency_code": "EUR",
            "amount": 1000000,
            "amount_eur_cents": 1000000,
            "fx_rate_used": 0,
            "shares": 10000000000,  # 100 shares * 10^8
            "note": None,
            "security": "S1",
            "source": "import",
            "updated_at": datetime(2024, 1, 15, 10, 0, 0),
            "other_account": None,
            "other_portfolio": None,
        }]
        items = build_line_items(txns, {}, portfolio_uuid="P1")
        assert len(items) == 1
        assert items[0].item_type == LineItemType.BUY
        assert items[0].shares == 10000000000
        assert items[0].amount == 1000000

    def test_sell_transaction(self):
        txns = [{
            "uuid": "TX1",
            "type": 1,  # SELL
            "account": "A1",
            "portfolio": "P1",
            "date": date(2024, 6, 1),
            "currency_code": "EUR",
            "amount": 1200000,
            "amount_eur_cents": 1200000,
            "fx_rate_used": 0,
            "shares": 10000000000,
            "note": None,
            "security": "S1",
            "source": "import",
            "updated_at": datetime(2024, 6, 1, 10, 0, 0),
            "other_account": None,
            "other_portfolio": None,
        }]
        items = build_line_items(txns, {}, portfolio_uuid="P1")
        assert len(items) == 1
        assert items[0].item_type == LineItemType.SELL

    def test_dividend_transaction(self):
        txns = [{
            "uuid": "TX1",
            "type": 8,  # DIVIDEND
            "account": "A1",
            "portfolio": "P1",
            "date": date(2024, 6, 15),
            "currency_code": "EUR",
            "amount": 50000,
            "amount_eur_cents": 50000,
            "fx_rate_used": 0,
            "shares": 0,
            "note": None,
            "security": "S1",
            "source": "import",
            "updated_at": datetime(2024, 6, 15, 10, 0, 0),
            "other_account": None,
            "other_portfolio": None,
        }]
        items = build_line_items(txns, {}, portfolio_uuid="P1")
        assert len(items) == 1
        assert items[0].item_type == LineItemType.DIVIDEND

    def test_with_start_valuation(self):
        items = build_line_items(
            [], {},
            portfolio_uuid="P1",
            start_valuation=50000,
            start_shares=100,
            start_date=date(2024, 1, 1),
        )
        assert len(items) == 1
        assert items[0].item_type == LineItemType.VALUATION_START
        assert items[0].amount == 50000

    def test_with_end_valuation(self):
        items = build_line_items(
            [], {},
            portfolio_uuid="P1",
            end_valuation=60000,
            end_date=date(2024, 12, 31),
        )
        assert len(items) == 1
        assert items[0].item_type == LineItemType.VALUATION_END
        assert items[0].amount == 60000

    def test_with_transaction_units(self):
        txns = [{
            "uuid": "TX1",
            "type": 0,  # BUY
            "account": "A1",
            "portfolio": "P1",
            "date": date(2024, 1, 15),
            "currency_code": "EUR",
            "amount": 1050000,
            "amount_eur_cents": 1050000,
            "fx_rate_used": 0,
            "shares": 10000000000,
            "note": None,
            "security": "S1",
            "source": "import",
            "updated_at": datetime(2024, 1, 15, 10, 0, 0),
            "other_account": None,
            "other_portfolio": None,
        }]
        units = {
            "TX1": [
                {"type": 2, "amount": 30000, "amount_eur_cents": 30000},  # FEE
                {"type": 1, "amount": 20000, "amount_eur_cents": 20000},  # TAX
            ]
        }
        items = build_line_items(txns, units, portfolio_uuid="P1")
        assert len(items) == 1
        assert items[0].fee == 30000
        assert items[0].tax == 20000

    def test_sorted_by_date(self):
        txns = [
            {
                "uuid": "TX2",
                "type": 1,  # SELL
                "account": "A1",
                "portfolio": "P1",
                "date": date(2024, 6, 1),
                "currency_code": "EUR",
                "amount": 1200000,
                "amount_eur_cents": 1200000,
                "fx_rate_used": 0,
                "shares": 10000000000,
                "note": None,
                "security": "S1",
                "source": "import",
                "updated_at": datetime(2024, 6, 1, 10, 0, 0),
                "other_account": None,
                "other_portfolio": None,
            },
            {
                "uuid": "TX1",
                "type": 0,  # BUY
                "account": "A1",
                "portfolio": "P1",
                "date": date(2024, 1, 15),
                "currency_code": "EUR",
                "amount": 1000000,
                "amount_eur_cents": 1000000,
                "fx_rate_used": 0,
                "shares": 10000000000,
                "note": None,
                "security": "S1",
                "source": "import",
                "updated_at": datetime(2024, 1, 15, 10, 0, 0),
                "other_account": None,
                "other_portfolio": None,
            },
        ]
        items = build_line_items(txns, {}, portfolio_uuid="P1")
        assert items[0].date < items[1].date

    def test_skips_deposit_removal(self):
        """DEPOSIT (6) and REMOVAL (7) should be skipped."""
        txns = [{
            "uuid": "TX1",
            "type": 6,  # DEPOSIT
            "account": "A1",
            "portfolio": "P1",
            "date": date(2024, 1, 15),
            "currency_code": "EUR",
            "amount": 500000,
            "amount_eur_cents": 500000,
            "fx_rate_used": 0,
            "shares": 0,
            "note": None,
            "security": "S1",
            "source": "import",
            "updated_at": datetime(2024, 1, 15, 10, 0, 0),
            "other_account": None,
            "other_portfolio": None,
        }]
        items = build_line_items(txns, {}, portfolio_uuid="P1")
        assert len(items) == 0

    def test_security_transfer_in(self):
        txns = [{
            "uuid": "TX1",
            "type": 4,  # SECURITY_TRANSFER
            "account": "A1",
            "portfolio": "P1",
            "date": date(2024, 3, 1),
            "currency_code": "EUR",
            "amount": 0,
            "amount_eur_cents": 0,
            "fx_rate_used": 0,
            "shares": 5000000000,
            "note": None,
            "security": "S1",
            "source": "import",
            "updated_at": datetime(2024, 3, 1, 10, 0, 0),
            "other_account": None,
            "other_portfolio": "P2",
        }]
        items = build_line_items(txns, {}, portfolio_uuid="P1")
        assert len(items) == 1
        assert items[0].item_type == LineItemType.TRANSFER_IN
        assert items[0].source_owner == "P2"


# ── _calculate_security_metrics tests ─────────────────────────────────

def _simple_items(
    buy_amount: int = 10000,
    end_amount: int = 12000,
    dividend_amount: int = 0,
) -> list[LineItem]:
    """Create a simple buy → (dividend) → valuation-end scenario."""
    items = [
        LineItem(
            date=date(2024, 1, 1),
            item_type=LineItemType.BUY,
            shares=100,
            amount=buy_amount,
            net_amount=buy_amount,
            owner="P1",
            ordering_hint=1,
        ),
    ]
    if dividend_amount > 0:
        items.append(LineItem(
            date=date(2024, 6, 15),
            item_type=LineItemType.DIVIDEND,
            amount=dividend_amount,
            net_amount=dividend_amount,
            owner="P1",
            ordering_hint=2,
        ))
    items.append(LineItem(
        date=date(2024, 12, 31),
        item_type=LineItemType.VALUATION_END,
        amount=end_amount,
        net_amount=end_amount,
        owner="P1",
        ordering_hint=2**63 - 1,
    ))
    return items


class TestCalculateSecurityMetrics:
    def test_basic_gain(self):
        items = _simple_items(buy_amount=10000, end_amount=12000)
        result = _calculate_security_metrics(
            items, security_uuid="S1", portfolio_uuid="P1", current_value=12000,
        )
        assert result.security_uuid == "S1"
        assert result.portfolio_uuid == "P1"
        assert result.shares_held == 100
        assert result.cost.fifo_cost == 10000
        assert result.cost.moving_avg_cost == 10000
        assert result.capital_gains_fifo.unrealized_gains == 2000
        assert result.delta.delta == 2000
        assert result.current_value == 12000
        assert result.purchase_value == 10000

    def test_with_dividend(self):
        items = _simple_items(buy_amount=10000, end_amount=10000, dividend_amount=500)
        result = _calculate_security_metrics(
            items, security_uuid="S1", portfolio_uuid="P1",
        )
        assert result.dividends.total_amount == 500
        assert result.dividends.num_events == 1
        assert result.delta.delta == 500

    def test_loss(self):
        items = _simple_items(buy_amount=10000, end_amount=8000)
        result = _calculate_security_metrics(
            items, security_uuid="S1", portfolio_uuid="P1",
        )
        assert result.capital_gains_fifo.unrealized_gains == -2000
        assert result.delta.delta == -2000

    def test_irr_finite(self):
        items = _simple_items(buy_amount=10000, end_amount=11000)
        result = _calculate_security_metrics(
            items, security_uuid="S1", portfolio_uuid="P1",
        )
        assert math.isfinite(result.irr)
        assert result.irr > 0

    def test_buy_and_sell(self):
        items = [
            LineItem(
                date=date(2024, 1, 1),
                item_type=LineItemType.BUY,
                shares=100, amount=10000, net_amount=10000,
                owner="P1", ordering_hint=1,
            ),
            LineItem(
                date=date(2024, 6, 1),
                item_type=LineItemType.SELL,
                shares=100, amount=12000, net_amount=12000,
                owner="P1", ordering_hint=2,
            ),
        ]
        result = _calculate_security_metrics(
            items, security_uuid="S1", portfolio_uuid="P1",
        )
        assert result.shares_held == 0
        assert result.cost.fifo_cost == 0
        assert result.capital_gains_fifo.realized_gains == 2000
        assert result.delta.delta == 2000
