"""Tests for dividend calculation with periodicity detection."""

from __future__ import annotations

from datetime import date

import pytest

from app.metrics.dividends import calculate_dividends
from app.metrics.types import LineItem, LineItemType, Periodicity


def _div(d: date, amount: int, owner: str = "P1") -> LineItem:
    return LineItem(
        date=d,
        item_type=LineItemType.DIVIDEND,
        amount=amount,
        net_amount=amount,
        owner=owner,
        ordering_hint=1,
    )


def _buy(d: date, shares: int = 100, amount: int = 10000) -> LineItem:
    return LineItem(
        date=d,
        item_type=LineItemType.BUY,
        shares=shares,
        amount=amount,
        net_amount=amount,
        owner="P1",
        ordering_hint=1,
    )


class TestDividendBasic:
    def test_no_dividends(self):
        result = calculate_dividends([_buy(date(2024, 1, 1))])
        assert result.total_amount == 0
        assert result.num_events == 0
        assert result.periodicity == Periodicity.NONE

    def test_empty_items(self):
        result = calculate_dividends([])
        assert result.periodicity == Periodicity.NONE

    def test_single_dividend(self):
        items = [_div(date(2024, 6, 15), 500)]
        result = calculate_dividends(items)
        assert result.total_amount == 500
        assert result.num_events == 1
        assert result.last_payment_date == date(2024, 6, 15)

    def test_multiple_dividends_sum(self):
        items = [
            _div(date(2024, 3, 15), 200),
            _div(date(2024, 6, 15), 250),
            _div(date(2024, 9, 15), 200),
            _div(date(2024, 12, 15), 250),
        ]
        result = calculate_dividends(items)
        assert result.total_amount == 900
        assert result.num_events == 4

    def test_last_payment_date(self):
        items = [
            _div(date(2024, 3, 15), 200),
            _div(date(2024, 12, 15), 200),
        ]
        result = calculate_dividends(items)
        assert result.last_payment_date == date(2024, 12, 15)


class TestDividendPeriodicity:
    def test_quarterly_detection(self):
        """4 payments per year, ~90 days apart → quarterly."""
        items = [
            _div(date(2024, 3, 15), 200),
            _div(date(2024, 6, 15), 200),
            _div(date(2024, 9, 15), 200),
            _div(date(2024, 12, 15), 200),
        ]
        result = calculate_dividends(items)
        assert result.periodicity == Periodicity.QUARTERLY

    def test_annual_detection(self):
        """1 payment per year → annual."""
        items = [
            _div(date(2023, 6, 15), 500),
            _div(date(2024, 6, 15), 500),
            _div(date(2025, 6, 15), 500),
        ]
        result = calculate_dividends(items)
        assert result.periodicity == Periodicity.ANNUAL

    def test_semiannual_detection(self):
        """2 payments per year → semiannual."""
        items = [
            _div(date(2024, 3, 15), 300),
            _div(date(2024, 9, 15), 300),
            _div(date(2025, 3, 15), 300),
            _div(date(2025, 9, 15), 300),
        ]
        result = calculate_dividends(items)
        assert result.periodicity == Periodicity.SEMIANNUAL

    def test_monthly_detection(self):
        """12 payments per year → monthly."""
        items = [_div(date(2024, m, 15), 100) for m in range(1, 13)]
        items.append(_div(date(2025, 1, 15), 100))
        result = calculate_dividends(items)
        assert result.periodicity == Periodicity.MONTHLY

    def test_single_payment_unknown(self):
        items = [_div(date(2024, 6, 15), 500)]
        result = calculate_dividends(items)
        assert result.periodicity == Periodicity.UNKNOWN

    def test_two_payments_same_year(self):
        items = [
            _div(date(2024, 3, 15), 200),
            _div(date(2024, 9, 15), 200),
        ]
        result = calculate_dividends(items)
        assert result.periodicity == Periodicity.SEMIANNUAL


class TestDividendRateOfReturn:
    def test_rate_with_cost(self):
        items = [
            _div(date(2024, 6, 15), 500),
        ]
        cost_map = {date(2024, 6, 15): 10000}
        result = calculate_dividends(items, moving_avg_cost_at_payment=cost_map)
        assert result.rate_of_return_per_year == pytest.approx(0.05, abs=0.001)

    def test_rate_without_cost(self):
        items = [_div(date(2024, 6, 15), 500)]
        result = calculate_dividends(items)
        # Without cost, rate should be NaN and not contribute
        assert result.rate_of_return_per_year == 0.0

    def test_rate_multi_year(self):
        items = [
            _div(date(2023, 6, 15), 500),
            _div(date(2024, 6, 15), 600),
        ]
        cost_map = {
            date(2023, 6, 15): 10000,
            date(2024, 6, 15): 10000,
        }
        result = calculate_dividends(items, moving_avg_cost_at_payment=cost_map)
        # Year 1: 5%, Year 2: 6% → average ≈ 5.5%
        assert result.rate_of_return_per_year == pytest.approx(0.055, abs=0.001)


class TestDividendInsignificant:
    def test_small_payment_excluded_from_count(self):
        """A very small payment (< 30% of expected) is insignificant."""
        items = [
            _div(date(2024, 3, 15), 500),
            _div(date(2024, 4, 1), 10),   # Tiny extra payment
            _div(date(2024, 9, 15), 500),
        ]
        result = calculate_dividends(items)
        # 3 events but only 2 significant → semiannual
        assert result.num_events == 3
        assert result.periodicity == Periodicity.SEMIANNUAL

    def test_gap_year_handled(self):
        """Year with no payment counts as insignificant."""
        items = [
            _div(date(2023, 6, 15), 500),
            # 2024: no payment
            _div(date(2025, 6, 15), 500),
        ]
        result = calculate_dividends(items)
        assert result.num_events == 2
