"""Tests for Time-Weighted Return (TWR) performance index."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from app.metrics.performance_index import (
    DailyData,
    PerformanceIndex,
    calculate_performance_index,
)


def _dd(d: date, total: int, inbound: int = 0, outbound: int = 0) -> DailyData:
    return DailyData(date=d, total_valuation=total, inbound=inbound, outbound=outbound)


class TestTWRBasic:
    def test_empty_data(self):
        idx = calculate_performance_index([])
        assert idx.final_accumulated == 0.0
        assert idx.dates == []

    def test_single_day(self):
        idx = calculate_performance_index([_dd(date(2024, 1, 1), 10000)])
        assert len(idx.dates) == 1
        assert idx.accumulated[0] == 0.0
        assert idx.delta[0] == 0.0

    def test_two_days_no_flow(self):
        """10% gain over 1 day."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 11000),
        ]
        idx = calculate_performance_index(data)
        assert idx.delta[1] == pytest.approx(0.10)
        assert idx.accumulated[1] == pytest.approx(0.10)

    def test_flat_performance(self):
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 10000),
            _dd(date(2024, 1, 3), 10000),
        ]
        idx = calculate_performance_index(data)
        assert all(d == pytest.approx(0.0) for d in idx.delta)

    def test_loss(self):
        """20% loss."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 8000),
        ]
        idx = calculate_performance_index(data)
        assert idx.delta[1] == pytest.approx(-0.20)

    def test_complete_loss(self):
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 0),
        ]
        idx = calculate_performance_index(data)
        assert idx.delta[1] == pytest.approx(-1.0)


class TestTWRCashFlows:
    def test_deposit_neutralized(self):
        """Deposit shouldn't count as performance."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 15000, inbound=5000),  # 5000 deposited
        ]
        idx = calculate_performance_index(data)
        # Performance = (15000 + 0) / (10000 + 5000) - 1 = 0
        assert idx.delta[1] == pytest.approx(0.0)

    def test_withdrawal_neutralized(self):
        """Withdrawal shouldn't count as negative performance."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 7000, outbound=3000),  # 3000 withdrawn
        ]
        idx = calculate_performance_index(data)
        # Performance = (7000 + 3000) / (10000 + 0) - 1 = 0
        assert idx.delta[1] == pytest.approx(0.0)

    def test_gain_with_deposit(self):
        """Gain of 10% while also depositing."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 16500, inbound=5000),
        ]
        idx = calculate_performance_index(data)
        # (16500) / (10000 + 5000) - 1 = 0.10
        assert idx.delta[1] == pytest.approx(0.10)

    def test_loss_with_withdrawal(self):
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 4500, outbound=5000),
        ]
        idx = calculate_performance_index(data)
        # (4500 + 5000) / 10000 - 1 = -0.05
        assert idx.delta[1] == pytest.approx(-0.05)


class TestTWRAccumulated:
    def test_two_day_accumulation(self):
        """10% day 1, 10% day 2 → 21% total."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 11000),
            _dd(date(2024, 1, 3), 12100),
        ]
        idx = calculate_performance_index(data)
        assert idx.accumulated[2] == pytest.approx(0.21)

    def test_up_then_down(self):
        """+20% then -16.67% → should be back to 0%."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 12000),   # +20%
            _dd(date(2024, 1, 3), 10000),   # -16.67%
        ]
        idx = calculate_performance_index(data)
        assert idx.accumulated[2] == pytest.approx(0.0, abs=0.001)

    def test_compound_return(self):
        """5 days of 1% → compound ~5.1%."""
        data = [_dd(date(2024, 1, 1), 10000)]
        val = 10000
        for i in range(1, 6):
            val = round(val * 1.01)
            data.append(_dd(date(2024, 1, 1 + i), val))
        idx = calculate_performance_index(data)
        expected = (1.01 ** 5) - 1
        assert idx.accumulated[5] == pytest.approx(expected, abs=0.001)


class TestTWRZeroDenominator:
    def test_zero_valuation_no_crash(self):
        """When previous total is 0 and no inbound, delta should be 0."""
        data = [
            _dd(date(2024, 1, 1), 0),
            _dd(date(2024, 1, 2), 10000),
        ]
        idx = calculate_performance_index(data)
        assert idx.delta[1] == 0.0

    def test_zero_with_deposit(self):
        """Start at 0, deposit 5000, end at 5500 → 10% gain."""
        data = [
            _dd(date(2024, 1, 1), 0),
            _dd(date(2024, 1, 2), 5500, inbound=5000),
        ]
        idx = calculate_performance_index(data)
        # (5500) / (0 + 5000) - 1 = 0.10
        assert idx.delta[1] == pytest.approx(0.10)


class TestPerformanceIndexProperties:
    def test_final_accumulated(self):
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 11000),
        ]
        idx = calculate_performance_index(data)
        assert idx.final_accumulated == pytest.approx(0.10)

    def test_final_annualized(self):
        """10% over 365 days = 10% annualized."""
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2025, 1, 1), 11000),
        ]
        idx = calculate_performance_index(data)
        # 366 days in 2024 (leap year), but approximately 10%
        assert idx.final_annualized == pytest.approx(0.10, abs=0.01)

    def test_invested_capital(self):
        data = [
            _dd(date(2024, 1, 1), 10000),
            _dd(date(2024, 1, 2), 16000, inbound=5000),
            _dd(date(2024, 1, 3), 13000, outbound=3000),
        ]
        idx = calculate_performance_index(data)
        cap = idx.get_invested_capital()
        assert cap[0] == 10000
        assert cap[1] == 15000   # 10000 + 5000
        assert cap[2] == 12000   # 15000 - 3000

    def test_get_performance_subinterval(self):
        data = [
            _dd(date(2024, 1, d), 10000 + d * 100) for d in range(1, 11)
        ]
        idx = calculate_performance_index(data)
        perf = idx.get_performance(date(2024, 1, 3), date(2024, 1, 8))
        assert math.isfinite(perf)


class TestTWRMultiDay:
    def test_week_of_trading(self):
        data = [
            _dd(date(2024, 1, 1), 100000),
            _dd(date(2024, 1, 2), 101000),   # +1%
            _dd(date(2024, 1, 3), 99990),    # -1%
            _dd(date(2024, 1, 4), 102990),   # +3%
            _dd(date(2024, 1, 5), 101930),   # -1%
        ]
        idx = calculate_performance_index(data)
        assert len(idx.dates) == 5
        assert all(math.isfinite(d) for d in idx.delta)
        assert all(math.isfinite(a) for a in idx.accumulated)
        # Net slightly positive
        assert idx.final_accumulated > 0

    def test_large_dataset(self):
        """Simulate 252 trading days (1 year)."""
        data = [_dd(date(2024, 1, 1), 100000)]
        val = 100000
        for i in range(1, 253):
            # Random-ish daily return
            daily_return = 0.001 if i % 3 != 0 else -0.002
            val = round(val * (1 + daily_return))
            data.append(_dd(date(2024, 1, 1) + timedelta(days=i), val))

        idx = calculate_performance_index(data)
        assert len(idx.dates) == 253
        assert math.isfinite(idx.final_accumulated)
        assert math.isfinite(idx.final_annualized)


class TestTWRWithTradeFlows:
    def test_buys_sells_tracked(self):
        data = [
            DailyData(date=date(2024, 1, 1), total_valuation=10000),
            DailyData(date=date(2024, 1, 2), total_valuation=15000, inbound=5000, buys=5000),
            DailyData(date=date(2024, 1, 3), total_valuation=12000, outbound=3000, sells=3000),
        ]
        idx = calculate_performance_index(data)
        assert idx.buys == [0, 5000, 0]
        assert idx.sells == [0, 0, 3000]
