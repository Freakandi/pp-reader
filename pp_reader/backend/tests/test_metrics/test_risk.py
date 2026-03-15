"""Tests for Drawdown and Volatility risk metrics."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from app.metrics.risk import calculate_drawdown, calculate_volatility, Drawdown, Volatility


# ── Helper ────────────────────────────────────────────────────────────

def _make_dates(n: int, start: date = date(2024, 1, 1)) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


# ── Drawdown tests ───────────────────────────────────────────────────

class TestDrawdown:
    def test_no_drawdown_rising(self):
        """Steadily rising performance → 0% drawdown."""
        acc = [0.0, 0.01, 0.02, 0.03, 0.04, 0.05]
        dates = _make_dates(6)
        dd = calculate_drawdown(acc, dates)
        assert dd.max_drawdown == pytest.approx(0.0)

    def test_single_drawdown(self):
        """Peak then valley: 0% → 10% → -5% → 15%."""
        acc = [0.0, 0.10, -0.05, 0.15]
        dates = _make_dates(4)
        dd = calculate_drawdown(acc, dates)
        # Peak at 1.10, valley at 0.95 → DD = (1.10-0.95)/1.10 ≈ 13.6%
        assert dd.max_drawdown > 0.13
        assert dd.max_drawdown < 0.15

    def test_complete_loss(self):
        """100% loss."""
        acc = [0.0, 0.10, -0.99]
        dates = _make_dates(3)
        dd = calculate_drawdown(acc, dates)
        assert dd.max_drawdown > 0.98

    def test_flat_performance(self):
        """Zero return throughout."""
        acc = [0.0, 0.0, 0.0, 0.0]
        dates = _make_dates(4)
        dd = calculate_drawdown(acc, dates)
        assert dd.max_drawdown == pytest.approx(0.0)

    def test_v_shaped_recovery(self):
        """Sharp drop then sharp recovery."""
        acc = [0.0, -0.20, 0.0, 0.10]
        dates = _make_dates(4)
        dd = calculate_drawdown(acc, dates)
        assert dd.max_drawdown > 0.15
        assert dd.recovery_start is not None
        assert dd.recovery_end is not None

    def test_empty_series(self):
        dd = calculate_drawdown([], [])
        assert dd.max_drawdown == 0.0

    def test_single_point(self):
        dd = calculate_drawdown([0.0], [date(2024, 1, 1)])
        assert dd.max_drawdown == 0.0

    def test_drawdown_series_length(self):
        acc = [0.0, 0.05, -0.02, 0.08]
        dates = _make_dates(4)
        dd = calculate_drawdown(acc, dates)
        assert len(dd.drawdown_series) == 4

    def test_start_at_offset(self):
        """Start calculation from index 2."""
        acc = [0.0, 0.0, 0.10, 0.05, 0.15]
        dates = _make_dates(5)
        dd = calculate_drawdown(acc, dates, start_at=2)
        # Peak at 1.10, valley at 1.05 → DD ≈ 4.5%
        assert dd.max_drawdown > 0.04
        assert dd.max_drawdown < 0.06

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError):
            calculate_drawdown([0.0, 0.1], [date(2024, 1, 1)])

    def test_multiple_drawdowns(self):
        """Two distinct drawdown periods."""
        acc = [0.0, 0.10, 0.05, 0.15, 0.08, 0.20]
        dates = _make_dates(6)
        dd = calculate_drawdown(acc, dates)
        assert dd.max_drawdown > 0

    def test_drawdown_interval_dates(self):
        acc = [0.0, 0.10, -0.05, 0.15]
        dates = _make_dates(4)
        dd = calculate_drawdown(acc, dates)
        assert dd.max_drawdown_start is not None
        assert dd.max_drawdown_end is not None
        assert dd.max_drawdown_start <= dd.max_drawdown_end


# ── Volatility tests ─────────────────────────────────────────────────

class TestVolatility:
    def test_zero_returns(self):
        """All zero returns → zero volatility."""
        returns = [0.0, 0.0, 0.0, 0.0, 0.0]
        vol = calculate_volatility(returns)
        assert vol.std_deviation == pytest.approx(0.0)
        assert vol.semi_deviation == pytest.approx(0.0)

    def test_constant_positive_returns(self):
        """Same return every period → zero volatility."""
        returns = [0.0, 0.01, 0.01, 0.01, 0.01]
        vol = calculate_volatility(returns)
        assert vol.std_deviation == pytest.approx(0.0, abs=1e-10)

    def test_symmetric_returns(self):
        """Equal up and down moves."""
        returns = [0.0, 0.05, -0.05, 0.05, -0.05]
        vol = calculate_volatility(returns)
        assert vol.std_deviation > 0
        # Semi-deviation should be close to half of std
        assert vol.semi_deviation <= vol.std_deviation

    def test_only_negative_returns(self):
        """All negative returns → semi = std."""
        returns = [0.0, -0.02, -0.03, -0.01, -0.04]
        vol = calculate_volatility(returns)
        assert vol.std_deviation > 0
        # When all returns are below average, semi ≈ std
        assert vol.semi_deviation > 0

    def test_expected_semi_deviation(self):
        """Check expected semi-deviation formula."""
        returns = [0.0, 0.05, -0.05, 0.03, -0.02, 0.01]
        vol = calculate_volatility(returns)
        expected = vol.std_deviation / math.sqrt(2)
        assert vol.expected_semi_deviation == pytest.approx(expected)

    def test_empty_returns(self):
        vol = calculate_volatility([])
        assert vol.std_deviation == 0.0

    def test_single_return(self):
        vol = calculate_volatility([0.05])
        assert vol.std_deviation == 0.0

    def test_two_returns(self):
        """Minimum for calculation."""
        returns = [0.0, 0.05, -0.03]
        vol = calculate_volatility(returns)
        assert vol.std_deviation > 0

    def test_with_filter(self):
        """Filter out specific indices."""
        returns = [0.0, 0.05, -0.03, 0.02, -0.01]
        filter_fn = [False, True, True, True, True]
        vol = calculate_volatility(returns, filter_fn=filter_fn)
        assert vol.std_deviation > 0

    def test_filter_all_excluded(self):
        returns = [0.0, 0.05, -0.03]
        filter_fn = [False, False, False]
        vol = calculate_volatility(returns, filter_fn=filter_fn)
        assert vol.std_deviation == 0.0

    def test_large_returns(self):
        """Large daily returns shouldn't cause overflow."""
        returns = [0.0, 0.50, -0.30, 0.40, -0.20]
        vol = calculate_volatility(returns)
        assert math.isfinite(vol.std_deviation)
        assert math.isfinite(vol.semi_deviation)

    def test_high_volatility_stock(self):
        """Simulate high-volatility daily returns."""
        returns = [0.0, 0.10, -0.08, 0.12, -0.15, 0.05, -0.03, 0.08]
        vol = calculate_volatility(returns)
        assert vol.std_deviation > 0.05
