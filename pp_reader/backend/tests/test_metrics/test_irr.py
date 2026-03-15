"""Tests for the IRR (Internal Rate of Return) solver.

Reference values derived from PP Java implementation and verified
against known financial examples.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from app.metrics.irr import calculate_irr, _npv, _pseudo_derivative, _newton_seek


# ── NPV function tests ───────────────────────────────────────────────

class TestNPV:
    def test_npv_single_cashflow(self):
        # Single cash flow on day 0 → NPV = value (no discounting)
        result = _npv([0], [100.0], 1.05)
        assert result == pytest.approx(100.0)

    def test_npv_two_cashflows(self):
        # -100 today, +110 in 1 year at 10% → NPV ≈ 0
        result = _npv([0, 365], [-100.0, 110.0], 1.10)
        assert result == pytest.approx(0.0, abs=0.01)

    def test_npv_at_rate_1(self):
        # rate=1 → no discounting → NPV = sum of values
        result = _npv([0, 365], [-100.0, 110.0], 1.0)
        assert result == pytest.approx(10.0, abs=0.01)

    def test_npv_multiple_cashflows(self):
        days = [0, 365, 730]
        values = [-1000.0, 500.0, 600.0]
        result = _npv(days, values, 1.05)
        # Should be positive at 5%
        assert result > 0

    def test_npv_zero_when_irr_found(self):
        # Known IRR: invest 1000, receive 1100 in 1 year → IRR = 10%
        days = [0, 365]
        values = [-1000.0, 1100.0]
        # At rate 1.10, NPV should be 0
        result = _npv(days, values, 1.10)
        assert abs(result) < 1.0


# ── Pseudo-derivative tests ──────────────────────────────────────────

class TestPseudoDerivative:
    def test_derivative_is_finite(self):
        days = [0, 365]
        values = [-100.0, 110.0]
        result = _pseudo_derivative(days, values, 1.05)
        assert math.isfinite(result)
        assert result != 0

    def test_derivative_negative_at_solution(self):
        # Near the solution, derivative should be negative
        # (NPV decreases as rate increases)
        days = [0, 365]
        values = [-100.0, 110.0]
        result = _pseudo_derivative(days, values, 1.10)
        assert result < 0


# ── Newton-Raphson seek tests ────────────────────────────────────────

class TestNewtonSeek:
    def test_simple_convergence(self):
        days = [0, 365]
        values = [-100.0, 110.0]
        result = _newton_seek(days, values, 1.05)
        # Should converge to 1.10 (rate), which means IRR = 0.10
        assert result == pytest.approx(1.10, abs=0.001)

    def test_convergence_with_multiple_flows(self):
        days = [0, 182, 365]
        values = [-1000.0, 50.0, 1050.0]
        result = _newton_seek(days, values, 1.05)
        assert math.isfinite(result)


# ── IRR calculation tests ────────────────────────────────────────────

class TestCalculateIRR:
    def test_simple_10_percent_return(self):
        """Invest 1000, receive 1100 after 1 year → IRR = 10%."""
        dates = [date(2024, 1, 1), date(2025, 1, 1)]
        values = [-1000.0, 1100.0]
        irr = calculate_irr(dates, values)
        assert irr == pytest.approx(0.10, abs=0.01)

    def test_zero_return(self):
        """Invest 1000, receive 1000 → IRR ≈ 0%."""
        dates = [date(2024, 1, 1), date(2025, 1, 1)]
        values = [-1000.0, 1000.0]
        irr = calculate_irr(dates, values)
        assert irr == pytest.approx(0.0, abs=0.01)

    def test_negative_return(self):
        """Invest 1000, receive 900 → IRR ≈ -10%."""
        dates = [date(2024, 1, 1), date(2025, 1, 1)]
        values = [-1000.0, 900.0]
        irr = calculate_irr(dates, values)
        assert irr == pytest.approx(-0.10, abs=0.01)

    def test_multiple_cashflows(self):
        """Multiple investments and returns."""
        dates = [
            date(2024, 1, 1),
            date(2024, 7, 1),
            date(2025, 1, 1),
        ]
        values = [-1000.0, -500.0, 1600.0]
        irr = calculate_irr(dates, values)
        assert math.isfinite(irr)
        # Should be slightly positive
        assert irr > -0.1
        assert irr < 0.2

    def test_with_intermediate_dividends(self):
        """Investment with intermediate dividend payments."""
        dates = [
            date(2024, 1, 1),
            date(2024, 4, 1),
            date(2024, 7, 1),
            date(2024, 10, 1),
            date(2025, 1, 1),
        ]
        values = [-10000.0, 100.0, 100.0, 100.0, 10300.0]
        irr = calculate_irr(dates, values)
        assert math.isfinite(irr)
        assert irr > 0

    def test_high_return(self):
        """Invest 1000, receive 2000 → IRR = 100%."""
        dates = [date(2024, 1, 1), date(2025, 1, 1)]
        values = [-1000.0, 2000.0]
        irr = calculate_irr(dates, values)
        assert irr == pytest.approx(1.0, abs=0.05)

    def test_empty_dates_returns_nan(self):
        assert math.isnan(calculate_irr([], []))

    def test_single_date_returns_nan(self):
        assert math.isnan(calculate_irr([date(2024, 1, 1)], [-100.0]))

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError):
            calculate_irr([date(2024, 1, 1)], [-100.0, 110.0])

    def test_same_day_cashflows(self):
        """All cash flows on same day."""
        dates = [date(2024, 1, 1), date(2024, 1, 1)]
        values = [-1000.0, 1100.0]
        irr = calculate_irr(dates, values)
        # When days=0, result may be extreme but should not crash
        assert math.isfinite(irr) or math.isnan(irr)

    def test_short_holding_period(self):
        """30-day holding period."""
        dates = [date(2024, 1, 1), date(2024, 1, 31)]
        values = [-10000.0, 10100.0]
        irr = calculate_irr(dates, values)
        # 1% in 30 days → annualized ~12%
        assert irr > 0.05

    def test_multi_year_investment(self):
        """3-year investment with annual dividends."""
        dates = [
            date(2022, 1, 1),
            date(2022, 12, 31),
            date(2023, 12, 31),
            date(2024, 12, 31),
        ]
        values = [-10000.0, 400.0, 400.0, 10800.0]
        irr = calculate_irr(dates, values)
        assert math.isfinite(irr)
        # ~5.3% annual return (400+400+10800 on 10000 over 3 years)
        assert irr == pytest.approx(0.053, abs=0.02)

    def test_dca_scenario(self):
        """Dollar cost averaging: regular monthly investments."""
        dates = [date(2024, m, 1) for m in range(1, 13)]
        dates.append(date(2025, 1, 1))
        values = [-100.0] * 12 + [1300.0]
        irr = calculate_irr(dates, values)
        assert math.isfinite(irr)
        assert irr > 0
