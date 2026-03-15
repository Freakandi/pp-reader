"""Internal Rate of Return — Newton-Raphson solver.

Ported from PP Java: math/IRR.java, math/NPVFunction.java,
math/NewtonGoalSeek.java, math/PseudoDerivativeFunction.java.

The IRR is the discount rate that makes the NPV of all cash flows equal
to zero.  Cash flows are (date, value) pairs where negative = outflow
(investment) and positive = inflow (sale / valuation at end).
"""

from __future__ import annotations

import math
from datetime import date

__all__ = ["calculate_irr"]


def _days_between(d1: date, d2: date) -> int:
    return (d2 - d1).days


def _npv(days: list[int], values: list[float], rate: float) -> float:
    """Net present value at the given discount rate.

    NPV = Σ values[i] / rate^(days[i]/365)

    where days[i] is the number of days from the first cash flow date.
    """
    total = 0.0
    for i in range(len(days)):
        total += values[i] / math.pow(rate, days[i] / 365.0)
    return total


def _pseudo_derivative(days: list[int], values: list[float], rate: float) -> float:
    """Numerical derivative using central differences."""
    delta = abs(rate) / 1e6
    if delta == 0:
        delta = 1e-12
    left = _npv(days, values, rate - delta)
    right = _npv(days, values, rate + delta)
    return (right - left) / (2.0 * delta)


def _newton_seek(
    days: list[int],
    values: list[float],
    x0: float,
    *,
    tolerance: float = 1e-5,
    max_iterations: int = 500,
) -> float:
    """Newton-Raphson root finder: x_{i+1} = x_i - f(x_i)/f'(x_i)."""
    xi = x0
    for _ in range(max_iterations):
        fxi = _npv(days, values, xi)
        fdxi = _pseudo_derivative(days, values, xi)
        if fdxi == 0:
            break
        xi1 = xi - fxi / fdxi
        if abs(xi1 - xi) <= tolerance:
            return xi1
        xi = xi1
    return xi


def _bisect(
    days: list[int],
    values: list[float],
    left: float,
    right: float,
    f_left: float,
    f_right: float,
) -> float:
    """Bisection method to find a crude initial guess in (left, right)."""
    if math.copysign(1.0, f_left) == math.copysign(1.0, f_right):
        raise ValueError("Endpoints must have different sign in f")

    center = (left + right) / 2.0
    if right - left < 0.001:
        return center

    f_center = _npv(days, values, center)
    if f_center == 0:
        return center
    elif math.copysign(1.0, f_center) == math.copysign(1.0, f_right):
        return _bisect(days, values, left, center, f_left, f_center)
    else:
        return _bisect(days, values, center, right, f_center, f_right)


def calculate_irr(dates: list[date], values: list[float]) -> float:
    """Calculate the Internal Rate of Return for a series of cash flows.

    Args:
        dates:  List of cash flow dates (must be same length as values).
        values: List of cash flow amounts (negative = outflow, positive = inflow).

    Returns:
        Annualized IRR as a decimal (e.g. 0.05 for 5%).
        Returns NaN if calculation is not possible.
    """
    if not dates or not values:
        return float("nan")
    if len(dates) != len(values):
        raise ValueError("dates and values must have equal length")
    if len(dates) < 2:
        return float("nan")

    # Convert dates to day offsets from first date
    day_offsets = [_days_between(dates[0], d) for d in dates]

    # npv(0) limit sign ~ last cash flow; npv(1) = sum of undiscounted flows
    f_left = values[-1]
    f_right = sum(values)

    if math.copysign(1.0, f_left) == math.copysign(1.0, f_right):
        guess = 1.05
    else:
        try:
            guess = _bisect(day_offsets, values, 0.001, 1.0, f_left, f_right)
        except (ValueError, RecursionError):
            guess = 1.05

    result = _newton_seek(day_offsets, values, guess)
    return result - 1.0
