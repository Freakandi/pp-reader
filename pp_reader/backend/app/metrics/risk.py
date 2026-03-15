"""Risk metrics — Drawdown and Volatility.

Ported from PP Java: math/Risk.java.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date

__all__ = ["Drawdown", "Volatility", "calculate_drawdown", "calculate_volatility"]


@dataclass(slots=True)
class Drawdown:
    """Maximum drawdown metrics for a performance series."""

    max_drawdown: float = 0.0
    max_drawdown_start: date | None = None
    max_drawdown_end: date | None = None
    max_duration_start: date | None = None
    max_duration_end: date | None = None
    recovery_start: date | None = None
    recovery_end: date | None = None
    drawdown_series: list[float] | None = None


@dataclass(slots=True)
class Volatility:
    """Volatility metrics for a return series."""

    std_deviation: float = 0.0
    semi_deviation: float = 0.0

    @property
    def expected_semi_deviation(self) -> float:
        return self.std_deviation / math.sqrt(2)


def calculate_drawdown(
    accumulated: list[float],
    dates: list[date],
    start_at: int = 0,
) -> Drawdown:
    """Calculate drawdown metrics from an accumulated performance series.

    Args:
        accumulated: Array of accumulated percentage returns (e.g. [0.0, 0.02, -0.01, ...]).
        dates: Corresponding dates.
        start_at: Index to start calculation (skip leading zeros).

    Returns:
        Drawdown result with max drawdown, duration, and recovery time.
    """
    n = len(accumulated)
    if n == 0 or start_at >= n:
        return Drawdown()
    if len(dates) != n:
        raise ValueError("accumulated and dates must have same length")

    peak = accumulated[start_at] + 1.0
    bottom = peak
    last_peak_date = dates[start_at]
    last_bottom_date = dates[start_at]

    result = Drawdown(
        max_drawdown=0.0,
        max_drawdown_start=last_peak_date,
        max_drawdown_end=last_peak_date,
        max_duration_start=last_peak_date,
        max_duration_end=last_peak_date,
        recovery_start=last_bottom_date,
        recovery_end=last_peak_date,
        drawdown_series=[0.0] * n,
    )

    current_duration_start = last_peak_date
    current_recovery_start = last_bottom_date
    max_duration_days = 0
    max_recovery_days = 0

    for i in range(start_at, n):
        value = accumulated[i] + 1.0

        current_duration_days = (dates[i] - last_peak_date).days
        current_recovery_days = (dates[i] - last_bottom_date).days

        if value > peak:
            peak = value
            last_peak_date = dates[i]
            result.drawdown_series[i] = 0.0  # type: ignore[index]

            if current_recovery_days > max_recovery_days:
                max_recovery_days = current_recovery_days
                result.recovery_start = current_recovery_start
                result.recovery_end = dates[i]

            last_bottom_date = dates[i]
            bottom = value
            current_recovery_start = dates[i]
        else:
            dd = (peak - value) / peak
            result.drawdown_series[i] = -dd  # type: ignore[index]

            if dd > result.max_drawdown:
                result.max_drawdown = dd
                result.max_drawdown_start = last_peak_date
                result.max_drawdown_end = dates[i]

            if value < bottom:
                bottom = value
                last_bottom_date = dates[i]
                current_recovery_start = dates[i]

        if current_duration_days > max_duration_days:
            max_duration_days = current_duration_days
            result.max_duration_start = last_peak_date
            result.max_duration_end = dates[i]

    # Check final recovery period
    final_recovery = (dates[-1] - last_bottom_date).days if last_bottom_date else 0
    if final_recovery > max_recovery_days:
        result.recovery_start = current_recovery_start
        result.recovery_end = dates[-1]

    return result


def calculate_volatility(
    delta_returns: list[float],
    *,
    filter_fn: list[bool] | None = None,
) -> Volatility:
    """Calculate standard and semi-deviation of log returns.

    Args:
        delta_returns: Array of period-over-period returns.
        filter_fn: Boolean mask; True = include in calculation.
            If None, all returns after index 0 are included.

    Returns:
        Volatility with std_deviation and semi_deviation.
    """
    n = len(delta_returns)
    if n == 0:
        return Volatility()

    if filter_fn is None:
        filter_fn = [i > 0 for i in range(n)]

    # Calculate average log return
    log_sum = 0.0
    count = 0
    for i in range(n):
        if not filter_fn[i]:
            continue
        log_sum += math.log(1 + delta_returns[i]) if (1 + delta_returns[i]) > 0 else 0.0
        count += 1

    if count <= 1:
        return Volatility()

    avg_log_return = log_sum / count

    # Calculate deviations
    temp_std = 0.0
    temp_semi = 0.0

    for i in range(n):
        if not filter_fn[i]:
            continue
        log_return = math.log(1 + delta_returns[i]) if (1 + delta_returns[i]) > 0 else 0.0
        deviation_sq = (log_return - avg_log_return) ** 2
        temp_std += deviation_sq
        if log_return < avg_log_return:
            temp_semi += deviation_sq

    std_dev = math.sqrt(temp_std / (count - 1) * count)
    semi_dev = math.sqrt(temp_semi / (count - 1) * count)

    return Volatility(std_deviation=std_dev, semi_deviation=semi_dev)
