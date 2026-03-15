"""Time-Weighted Return (TWR) performance index.

Ported from PP Java: snapshot/PerformanceIndex.java (ClientIndex).

The TWR formula:
    delta[i] = (valuation[i] + outbound[i]) / (valuation[i-1] + inbound[i]) - 1
    accumulated[i] = (accumulated[i-1] + 1) * (delta[i] + 1) - 1

This avoids bias from cash flow timing (unlike money-weighted returns).

All monetary values are in term-currency cents (10^-2 scaled).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date

__all__ = ["PerformanceIndex", "calculate_performance_index"]


@dataclass(slots=True)
class PerformanceIndex:
    """Daily performance index arrays."""

    dates: list[date] = field(default_factory=list)
    totals: list[int] = field(default_factory=list)              # daily valuations, cents
    inbound_transferals: list[int] = field(default_factory=list)  # inflows per day, cents
    outbound_transferals: list[int] = field(default_factory=list) # outflows per day, cents
    taxes: list[int] = field(default_factory=list)
    dividends: list[int] = field(default_factory=list)
    fees: list[int] = field(default_factory=list)
    interest: list[int] = field(default_factory=list)
    interest_charge: list[int] = field(default_factory=list)
    buys: list[int] = field(default_factory=list)
    sells: list[int] = field(default_factory=list)
    delta: list[float] = field(default_factory=list)              # daily returns
    accumulated: list[float] = field(default_factory=list)        # cumulative returns

    @property
    def final_accumulated(self) -> float:
        return self.accumulated[-1] if self.accumulated else 0.0

    @property
    def final_annualized(self) -> float:
        if not self.dates or len(self.dates) < 2:
            return 0.0
        days = (self.dates[-1] - self.dates[0]).days
        if days <= 0:
            return 0.0
        acc = self.final_accumulated
        return math.pow(1 + acc, 365.0 / days) - 1

    def get_invested_capital(self, start_value: int | None = None) -> list[int]:
        """Calculate invested capital series.

        Args:
            start_value: Starting capital; defaults to totals[0].
        """
        if not self.totals:
            return []
        if start_value is None:
            start_value = self.totals[0]

        result = [0] * len(self.inbound_transferals)
        result[0] = start_value
        current = start_value
        for i in range(1, len(result)):
            current = current + self.inbound_transferals[i] - self.outbound_transferals[i]
            result[i] = current
        return result

    def get_performance(self, start_date: date, end_date: date) -> float:
        """Return performance for a sub-interval."""
        if not self.dates:
            return 0.0

        start_idx = _find_index(self.dates, start_date)
        end_idx = _find_index(self.dates, end_date)

        if start_idx is None or end_idx is None:
            return 0.0
        if start_idx >= end_idx:
            return 0.0

        start_val = self.accumulated[start_idx]
        end_val = self.accumulated[end_idx]
        return ((end_val + 1) / (start_val + 1)) - 1


def _find_index(dates: list[date], target: date) -> int | None:
    """Find the index of target in sorted dates, or nearest preceding."""
    if not dates:
        return None
    if target <= dates[0]:
        return 0
    if target >= dates[-1]:
        return len(dates) - 1

    # Binary search
    lo, hi = 0, len(dates) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if dates[mid] == target:
            return mid
        elif dates[mid] < target:
            lo = mid + 1
        else:
            hi = mid - 1
    return hi  # nearest preceding


@dataclass(slots=True)
class DailyData:
    """Aggregated daily data for TWR calculation."""

    date: date
    total_valuation: int = 0      # end-of-day valuation, cents
    inbound: int = 0              # cash inflows, cents
    outbound: int = 0             # cash outflows, cents
    taxes: int = 0
    dividends: int = 0
    fees: int = 0
    interest: int = 0
    interest_charge: int = 0
    buys: int = 0
    sells: int = 0


def calculate_performance_index(daily_data: list[DailyData]) -> PerformanceIndex:
    """Build a TWR performance index from daily aggregated data.

    Args:
        daily_data: List of DailyData, one per day, sorted by date ascending.
            Each entry contains the end-of-day valuation and the day's cash flows.

    Returns:
        PerformanceIndex with daily delta and accumulated return arrays.
    """
    if not daily_data:
        return PerformanceIndex()

    n = len(daily_data)
    index = PerformanceIndex(
        dates=[d.date for d in daily_data],
        totals=[d.total_valuation for d in daily_data],
        inbound_transferals=[d.inbound for d in daily_data],
        outbound_transferals=[d.outbound for d in daily_data],
        taxes=[d.taxes for d in daily_data],
        dividends=[d.dividends for d in daily_data],
        fees=[d.fees for d in daily_data],
        interest=[d.interest for d in daily_data],
        interest_charge=[d.interest_charge for d in daily_data],
        buys=[d.buys for d in daily_data],
        sells=[d.sells for d in daily_data],
        delta=[0.0] * n,
        accumulated=[0.0] * n,
    )

    # TWR calculation
    for i in range(1, n):
        # denominator = previous valuation + today's inflows
        denominator = daily_data[i - 1].total_valuation + daily_data[i].inbound
        # numerator = today's valuation + today's outflows
        numerator = daily_data[i].total_valuation + daily_data[i].outbound

        if denominator == 0:
            # Division by zero: no assets → no return (PP Java sets delta to 0)
            index.delta[i] = 0.0
        else:
            index.delta[i] = (numerator / denominator) - 1.0

        index.accumulated[i] = (index.accumulated[i - 1] + 1.0) * (index.delta[i] + 1.0) - 1.0

    return index
