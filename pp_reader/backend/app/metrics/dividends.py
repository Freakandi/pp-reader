"""Dividend calculation with periodicity detection.

Ported from PP Java: snapshot/security/DividendCalculation.java.

Tracks dividend payments, determines payment frequency, and calculates
rate of return per year based on moving-average cost at time of payment.

All monetary values are in term-currency cents (10^-2 scaled).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from app.metrics.types import (
    DividendResult,
    LineItem,
    LineItemType,
    Periodicity,
)

__all__ = ["calculate_dividends"]


@dataclass(slots=True)
class _Payment:
    """A single dividend payment."""
    amount: int        # term-currency cents
    payment_date: date
    year: int
    rate_of_return: float


def _days_between(d1: date, d2: date) -> int:
    return abs((d2 - d1).days)


def calculate_dividends(
    items: list[LineItem],
    *,
    moving_avg_cost_at_payment: dict[date, int] | None = None,
) -> DividendResult:
    """Calculate dividend metrics from sorted line items.

    Args:
        items: Sorted line items for the security.
        moving_avg_cost_at_payment: Optional mapping of payment date → moving
            average cost (term-currency cents) at the time of each payment.
            Used for rate-of-return calculation.

    Returns:
        DividendResult with totals, periodicity, and rate of return.
    """
    if moving_avg_cost_at_payment is None:
        moving_avg_cost_at_payment = {}

    payments: list[_Payment] = []

    for item in items:
        if item.item_type != LineItemType.DIVIDEND:
            continue

        # Rate of return: dividend amount / moving average cost
        cost = moving_avg_cost_at_payment.get(item.date, 0)
        rr = float("nan")
        if cost and cost > 0:
            rr = item.amount / cost
        elif rr == 0:
            rr = float("nan")

        payments.append(_Payment(
            amount=item.amount,
            payment_date=item.date,
            year=item.date.year,
            rate_of_return=rr,
        ))

    if not payments:
        return DividendResult(periodicity=Periodicity.NONE)

    # Sort by date
    payments.sort(key=lambda p: p.payment_date)

    total_amount = sum(p.amount for p in payments)
    last_payment = payments[-1].payment_date
    first_payment = payments[0].payment_date

    # Walk through years for periodicity detection
    first_year = first_payment.year
    last_year = last_payment.year

    significant_count = 0
    insignificant_years = 0
    sum_rate_of_return = 0.0
    years = 0

    for year in range(first_year, last_year + 1):
        years += 1
        year_payments = [p for p in payments if p.year == year]

        if not year_payments:
            insignificant_years += 1
            continue

        count_per_year = len(year_payments)
        sum_per_year = sum(p.amount for p in year_payments)
        expected_amount = sum_per_year / count_per_year

        last_date: date | None = None
        for p in year_payments:
            significance = p.amount / expected_amount if expected_amount > 0 else 0
            if significance > 0.3:
                if last_date is None or p.payment_date != last_date:
                    significant_count += 1
            last_date = p.payment_date

    # Sum rate of return across all payments
    for p in payments:
        if not (p.rate_of_return != p.rate_of_return):  # not NaN
            sum_rate_of_return += p.rate_of_return

    rate_of_return_per_year = sum_rate_of_return / years if years > 0 else 0.0

    # Determine periodicity
    periodicity = Periodicity.UNKNOWN

    if significant_count > 1:
        total_days = _days_between(first_payment, last_payment)
        adjusted_days = total_days - (insignificant_years * 365)
        days_between = round(adjusted_days / (significant_count - 1))

        if days_between < 430:
            if days_between > 270:
                periodicity = Periodicity.ANNUAL
            elif days_between > 130:
                periodicity = Periodicity.SEMIANNUAL
            elif days_between > 60:
                periodicity = Periodicity.QUARTERLY
            elif days_between > 20:
                periodicity = Periodicity.MONTHLY
    elif significant_count == 1:
        periodicity = Periodicity.UNKNOWN

    return DividendResult(
        total_amount=total_amount,
        last_payment_date=last_payment,
        num_events=len(payments),
        periodicity=periodicity,
        rate_of_return_per_year=rate_of_return_per_year,
    )
