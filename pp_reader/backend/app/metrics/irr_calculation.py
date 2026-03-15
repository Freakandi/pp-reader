"""Security-level IRR calculation from line items.

Ported from PP Java: snapshot/security/IRRCalculation.java.

Collects cash flows from line items and feeds them into the IRR solver.
Convention: outflows (purchases) are negative, inflows (sales, dividends,
end valuation) are positive.

All monetary values are in term-currency cents (10^-2 scaled).
Values are converted to float for the IRR solver.
"""

from __future__ import annotations

from datetime import date

from app.metrics.irr import calculate_irr
from app.metrics.types import LineItem, LineItemType

__all__ = ["calculate_security_irr"]

_AMOUNT_DIVIDER = 100.0  # cents → base currency units


def calculate_security_irr(items: list[LineItem]) -> float:
    """Calculate the IRR for a security from its line items.

    Args:
        items: Sorted line items for the security.

    Returns:
        Annualized IRR as a decimal, or NaN if not computable.
    """
    dates: list[date] = []
    values: list[float] = []

    for item in items:
        t = item.item_type

        if t == LineItemType.VALUATION_START:
            # Start valuation is an outflow (we "invest" this amount)
            dates.append(item.date)
            values.append(-item.amount / _AMOUNT_DIVIDER)

        elif t == LineItemType.VALUATION_END:
            # End valuation is an inflow (we "receive" this amount)
            dates.append(item.date)
            values.append(item.amount / _AMOUNT_DIVIDER)

        elif t == LineItemType.DIVIDEND:
            # Dividend is an inflow; add back taxes for gross
            dates.append(item.date)
            values.append((item.amount + item.tax) / _AMOUNT_DIVIDER)

        elif t in (LineItemType.BUY, LineItemType.INBOUND_DELIVERY, LineItemType.TRANSFER_IN):
            # Purchase is an outflow; add back taxes
            dates.append(item.date)
            values.append((-item.amount + item.tax) / _AMOUNT_DIVIDER)

        elif t in (LineItemType.SELL, LineItemType.OUTBOUND_DELIVERY, LineItemType.TRANSFER_OUT):
            # Sale is an inflow; add back taxes
            dates.append(item.date)
            values.append((item.amount + item.tax) / _AMOUNT_DIVIDER)

        elif t == LineItemType.FEE:
            # Fee is an outflow
            dates.append(item.date)
            values.append(-item.amount / _AMOUNT_DIVIDER)

        elif t == LineItemType.FEE_REFUND:
            # Fee refund is an inflow
            dates.append(item.date)
            values.append(item.amount / _AMOUNT_DIVIDER)

        elif t in (LineItemType.TAX, LineItemType.TAX_REFUND):
            # Tax events are ignored for security-level IRR
            pass

    if not dates:
        return float("nan")

    return calculate_irr(dates, values)
