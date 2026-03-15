"""Simple profit/loss delta calculation.

Ported from PP Java: snapshot/security/DeltaCalculation.java.

Computes the difference between all inflows (purchases, start valuation)
and all outflows (sales, dividends, end valuation, fees, taxes).

All monetary values are in term-currency cents (10^-2 scaled).
"""

from __future__ import annotations

from app.metrics.types import (
    DeltaResult,
    LineItem,
    LineItemType,
)

__all__ = ["calculate_delta"]


def calculate_delta(items: list[LineItem]) -> DeltaResult:
    """Calculate the simple profit/loss delta for sorted line items.

    Args:
        items: Line items sorted by (date, ordering_hint).

    Returns:
        DeltaResult with delta (P&L) and total cost.
    """
    delta: int = 0
    cost: int = 0

    for item in items:
        t = item.item_type

        if t == LineItemType.VALUATION_START:
            delta -= item.amount
            cost += item.amount

        elif t == LineItemType.VALUATION_END:
            delta += item.amount

        elif t == LineItemType.DIVIDEND:
            delta += item.amount

        elif t in (LineItemType.BUY, LineItemType.INBOUND_DELIVERY):
            delta -= item.amount
            cost += item.amount

        elif t in (LineItemType.SELL, LineItemType.OUTBOUND_DELIVERY):
            delta += item.amount

        elif t in (LineItemType.TRANSFER_IN, LineItemType.TRANSFER_OUT):
            pass  # transfers don't affect delta

        elif t in (LineItemType.TAX, LineItemType.FEE):
            delta -= item.amount

        elif t in (LineItemType.TAX_REFUND, LineItemType.FEE_REFUND):
            delta += item.amount

    return DeltaResult(delta=delta, cost=cost)
