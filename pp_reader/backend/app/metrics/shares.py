"""Shares held calculation.

Ported from PP Java: snapshot/security/SharesHeldCalculation.java.

Tracks the current number of shares held across all transactions.
Shares are 10^-8 scaled BIGINT.
"""

from __future__ import annotations

from app.metrics.types import LineItem, LineItemType

__all__ = ["calculate_shares_held"]


def calculate_shares_held(items: list[LineItem]) -> int:
    """Calculate the number of shares held after processing all line items.

    Args:
        items: Sorted line items for the security.

    Returns:
        Shares held (10^-8 scaled).
    """
    held: int = 0

    for item in items:
        t = item.item_type

        if t == LineItemType.VALUATION_START:
            held += item.shares

        elif t in (LineItemType.BUY, LineItemType.INBOUND_DELIVERY):
            held += item.shares

        elif t in (LineItemType.SELL, LineItemType.OUTBOUND_DELIVERY):
            held -= item.shares

        elif t in (LineItemType.TRANSFER_IN, LineItemType.TRANSFER_OUT):
            pass  # transfers don't change net shares

    return held
