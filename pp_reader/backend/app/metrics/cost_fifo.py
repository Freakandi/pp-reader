"""FIFO cost basis calculation.

Ported from PP Java: snapshot/security/CostCalculation.java.

Tracks purchase lots in FIFO order.  When shares are sold, the oldest
lots are consumed first.  Also maintains a parallel moving-average cost
for comparison.

All monetary values are in term-currency cents (10^-2 scaled).
Shares are 10^-8 scaled.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.metrics.types import (
    CostResult,
    LineItem,
    LineItemType,
)

__all__ = ["calculate_cost"]


@dataclass
class _Lot:
    """A single FIFO purchase lot."""

    owner: str
    shares: int           # remaining shares (10^8 scaled)
    gross_amount: int     # remaining gross cost, term-currency cents
    net_amount: int       # remaining net cost (no fees/taxes), term-currency cents
    original_shares: int  # shares at creation time (for fractional calc)


def calculate_cost(items: list[LineItem]) -> CostResult:
    """Run FIFO + moving-average cost calculation over sorted line items.

    Args:
        items: Line items sorted by (date, ordering_hint).

    Returns:
        CostResult with FIFO cost, moving-average cost, fees, taxes, shares held.
    """
    fifo: list[_Lot] = []
    moving_cost: int = 0          # gross moving average cost
    moving_net_cost: int = 0      # net moving average cost
    held_shares: int = 0
    total_fees: int = 0
    total_taxes: int = 0

    for item in items:
        t = item.item_type

        if t == LineItemType.VALUATION_START:
            fifo.append(_Lot(
                owner=item.owner,
                shares=item.shares,
                gross_amount=item.amount,
                net_amount=item.amount,
                original_shares=item.shares,
            ))
            moving_cost += item.amount
            moving_net_cost += item.amount
            held_shares += item.shares

        elif t in (LineItemType.BUY, LineItemType.INBOUND_DELIVERY):
            total_fees += item.fee
            total_taxes += item.tax

            fifo.append(_Lot(
                owner=item.owner,
                shares=item.shares,
                gross_amount=item.amount,
                net_amount=item.net_amount,
                original_shares=item.shares,
            ))
            moving_cost += item.amount
            moving_net_cost += item.net_amount
            held_shares += item.shares

        elif t in (LineItemType.SELL, LineItemType.OUTBOUND_DELIVERY):
            total_fees += item.fee
            total_taxes += item.tax

            sold = item.shares
            remaining = held_shares - sold

            # Update moving average
            if remaining <= 0:
                moving_cost = 0
                moving_net_cost = 0
                held_shares = 0
            else:
                moving_cost = round(moving_cost / held_shares * remaining)
                moving_net_cost = round(moving_net_cost / held_shares * remaining)
                held_shares = remaining

            # Consume FIFO lots
            for lot in fifo:
                if sold <= 0:
                    break
                if lot.owner != item.owner:
                    continue
                if lot.shares == 0:
                    continue

                n = min(sold, lot.shares)
                lot.gross_amount -= round(n / lot.shares * lot.gross_amount)
                lot.net_amount -= round(n / lot.shares * lot.net_amount)
                lot.shares -= n
                sold -= n

        elif t == LineItemType.TRANSFER_IN:
            moved = item.shares
            source = item.source_owner

            # Iterate on a snapshot to allow mutation
            for lot in list(fifo):
                if moved <= 0:
                    break
                if lot.owner != source:
                    continue
                if lot.shares == 0:
                    continue

                n = min(moved, lot.shares)

                if n == lot.shares:
                    lot.owner = item.owner
                else:
                    transferred_gross = round(n / lot.shares * lot.gross_amount)
                    transferred_net = round(n / lot.shares * lot.net_amount)

                    new_lot = _Lot(
                        owner=item.owner,
                        shares=n,
                        gross_amount=transferred_gross,
                        net_amount=transferred_net,
                        original_shares=n,
                    )

                    lot.gross_amount -= transferred_gross
                    lot.net_amount -= transferred_net
                    lot.shares -= n

                    idx = fifo.index(lot)
                    fifo.insert(idx + 1, new_lot)

                moved -= n

        elif t == LineItemType.TRANSFER_OUT:
            # Handled via TRANSFER_IN
            pass

        elif t == LineItemType.DIVIDEND:
            total_taxes += item.tax

        elif t == LineItemType.TAX:
            total_taxes += item.amount

        elif t == LineItemType.TAX_REFUND:
            total_taxes -= item.amount

        elif t == LineItemType.FEE:
            total_fees += item.amount

        elif t == LineItemType.FEE_REFUND:
            total_fees -= item.amount

    # Sum FIFO lots
    fifo_cost = sum(lot.gross_amount for lot in fifo)
    net_fifo_cost = sum(lot.net_amount for lot in fifo)
    shares_held = sum(lot.shares for lot in fifo)

    return CostResult(
        shares_held=shares_held,
        fifo_cost=fifo_cost,
        net_fifo_cost=net_fifo_cost,
        moving_avg_cost=moving_cost,
        net_moving_avg_cost=moving_net_cost,
        total_fees=total_fees,
        total_taxes=total_taxes,
    )
