"""Capital gains calculation — FIFO and Moving Average methods.

Ported from PP Java:
  snapshot/security/CapitalGainsCalculation.java (FIFO)
  snapshot/security/CapitalGainsCalculationMovingAverage.java

Computes realized gains (from sales) and unrealized gains (from
end-of-period valuation) separately.  Forex gains are tracked when
the security trades in a different currency than the term currency.

All monetary values are in term-currency cents (10^-2 scaled).
"""

from __future__ import annotations

from dataclasses import dataclass

from app.metrics.types import (
    CapitalGainsResult,
    LineItem,
    LineItemType,
)

__all__ = ["calculate_capital_gains_fifo", "calculate_capital_gains_moving_avg"]


# ── FIFO Capital Gains ────────────────────────────────────────────────

@dataclass
class _FifoLot:
    """Lot for FIFO capital gains tracking."""

    shares: int
    value: int           # net value (gross value of position, term-currency)
    original_shares: int
    owner: str


def calculate_capital_gains_fifo(
    items: list[LineItem],
    *,
    is_forex: bool = False,
) -> CapitalGainsResult:
    """Calculate capital gains using FIFO lot matching.

    Args:
        items: Sorted line items for the security.
        is_forex: True if security currency differs from term currency.

    Returns:
        CapitalGainsResult with realized and unrealized gains.
    """
    fifo: list[_FifoLot] = []
    result = CapitalGainsResult()

    for item in items:
        t = item.item_type

        if t == LineItemType.VALUATION_START:
            fifo.append(_FifoLot(
                shares=item.shares,
                value=item.net_amount,
                original_shares=item.shares,
                owner=item.owner,
            ))

        elif t in (LineItemType.BUY, LineItemType.INBOUND_DELIVERY):
            fifo.append(_FifoLot(
                shares=item.shares,
                value=item.net_amount,
                original_shares=item.shares,
                owner=item.owner,
            ))

        elif t in (LineItemType.SELL, LineItemType.OUTBOUND_DELIVERY):
            sale_value = item.net_amount
            sold = item.shares

            for lot in fifo:
                if lot.shares == 0:
                    continue
                if lot.owner != item.owner:
                    continue
                if sold <= 0:
                    break

                sold_shares = min(sold, lot.shares)
                start = round(sold_shares / lot.shares * lot.value)
                end = round(sold_shares / item.shares * sale_value)

                # Forex gains calculation
                forex_gain = 0
                if is_forex and item.security_currency_amount != 0:
                    # Simplified forex gain: difference between cost converted
                    # at current rate vs original rate
                    if item.fx_rate != 0 and lot.shares > 0:
                        # The forex gain is the change in value due solely to
                        # exchange rate movement
                        pass  # forex gains require full FX rate history; tracked at engine level

                result.realized_gains += end - start

                lot.shares -= sold_shares
                lot.value -= start
                sold -= sold_shares

        elif t == LineItemType.TRANSFER_IN:
            moved = item.shares
            source = item.source_owner

            for lot in list(fifo):
                if moved <= 0:
                    break
                if lot.owner != source:
                    continue
                if lot.shares == 0:
                    continue

                n = min(moved, lot.shares)
                transferred_value = round(n / lot.shares * lot.value)

                new_lot = _FifoLot(
                    shares=n,
                    value=transferred_value,
                    original_shares=n,
                    owner=item.owner,
                )

                if n == lot.shares:
                    idx = fifo.index(lot)
                    fifo.insert(idx + 1, new_lot)
                    fifo.remove(lot)
                else:
                    lot.value -= transferred_value
                    lot.shares -= n
                    idx = fifo.index(lot)
                    fifo.insert(idx + 1, new_lot)

                moved -= n

        elif t == LineItemType.TRANSFER_OUT:
            pass  # handled via TRANSFER_IN

        elif t == LineItemType.VALUATION_END:
            # Unrealized gains = end valuation - remaining FIFO cost
            start_value = sum(lot.value for lot in fifo)
            end_value = item.net_amount
            result.unrealized_gains += end_value - start_value
            fifo.clear()

    return result


# ── Moving Average Capital Gains ──────────────────────────────────────

def calculate_capital_gains_moving_avg(
    items: list[LineItem],
    *,
    is_forex: bool = False,
) -> CapitalGainsResult:
    """Calculate capital gains using moving average cost basis.

    Args:
        items: Sorted line items for the security.
        is_forex: True if security currency differs from term currency.

    Returns:
        CapitalGainsResult with realized and unrealized gains.
    """
    held_shares: int = 0
    moving_avg_net_cost: int = 0
    moving_avg_net_cost_forex: int = 0
    result = CapitalGainsResult()

    for item in items:
        t = item.item_type

        if t == LineItemType.VALUATION_START:
            moving_avg_net_cost += item.net_amount
            moving_avg_net_cost_forex += item.security_currency_amount
            held_shares += item.shares

        elif t in (LineItemType.BUY, LineItemType.INBOUND_DELIVERY):
            moving_avg_net_cost += item.net_amount
            moving_avg_net_cost_forex += item.security_currency_amount
            held_shares += item.shares

        elif t in (LineItemType.SELL, LineItemType.OUTBOUND_DELIVERY):
            sold = item.shares
            remaining = held_shares - sold

            if remaining < 0:
                # More sold than held — reset
                moving_avg_net_cost = 0
                moving_avg_net_cost_forex = 0
                held_shares = 0
            else:
                avg_costs = round(moving_avg_net_cost / held_shares * sold)
                avg_costs_forex = round(moving_avg_net_cost_forex / held_shares * sold) if held_shares else 0

                gain = item.net_amount - avg_costs
                result.realized_gains += gain

                # Forex gains for moving average
                if is_forex and item.security_currency_amount != 0 and avg_costs_forex != 0:
                    # Exchange rate from the sale transaction
                    if item.security_currency_amount != 0:
                        exchange_rate = item.net_amount / item.security_currency_amount
                        forex_gain = round(avg_costs_forex * exchange_rate) - avg_costs
                        result.realized_forex_gains += forex_gain

                moving_avg_net_cost -= avg_costs
                moving_avg_net_cost_forex -= avg_costs_forex
                held_shares = remaining

        elif t in (LineItemType.TRANSFER_IN, LineItemType.TRANSFER_OUT):
            pass  # Not relevant for moving average

        elif t == LineItemType.VALUATION_END:
            net_amount = item.net_amount
            gain = net_amount - moving_avg_net_cost
            result.unrealized_gains += gain

            if is_forex and item.security_currency_amount != 0:
                if item.security_currency_amount != 0:
                    exchange_rate = net_amount / item.security_currency_amount
                    forex_gain = round(moving_avg_net_cost_forex * exchange_rate) - moving_avg_net_cost
                    result.unrealized_forex_gains += forex_gain

    return result
