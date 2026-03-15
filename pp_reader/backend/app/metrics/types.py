"""Domain types for the Metrics Engine.

These dataclasses model the calculation pipeline's input events and output
results.  They are the Python equivalent of Java's CalculationLineItem
hierarchy and various *Result records.

All monetary values are in term-currency **cents** (10^-2 scaled BIGINT).
Shares are 10^-8 scaled BIGINT.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum, auto


__all__ = [
    "LineItemType",
    "LineItem",
    "Periodicity",
    "CostResult",
    "CapitalGainsResult",
    "DividendResult",
    "DeltaResult",
    "SecurityMetricsResult",
]


# ── Line-item classification ──────────────────────────────────────────

class LineItemType(Enum):
    """Mirrors Java's CalculationLineItem subclass hierarchy."""
    VALUATION_START = auto()
    VALUATION_END = auto()
    BUY = auto()
    SELL = auto()
    INBOUND_DELIVERY = auto()
    OUTBOUND_DELIVERY = auto()
    TRANSFER_IN = auto()
    TRANSFER_OUT = auto()
    DIVIDEND = auto()
    TAX = auto()
    TAX_REFUND = auto()
    FEE = auto()
    FEE_REFUND = auto()


# Transaction types that add shares
INBOUND_TYPES = frozenset({
    LineItemType.BUY,
    LineItemType.INBOUND_DELIVERY,
    LineItemType.TRANSFER_IN,
})

# Transaction types that remove shares
OUTBOUND_TYPES = frozenset({
    LineItemType.SELL,
    LineItemType.OUTBOUND_DELIVERY,
    LineItemType.TRANSFER_OUT,
})


@dataclass(slots=True)
class LineItem:
    """A single event in a security's calculation timeline.

    Sorted by (date, ordering_hint) before visiting calculations.
    ordering_hint: 0 for ValuationAtStart, MAX_INT for ValuationAtEnd,
    transaction updated_at epoch-seconds otherwise.
    """

    date: date
    item_type: LineItemType
    shares: int = 0               # 10^8 scaled
    amount: int = 0               # gross monetary amount, term-currency cents
    net_amount: int = 0           # net amount (gross - fees - taxes), term-currency cents
    tax: int = 0                  # tax component, term-currency cents
    fee: int = 0                  # fee component, term-currency cents
    owner: str = ""               # portfolio UUID (for multi-portfolio lot tracking)
    source_owner: str = ""        # cross-entry owner for TRANSFER_IN
    ordering_hint: int = 0        # tie-breaker within same date
    security_currency_amount: int = 0   # amount in security currency cents (for forex gains)
    fx_rate: int = 0              # FX rate used (10^8 scaled), 0 if same currency

    def __post_init__(self) -> None:
        if self.net_amount == 0 and self.amount != 0:
            self.net_amount = self.amount


# ── Result types ──────────────────────────────────────────────────────

@dataclass(slots=True)
class CostResult:
    """Output of FIFO and moving-average cost calculations."""

    shares_held: int = 0                # 10^8 scaled
    fifo_cost: int = 0                  # term-currency cents (gross)
    net_fifo_cost: int = 0              # term-currency cents (net of fees/taxes)
    moving_avg_cost: int = 0            # term-currency cents (gross)
    net_moving_avg_cost: int = 0        # term-currency cents (net)
    total_fees: int = 0                 # term-currency cents
    total_taxes: int = 0                # term-currency cents


@dataclass(slots=True)
class CapitalGainsResult:
    """Output of capital gains calculations (FIFO or moving average)."""

    realized_gains: int = 0             # term-currency cents
    realized_forex_gains: int = 0       # term-currency cents
    unrealized_gains: int = 0           # term-currency cents
    unrealized_forex_gains: int = 0     # term-currency cents

    @property
    def total_realized(self) -> int:
        return self.realized_gains + self.realized_forex_gains

    @property
    def total_unrealized(self) -> int:
        return self.unrealized_gains + self.unrealized_forex_gains


class Periodicity(Enum):
    """Dividend payment frequency classification."""
    NONE = "none"
    UNKNOWN = "unknown"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    SEMIANNUAL = "semiannual"
    ANNUAL = "annual"


@dataclass(slots=True)
class DividendResult:
    """Output of dividend calculation."""

    total_amount: int = 0               # term-currency cents
    last_payment_date: date | None = None
    num_events: int = 0
    periodicity: Periodicity = Periodicity.NONE
    rate_of_return_per_year: float = 0.0


@dataclass(slots=True)
class DeltaResult:
    """Simple profit/loss delta."""

    delta: int = 0                      # term-currency cents
    cost: int = 0                       # total cost, term-currency cents

    @property
    def delta_percent(self) -> float:
        if self.delta == 0 and self.cost == 0:
            return 0.0
        if self.cost == 0:
            return 0.0
        return self.delta / self.cost


@dataclass(slots=True)
class SecurityMetricsResult:
    """Aggregated metrics for a single security within a portfolio."""

    security_uuid: str = ""
    portfolio_uuid: str = ""
    shares_held: int = 0                # 10^8 scaled
    cost: CostResult = field(default_factory=CostResult)
    capital_gains_fifo: CapitalGainsResult = field(default_factory=CapitalGainsResult)
    capital_gains_moving_avg: CapitalGainsResult = field(default_factory=CapitalGainsResult)
    dividends: DividendResult = field(default_factory=DividendResult)
    delta: DeltaResult = field(default_factory=DeltaResult)
    irr: float = float("nan")
    current_value: int = 0              # term-currency cents
    purchase_value: int = 0             # term-currency cents
