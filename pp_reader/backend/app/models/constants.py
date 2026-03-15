"""Constants for the PP Reader pipeline — transaction types, unit types, scale factors."""

from __future__ import annotations

from enum import IntEnum

__all__ = [
    "TransactionType",
    "UnitType",
    "SHARE_EPSILON",
    "EIGHT_DECIMAL_SCALE",
    "MONETARY_SCALE",
]

# Immutable scaling convention (Architecture Decision 3)
EIGHT_DECIMAL_SCALE: int = 10**8   # quotes, prices, shares, FX rates
MONETARY_SCALE: int = 10**2        # monetary amounts (cents)
SHARE_EPSILON: float = 1e-9        # minimum meaningful share quantity


class TransactionType(IntEnum):
    """Transaction types matching PTransaction.Type in the protobuf schema.

    Note: protobuf uses PURCHASE/SALE; we keep BUY/SELL internally to match
    the legacy domain language. Integer values are identical.
    """

    BUY = 0              # PURCHASE in proto
    SELL = 1             # SALE in proto
    INBOUND_DELIVERY = 2
    OUTBOUND_DELIVERY = 3
    SECURITY_TRANSFER = 4
    CASH_TRANSFER = 5
    DEPOSIT = 6
    REMOVAL = 7
    DIVIDEND = 8
    INTEREST = 9
    INTEREST_CHARGE = 10
    TAX = 11
    TAX_REFUND = 12
    FEE = 13
    FEE_REFUND = 14


class UnitType(IntEnum):
    """Transaction unit types matching PTransactionUnit.Type in the protobuf schema."""

    GROSS_VALUE = 0
    TAX = 1
    FEE = 2
