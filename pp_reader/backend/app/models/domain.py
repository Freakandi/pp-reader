"""Internal domain types for the PP Reader pipeline.

These types are not tied to the protobuf wire format. They provide
Pythonic representations of PP data used between pipeline stages
(parsing → ingestion → canonical sync → metrics).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final

__all__ = [
    "EPOCH",
    "epoch_day_to_date",
    "date_to_epoch_day",
    "PipelineError",
    "PortfolioParseError",
    "PortfolioValidationError",
    "ParseProgress",
    "StageName",
]

# PP uses epoch days relative to 1970-01-01 (Java's LocalDate epoch)
EPOCH: Final[date] = date(1970, 1, 1)

# Valid pipeline stage names (used for progress reporting)
StageName = str  # "accounts" | "portfolios" | "securities" | "transactions"


def epoch_day_to_date(epoch_day: int) -> date:
    """Convert a PP epoch day (days since 1970-01-01) to a Python date."""
    return EPOCH + timedelta(days=epoch_day)


def date_to_epoch_day(d: date) -> int:
    """Convert a Python date to a PP epoch day (days since 1970-01-01)."""
    return (d - EPOCH).days


class PipelineError(Exception):
    """Base class for all PP Reader pipeline errors."""


class PortfolioParseError(PipelineError):
    """Raised when the protobuf payload cannot be decoded."""


class PortfolioValidationError(PipelineError):
    """Raised when the parsed data fails validation invariants."""


@dataclass(slots=True)
class ParseProgress:
    """Progress payload for parse stage callbacks."""

    stage: StageName
    processed: int
    total: int
