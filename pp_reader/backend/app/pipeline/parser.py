"""Protobuf parser pipeline — reads a .portfolio file and returns a ParsedClient.

Ported from legacy _legacy_v1/custom_components/pp_reader/services/parser_pipeline.py.
Home Assistant dependencies removed; pure async Python using asyncio executor
for the CPU-bound protobuf decode step.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Final

from app.models import parsed
from app.models.domain import (
    ParseProgress,
    PortfolioParseError,
    PortfolioValidationError,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Security type whitelist (matches PP desktop's current type list)
# ---------------------------------------------------------------------------

SUPPORTED_SECURITY_TYPES: Final[frozenset[str]] = frozenset(
    {
        "STOCK",
        "FUND",
        "ETF",
        "BOND",
        "CASH",
        "INDEX",
        "COMMODITY",
        "CRYPTO",
        "CERTIFICATE",
        "STRUCTURED_PRODUCT",
        "DERIVATIVE",
        "MUTUAL_FUND",
        "OTHER",
    }
)

ProgressCallback = Callable[[ParseProgress], Any] | None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def parse_portfolio_file(
    path: Path,
    progress_cb: ProgressCallback = None,
) -> parsed.ParsedClient:
    """Parse a .portfolio file and return a fully-validated ParsedClient.

    The protobuf decode runs in the default asyncio thread pool executor so it
    does not block the event loop. Validation (UUID uniqueness, security type
    whitelist) runs synchronously after decode.

    Args:
        path: Absolute path to the .portfolio file.
        progress_cb: Optional async or sync callback invoked after each stage.

    Returns:
        Fully-parsed and validated ParsedClient.

    Raises:
        FileNotFoundError: If the file does not exist.
        PortfolioParseError: If the protobuf payload cannot be decoded.
        PortfolioValidationError: If parsed data fails validation invariants.
    """
    raw_payload = await _read_file(path)
    proto_client = await _decode_proto(raw_payload)
    parsed_client = await asyncio.get_event_loop().run_in_executor(
        None, _build_parsed_client, proto_client
    )

    stages = [
        ("accounts", parsed_client.accounts),
        ("portfolios", parsed_client.portfolios),
        ("securities", parsed_client.securities),
        ("transactions", parsed_client.transactions),
    ]

    for stage_name, items in stages:
        total = len(items)
        if progress_cb is not None:
            result = progress_cb(ParseProgress(stage_name, total, total))
            if asyncio.iscoroutine(result):
                await result

    return parsed_client


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _read_file(path: Path) -> bytes:
    """Read the .portfolio file bytes via thread pool (non-blocking)."""
    if not path.exists():
        raise FileNotFoundError(f"Portfolio file not found: {path}")

    def _read() -> bytes:
        return path.read_bytes()

    return await asyncio.get_event_loop().run_in_executor(None, _read)


async def _decode_proto(payload: bytes) -> Any:
    """Decode raw protobuf bytes into a PClient message (thread pool)."""
    from google.protobuf import message as protobuf_message

    from app.generated import client_pb2

    def _decode() -> Any:
        client = client_pb2.PClient()
        client.ParseFromString(payload)
        return client

    try:
        return await asyncio.get_event_loop().run_in_executor(None, _decode)
    except protobuf_message.DecodeError as exc:
        msg = f"Failed to decode protobuf payload: {exc}"
        raise PortfolioParseError(msg) from exc


def _build_parsed_client(proto_client: Any) -> parsed.ParsedClient:
    """Convert a raw PClient proto message into a validated ParsedClient."""
    accounts = list(_iter_accounts(proto_client))
    portfolios = list(_iter_portfolios(proto_client))
    securities = list(_iter_securities(proto_client))
    transactions = list(_iter_transactions(proto_client))
    plans = list(_iter_plans(proto_client))
    watchlists = list(_iter_watchlists(proto_client))
    taxonomies = list(_iter_taxonomies(proto_client))
    dashboards = list(_iter_dashboards(proto_client))
    settings = _extract_settings(proto_client)
    properties = _extract_properties(proto_client)

    return parsed.ParsedClient(
        version=int(getattr(proto_client, "version", 0)),
        base_currency=getattr(proto_client, "baseCurrency", None) or None,
        accounts=accounts,
        portfolios=portfolios,
        securities=securities,
        transactions=transactions,
        plans=plans,
        watchlists=watchlists,
        taxonomies=taxonomies,
        dashboards=dashboards,
        settings=settings,
        properties=properties,
    )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _ensure_unique(seen: set[str], uuid: str, entity: str) -> None:
    if not uuid:
        msg = f"{entity} missing uuid"
        raise PortfolioValidationError(msg)
    if uuid in seen:
        msg = f"Duplicate {entity} uuid '{uuid}'"
        raise PortfolioValidationError(msg)
    seen.add(uuid)


def _validate_security_type(security: parsed.ParsedSecurity) -> None:
    sec_type = security.properties.get("type")
    if not sec_type:
        return
    if not isinstance(sec_type, str):
        msg = f"Unsupported security type '{sec_type}' (expected string)"
        raise PortfolioValidationError(msg)
    if sec_type.upper() not in SUPPORTED_SECURITY_TYPES:
        msg = f"Unsupported security type '{sec_type}'"
        raise PortfolioValidationError(msg)


def _validate_transaction_units(transaction: parsed.ParsedTransaction) -> None:
    for unit in transaction.units:
        if unit.type not in (0, 1, 2):
            msg = f"Unsupported transaction unit type '{unit.type}'"
            raise PortfolioValidationError(msg)


# ---------------------------------------------------------------------------
# Iterator helpers (one per entity collection)
# ---------------------------------------------------------------------------


def _iter_accounts(proto_client: Any) -> Iterable[parsed.ParsedAccount]:
    seen: set[str] = set()
    for account in getattr(proto_client, "accounts", []):
        uuid = getattr(account, "uuid", "")
        _ensure_unique(seen, uuid, "account")
        yield parsed.ParsedAccount.from_proto(account)


def _iter_portfolios(proto_client: Any) -> Iterable[parsed.ParsedPortfolio]:
    seen: set[str] = set()
    for portfolio in getattr(proto_client, "portfolios", []):
        uuid = getattr(portfolio, "uuid", "")
        _ensure_unique(seen, uuid, "portfolio")
        yield parsed.ParsedPortfolio.from_proto(portfolio)


def _iter_securities(proto_client: Any) -> Iterable[parsed.ParsedSecurity]:
    seen: set[str] = set()
    for security in getattr(proto_client, "securities", []):
        uuid = getattr(security, "uuid", "")
        _ensure_unique(seen, uuid, "security")
        parsed_security = parsed.ParsedSecurity.from_proto(security)
        _validate_security_type(parsed_security)
        yield parsed_security


def _iter_transactions(proto_client: Any) -> Iterable[parsed.ParsedTransaction]:
    seen: set[str] = set()
    for transaction in getattr(proto_client, "transactions", []):
        uuid = getattr(transaction, "uuid", "")
        _ensure_unique(seen, uuid, "transaction")
        parsed_transaction = parsed.ParsedTransaction.from_proto(transaction)
        _validate_transaction_units(parsed_transaction)
        yield parsed_transaction


def _iter_plans(proto_client: Any) -> Iterable[parsed.ParsedInvestmentPlan]:
    for plan in getattr(proto_client, "plans", []):
        yield parsed.ParsedInvestmentPlan.from_proto(plan)


def _iter_watchlists(proto_client: Any) -> Iterable[parsed.ParsedWatchlist]:
    for watchlist in getattr(proto_client, "watchlists", []):
        yield parsed.ParsedWatchlist.from_proto(watchlist)


def _iter_taxonomies(proto_client: Any) -> Iterable[parsed.ParsedTaxonomy]:
    for taxonomy in getattr(proto_client, "taxonomies", []):
        yield parsed.ParsedTaxonomy.from_proto(taxonomy)


def _iter_dashboards(proto_client: Any) -> Iterable[parsed.ParsedDashboard]:
    for dashboard in getattr(proto_client, "dashboards", []):
        yield parsed.ParsedDashboard.from_proto(dashboard)


def _extract_properties(proto_client: Any) -> dict[str, str]:
    properties = getattr(proto_client, "properties", None)
    if properties is None:
        return {}
    try:
        return dict(properties)
    except TypeError:
        result: dict[str, str] = {}
        for key, value in properties.items():
            result[str(key)] = str(value)
        return result


def _extract_settings(proto_client: Any) -> parsed.ParsedSettings | None:
    settings_msg = getattr(proto_client, "settings", None)
    if settings_msg is None:
        return None

    has_field = getattr(proto_client, "HasField", None)
    if callable(has_field):
        try:
            if not has_field("settings"):
                return None
        except ValueError:
            pass

    return parsed.ParsedSettings.from_proto(settings_msg)
