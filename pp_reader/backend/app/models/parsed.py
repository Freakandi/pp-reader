"""Typed dataclasses for parsed Portfolio Performance protobuf payloads.

Ported from legacy _legacy_v1/custom_components/pp_reader/models/parsed.py.
HA-specific imports removed; uses generated protobuf stubs directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from datetime import datetime

    from google.protobuf.timestamp_pb2 import Timestamp

    from app.generated import client_pb2
else:  # pragma: no cover - runtime fallbacks
    Iterable = Mapping = Any  # type: ignore[assignment]
    datetime = Any  # type: ignore[assignment]
    client_pb2 = Any  # type: ignore[assignment]
    Timestamp = Any  # type: ignore[assignment]

__all__ = [
    "ParsedAccount",
    "ParsedAttributeType",
    "ParsedBookmark",
    "ParsedClient",
    "ParsedConfigurationSet",
    "ParsedDashboard",
    "ParsedDashboardColumn",
    "ParsedDashboardWidget",
    "ParsedHistoricalPrice",
    "ParsedInvestmentPlan",
    "ParsedPortfolio",
    "ParsedSecurity",
    "ParsedSecurityEvent",
    "ParsedSettings",
    "ParsedTaxonomy",
    "ParsedTaxonomyAssignment",
    "ParsedTaxonomyClassification",
    "ParsedTransaction",
    "ParsedTransactionUnit",
    "ParsedWatchlist",
]


# ---------------------------------------------------------------------------
# Low-level proto helpers
# ---------------------------------------------------------------------------


def _timestamp_to_datetime(ts: Timestamp | None) -> datetime | None:
    """Convert a protobuf Timestamp into a timezone-aware datetime."""
    if ts is None:
        return None
    if ts.seconds == 0 and ts.nanos == 0:
        return None
    return ts.ToDatetime().replace(tzinfo=UTC)


def _maybe_field(message: Any, field_name: str) -> Any:
    """Return an optional protobuf attribute if presence is set.

    Proto3 optional fields support HasField(); non-optional scalar fields
    raise ValueError — we fall back to returning the raw value in that case.
    """
    if not hasattr(message, field_name):
        return None

    value = getattr(message, field_name)
    has_field = getattr(message, "HasField", None)
    if callable(has_field):
        try:
            if has_field(field_name):
                return value
        except ValueError:
            # Not a singular message or oneof field — return directly.
            return value
        return None
    return value


def _parse_decimal_value(pdecimal: client_pb2.PDecimalValue | None) -> float | None:
    """Convert a PDecimalValue (little-endian bytes + scale) to float.

    This is used for FX rates on transaction units. The result is a float
    for display convenience; wire storage uses BIGINT scaled at 10^8.
    """
    if pdecimal is None:
        return None

    has_field = getattr(pdecimal, "HasField", None)
    if callable(has_field):
        try:
            if not has_field("value"):
                return None
        except ValueError:
            # bytes field — check for empty
            if not pdecimal.value:
                return None
    elif not getattr(pdecimal, "value", None):
        return None

    raw = int.from_bytes(pdecimal.value, byteorder="little", signed=True)
    return raw / (10 ** pdecimal.scale)


def _parse_any_value(value: client_pb2.PAnyValue | None) -> Any:
    """Deserialize a polymorphic PAnyValue message into a Python primitive."""
    if value is None:
        return None

    kind: str | None = None
    for oneof_name in ("value", "kind"):
        which = None
        try:
            which = value.WhichOneof(oneof_name)
        except ValueError:
            continue
        if which is not None:
            kind = which
            break

    if kind is None or kind == "null":
        return None
    if kind == "map":
        return _parse_map(value.map)

    attr_map = {
        "string": value.string,
        "int32": int(value.int32),
        "int64": int(value.int64),
        "double": float(value.double),
        "bool": bool(value.bool),
    }
    return attr_map.get(kind)


def _parse_key_value_entries(entries: Iterable[client_pb2.PKeyValue]) -> dict[str, Any]:
    """Convert repeated PKeyValue entries into a Python dictionary."""
    result: dict[str, Any] = {}
    for entry in entries:
        result[entry.key] = _parse_any_value(_maybe_field(entry, "value"))
    return result


def _parse_map(value: client_pb2.PMap | None) -> dict[str, Any]:
    """Convert a PMap helper message to a Python dictionary."""
    if value is None:
        return {}
    return _parse_key_value_entries(value.entries)


def _parse_any_sequence(values: Iterable[client_pb2.PAnyValue]) -> list[Any]:
    """Convert a repeated PAnyValue list into native Python values."""
    return [_parse_any_value(value) for value in values]


def _parse_configuration_entries(entries: Iterable[Any]) -> dict[str, str]:
    """Convert dashboard configuration map entries to a plain dict."""
    config: dict[str, str] = {}
    for entry in entries:
        key = getattr(entry, "key", None)
        if not key:
            continue
        value = getattr(entry, "value", "")
        config[str(key)] = str(value) if value is not None else ""
    return config


# ---------------------------------------------------------------------------
# Parsed dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ParsedTransactionUnit:
    """Representation of a PTransactionUnit message."""

    type: int
    amount: int | None
    currency_code: str | None
    fx_amount: int | None = None
    fx_currency_code: str | None = None
    fx_rate_to_base: float | None = None

    @classmethod
    def from_proto(cls, unit: client_pb2.PTransactionUnit) -> ParsedTransactionUnit:
        return cls(
            type=int(unit.type),
            amount=_maybe_field(unit, "amount"),
            currency_code=_maybe_field(unit, "currencyCode"),
            fx_amount=_maybe_field(unit, "fxAmount"),
            fx_currency_code=_maybe_field(unit, "fxCurrencyCode"),
            fx_rate_to_base=_parse_decimal_value(_maybe_field(unit, "fxRateToBase")),
        )


@dataclass(slots=True)
class ParsedHistoricalPrice:
    """Historical price point from the portfolio export.

    ``date`` is an epoch day (days since 1970-01-01), matching the proto wire format.
    Use :func:`app.models.domain.epoch_day_to_date` to convert to ``datetime.date``.
    """

    date: int
    close: int | None
    high: int | None = None
    low: int | None = None
    volume: int | None = None

    @classmethod
    def from_proto(
        cls,
        message: client_pb2.PHistoricalPrice | client_pb2.PFullHistoricalPrice,
    ) -> ParsedHistoricalPrice:
        return cls(
            date=int(getattr(message, "date", 0)),
            close=_maybe_field(message, "close"),
            high=_maybe_field(message, "high"),
            low=_maybe_field(message, "low"),
            volume=_maybe_field(message, "volume"),
        )


@dataclass(slots=True)
class ParsedSecurityEvent:
    """Security event metadata such as splits or dividend payments."""

    type: int
    date: int | None
    details: str | None
    data: list[Any] = field(default_factory=list)
    source: str | None = None

    @classmethod
    def from_proto(cls, event: client_pb2.PSecurityEvent) -> ParsedSecurityEvent:
        date_value = _maybe_field(event, "date")
        return cls(
            type=int(event.type),
            date=int(date_value) if date_value is not None else None,
            details=_maybe_field(event, "details"),
            data=_parse_any_sequence(getattr(event, "data", [])),
            source=_maybe_field(event, "source"),
        )


@dataclass(slots=True)
class ParsedAccount:
    """Account metadata from the portfolio export."""

    uuid: str
    name: str
    currency_code: str | None
    note: str | None
    is_retired: bool
    attributes: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime | None = None

    @classmethod
    def from_proto(cls, account: client_pb2.PAccount) -> ParsedAccount:
        return cls(
            uuid=account.uuid,
            name=account.name,
            currency_code=account.currencyCode or None,
            note=_maybe_field(account, "note"),
            is_retired=bool(account.isRetired),
            attributes=_parse_key_value_entries(account.attributes),
            updated_at=_timestamp_to_datetime(_maybe_field(account, "updatedAt")),
        )


@dataclass(slots=True)
class ParsedPortfolio:
    """Portfolio metadata from the portfolio export."""

    uuid: str
    name: str
    note: str | None
    is_retired: bool
    reference_account: str | None
    attributes: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime | None = None

    @classmethod
    def from_proto(cls, portfolio: client_pb2.PPortfolio) -> ParsedPortfolio:
        return cls(
            uuid=portfolio.uuid,
            name=portfolio.name,
            note=_maybe_field(portfolio, "note"),
            is_retired=bool(portfolio.isRetired),
            reference_account=_maybe_field(portfolio, "referenceAccount"),
            attributes=_parse_key_value_entries(portfolio.attributes),
            updated_at=_timestamp_to_datetime(_maybe_field(portfolio, "updatedAt")),
        )


@dataclass(slots=True)
class ParsedSecurity:
    """Security metadata including historical pricing."""

    uuid: str
    name: str
    currency_code: str | None
    target_currency_code: str | None
    isin: str | None
    ticker_symbol: str | None
    wkn: str | None
    note: str | None
    online_id: str | None
    feed: str | None
    feed_url: str | None
    latest_feed: str | None
    latest_feed_url: str | None
    calendar: str | None
    is_retired: bool
    attributes: dict[str, Any] = field(default_factory=dict)
    properties: dict[str, Any] = field(default_factory=dict)
    prices: list[ParsedHistoricalPrice] = field(default_factory=list)
    latest: ParsedHistoricalPrice | None = None
    events: list[ParsedSecurityEvent] = field(default_factory=list)
    updated_at: datetime | None = None

    @classmethod
    def from_proto(cls, security: client_pb2.PSecurity) -> ParsedSecurity:
        latest_price: ParsedHistoricalPrice | None = None
        if security.HasField("latest"):
            latest_price = ParsedHistoricalPrice.from_proto(security.latest)

        return cls(
            uuid=security.uuid,
            name=security.name,
            currency_code=_maybe_field(security, "currencyCode"),
            target_currency_code=_maybe_field(security, "targetCurrencyCode"),
            isin=_maybe_field(security, "isin"),
            ticker_symbol=_maybe_field(security, "tickerSymbol"),
            wkn=_maybe_field(security, "wkn"),
            note=_maybe_field(security, "note"),
            online_id=_maybe_field(security, "onlineId"),
            feed=_maybe_field(security, "feed"),
            feed_url=_maybe_field(security, "feedURL"),
            latest_feed=_maybe_field(security, "latestFeed"),
            latest_feed_url=_maybe_field(security, "latestFeedURL"),
            calendar=_maybe_field(security, "calendar"),
            is_retired=bool(security.isRetired),
            attributes=_parse_key_value_entries(security.attributes),
            properties=_parse_key_value_entries(security.properties),
            prices=[ParsedHistoricalPrice.from_proto(p) for p in security.prices],
            latest=latest_price,
            events=[ParsedSecurityEvent.from_proto(e) for e in security.events],
            updated_at=_timestamp_to_datetime(_maybe_field(security, "updatedAt")),
        )


@dataclass(slots=True)
class ParsedTransaction:
    """Transaction data emitted by the portfolio export."""

    uuid: str
    type: int
    account: str | None
    portfolio: str | None
    other_account: str | None
    other_portfolio: str | None
    other_uuid: str | None
    other_updated_at: datetime | None
    date: datetime | None
    currency_code: str | None
    amount: int | None
    shares: int | None
    note: str | None
    security: str | None
    source: str | None
    updated_at: datetime | None
    units: list[ParsedTransactionUnit] = field(default_factory=list)

    @classmethod
    def from_proto(cls, transaction: client_pb2.PTransaction) -> ParsedTransaction:
        return cls(
            uuid=transaction.uuid,
            type=int(transaction.type),
            account=_maybe_field(transaction, "account"),
            portfolio=_maybe_field(transaction, "portfolio"),
            other_account=_maybe_field(transaction, "otherAccount"),
            other_portfolio=_maybe_field(transaction, "otherPortfolio"),
            other_uuid=_maybe_field(transaction, "otherUuid"),
            other_updated_at=_timestamp_to_datetime(
                _maybe_field(transaction, "otherUpdatedAt")
            ),
            date=_timestamp_to_datetime(_maybe_field(transaction, "date")),
            currency_code=_maybe_field(transaction, "currencyCode"),
            amount=_maybe_field(transaction, "amount"),
            shares=_maybe_field(transaction, "shares"),
            note=_maybe_field(transaction, "note"),
            security=_maybe_field(transaction, "security"),
            source=_maybe_field(transaction, "source"),
            updated_at=_timestamp_to_datetime(_maybe_field(transaction, "updatedAt")),
            units=[ParsedTransactionUnit.from_proto(u) for u in transaction.units],
        )


@dataclass(slots=True)
class ParsedBookmark:
    """Bookmark definition stored in Portfolio Performance settings."""

    label: str
    pattern: str

    @classmethod
    def from_proto(cls, bookmark: client_pb2.PBookmark) -> ParsedBookmark:
        return cls(label=bookmark.label, pattern=bookmark.pattern)


@dataclass(slots=True)
class ParsedAttributeType:
    """Attribute type metadata used for custom fields."""

    id: str
    name: str
    column_label: str | None
    source: str | None
    target: str
    type: str
    converter_class: str | None
    properties: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_proto(cls, attribute: client_pb2.PAttributeType) -> ParsedAttributeType:
        return cls(
            id=attribute.id,
            name=attribute.name,
            column_label=_maybe_field(attribute, "columnLabel"),
            source=_maybe_field(attribute, "source"),
            target=attribute.target,
            type=attribute.type,
            converter_class=_maybe_field(attribute, "converterClass"),
            properties=_parse_map(_maybe_field(attribute, "properties")),
        )


@dataclass(slots=True)
class ParsedConfigurationSet:
    """Configuration set stored inside Portfolio Performance settings."""

    key: str
    uuid: str | None
    name: str | None
    data: str | None

    @classmethod
    def from_proto(cls, config: client_pb2.PConfigurationSet) -> ParsedConfigurationSet:
        return cls(
            key=config.key,
            uuid=_maybe_field(config, "uuid"),
            name=_maybe_field(config, "name"),
            data=_maybe_field(config, "data"),
        )


@dataclass(slots=True)
class ParsedSettings:
    """Structured representation of Portfolio Performance settings."""

    bookmarks: list[ParsedBookmark] = field(default_factory=list)
    attribute_types: list[ParsedAttributeType] = field(default_factory=list)
    configuration_sets: list[ParsedConfigurationSet] = field(default_factory=list)

    @classmethod
    def from_proto(cls, settings: client_pb2.PSettings) -> ParsedSettings:
        return cls(
            bookmarks=[
                ParsedBookmark.from_proto(b) for b in getattr(settings, "bookmarks", [])
            ],
            attribute_types=[
                ParsedAttributeType.from_proto(a)
                for a in getattr(settings, "attributeTypes", [])
            ],
            configuration_sets=[
                ParsedConfigurationSet.from_proto(c)
                for c in getattr(settings, "configurationSets", [])
            ],
        )


@dataclass(slots=True)
class ParsedInvestmentPlan:
    """Investment plan metadata."""

    name: str
    note: str | None
    security: str | None
    portfolio: str | None
    account: str | None
    attributes: dict[str, Any] = field(default_factory=dict)
    auto_generate: bool = False
    date: int | None = None
    interval: int | None = None
    amount: int | None = None
    fees: int | None = None
    transactions: list[str] = field(default_factory=list)
    taxes: int | None = None
    plan_type: int | None = None

    @classmethod
    def from_proto(cls, plan: client_pb2.PInvestmentPlan) -> ParsedInvestmentPlan:
        plan_type = _maybe_field(plan, "type")
        return cls(
            name=plan.name,
            note=_maybe_field(plan, "note"),
            security=_maybe_field(plan, "security"),
            portfolio=_maybe_field(plan, "portfolio"),
            account=_maybe_field(plan, "account"),
            attributes=_parse_key_value_entries(plan.attributes),
            auto_generate=bool(getattr(plan, "autoGenerate", False)),
            date=_maybe_field(plan, "date"),
            interval=_maybe_field(plan, "interval"),
            amount=_maybe_field(plan, "amount"),
            fees=_maybe_field(plan, "fees"),
            transactions=list(getattr(plan, "transactions", [])),
            taxes=_maybe_field(plan, "taxes"),
            plan_type=int(plan_type) if plan_type is not None else None,
        )


@dataclass(slots=True)
class ParsedWatchlist:
    """Watchlist definition from the portfolio export."""

    name: str
    securities: list[str] = field(default_factory=list)

    @classmethod
    def from_proto(cls, watchlist: client_pb2.PWatchlist) -> ParsedWatchlist:
        return cls(
            name=watchlist.name,
            securities=list(getattr(watchlist, "securities", [])),
        )


@dataclass(slots=True)
class ParsedTaxonomyAssignment:
    """Assignment entry within a taxonomy classification."""

    investment_vehicle: str
    weight: int | None
    rank: int | None
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_proto(
        cls,
        assignment: client_pb2.PTaxonomy.Assignment,
    ) -> ParsedTaxonomyAssignment:
        return cls(
            investment_vehicle=assignment.investmentVehicle,
            weight=_maybe_field(assignment, "weight"),
            rank=_maybe_field(assignment, "rank"),
            data=_parse_key_value_entries(assignment.data),
        )


@dataclass(slots=True)
class ParsedTaxonomyClassification:
    """Classification entry detailing taxonomy hierarchy."""

    id: str
    name: str
    parent_id: str | None
    note: str | None
    color: str | None
    weight: int | None
    rank: int | None
    data: dict[str, Any] = field(default_factory=dict)
    assignments: list[ParsedTaxonomyAssignment] = field(default_factory=list)

    @classmethod
    def from_proto(
        cls,
        classification: client_pb2.PTaxonomy.Classification,
    ) -> ParsedTaxonomyClassification:
        return cls(
            id=classification.id,
            name=classification.name,
            parent_id=_maybe_field(classification, "parentId"),
            note=_maybe_field(classification, "note"),
            color=_maybe_field(classification, "color"),
            weight=_maybe_field(classification, "weight"),
            rank=_maybe_field(classification, "rank"),
            data=_parse_key_value_entries(classification.data),
            assignments=[
                ParsedTaxonomyAssignment.from_proto(a)
                for a in getattr(classification, "assignments", [])
            ],
        )


@dataclass(slots=True)
class ParsedTaxonomy:
    """Taxonomy definition for classification hierarchies."""

    id: str
    name: str
    source: str | None
    dimensions: list[str] = field(default_factory=list)
    classifications: list[ParsedTaxonomyClassification] = field(default_factory=list)

    @classmethod
    def from_proto(cls, taxonomy: client_pb2.PTaxonomy) -> ParsedTaxonomy:
        return cls(
            id=taxonomy.id,
            name=taxonomy.name,
            source=_maybe_field(taxonomy, "source"),
            dimensions=list(getattr(taxonomy, "dimensions", [])),
            classifications=[
                ParsedTaxonomyClassification.from_proto(c)
                for c in getattr(taxonomy, "classifications", [])
            ],
        )


@dataclass(slots=True)
class ParsedDashboardWidget:
    """Dashboard widget definition."""

    type: str
    label: str | None
    configuration: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_proto(cls, widget: client_pb2.PDashboard.Widget) -> ParsedDashboardWidget:
        return cls(
            type=widget.type,
            label=_maybe_field(widget, "label"),
            configuration=_parse_configuration_entries(widget.configuration),
        )


@dataclass(slots=True)
class ParsedDashboardColumn:
    """Dashboard column containing ordered widgets."""

    weight: int | None
    widgets: list[ParsedDashboardWidget] = field(default_factory=list)

    @classmethod
    def from_proto(cls, column: client_pb2.PDashboard.Column) -> ParsedDashboardColumn:
        return cls(
            weight=_maybe_field(column, "weight"),
            widgets=[
                ParsedDashboardWidget.from_proto(w)
                for w in getattr(column, "widgets", [])
            ],
        )


@dataclass(slots=True)
class ParsedDashboard:
    """Dashboard definition with layout metadata."""

    name: str
    configuration: dict[str, str] = field(default_factory=dict)
    columns: list[ParsedDashboardColumn] = field(default_factory=list)
    dashboard_id: str | None = None

    @classmethod
    def from_proto(cls, dashboard: client_pb2.PDashboard) -> ParsedDashboard:
        return cls(
            name=dashboard.name,
            configuration=_parse_configuration_entries(dashboard.configuration),
            columns=[
                ParsedDashboardColumn.from_proto(c)
                for c in getattr(dashboard, "columns", [])
            ],
            dashboard_id=_maybe_field(dashboard, "id"),
        )


@dataclass(slots=True)
class ParsedClient:
    """Container aggregating all parsed Portfolio Performance data."""

    version: int
    base_currency: str | None
    accounts: list[ParsedAccount] = field(default_factory=list)
    portfolios: list[ParsedPortfolio] = field(default_factory=list)
    securities: list[ParsedSecurity] = field(default_factory=list)
    transactions: list[ParsedTransaction] = field(default_factory=list)
    plans: list[ParsedInvestmentPlan] = field(default_factory=list)
    watchlists: list[ParsedWatchlist] = field(default_factory=list)
    taxonomies: list[ParsedTaxonomy] = field(default_factory=list)
    dashboards: list[ParsedDashboard] = field(default_factory=list)
    settings: ParsedSettings | None = None
    properties: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_proto(cls, client: client_pb2.PClient) -> ParsedClient:
        """Build the parsed client container from a protobuf PClient message."""
        try:
            properties: Mapping[str, str] = dict(client.properties)
        except TypeError:
            properties = {}

        settings: ParsedSettings | None = None
        has_field = getattr(client, "HasField", None)
        if callable(has_field):
            try:
                if has_field("settings"):
                    settings = ParsedSettings.from_proto(client.settings)
            except ValueError:
                settings = ParsedSettings.from_proto(client.settings)
        elif getattr(client, "settings", None):
            settings = ParsedSettings.from_proto(client.settings)

        return cls(
            version=int(client.version),
            base_currency=client.baseCurrency or None,
            accounts=[ParsedAccount.from_proto(a) for a in client.accounts],
            portfolios=[ParsedPortfolio.from_proto(p) for p in client.portfolios],
            securities=[ParsedSecurity.from_proto(s) for s in client.securities],
            transactions=[ParsedTransaction.from_proto(t) for t in client.transactions],
            plans=[ParsedInvestmentPlan.from_proto(p) for p in client.plans],
            watchlists=[ParsedWatchlist.from_proto(w) for w in client.watchlists],
            taxonomies=[ParsedTaxonomy.from_proto(t) for t in client.taxonomies],
            dashboards=[ParsedDashboard.from_proto(d) for d in client.dashboards],
            settings=settings,
            properties=properties,
        )
