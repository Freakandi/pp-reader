"""Unit tests for the protobuf parser pipeline and domain models.

Tests use mocked protobuf data — no file I/O, no network calls.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.models.constants import (
    EIGHT_DECIMAL_SCALE,
    MONETARY_SCALE,
    SHARE_EPSILON,
    TransactionType,
    UnitType,
)
from app.models.domain import (
    EPOCH,
    ParseProgress,
    PortfolioParseError,
    PortfolioValidationError,
    date_to_epoch_day,
    epoch_day_to_date,
)
from app.models.parsed import (
    ParsedAccount,
    ParsedClient,
    ParsedHistoricalPrice,
    ParsedPortfolio,
    ParsedSecurity,
    ParsedSettings,
    ParsedTransaction,
    ParsedTransactionUnit,
    _parse_decimal_value,
    _maybe_field,
    _timestamp_to_datetime,
)
from app.pipeline.parser import (
    SUPPORTED_SECURITY_TYPES,
    _build_parsed_client,
    _ensure_unique,
    _validate_security_type,
    _validate_transaction_units,
    parse_portfolio_file,
)


# ---------------------------------------------------------------------------
# constants.py tests
# ---------------------------------------------------------------------------


class TestTransactionType:
    def test_buy_is_zero(self) -> None:
        assert TransactionType.BUY == 0

    def test_sell_is_one(self) -> None:
        assert TransactionType.SELL == 1

    def test_all_fifteen_types(self) -> None:
        assert len(TransactionType) == 15

    def test_dividend(self) -> None:
        assert TransactionType.DIVIDEND == 8

    def test_fee_refund(self) -> None:
        assert TransactionType.FEE_REFUND == 14


class TestUnitType:
    def test_gross_value_is_zero(self) -> None:
        assert UnitType.GROSS_VALUE == 0

    def test_tax_is_one(self) -> None:
        assert UnitType.TAX == 1

    def test_fee_is_two(self) -> None:
        assert UnitType.FEE == 2

    def test_three_unit_types(self) -> None:
        assert len(UnitType) == 3


class TestScaleFactors:
    def test_eight_decimal_scale(self) -> None:
        assert EIGHT_DECIMAL_SCALE == 100_000_000

    def test_monetary_scale(self) -> None:
        assert MONETARY_SCALE == 100

    def test_share_epsilon(self) -> None:
        assert SHARE_EPSILON == 1e-9


# ---------------------------------------------------------------------------
# domain.py tests
# ---------------------------------------------------------------------------


class TestEpochDayConversion:
    def test_epoch_base(self) -> None:
        assert epoch_day_to_date(0) == date(1970, 1, 1)

    def test_known_date(self) -> None:
        # 2024-01-01 is 19723 days after epoch
        result = epoch_day_to_date(19723)
        assert result == date(2024, 1, 1)

    def test_roundtrip(self) -> None:
        d = date(2023, 6, 15)
        assert epoch_day_to_date(date_to_epoch_day(d)) == d

    def test_date_to_epoch_day_epoch(self) -> None:
        assert date_to_epoch_day(date(1970, 1, 1)) == 0

    def test_date_to_epoch_day_positive(self) -> None:
        assert date_to_epoch_day(date(1970, 1, 2)) == 1


class TestDomainExceptions:
    def test_portfolio_parse_error_is_pipeline_error(self) -> None:
        from app.models.domain import PipelineError
        err = PortfolioParseError("test")
        assert isinstance(err, PipelineError)

    def test_portfolio_validation_error_is_pipeline_error(self) -> None:
        from app.models.domain import PipelineError
        err = PortfolioValidationError("test")
        assert isinstance(err, PipelineError)

    def test_parse_progress_fields(self) -> None:
        p = ParseProgress("accounts", 5, 10)
        assert p.stage == "accounts"
        assert p.processed == 5
        assert p.total == 10


# ---------------------------------------------------------------------------
# parsed.py helper function tests
# ---------------------------------------------------------------------------


class TestTimestampToDatetime:
    def test_none_returns_none(self) -> None:
        assert _timestamp_to_datetime(None) is None

    def test_zero_timestamp_returns_none(self) -> None:
        ts = MagicMock()
        ts.seconds = 0
        ts.nanos = 0
        assert _timestamp_to_datetime(ts) is None

    def test_valid_timestamp(self) -> None:
        ts = MagicMock()
        ts.seconds = 1
        ts.nanos = 0
        ts.ToDatetime.return_value = datetime(2024, 1, 1, 12, 0, 0)
        result = _timestamp_to_datetime(ts)
        assert result is not None
        assert result.tzinfo == UTC


class TestMaybeField:
    def test_missing_attribute_returns_none(self) -> None:
        obj = MagicMock(spec=[])
        assert _maybe_field(obj, "nonexistent") is None

    def test_has_field_true(self) -> None:
        obj = MagicMock()
        obj.myfield = "hello"
        obj.HasField.return_value = True
        assert _maybe_field(obj, "myfield") == "hello"

    def test_has_field_false(self) -> None:
        obj = MagicMock()
        obj.myfield = "hello"
        obj.HasField.return_value = False
        assert _maybe_field(obj, "myfield") is None

    def test_has_field_value_error_returns_value(self) -> None:
        obj = MagicMock()
        obj.myfield = 42
        obj.HasField.side_effect = ValueError("not a message field")
        assert _maybe_field(obj, "myfield") == 42

    def test_no_has_field_method_returns_value(self) -> None:
        class Plain:
            myfield = "direct"

        obj = Plain()
        assert _maybe_field(obj, "myfield") == "direct"


class TestParseDecimalValue:
    def test_none_returns_none(self) -> None:
        assert _parse_decimal_value(None) is None

    def test_empty_bytes_returns_none(self) -> None:
        pdecimal = MagicMock()
        pdecimal.HasField.side_effect = ValueError
        pdecimal.value = b""
        assert _parse_decimal_value(pdecimal) is None

    def test_positive_value(self) -> None:
        # 1.0845 → bytes little-endian: 108450000 (10^8 scale=8)
        # Store raw = 108450000, scale = 8 → 108450000 / 10^8 = 1.0845
        raw = 108450000
        pdecimal = MagicMock()
        pdecimal.HasField.side_effect = ValueError
        pdecimal.value = raw.to_bytes(8, byteorder="little", signed=True)
        pdecimal.scale = 8
        result = _parse_decimal_value(pdecimal)
        assert result is not None
        assert abs(result - 1.0845) < 1e-6

    def test_negative_value(self) -> None:
        # -0.5 → raw = -50000000 (scale=8)
        raw = -50000000
        pdecimal = MagicMock()
        pdecimal.HasField.side_effect = ValueError
        pdecimal.value = raw.to_bytes(8, byteorder="little", signed=True)
        pdecimal.scale = 8
        result = _parse_decimal_value(pdecimal)
        assert result is not None
        assert abs(result - (-0.5)) < 1e-6


# ---------------------------------------------------------------------------
# ParsedHistoricalPrice tests
# ---------------------------------------------------------------------------


class TestParsedHistoricalPrice:
    def test_from_proto_basic(self) -> None:
        msg = MagicMock()
        msg.date = 19723
        msg.close = MagicMock()
        # Simulate _maybe_field returning close value
        with patch("app.models.parsed._maybe_field") as mf:
            mf.side_effect = lambda obj, field: {
                "close": 4567000000,
                "high": None,
                "low": None,
                "volume": None,
            }.get(field)
            price = ParsedHistoricalPrice.from_proto(msg)

        assert price.date == 19723
        assert price.close == 4567000000
        assert price.high is None

    def test_epoch_day_to_date_integration(self) -> None:
        price = ParsedHistoricalPrice(date=0, close=100_00000000)
        assert epoch_day_to_date(price.date) == date(1970, 1, 1)


# ---------------------------------------------------------------------------
# ParsedAccount tests
# ---------------------------------------------------------------------------


def _make_proto_account(
    uuid: str = "acc-1",
    name: str = "Main Account",
    currency: str = "EUR",
    note: str | None = None,
    is_retired: bool = False,
) -> MagicMock:
    obj = MagicMock()
    obj.uuid = uuid
    obj.name = name
    obj.currencyCode = currency
    obj.isRetired = is_retired
    obj.attributes = []
    # HasField for 'note' and 'updatedAt'
    obj.HasField.side_effect = ValueError  # treat all optional fields via ValueError path
    obj.note = note or ""
    obj.updatedAt = MagicMock(seconds=0, nanos=0)
    obj.updatedAt.ToDatetime.return_value = datetime(1970, 1, 1)
    return obj


class TestParsedAccount:
    def test_basic_parse(self) -> None:
        proto = _make_proto_account()
        account = ParsedAccount.from_proto(proto)
        assert account.uuid == "acc-1"
        assert account.name == "Main Account"
        assert account.currency_code == "EUR"
        assert account.is_retired is False

    def test_empty_currency_becomes_none(self) -> None:
        proto = _make_proto_account(currency="")
        account = ParsedAccount.from_proto(proto)
        assert account.currency_code is None


# ---------------------------------------------------------------------------
# ParsedSecurity tests
# ---------------------------------------------------------------------------


def _make_proto_security(uuid: str = "sec-1", name: str = "ACME Corp") -> MagicMock:
    obj = MagicMock()
    obj.uuid = uuid
    obj.name = name
    obj.isRetired = False
    obj.prices = []
    obj.events = []
    obj.attributes = []
    obj.properties = []
    # HasField raises for optional fields, but 'latest' is a message field
    obj.HasField.return_value = False
    obj.updatedAt = MagicMock(seconds=0, nanos=0)
    return obj


class TestParsedSecurity:
    def test_basic_parse(self) -> None:
        proto = _make_proto_security()
        with patch("app.models.parsed._maybe_field", return_value=None):
            with patch("app.models.parsed._parse_key_value_entries", return_value={}):
                security = ParsedSecurity.from_proto(proto)
        assert security.uuid == "sec-1"
        assert security.name == "ACME Corp"
        assert security.is_retired is False
        assert security.prices == []
        assert security.latest is None


# ---------------------------------------------------------------------------
# Validation helper tests
# ---------------------------------------------------------------------------


class TestEnsureUnique:
    def test_first_uuid_passes(self) -> None:
        seen: set[str] = set()
        _ensure_unique(seen, "abc-123", "account")
        assert "abc-123" in seen

    def test_empty_uuid_raises(self) -> None:
        seen: set[str] = set()
        with pytest.raises(PortfolioValidationError, match="missing uuid"):
            _ensure_unique(seen, "", "account")

    def test_duplicate_uuid_raises(self) -> None:
        seen: set[str] = {"abc-123"}
        with pytest.raises(PortfolioValidationError, match="Duplicate"):
            _ensure_unique(seen, "abc-123", "account")


class TestValidateSecurityType:
    def test_no_type_passes(self) -> None:
        security = MagicMock(spec=ParsedSecurity)
        security.properties = {}
        _validate_security_type(security)  # no exception

    def test_valid_type_passes(self) -> None:
        security = MagicMock(spec=ParsedSecurity)
        security.properties = {"type": "STOCK"}
        _validate_security_type(security)  # no exception

    def test_case_insensitive(self) -> None:
        security = MagicMock(spec=ParsedSecurity)
        security.properties = {"type": "stock"}
        _validate_security_type(security)  # no exception

    def test_invalid_type_raises(self) -> None:
        security = MagicMock(spec=ParsedSecurity)
        security.properties = {"type": "BANANA"}
        with pytest.raises(PortfolioValidationError, match="Unsupported security type"):
            _validate_security_type(security)

    def test_all_supported_types_pass(self) -> None:
        for sec_type in SUPPORTED_SECURITY_TYPES:
            security = MagicMock(spec=ParsedSecurity)
            security.properties = {"type": sec_type}
            _validate_security_type(security)  # no exception


class TestValidateTransactionUnits:
    def test_valid_unit_types_pass(self) -> None:
        transaction = MagicMock(spec=ParsedTransaction)
        transaction.units = [
            MagicMock(type=0),  # GROSS_VALUE
            MagicMock(type=1),  # TAX
            MagicMock(type=2),  # FEE
        ]
        _validate_transaction_units(transaction)  # no exception

    def test_invalid_unit_type_raises(self) -> None:
        transaction = MagicMock(spec=ParsedTransaction)
        transaction.units = [MagicMock(type=99)]
        with pytest.raises(PortfolioValidationError, match="Unsupported transaction unit type"):
            _validate_transaction_units(transaction)


# ---------------------------------------------------------------------------
# parse_portfolio_file tests (mocked I/O and proto decode)
# ---------------------------------------------------------------------------


class TestParsePortfolioFile:
    @pytest.mark.asyncio
    async def test_file_not_found_raises(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.portfolio"
        with pytest.raises(FileNotFoundError):
            await parse_portfolio_file(missing)

    @pytest.mark.asyncio
    async def test_invalid_proto_raises_parse_error(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "bad.portfolio"
        bad_file.write_bytes(b"\xff\xfe garbage")

        with pytest.raises(PortfolioParseError):
            await parse_portfolio_file(bad_file)

    @pytest.mark.asyncio
    async def test_progress_callback_called(self, tmp_path: Path) -> None:
        """Progress callback is invoked once per stage."""
        from app.generated import client_pb2

        # Build a minimal valid PClient
        proto_client = client_pb2.PClient()
        proto_client.version = 12
        proto_client.baseCurrency = "EUR"
        payload = proto_client.SerializeToString()

        portfolio_file = tmp_path / "test.portfolio"
        portfolio_file.write_bytes(payload)

        progress_events: list[ParseProgress] = []

        def cb(p: ParseProgress) -> None:
            progress_events.append(p)

        result = await parse_portfolio_file(portfolio_file, progress_cb=cb)
        assert result.version == 12
        assert result.base_currency == "EUR"
        # 4 stages: accounts, portfolios, securities, transactions
        assert len(progress_events) == 4
        stage_names = [p.stage for p in progress_events]
        assert "accounts" in stage_names
        assert "securities" in stage_names

    @pytest.mark.asyncio
    async def test_duplicate_account_uuid_raises(self, tmp_path: Path) -> None:
        """Duplicate UUIDs in accounts should raise PortfolioValidationError."""
        from app.generated import client_pb2

        proto_client = client_pb2.PClient()
        proto_client.version = 12
        proto_client.baseCurrency = "EUR"

        for name in ("Account A", "Account B"):
            acc = proto_client.accounts.add()
            acc.uuid = "same-uuid"
            acc.name = name
            acc.currencyCode = "EUR"

        payload = proto_client.SerializeToString()
        portfolio_file = tmp_path / "dup.portfolio"
        portfolio_file.write_bytes(payload)

        with pytest.raises(PortfolioValidationError, match="Duplicate"):
            await parse_portfolio_file(portfolio_file)

    @pytest.mark.asyncio
    async def test_full_round_trip(self, tmp_path: Path) -> None:
        """Serialize a complete PClient and verify all fields round-trip."""
        from app.generated import client_pb2
        from google.protobuf import timestamp_pb2

        proto_client = client_pb2.PClient()
        proto_client.version = 12
        proto_client.baseCurrency = "EUR"

        # Account
        acc = proto_client.accounts.add()
        acc.uuid = "acc-uuid-1"
        acc.name = "Giro EUR"
        acc.currencyCode = "EUR"

        # Portfolio
        port = proto_client.portfolios.add()
        port.uuid = "port-uuid-1"
        port.name = "Depot"

        # Security with historical price
        sec = proto_client.securities.add()
        sec.uuid = "sec-uuid-1"
        sec.name = "Apple Inc"
        sec.currencyCode = "USD"
        hp = sec.prices.add()
        hp.date = 19723  # 2024-01-01
        hp.close = 18500000000  # $185.00 at 10^8 scale

        # Transaction (BUY = PURCHASE = 0)
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC))
        txn = proto_client.transactions.add()
        txn.uuid = "txn-uuid-1"
        txn.type = 0  # PURCHASE
        txn.currencyCode = "EUR"
        txn.amount = 18500
        txn.date.CopyFrom(ts)
        txn.updatedAt.CopyFrom(ts)

        payload = proto_client.SerializeToString()
        portfolio_file = tmp_path / "full.portfolio"
        portfolio_file.write_bytes(payload)

        result = await parse_portfolio_file(portfolio_file)

        assert result.version == 12
        assert result.base_currency == "EUR"
        assert len(result.accounts) == 1
        assert result.accounts[0].uuid == "acc-uuid-1"
        assert result.accounts[0].name == "Giro EUR"
        assert len(result.portfolios) == 1
        assert result.portfolios[0].uuid == "port-uuid-1"
        assert len(result.securities) == 1
        sec_out = result.securities[0]
        assert sec_out.uuid == "sec-uuid-1"
        assert sec_out.currency_code == "USD"
        assert len(sec_out.prices) == 1
        assert sec_out.prices[0].date == 19723
        assert sec_out.prices[0].close == 18500000000
        assert len(result.transactions) == 1
        txn_out = result.transactions[0]
        assert txn_out.uuid == "txn-uuid-1"
        assert txn_out.type == TransactionType.BUY  # 0
        assert txn_out.amount == 18500


# ---------------------------------------------------------------------------
# Supported security types completeness
# ---------------------------------------------------------------------------


class TestSupportedSecurityTypes:
    def test_count(self) -> None:
        assert len(SUPPORTED_SECURITY_TYPES) == 13

    def test_contains_expected(self) -> None:
        for expected in ("STOCK", "FUND", "ETF", "BOND", "CRYPTO", "OTHER"):
            assert expected in SUPPORTED_SECURITY_TYPES
