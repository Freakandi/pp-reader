"""Tests for the ingestion pipeline.

Uses mocked asyncpg connections — no database required.
Verifies:
  - Row counts for each staging table
  - Correct numeric values / type conversions
  - Metadata blob serialization
  - Transaction atomicity (rollback on error)
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from app.models.constants import EIGHT_DECIMAL_SCALE
from app.models.domain import epoch_day_to_date
from app.models.parsed import (
    ParsedAccount,
    ParsedAttributeType,
    ParsedBookmark,
    ParsedClient,
    ParsedConfigurationSet,
    ParsedDashboard,
    ParsedDashboardColumn,
    ParsedDashboardWidget,
    ParsedHistoricalPrice,
    ParsedInvestmentPlan,
    ParsedPortfolio,
    ParsedSecurity,
    ParsedSettings,
    ParsedTaxonomy,
    ParsedTaxonomyAssignment,
    ParsedTaxonomyClassification,
    ParsedTransaction,
    ParsedTransactionUnit,
    ParsedWatchlist,
)
from app.pipeline.ingestion import _build_metadata_blob, _json_or_none, ingest


# ---------------------------------------------------------------------------
# Helpers — mock connection factory
# ---------------------------------------------------------------------------


def make_conn() -> MagicMock:
    """Return a mock asyncpg connection with async execute/executemany/copy."""
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.executemany = AsyncMock()
    conn.copy_records_to_table = AsyncMock()

    # Minimal async context manager for conn.transaction()
    txn = MagicMock()
    txn.__aenter__ = AsyncMock(return_value=None)
    txn.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=txn)

    return conn


# ---------------------------------------------------------------------------
# Helpers — minimal ParsedClient builders
# ---------------------------------------------------------------------------


def make_account(
    uuid: str = "acc-1",
    name: str = "Test Account",
    currency_code: str = "EUR",
) -> ParsedAccount:
    return ParsedAccount(
        uuid=uuid,
        name=name,
        currency_code=currency_code,
        note=None,
        is_retired=False,
        attributes={},
        updated_at=None,
    )


def make_portfolio(
    uuid: str = "pf-1",
    name: str = "Test Portfolio",
) -> ParsedPortfolio:
    return ParsedPortfolio(
        uuid=uuid,
        name=name,
        note=None,
        is_retired=False,
        reference_account="acc-1",
        attributes={},
        updated_at=None,
    )


def make_security(
    uuid: str = "sec-1",
    name: str = "ACME Corp",
    prices: list[ParsedHistoricalPrice] | None = None,
    latest: ParsedHistoricalPrice | None = None,
) -> ParsedSecurity:
    return ParsedSecurity(
        uuid=uuid,
        name=name,
        currency_code="EUR",
        target_currency_code=None,
        isin="DE000000001",
        ticker_symbol="ACME",
        wkn=None,
        note=None,
        online_id=None,
        feed=None,
        feed_url=None,
        latest_feed=None,
        latest_feed_url=None,
        calendar=None,
        is_retired=False,
        attributes={},
        properties={},
        prices=prices or [],
        latest=latest,
        events=[],
        updated_at=None,
    )


def make_transaction(
    uuid: str = "txn-1",
    units: list[ParsedTransactionUnit] | None = None,
) -> ParsedTransaction:
    return ParsedTransaction(
        uuid=uuid,
        type=0,  # BUY
        account="acc-1",
        portfolio="pf-1",
        other_account=None,
        other_portfolio=None,
        other_uuid=None,
        other_updated_at=None,
        date=datetime(2022, 1, 15, 0, 0, 0, tzinfo=UTC),
        currency_code="EUR",
        amount=100_000,  # 1000.00 EUR in cents
        shares=500_000_000,  # 5 shares (10^8 scaled)
        note=None,
        security="sec-1",
        source=None,
        updated_at=None,
        units=units or [],
    )


def make_parsed_client(**kwargs) -> ParsedClient:
    defaults: dict = dict(
        version=1,
        base_currency="EUR",
        accounts=[],
        portfolios=[],
        securities=[],
        transactions=[],
        plans=[],
        watchlists=[],
        taxonomies=[],
        dashboards=[],
        settings=None,
        properties={},
    )
    defaults.update(kwargs)
    return ParsedClient(**defaults)


# ---------------------------------------------------------------------------
# Tests — _json_or_none
# ---------------------------------------------------------------------------


class TestJsonOrNone:
    def test_returns_none_for_empty_dict(self) -> None:
        assert _json_or_none({}) is None

    def test_returns_none_for_none(self) -> None:
        assert _json_or_none(None) is None

    def test_serializes_dict(self) -> None:
        result = _json_or_none({"key": "value"})
        assert result is not None
        assert json.loads(result) == {"key": "value"}

    def test_sorts_keys(self) -> None:
        result = _json_or_none({"z": 1, "a": 2})
        assert result is not None
        assert result.startswith('{"a"')


# ---------------------------------------------------------------------------
# Tests — _build_metadata_blob
# ---------------------------------------------------------------------------


class TestBuildMetadataBlob:
    def test_returns_none_for_empty_client(self) -> None:
        parsed = make_parsed_client()
        assert _build_metadata_blob(parsed, {}) is None

    def test_properties_included(self) -> None:
        parsed = make_parsed_client()
        result = _build_metadata_blob(parsed, {"version": "12"})
        assert result is not None
        assert result["properties"] == {"version": "12"}

    def test_watchlists_included(self) -> None:
        wl = ParsedWatchlist(name="MyWatchlist", securities=["sec-1", "sec-2"])
        parsed = make_parsed_client(watchlists=[wl])
        result = _build_metadata_blob(parsed, {})
        assert result is not None
        assert result["watchlists"] == [
            {"name": "MyWatchlist", "securities": ["sec-1", "sec-2"]}
        ]

    def test_watchlist_without_name_excluded(self) -> None:
        wl = ParsedWatchlist(name="", securities=["sec-1"])
        parsed = make_parsed_client(watchlists=[wl])
        assert _build_metadata_blob(parsed, {}) is None

    def test_settings_bookmarks_included(self) -> None:
        settings = ParsedSettings(
            bookmarks=[ParsedBookmark(label="Home", pattern="*")],
        )
        parsed = make_parsed_client(settings=settings)
        result = _build_metadata_blob(parsed, {})
        assert result is not None
        assert result["settings"]["bookmarks"] == [
            {"label": "Home", "pattern": "*"}
        ]

    def test_settings_empty_returns_none_blob(self) -> None:
        settings = ParsedSettings()
        parsed = make_parsed_client(settings=settings)
        assert _build_metadata_blob(parsed, {}) is None

    def test_taxonomy_included(self) -> None:
        assignment = ParsedTaxonomyAssignment(
            investment_vehicle="sec-1",
            weight=100,
            rank=0,
            data={},
        )
        cls = ParsedTaxonomyClassification(
            id="cls-1",
            name="Equities",
            parent_id=None,
            note=None,
            color=None,
            weight=None,
            rank=None,
            data={},
            assignments=[assignment],
        )
        taxonomy = ParsedTaxonomy(
            id="tax-1",
            name="Asset Class",
            source=None,
            dimensions=[],
            classifications=[cls],
        )
        parsed = make_parsed_client(taxonomies=[taxonomy])
        result = _build_metadata_blob(parsed, {})
        assert result is not None
        assert len(result["taxonomies"]) == 1
        assert result["taxonomies"][0]["id"] == "tax-1"

    def test_plan_included(self) -> None:
        plan = ParsedInvestmentPlan(
            name="Monthly Savings",
            note=None,
            security="sec-1",
            portfolio="pf-1",
            account=None,
            attributes={},
            auto_generate=False,
            amount=50000,
        )
        parsed = make_parsed_client(plans=[plan])
        result = _build_metadata_blob(parsed, {})
        assert result is not None
        assert result["plans"][0]["name"] == "Monthly Savings"
        assert result["plans"][0]["amount"] == 50000

    def test_dashboard_included(self) -> None:
        widget = ParsedDashboardWidget(type="CHART", label="My Chart", configuration={})
        col = ParsedDashboardColumn(weight=1, widgets=[widget])
        db = ParsedDashboard(
            name="Main",
            configuration={},
            columns=[col],
            dashboard_id="db-1",
        )
        parsed = make_parsed_client(dashboards=[db])
        result = _build_metadata_blob(parsed, {})
        assert result is not None
        assert result["dashboards"][0]["name"] == "Main"
        assert result["dashboards"][0]["columns"][0]["widgets"][0]["type"] == "CHART"


# ---------------------------------------------------------------------------
# Tests — ingest (empty client)
# ---------------------------------------------------------------------------


class TestIngestEmptyClient:
    async def test_returns_run_id_hex(self) -> None:
        conn = make_conn()
        parsed = make_parsed_client()
        run_id = await ingest(conn, parsed)
        assert isinstance(run_id, str)
        assert len(run_id) == 32

    async def test_clears_all_staging_tables(self) -> None:
        conn = make_conn()
        parsed = make_parsed_client()
        await ingest(conn, parsed)

        delete_calls = [
            c for c in conn.execute.call_args_list if "DELETE FROM" in str(c)
        ]
        # 7 staging tables must be cleared
        assert len(delete_calls) == 7

    async def test_metadata_row_inserted(self) -> None:
        conn = make_conn()
        parsed = make_parsed_client(version=42, base_currency="USD")
        await ingest(conn, parsed, file_path="/data/test.portfolio")

        # The metadata INSERT is done via conn.execute (not executemany)
        insert_calls = [
            c for c in conn.execute.call_args_list
            if "INSERT INTO stg_metadata" in str(c)
        ]
        assert len(insert_calls) == 1
        args = insert_calls[0].args
        # $3=run_id, $4=file_path, $5=parsed_at, $6=pp_version, $7=base_currency
        assert args[2] == "/data/test.portfolio"  # file_path
        assert args[4] == 42                       # pp_version
        assert args[5] == "USD"                    # base_currency

    async def test_no_executemany_for_empty_collections(self) -> None:
        conn = make_conn()
        parsed = make_parsed_client()
        await ingest(conn, parsed)
        conn.executemany.assert_not_called()

    async def test_no_copy_for_empty_prices(self) -> None:
        conn = make_conn()
        parsed = make_parsed_client()
        await ingest(conn, parsed)
        conn.copy_records_to_table.assert_not_called()

    async def test_uses_transaction(self) -> None:
        conn = make_conn()
        parsed = make_parsed_client()
        await ingest(conn, parsed)
        conn.transaction.assert_called_once()


# ---------------------------------------------------------------------------
# Tests — ingest with accounts and portfolios
# ---------------------------------------------------------------------------


class TestIngestAccountsAndPortfolios:
    async def test_account_row_count(self) -> None:
        conn = make_conn()
        accounts = [make_account("a1"), make_account("a2"), make_account("a3")]
        parsed = make_parsed_client(accounts=accounts)
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_accounts" in str(c)]
        assert len(calls) == 1
        rows = calls[0].args[1]
        assert len(rows) == 3

    async def test_account_fields(self) -> None:
        conn = make_conn()
        updated = datetime(2023, 6, 1, 12, 0, 0, tzinfo=UTC)
        acc = ParsedAccount(
            uuid="acc-x",
            name="Broker Account",
            currency_code="USD",
            note="test note",
            is_retired=True,
            attributes={"key": "val"},
            updated_at=updated,
        )
        parsed = make_parsed_client(accounts=[acc])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_accounts" in str(c)]
        row = calls[0].args[1][0]
        assert row[0] == "acc-x"
        assert row[1] == "Broker Account"
        assert row[2] == "USD"
        assert row[3] == "test note"
        assert row[4] is True
        assert json.loads(row[5]) == {"key": "val"}
        assert row[6] == updated

    async def test_portfolio_row_count(self) -> None:
        conn = make_conn()
        portfolios = [make_portfolio("p1"), make_portfolio("p2")]
        parsed = make_parsed_client(portfolios=portfolios)
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_portfolios" in str(c)]
        assert len(calls) == 1
        assert len(calls[0].args[1]) == 2

    async def test_portfolio_fields(self) -> None:
        conn = make_conn()
        pf = ParsedPortfolio(
            uuid="pf-x",
            name="Growth Portfolio",
            note="long note",
            is_retired=False,
            reference_account="acc-1",
            attributes={},
            updated_at=None,
        )
        parsed = make_parsed_client(portfolios=[pf])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_portfolios" in str(c)]
        row = calls[0].args[1][0]
        assert row[0] == "pf-x"
        assert row[1] == "Growth Portfolio"
        assert row[2] == "long note"
        assert row[3] == "acc-1"
        assert row[4] is False


# ---------------------------------------------------------------------------
# Tests — ingest with securities
# ---------------------------------------------------------------------------


class TestIngestSecurities:
    async def test_security_row_count(self) -> None:
        conn = make_conn()
        securities = [make_security("s1"), make_security("s2")]
        parsed = make_parsed_client(securities=securities)
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_securities" in str(c)]
        assert len(calls) == 1
        assert len(calls[0].args[1]) == 2

    async def test_security_latest_date_converted(self) -> None:
        """Epoch day in latest price must be converted to datetime.date."""
        epoch_day = 19000  # 2022-01-11
        latest = ParsedHistoricalPrice(date=epoch_day, close=50_000_000_000)
        sec = make_security(latest=latest)
        conn = make_conn()
        parsed = make_parsed_client(securities=[sec])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_securities" in str(c)]
        row = calls[0].args[1][0]
        # latest_date is $14 (index 13)
        assert row[13] == epoch_day_to_date(epoch_day)
        assert isinstance(row[13], date)

    async def test_security_latest_close_preserved(self) -> None:
        latest = ParsedHistoricalPrice(date=19000, close=12_345_678_900)
        sec = make_security(latest=latest)
        conn = make_conn()
        parsed = make_parsed_client(securities=[sec])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_securities" in str(c)]
        row = calls[0].args[1][0]
        assert row[14] == 12_345_678_900  # latest_close

    async def test_historical_prices_bulk_inserted(self) -> None:
        prices = [
            ParsedHistoricalPrice(date=18000, close=100_000_000),
            ParsedHistoricalPrice(date=18001, close=101_000_000),
            ParsedHistoricalPrice(date=18002, close=102_000_000),
        ]
        sec = make_security(prices=prices)
        conn = make_conn()
        parsed = make_parsed_client(securities=[sec])
        await ingest(conn, parsed)

        conn.copy_records_to_table.assert_called_once()
        call_kwargs = conn.copy_records_to_table.call_args
        assert call_kwargs.args[0] == "stg_historical_prices"
        records = call_kwargs.kwargs["records"]
        assert len(records) == 3

    async def test_historical_price_date_converted(self) -> None:
        epoch_day = 18000
        price = ParsedHistoricalPrice(date=epoch_day, close=100_000_000)
        sec = make_security(prices=[price])
        conn = make_conn()
        parsed = make_parsed_client(securities=[sec])
        await ingest(conn, parsed)

        records = conn.copy_records_to_table.call_args.kwargs["records"]
        assert records[0][1] == epoch_day_to_date(epoch_day)
        assert isinstance(records[0][1], date)

    async def test_latest_only_security_falls_back_to_price(self) -> None:
        """Security with no prices[] but a latest should get one price row."""
        latest = ParsedHistoricalPrice(date=19000, close=5_000_000_000)
        sec = make_security(prices=[], latest=latest)
        conn = make_conn()
        parsed = make_parsed_client(securities=[sec])
        await ingest(conn, parsed)

        conn.copy_records_to_table.assert_called_once()
        records = conn.copy_records_to_table.call_args.kwargs["records"]
        assert len(records) == 1

    async def test_security_no_prices_no_latest_no_copy(self) -> None:
        sec = make_security(prices=[], latest=None)
        conn = make_conn()
        parsed = make_parsed_client(securities=[sec])
        await ingest(conn, parsed)
        conn.copy_records_to_table.assert_not_called()


# ---------------------------------------------------------------------------
# Tests — ingest with transactions
# ---------------------------------------------------------------------------


class TestIngestTransactions:
    async def test_transaction_row_count(self) -> None:
        conn = make_conn()
        txns = [make_transaction("t1"), make_transaction("t2")]
        parsed = make_parsed_client(transactions=txns)
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transactions" in str(c)]
        assert len(calls) == 1
        assert len(calls[0].args[1]) == 2

    async def test_transaction_date_converted_to_date(self) -> None:
        """datetime field must be stored as date (DATE column)."""
        txn = make_transaction()
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transactions" in str(c)]
        row = calls[0].args[1][0]
        # date is $9 (index 8)
        assert row[8] == date(2022, 1, 15)
        assert isinstance(row[8], date)

    async def test_transaction_amount_preserved(self) -> None:
        txn = make_transaction()
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transactions" in str(c)]
        row = calls[0].args[1][0]
        assert row[10] == 100_000  # amount

    async def test_transaction_shares_preserved(self) -> None:
        txn = make_transaction()
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transactions" in str(c)]
        row = calls[0].args[1][0]
        assert row[13] == 500_000_000  # shares

    async def test_amount_eur_cents_is_null(self) -> None:
        """FX enrichment is Phase 5; amount_eur_cents must be NULL."""
        txn = make_transaction()
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transactions" in str(c)]
        row = calls[0].args[1][0]
        assert row[11] is None  # amount_eur_cents
        assert row[12] is None  # fx_rate_used

    async def test_transaction_with_null_date(self) -> None:
        txn = ParsedTransaction(
            uuid="txn-nd",
            type=0,
            account=None,
            portfolio=None,
            other_account=None,
            other_portfolio=None,
            other_uuid=None,
            other_updated_at=None,
            date=None,
            currency_code=None,
            amount=None,
            shares=None,
            note=None,
            security=None,
            source=None,
            updated_at=None,
            units=[],
        )
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transactions" in str(c)]
        row = calls[0].args[1][0]
        assert row[8] is None  # date must be None (not crash)


# ---------------------------------------------------------------------------
# Tests — ingest with transaction units
# ---------------------------------------------------------------------------


class TestIngestTransactionUnits:
    async def test_unit_rows_inserted(self) -> None:
        unit = ParsedTransactionUnit(
            type=0,  # GROSS_VALUE
            amount=100_000,
            currency_code="EUR",
            fx_amount=None,
            fx_currency_code=None,
            fx_rate_to_base=None,
        )
        txn = make_transaction(units=[unit])
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transaction_units" in str(c)]
        assert len(calls) == 1
        assert len(calls[0].args[1]) == 1

    async def test_unit_index_assigned(self) -> None:
        units = [
            ParsedTransactionUnit(type=0, amount=100_000, currency_code="EUR"),
            ParsedTransactionUnit(type=1, amount=500, currency_code="EUR"),
        ]
        txn = make_transaction(units=units)
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transaction_units" in str(c)]
        rows = calls[0].args[1]
        assert rows[0][1] == 0  # unit_index
        assert rows[1][1] == 1

    async def test_fx_rate_to_base_scaled(self) -> None:
        """fx_rate_to_base float must be stored as BIGINT at 10^8 scale."""
        unit = ParsedTransactionUnit(
            type=0,
            amount=12_000,
            currency_code="USD",
            fx_rate_to_base=1.0842,
        )
        txn = make_transaction(units=[unit])
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transaction_units" in str(c)]
        row = calls[0].args[1][0]
        expected = round(1.0842 * EIGHT_DECIMAL_SCALE)
        assert row[9] == expected  # fx_rate_to_base

    async def test_fx_rate_to_base_none_stays_none(self) -> None:
        unit = ParsedTransactionUnit(
            type=0,
            amount=100_000,
            currency_code="EUR",
            fx_rate_to_base=None,
        )
        txn = make_transaction(units=[unit])
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transaction_units" in str(c)]
        row = calls[0].args[1][0]
        assert row[9] is None

    async def test_unit_amount_eur_cents_null(self) -> None:
        """FX enrichment is Phase 5; unit amount_eur_cents must be NULL."""
        unit = ParsedTransactionUnit(type=0, amount=1000, currency_code="USD")
        txn = make_transaction(units=[unit])
        conn = make_conn()
        parsed = make_parsed_client(transactions=[txn])
        await ingest(conn, parsed)

        calls = [c for c in conn.executemany.call_args_list if "stg_transaction_units" in str(c)]
        row = calls[0].args[1][0]
        assert row[4] is None  # amount_eur_cents
        assert row[5] is None  # fx_rate_used


# ---------------------------------------------------------------------------
# Tests — transaction atomicity
# ---------------------------------------------------------------------------


class TestIngestAtomicity:
    async def test_exception_propagates(self) -> None:
        conn = make_conn()
        conn.executemany.side_effect = RuntimeError("db error")
        accounts = [make_account()]
        parsed = make_parsed_client(accounts=accounts)

        with pytest.raises(RuntimeError, match="db error"):
            await ingest(conn, parsed)

    async def test_transaction_context_manager_exited_on_error(self) -> None:
        """__aexit__ must be called even when an error occurs."""
        conn = make_conn()
        conn.executemany.side_effect = RuntimeError("boom")
        parsed = make_parsed_client(accounts=[make_account()])

        try:
            await ingest(conn, parsed)
        except RuntimeError:
            pass

        # The transaction context manager must have been exited
        txn = conn.transaction.return_value
        txn.__aexit__.assert_called_once()

    async def test_run_id_format(self) -> None:
        """run_id must be a 32-char lowercase hex string."""
        conn = make_conn()
        parsed = make_parsed_client()
        run_id = await ingest(conn, parsed)
        assert len(run_id) == 32
        assert run_id == run_id.lower()
        int(run_id, 16)  # must be valid hex

    async def test_two_ingestions_produce_different_run_ids(self) -> None:
        conn = make_conn()
        parsed = make_parsed_client()
        id1 = await ingest(conn, parsed)
        id2 = await ingest(conn, parsed)
        assert id1 != id2


# ---------------------------------------------------------------------------
# Tests — full round-trip with all entity types
# ---------------------------------------------------------------------------


class TestIngestFullRoundTrip:
    async def test_all_tables_populated(self) -> None:
        """All 6 entity executemany calls + 1 copy + DELETE + metadata execute."""
        price = ParsedHistoricalPrice(date=18000, close=10_000_000_000)
        sec = make_security(prices=[price])
        unit = ParsedTransactionUnit(type=0, amount=100_000, currency_code="EUR")
        txn = make_transaction(units=[unit])

        parsed = make_parsed_client(
            accounts=[make_account()],
            portfolios=[make_portfolio()],
            securities=[sec],
            transactions=[txn],
        )
        conn = make_conn()
        await ingest(conn, parsed)

        # 4 executemany calls: accounts, portfolios, securities, transactions, units
        assert conn.executemany.call_count == 5
        # 1 copy_records_to_table for prices
        assert conn.copy_records_to_table.call_count == 1

    async def test_multiple_securities_prices_flattened(self) -> None:
        prices_a = [
            ParsedHistoricalPrice(date=18000, close=10_000_000),
            ParsedHistoricalPrice(date=18001, close=11_000_000),
        ]
        prices_b = [
            ParsedHistoricalPrice(date=18000, close=20_000_000),
        ]
        sec_a = make_security("s1", prices=prices_a)
        sec_b = make_security("s2", prices=prices_b)

        conn = make_conn()
        parsed = make_parsed_client(securities=[sec_a, sec_b])
        await ingest(conn, parsed)

        records = conn.copy_records_to_table.call_args.kwargs["records"]
        assert len(records) == 3  # 2 + 1

    async def test_price_security_uuid_matches(self) -> None:
        price = ParsedHistoricalPrice(date=18000, close=5_000_000)
        sec = make_security("my-sec", prices=[price])

        conn = make_conn()
        parsed = make_parsed_client(securities=[sec])
        await ingest(conn, parsed)

        records = conn.copy_records_to_table.call_args.kwargs["records"]
        assert records[0][0] == "my-sec"  # security_uuid
