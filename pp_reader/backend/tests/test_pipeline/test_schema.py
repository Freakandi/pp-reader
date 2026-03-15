"""Schema validation tests — verify all expected tables and key columns exist in PostgreSQL.

These tests require a live PostgreSQL database. They are skipped automatically when
DATABASE_URL is not reachable (e.g. in pure unit-test environments without Docker).

Run with:
    docker compose -f docker/docker-compose.yml up -d db
    cd backend && python -m pytest tests/test_pipeline/test_schema.py -v
"""

import os

import asyncpg
import pytest

# ── Expected tables (grouped for readability) ──────────────────────────────

CANONICAL_TABLES = {
    "accounts",
    "account_attributes",
    "securities",
    "historical_prices",
    "portfolios",
    "portfolio_attributes",
    "portfolio_securities",
    "transactions",
    "transaction_units",
}

PLAN_TABLES = {
    "plans",
    "plan_attributes",
    "plan_transactions",
}

WATCHLIST_TABLES = {
    "watchlists",
    "watchlist_securities",
}

TAXONOMY_TABLES = {
    "taxonomies",
    "taxonomy_dimensions",
    "taxonomy_classifications",
    "taxonomy_assignments",
}

DASHBOARD_TABLES = {
    "dashboards",
    "dashboard_configuration",
    "dashboard_columns",
    "dashboard_widgets",
    "widget_configuration",
}

SETTINGS_TABLES = {
    "settings_bookmarks",
    "settings_attribute_types",
    "settings_configuration_sets",
    "client_properties",
}

REFERENCE_TABLES = {
    "exchange_rate_series",
    "exchange_rates",
    "fx_rates",
    "price_history_queue",
    "metadata",
}

STAGING_TABLES = {
    "stg_metadata",
    "stg_accounts",
    "stg_portfolios",
    "stg_securities",
    "stg_transactions",
    "stg_transaction_units",
    "stg_historical_prices",
}

METRIC_TABLES = {
    "metric_runs",
    "portfolio_metrics",
    "account_metrics",
    "security_metrics",
}

SNAPSHOT_TABLES = {
    "portfolio_snapshots",
    "account_snapshots",
    "daily_wealth",
}

ALL_EXPECTED_TABLES = (
    CANONICAL_TABLES
    | PLAN_TABLES
    | WATCHLIST_TABLES
    | TAXONOMY_TABLES
    | DASHBOARD_TABLES
    | SETTINGS_TABLES
    | REFERENCE_TABLES
    | STAGING_TABLES
    | METRIC_TABLES
    | SNAPSHOT_TABLES
)

# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def db_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql://pp_reader:pp_reader@localhost:5432/pp_reader",
    )


@pytest.fixture(scope="module")
async def conn(db_url: str) -> asyncpg.Connection:
    """Open a direct asyncpg connection for schema introspection."""
    try:
        connection = await asyncpg.connect(dsn=db_url)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    yield connection
    await connection.close()


# ── Helpers ─────────────────────────────────────────────────────────────────


async def _get_tables(conn: asyncpg.Connection) -> set[str]:
    rows = await conn.fetch(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
    )
    return {row["table_name"] for row in rows}


async def _get_columns(conn: asyncpg.Connection, table: str) -> dict[str, str]:
    """Return {column_name: data_type} for a given table."""
    rows = await conn.fetch(
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = $1",
        table,
    )
    return {row["column_name"]: row["data_type"] for row in rows}


# ── Tests ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_all_expected_tables_exist(conn: asyncpg.Connection) -> None:
    """All tables from the initial migration must be present."""
    actual = await _get_tables(conn)
    missing = ALL_EXPECTED_TABLES - actual
    assert not missing, f"Missing tables after migration: {sorted(missing)}"


@pytest.mark.asyncio
async def test_no_legacy_ingestion_tables(conn: asyncpg.Connection) -> None:
    """Legacy ingestion_* tables must NOT exist (renamed to stg_*)."""
    actual = await _get_tables(conn)
    legacy = {t for t in actual if t.startswith("ingestion_")}
    assert not legacy, f"Legacy ingestion_* tables found (should be stg_*): {legacy}"


@pytest.mark.asyncio
async def test_table_count(conn: asyncpg.Connection) -> None:
    """Total public table count must match the expected schema size."""
    actual = await _get_tables(conn)
    assert len(actual) >= len(ALL_EXPECTED_TABLES), (
        f"Expected at least {len(ALL_EXPECTED_TABLES)} tables, found {len(actual)}"
    )


@pytest.mark.asyncio
async def test_accounts_columns(conn: asyncpg.Connection) -> None:
    cols = await _get_columns(conn, "accounts")
    assert "uuid" in cols
    assert "name" in cols
    assert "currency_code" in cols
    assert "balance" in cols
    assert cols["balance"] == "bigint", f"accounts.balance should be bigint, got {cols['balance']}"
    assert cols.get("updated_at") in ("timestamp with time zone",), (
        f"accounts.updated_at should be timestamptz, got {cols.get('updated_at')}"
    )


@pytest.mark.asyncio
async def test_securities_columns(conn: asyncpg.Connection) -> None:
    cols = await _get_columns(conn, "securities")
    assert "uuid" in cols
    assert "last_price" in cols
    assert cols["last_price"] == "bigint"
    assert cols.get("last_price_date") == "date", (
        f"securities.last_price_date should be date, got {cols.get('last_price_date')}"
    )
    assert cols.get("last_price_fetched_at") == "timestamp with time zone"


@pytest.mark.asyncio
async def test_historical_prices_columns(conn: asyncpg.Connection) -> None:
    cols = await _get_columns(conn, "historical_prices")
    assert cols.get("date") == "date", (
        f"historical_prices.date should be date (not integer epoch), got {cols.get('date')}"
    )
    assert cols["close"] == "bigint"
    assert cols.get("provenance") == "jsonb"


@pytest.mark.asyncio
async def test_transactions_columns(conn: asyncpg.Connection) -> None:
    cols = await _get_columns(conn, "transactions")
    assert cols.get("date") == "date"
    assert cols["amount"] == "bigint"
    # fx_rate_used must be BIGINT (10^8 scaled) per Architecture Decision 3
    assert cols.get("fx_rate_used") == "bigint", (
        f"transactions.fx_rate_used should be bigint (10^8 scaled), got {cols.get('fx_rate_used')}"
    )


@pytest.mark.asyncio
async def test_portfolio_securities_no_generated_column(conn: asyncpg.Connection) -> None:
    """avg_price generated column must NOT exist — computed at query time instead."""
    cols = await _get_columns(conn, "portfolio_securities")
    assert "avg_price" not in cols, (
        "portfolio_securities.avg_price generated column should not exist"
    )
    assert "current_holdings" in cols
    assert "purchase_value" in cols


@pytest.mark.asyncio
async def test_fx_rates_columns(conn: asyncpg.Connection) -> None:
    cols = await _get_columns(conn, "fx_rates")
    assert cols.get("date") == "date"
    assert cols["rate"] == "bigint", (
        f"fx_rates.rate should be bigint (10^8 scaled), got {cols['rate']}"
    )
    assert cols.get("provenance") == "jsonb"


@pytest.mark.asyncio
async def test_price_history_queue_identity_pk(conn: asyncpg.Connection) -> None:
    """price_history_queue.id must be a generated identity column."""
    row = await conn.fetchrow(
        "SELECT is_identity FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'price_history_queue' "
        "AND column_name = 'id'"
    )
    assert row is not None
    assert row["is_identity"] == "YES", "price_history_queue.id should be GENERATED ALWAYS AS IDENTITY"


@pytest.mark.asyncio
async def test_staging_tables_stg_prefix(conn: asyncpg.Connection) -> None:
    """All staging tables must use the stg_* prefix."""
    actual = await _get_tables(conn)
    for table in STAGING_TABLES:
        assert table in actual, f"Staging table {table!r} not found"


@pytest.mark.asyncio
async def test_stg_transactions_date_is_date_type(conn: asyncpg.Connection) -> None:
    cols = await _get_columns(conn, "stg_transactions")
    assert cols.get("date") == "date", (
        f"stg_transactions.date should be date, got {cols.get('date')}"
    )


@pytest.mark.asyncio
async def test_metric_runs_columns(conn: asyncpg.Connection) -> None:
    cols = await _get_columns(conn, "metric_runs")
    assert "run_uuid" in cols
    assert cols.get("started_at") == "timestamp with time zone"
    assert cols.get("provenance") == "jsonb"
