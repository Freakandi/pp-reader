"""Parameterized SQL statements for writing to staging tables during ingestion.

All INSERT statements use positional parameters ($1, $2, ...) compatible with asyncpg.
Staging tables are cleared in child-first order before each ingestion run.
"""

# ---------------------------------------------------------------------------
# Clear order: child tables first to satisfy foreign key constraints.
# ---------------------------------------------------------------------------
CLEAR_STAGING_TABLES: list[str] = [
    "stg_historical_prices",
    "stg_transaction_units",
    "stg_transactions",
    "stg_securities",
    "stg_portfolios",
    "stg_accounts",
    "stg_metadata",
]

# ---------------------------------------------------------------------------
# stg_metadata
# ---------------------------------------------------------------------------
INSERT_METADATA = """
    INSERT INTO stg_metadata (
        run_id, file_path, parsed_at, pp_version, base_currency, properties
    ) VALUES ($1, $2, $3, $4, $5, $6)
"""

# ---------------------------------------------------------------------------
# stg_accounts
# ---------------------------------------------------------------------------
INSERT_ACCOUNT = """
    INSERT INTO stg_accounts (
        uuid, name, currency_code, note, is_retired, attributes, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

# ---------------------------------------------------------------------------
# stg_portfolios
# ---------------------------------------------------------------------------
INSERT_PORTFOLIO = """
    INSERT INTO stg_portfolios (
        uuid, name, note, reference_account, is_retired, attributes, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

# ---------------------------------------------------------------------------
# stg_securities
# ---------------------------------------------------------------------------
INSERT_SECURITY = """
    INSERT INTO stg_securities (
        uuid, name, currency_code, target_currency_code, isin, ticker_symbol,
        wkn, note, online_id, feed, feed_url, latest_feed, latest_feed_url,
        latest_date, latest_close, latest_high, latest_low, latest_volume,
        is_retired, attributes, properties, updated_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
        $14, $15, $16, $17, $18, $19, $20, $21, $22
    )
"""

# ---------------------------------------------------------------------------
# stg_transactions
# Columns amount_eur_cents and fx_rate_used are left NULL at ingestion;
# they are populated during Phase 5 (FX enrichment).
# ---------------------------------------------------------------------------
INSERT_TRANSACTION = """
    INSERT INTO stg_transactions (
        uuid, type, account, portfolio, other_account, other_portfolio,
        other_uuid, other_updated_at, date, currency_code, amount,
        amount_eur_cents, fx_rate_used, shares, note, security, source, updated_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
    )
"""

# ---------------------------------------------------------------------------
# stg_transaction_units
# Columns amount_eur_cents and fx_rate_used are left NULL at ingestion;
# they are populated during Phase 5 (FX enrichment).
# fx_rate_to_base is stored as BIGINT scaled at 10^8 (EIGHT_DECIMAL_SCALE).
# ---------------------------------------------------------------------------
INSERT_TRANSACTION_UNIT = """
    INSERT INTO stg_transaction_units (
        transaction_uuid, unit_index, type, amount, amount_eur_cents,
        fx_rate_used, currency_code, fx_amount, fx_currency_code, fx_rate_to_base
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""

# ---------------------------------------------------------------------------
# stg_historical_prices — bulk-inserted via asyncpg.copy_records_to_table().
# Only ingestion-time columns are populated; enrichment columns are NULL.
# ---------------------------------------------------------------------------
HISTORICAL_PRICES_COLUMNS: list[str] = [
    "security_uuid",
    "date",
    "close",
    "high",
    "low",
    "volume",
]
