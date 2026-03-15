"""SQL statements for the canonical sync pipeline stage.

Syncs data from staging (stg_*) tables to canonical tables.

Accounts, portfolios, securities: upsert (ON CONFLICT DO UPDATE).
Transactions: delete-replace (CASCADE removes transaction_units).
Transaction units: inserted after transactions (no explicit delete needed).
Historical prices: upsert (preserves Phase 5+ enriched data).
Portfolio securities: delete-replace.
"""

# ---------------------------------------------------------------------------
# Load staging data
# ---------------------------------------------------------------------------

LOAD_STG_ACCOUNTS = """
    SELECT uuid, name, currency_code, note, is_retired, updated_at
    FROM stg_accounts
    ORDER BY name
"""

LOAD_STG_PORTFOLIOS = """
    SELECT uuid, name, note, reference_account, is_retired, updated_at
    FROM stg_portfolios
"""

LOAD_STG_SECURITIES = """
    SELECT uuid, name, isin, wkn, ticker_symbol, feed,
           currency_code, is_retired, updated_at,
           latest_close, latest_date, latest_feed
    FROM stg_securities
"""

LOAD_STG_TRANSACTIONS = """
    SELECT uuid, type, account, portfolio, other_account, other_portfolio,
           other_uuid, other_updated_at, date, currency_code, amount,
           amount_eur_cents, fx_rate_used, shares, note, security, source, updated_at
    FROM stg_transactions
    ORDER BY date
"""

LOAD_STG_TRANSACTION_UNITS = """
    SELECT transaction_uuid, unit_index, type, amount, amount_eur_cents,
           fx_rate_used, currency_code, fx_amount, fx_currency_code, fx_rate_to_base
    FROM stg_transaction_units
"""

LOAD_STG_HISTORICAL_PRICES = """
    SELECT security_uuid, date, close, high, low, volume
    FROM stg_historical_prices
"""

# ---------------------------------------------------------------------------
# Transfer protocol
# ---------------------------------------------------------------------------

# Fill amount_eur_cents for EUR transactions (amount is already in EUR cents).
FILL_EUR_AMOUNT_EUR_CENTS = """
    UPDATE stg_transactions
    SET amount_eur_cents = amount
    WHERE currency_code IN ('EUR', '')
      AND amount_eur_cents IS NULL
      AND amount IS NOT NULL
"""

# Select paired transfers where both legs have amount_eur_cents set.
# Processes from the outbound leg (amount < 0).
# Types 4 (SECURITY_TRANSFER) and 5 (CASH_TRANSFER) can both be paired.
SELECT_TRANSFER_PAIRS = """
    SELECT
        t1.uuid             AS uuid1,
        t1.currency_code    AS currency1,
        t1.amount_eur_cents AS eur1,
        t1.amount           AS amount1,
        t2.uuid             AS uuid2,
        t2.currency_code    AS currency2,
        t2.amount_eur_cents AS eur2,
        t2.amount           AS amount2
    FROM stg_transactions t1
    JOIN stg_transactions t2
      ON t1.other_uuid = t2.uuid AND t1.uuid = t2.other_uuid
    WHERE t1.type IN (4, 5)
      AND t2.type IN (4, 5)
      AND t1.amount < 0
      AND t1.amount_eur_cents IS NOT NULL
      AND t2.amount_eur_cents IS NOT NULL
"""

# Update a single transfer leg's EUR amount and FX rate.
UPDATE_TRANSFER_LEG = """
    UPDATE stg_transactions
    SET amount_eur_cents = $1,
        fx_rate_used     = $2
    WHERE uuid = $3
"""

# ---------------------------------------------------------------------------
# Canonical table upserts
# ---------------------------------------------------------------------------

UPSERT_ACCOUNT = """
    INSERT INTO accounts (uuid, name, currency_code, note, is_retired, updated_at, balance)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (uuid) DO UPDATE SET
        name          = EXCLUDED.name,
        currency_code = EXCLUDED.currency_code,
        note          = EXCLUDED.note,
        is_retired    = EXCLUDED.is_retired,
        updated_at    = EXCLUDED.updated_at,
        balance       = EXCLUDED.balance
"""

UPSERT_PORTFOLIO = """
    INSERT INTO portfolios (uuid, name, note, reference_account, is_retired, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (uuid) DO UPDATE SET
        name              = EXCLUDED.name,
        note              = EXCLUDED.note,
        reference_account = EXCLUDED.reference_account,
        is_retired        = EXCLUDED.is_retired,
        updated_at        = EXCLUDED.updated_at
"""

UPSERT_SECURITY = """
    INSERT INTO securities (
        uuid, name, isin, wkn, ticker_symbol, feed, type, currency_code,
        retired, updated_at, last_price, last_price_date, last_price_source,
        last_price_fetched_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT (uuid) DO UPDATE SET
        name                  = EXCLUDED.name,
        isin                  = EXCLUDED.isin,
        wkn                   = EXCLUDED.wkn,
        ticker_symbol         = EXCLUDED.ticker_symbol,
        feed                  = EXCLUDED.feed,
        currency_code         = EXCLUDED.currency_code,
        retired               = EXCLUDED.retired,
        updated_at            = EXCLUDED.updated_at,
        last_price            = EXCLUDED.last_price,
        last_price_date       = EXCLUDED.last_price_date,
        last_price_source     = EXCLUDED.last_price_source,
        last_price_fetched_at = EXCLUDED.last_price_fetched_at
"""

# Transactions: delete-replace. Deleting transactions CASCADE-deletes transaction_units.
DELETE_TRANSACTIONS = "DELETE FROM transactions"

INSERT_TRANSACTION = """
    INSERT INTO transactions (
        uuid, type, account, portfolio, other_account, other_portfolio,
        other_uuid, other_updated_at, date, currency_code, amount,
        amount_eur_cents, fx_rate_used, shares, note, security, source, updated_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
        $14, $15, $16, $17, $18
    )
"""

INSERT_TRANSACTION_UNIT = """
    INSERT INTO transaction_units (
        transaction_uuid, type, amount, amount_eur_cents, fx_rate_used,
        currency_code, fx_amount, fx_currency_code, fx_rate_to_base
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
"""

# Historical prices: upsert to preserve Phase 5+ enrichment.
UPSERT_HISTORICAL_PRICE = """
    INSERT INTO historical_prices (
        security_uuid, date, close, high, low, volume,
        fetched_at, data_source, provider, provenance
    ) VALUES ($1, $2, $3, $4, $5, $6, NULL, 'portfolio', 'portfolio', NULL)
    ON CONFLICT (security_uuid, date) DO UPDATE SET
        close       = EXCLUDED.close,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        volume      = EXCLUDED.volume,
        data_source = EXCLUDED.data_source,
        provider    = EXCLUDED.provider
"""

# Portfolio securities: delete-replace.
DELETE_PORTFOLIO_SECURITIES = "DELETE FROM portfolio_securities"

INSERT_PORTFOLIO_SECURITY = """
    INSERT INTO portfolio_securities (
        portfolio_uuid, security_uuid, current_holdings, purchase_value,
        avg_price_native, avg_price_security, avg_price_account,
        security_currency_total, account_currency_total, current_value
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""
