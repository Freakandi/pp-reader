"""Initial schema — all PostgreSQL tables for PP Reader.

Revision ID: 0001
Revises:
Create Date: 2026-03-14

Translated from legacy SQLite DDL (_legacy_v1/custom_components/pp_reader/data/db_schema.py).

Key translation rules applied:
  - INTEGER PRIMARY KEY AUTOINCREMENT → INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  - TEXT ISO-8601 timestamps       → TIMESTAMPTZ
  - TEXT date-only columns          → DATE
  - INTEGER epoch-day columns       → DATE
  - REAL FX/exchange-rate columns   → BIGINT (10^8 scaled), per Architecture Decision 3
  - REAL percentage/ratio columns   → DOUBLE PRECISION
  - INTEGER monetary amounts        → BIGINT
  - GENERATED ALWAYS AS (expr)      → removed; compute avg_price at query time
  - strftime(…,'now')               → CURRENT_TIMESTAMP
  - ingestion_* staging tables      → stg_* prefix
  - provenance JSON TEXT columns    → JSONB
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _exec(sql: str) -> None:
    op.execute(sql)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Upgrade
# ---------------------------------------------------------------------------

def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Canonical — accounts
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE accounts (
            uuid            TEXT        PRIMARY KEY,
            name            TEXT        NOT NULL,
            currency_code   TEXT        NOT NULL,
            note            TEXT,
            is_retired      BOOLEAN,
            updated_at      TIMESTAMPTZ,
            balance         BIGINT      DEFAULT 0   -- account balance in minor currency units
        )
    """)

    _exec("""
        CREATE TABLE account_attributes (
            account_uuid    TEXT        NOT NULL REFERENCES accounts(uuid) ON DELETE CASCADE,
            key             TEXT        NOT NULL,
            value           TEXT
        )
    """)

    # ------------------------------------------------------------------
    # 2. Canonical — securities
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE securities (
            uuid                    TEXT        PRIMARY KEY,
            name                    TEXT        NOT NULL,
            isin                    TEXT,
            wkn                     TEXT,
            ticker_symbol           TEXT,
            feed                    TEXT,
            type                    TEXT,
            currency_code           TEXT,
            retired                 BOOLEAN,
            updated_at              TIMESTAMPTZ,
            last_price              BIGINT,         -- last price in 10^-8 units
            last_price_date         DATE,           -- date of last price
            last_price_source       TEXT,
            last_price_fetched_at   TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE TABLE historical_prices (
            security_uuid   TEXT        NOT NULL REFERENCES securities(uuid) ON DELETE CASCADE,
            date            DATE        NOT NULL,   -- trading date
            close           BIGINT      NOT NULL,   -- closing price in 10^-8 units
            high            BIGINT,                 -- daily high in 10^-8 units
            low             BIGINT,                 -- daily low in 10^-8 units
            volume          BIGINT,
            fetched_at      TIMESTAMPTZ,
            data_source     TEXT,
            provider        TEXT,
            provenance      JSONB,
            PRIMARY KEY (security_uuid, date)
        )
    """)

    _exec("""
        CREATE INDEX idx_historical_prices_security_date
        ON historical_prices (security_uuid, date)
    """)

    # ------------------------------------------------------------------
    # 3. Canonical — portfolios
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE portfolios (
            uuid                TEXT        PRIMARY KEY,
            name                TEXT        NOT NULL,
            note                TEXT,
            reference_account   TEXT        REFERENCES accounts(uuid),
            is_retired          BOOLEAN,
            updated_at          TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE TABLE portfolio_attributes (
            portfolio_uuid  TEXT        NOT NULL REFERENCES portfolios(uuid) ON DELETE CASCADE,
            key             TEXT        NOT NULL,
            value           TEXT
        )
    """)

    # avg_price removed — was a SQLite GENERATED ALWAYS AS column.
    # Compute at query time: ROUND((purchase_value / 100.0) * 1e8 / (current_holdings / 1e8))
    _exec("""
        CREATE TABLE portfolio_securities (
            portfolio_uuid          TEXT    NOT NULL REFERENCES portfolios(uuid) ON DELETE CASCADE,
            security_uuid           TEXT    NOT NULL REFERENCES securities(uuid) ON DELETE CASCADE,
            current_holdings        BIGINT  DEFAULT 0,  -- shares in 10^-8 units
            purchase_value          BIGINT  DEFAULT 0,  -- total purchase cost in minor currency units
            avg_price_native        BIGINT,             -- avg purchase price in native currency (10^-8)
            avg_price_security      BIGINT,             -- avg price in security currency (10^-8)
            avg_price_account       BIGINT,             -- avg price in account currency (10^-8)
            security_currency_total BIGINT  DEFAULT 0,  -- purchase value in security currency (10^-8)
            account_currency_total  BIGINT  DEFAULT 0,  -- purchase value in account currency (10^-8)
            current_value           BIGINT  DEFAULT 0,  -- current market value (10^-8)
            PRIMARY KEY (portfolio_uuid, security_uuid)
        )
    """)

    _exec("""
        CREATE INDEX idx_portfolio_securities_portfolio
        ON portfolio_securities (portfolio_uuid)
    """)

    # ------------------------------------------------------------------
    # 4. Canonical — transactions
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE transactions (
            uuid                TEXT        PRIMARY KEY,
            type                INTEGER     NOT NULL,   -- TransactionType enum value
            account             TEXT        REFERENCES accounts(uuid),
            portfolio           TEXT        REFERENCES portfolios(uuid),
            other_account       TEXT,
            other_portfolio     TEXT,
            other_uuid          TEXT,
            other_updated_at    TIMESTAMPTZ,
            date                DATE        NOT NULL,
            currency_code       TEXT,
            amount              BIGINT,                 -- amount in minor currency units
            amount_eur_cents    BIGINT,                 -- EUR-equivalent amount in cents
            fx_rate_used        BIGINT,                 -- FX rate used, 10^8 scaled (Decision 3)
            shares              BIGINT,                 -- share count in 10^-8 units
            note                TEXT,
            security            TEXT        REFERENCES securities(uuid),
            source              TEXT,
            updated_at          TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE INDEX idx_transactions_security
        ON transactions (security)
    """)

    _exec("""
        CREATE INDEX idx_transactions_security_date
        ON transactions (security, date)
    """)

    _exec("""
        CREATE TABLE transaction_units (
            transaction_uuid    TEXT        NOT NULL REFERENCES transactions(uuid) ON DELETE CASCADE,
            type                INTEGER     NOT NULL,   -- UnitType enum value
            amount              BIGINT,                 -- amount in minor currency units
            amount_eur_cents    BIGINT,
            fx_rate_used        BIGINT,                 -- FX rate used, 10^8 scaled (Decision 3)
            currency_code       TEXT,
            fx_amount           BIGINT,                 -- foreign-currency amount in minor units
            fx_currency_code    TEXT,
            fx_rate_to_base     BIGINT                  -- FX rate to base currency, 10^8 scaled
        )
    """)

    _exec("""
        CREATE INDEX idx_transaction_units_currency
        ON transaction_units (fx_currency_code)
    """)

    # ------------------------------------------------------------------
    # 5. Plans
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE plans (
            name            TEXT    PRIMARY KEY,
            note            TEXT,
            security        TEXT    REFERENCES securities(uuid),
            portfolio       TEXT    REFERENCES portfolios(uuid),
            account         TEXT    REFERENCES accounts(uuid),
            amount_str      TEXT,
            amount          BIGINT,     -- amount in 10^-8 units
            fees            BIGINT,     -- fees in 10^-8 units
            taxes           BIGINT,     -- taxes in 10^-8 units
            auto_generate   BOOLEAN,
            date            DATE,
            interval        INTEGER,
            type            TEXT
        )
    """)

    _exec("""
        CREATE TABLE plan_attributes (
            plan_name   TEXT    NOT NULL REFERENCES plans(name) ON DELETE CASCADE,
            key         TEXT    NOT NULL,
            value       TEXT
        )
    """)

    _exec("""
        CREATE TABLE plan_transactions (
            plan_name           TEXT    NOT NULL REFERENCES plans(name) ON DELETE CASCADE,
            transaction_uuid    TEXT    NOT NULL REFERENCES transactions(uuid) ON DELETE CASCADE
        )
    """)

    # ------------------------------------------------------------------
    # 6. Watchlists
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE watchlists (
            name    TEXT    PRIMARY KEY
        )
    """)

    _exec("""
        CREATE TABLE watchlist_securities (
            watchlist_name  TEXT    NOT NULL REFERENCES watchlists(name) ON DELETE CASCADE,
            security_uuid   TEXT    NOT NULL REFERENCES securities(uuid) ON DELETE CASCADE
        )
    """)

    # ------------------------------------------------------------------
    # 7. Taxonomies
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE taxonomies (
            id      TEXT    PRIMARY KEY,
            name    TEXT    NOT NULL,
            source  TEXT
        )
    """)

    _exec("""
        CREATE TABLE taxonomy_dimensions (
            taxonomy_id TEXT    NOT NULL REFERENCES taxonomies(id) ON DELETE CASCADE,
            dimension   TEXT    NOT NULL
        )
    """)

    _exec("""
        CREATE TABLE taxonomy_classifications (
            id              TEXT    PRIMARY KEY,
            taxonomy_id     TEXT    NOT NULL REFERENCES taxonomies(id) ON DELETE CASCADE,
            parent_id       TEXT,
            name            TEXT,
            note            TEXT,
            color           TEXT,
            weight          INTEGER,
            rank            INTEGER
        )
    """)

    _exec("""
        CREATE TABLE taxonomy_assignments (
            classification_id   TEXT    NOT NULL REFERENCES taxonomy_classifications(id) ON DELETE CASCADE,
            investment_vehicle  TEXT    NOT NULL,
            weight              INTEGER,
            rank                INTEGER
        )
    """)

    # ------------------------------------------------------------------
    # 8. Dashboards
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE dashboards (
            id      TEXT    PRIMARY KEY,
            name    TEXT    NOT NULL
        )
    """)

    _exec("""
        CREATE TABLE dashboard_configuration (
            dashboard_id    TEXT    NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
            key             TEXT    NOT NULL,
            value           TEXT
        )
    """)

    _exec("""
        CREATE TABLE dashboard_columns (
            dashboard_id    TEXT    NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
            column_index    INTEGER,
            weight          INTEGER
        )
    """)

    _exec("""
        CREATE TABLE dashboard_widgets (
            dashboard_id    TEXT    NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
            column_index    INTEGER,
            widget_index    INTEGER,
            type            TEXT,
            label           TEXT
        )
    """)

    _exec("""
        CREATE TABLE widget_configuration (
            dashboard_id    TEXT    NOT NULL,
            column_index    INTEGER,
            widget_index    INTEGER,
            key             TEXT    NOT NULL,
            value           TEXT
        )
    """)

    # ------------------------------------------------------------------
    # 9. Settings
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE settings_bookmarks (
            label   TEXT    NOT NULL,
            pattern TEXT    NOT NULL
        )
    """)

    _exec("""
        CREATE TABLE settings_attribute_types (
            id              TEXT    PRIMARY KEY,
            name            TEXT,
            column_label    TEXT,
            source          TEXT,
            target          TEXT,
            type            TEXT,
            converter_class TEXT
        )
    """)

    _exec("""
        CREATE TABLE settings_configuration_sets (
            key     TEXT    PRIMARY KEY,
            uuid    TEXT,
            name    TEXT,
            data    JSONB
        )
    """)

    # ------------------------------------------------------------------
    # 10. Client properties
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE client_properties (
            key     TEXT    PRIMARY KEY,
            value   TEXT
        )
    """)

    # ------------------------------------------------------------------
    # 11. Reference — exchange rates
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE exchange_rate_series (
            base_currency   TEXT        NOT NULL,
            term_currency   TEXT        NOT NULL,
            last_modified   TIMESTAMPTZ,
            PRIMARY KEY (base_currency, term_currency)
        )
    """)

    _exec("""
        CREATE TABLE exchange_rates (
            base_currency   TEXT    NOT NULL,
            term_currency   TEXT    NOT NULL,
            date            DATE    NOT NULL,
            rate            BIGINT  NOT NULL,   -- exchange rate in 10^8 scaled units (Decision 3)
            PRIMARY KEY (base_currency, term_currency, date)
        )
    """)

    # ------------------------------------------------------------------
    # 12. Reference — FX rates (ECB / Frankfurter)
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE fx_rates (
            date        DATE        NOT NULL,
            currency    TEXT        NOT NULL,
            rate        BIGINT      NOT NULL,   -- exchange rate in 10^8 scaled units (Decision 3)
            fetched_at  TIMESTAMPTZ,
            data_source TEXT,
            provider    TEXT,
            provenance  JSONB,
            PRIMARY KEY (date, currency)
        )
    """)

    # ------------------------------------------------------------------
    # 13. Reference — price history fetch queue
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE price_history_queue (
            id              INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            security_uuid   TEXT        NOT NULL REFERENCES securities(uuid) ON DELETE CASCADE,
            requested_date  DATE,
            status          TEXT        NOT NULL DEFAULT 'pending',
            priority        INTEGER     NOT NULL DEFAULT 0,
            attempts        INTEGER     NOT NULL DEFAULT 0,
            scheduled_at    TIMESTAMPTZ,
            started_at      TIMESTAMPTZ,
            finished_at     TIMESTAMPTZ,
            last_error      TEXT,
            data_source     TEXT,
            provenance      JSONB,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE INDEX idx_price_history_queue_status
        ON price_history_queue (status, priority, scheduled_at)
    """)

    _exec("""
        CREATE INDEX idx_price_history_queue_security_date
        ON price_history_queue (security_uuid, requested_date)
    """)

    # ------------------------------------------------------------------
    # 14. Metadata (file-level, tracks last sync date per portfolio file)
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE metadata (
            key     TEXT    PRIMARY KEY,
            date    DATE    NOT NULL
        )
    """)

    # ------------------------------------------------------------------
    # 15. Staging tables (stg_*) — cleared and reloaded on each ingestion
    #     Renamed from ingestion_* to stg_* per EXECUTION_PLAN Phase 1.
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE stg_metadata (
            run_id          TEXT        PRIMARY KEY,
            file_path       TEXT,
            parsed_at       TIMESTAMPTZ,
            pp_version      INTEGER,
            base_currency   TEXT,
            properties      JSONB
        )
    """)

    _exec("""
        CREATE TABLE stg_accounts (
            uuid            TEXT        PRIMARY KEY,
            name            TEXT        NOT NULL,
            currency_code   TEXT,
            note            TEXT,
            is_retired      BOOLEAN,
            attributes      JSONB,
            updated_at      TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE TABLE stg_portfolios (
            uuid                TEXT        PRIMARY KEY,
            name                TEXT        NOT NULL,
            note                TEXT,
            reference_account   TEXT,
            is_retired          BOOLEAN,
            attributes          JSONB,
            updated_at          TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE TABLE stg_securities (
            uuid                    TEXT        PRIMARY KEY,
            name                    TEXT        NOT NULL,
            currency_code           TEXT,
            target_currency_code    TEXT,
            isin                    TEXT,
            ticker_symbol           TEXT,
            wkn                     TEXT,
            note                    TEXT,
            online_id               TEXT,
            feed                    TEXT,
            feed_url                TEXT,
            latest_feed             TEXT,
            latest_feed_url         TEXT,
            latest_date             DATE,       -- converted from epoch day
            latest_close            BIGINT,     -- latest closing price in 10^-8 units
            latest_high             BIGINT,
            latest_low              BIGINT,
            latest_volume           BIGINT,
            is_retired              BOOLEAN,
            attributes              JSONB,
            properties              JSONB,
            updated_at              TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE TABLE stg_transactions (
            uuid                TEXT        PRIMARY KEY,
            type                INTEGER     NOT NULL,
            account             TEXT        REFERENCES stg_accounts(uuid),
            portfolio           TEXT        REFERENCES stg_portfolios(uuid),
            other_account       TEXT,
            other_portfolio     TEXT,
            other_uuid          TEXT,
            other_updated_at    TIMESTAMPTZ,
            date                DATE,
            currency_code       TEXT,
            amount              BIGINT,
            amount_eur_cents    BIGINT,
            fx_rate_used        BIGINT,     -- FX rate, 10^8 scaled (Decision 3)
            shares              BIGINT,
            note                TEXT,
            security            TEXT        REFERENCES stg_securities(uuid),
            source              TEXT,
            updated_at          TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE TABLE stg_transaction_units (
            transaction_uuid    TEXT        NOT NULL REFERENCES stg_transactions(uuid) ON DELETE CASCADE,
            unit_index          INTEGER     NOT NULL,
            type                INTEGER     NOT NULL,
            amount              BIGINT,
            amount_eur_cents    BIGINT,
            fx_rate_used        BIGINT,     -- FX rate, 10^8 scaled (Decision 3)
            currency_code       TEXT,
            fx_amount           BIGINT,
            fx_currency_code    TEXT,
            fx_rate_to_base     BIGINT,     -- FX rate to base currency, 10^8 scaled
            PRIMARY KEY (transaction_uuid, unit_index)
        )
    """)

    _exec("""
        CREATE TABLE stg_historical_prices (
            security_uuid   TEXT    NOT NULL REFERENCES stg_securities(uuid) ON DELETE CASCADE,
            date            DATE    NOT NULL,
            close           BIGINT,
            high            BIGINT,
            low             BIGINT,
            volume          BIGINT,
            fetched_at      TIMESTAMPTZ,
            data_source     TEXT,
            provider        TEXT,
            provenance      JSONB,
            PRIMARY KEY (security_uuid, date)
        )
    """)

    # ------------------------------------------------------------------
    # 16. Metric tables
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE metric_runs (
            run_uuid                TEXT        PRIMARY KEY,
            status                  TEXT        NOT NULL,
            trigger                 TEXT,
            started_at              TIMESTAMPTZ NOT NULL,
            finished_at             TIMESTAMPTZ,
            duration_ms             INTEGER,
            total_entities          INTEGER,
            processed_portfolios    INTEGER,
            processed_accounts      INTEGER,
            processed_securities    INTEGER,
            error_message           TEXT,
            provenance              JSONB,
            created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMPTZ
        )
    """)

    _exec("""
        CREATE INDEX idx_metric_runs_status
        ON metric_runs (status)
    """)

    _exec("""
        CREATE INDEX idx_metric_runs_started_at
        ON metric_runs (started_at)
    """)

    _exec("""
        CREATE TABLE portfolio_metrics (
            id                          INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            metric_run_uuid             TEXT        NOT NULL REFERENCES metric_runs(run_uuid) ON DELETE CASCADE,
            portfolio_uuid              TEXT        NOT NULL REFERENCES portfolios(uuid) ON DELETE CASCADE,
            valuation_currency          TEXT        NOT NULL DEFAULT 'EUR',
            current_value_cents         BIGINT      NOT NULL DEFAULT 0,
            purchase_value_cents        BIGINT      NOT NULL DEFAULT 0,
            gain_abs_cents              BIGINT      NOT NULL DEFAULT 0,
            gain_pct                    DOUBLE PRECISION,
            total_change_eur_cents      BIGINT      NOT NULL DEFAULT 0,
            total_change_pct            DOUBLE PRECISION,
            source                      TEXT,
            coverage_ratio              DOUBLE PRECISION,
            position_count              INTEGER     DEFAULT 0,
            missing_value_positions     INTEGER     DEFAULT 0,
            provenance                  JSONB,
            created_at                  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at                  TIMESTAMPTZ,
            UNIQUE (metric_run_uuid, portfolio_uuid)
        )
    """)

    _exec("""
        CREATE INDEX idx_portfolio_metrics_portfolio
        ON portfolio_metrics (portfolio_uuid)
    """)

    _exec("""
        CREATE INDEX idx_portfolio_metrics_run
        ON portfolio_metrics (metric_run_uuid)
    """)

    _exec("""
        CREATE TABLE account_metrics (
            id                      INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            metric_run_uuid         TEXT        NOT NULL REFERENCES metric_runs(run_uuid) ON DELETE CASCADE,
            account_uuid            TEXT        NOT NULL REFERENCES accounts(uuid) ON DELETE CASCADE,
            currency_code           TEXT        NOT NULL,
            valuation_currency      TEXT        NOT NULL DEFAULT 'EUR',
            balance_native_cents    BIGINT      NOT NULL DEFAULT 0,
            balance_eur_cents       BIGINT,
            fx_rate                 BIGINT,     -- FX rate used, 10^8 scaled (Decision 3)
            fx_rate_source          TEXT,
            fx_rate_timestamp       TIMESTAMPTZ,
            coverage_ratio          DOUBLE PRECISION,
            provenance              JSONB,
            created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMPTZ,
            UNIQUE (metric_run_uuid, account_uuid)
        )
    """)

    _exec("""
        CREATE INDEX idx_account_metrics_account
        ON account_metrics (account_uuid)
    """)

    _exec("""
        CREATE INDEX idx_account_metrics_run
        ON account_metrics (metric_run_uuid)
    """)

    _exec("""
        CREATE TABLE security_metrics (
            id                              INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            metric_run_uuid                 TEXT        NOT NULL REFERENCES metric_runs(run_uuid) ON DELETE CASCADE,
            portfolio_uuid                  TEXT        NOT NULL REFERENCES portfolios(uuid) ON DELETE CASCADE,
            security_uuid                   TEXT        NOT NULL REFERENCES securities(uuid) ON DELETE CASCADE,
            valuation_currency              TEXT        NOT NULL DEFAULT 'EUR',
            security_currency_code          TEXT        NOT NULL,
            holdings_raw                    BIGINT      NOT NULL DEFAULT 0,
            current_value_cents             BIGINT      NOT NULL DEFAULT 0,
            purchase_value_cents            BIGINT      NOT NULL DEFAULT 0,
            purchase_security_value_raw     BIGINT,
            purchase_account_value_cents    BIGINT,
            gain_abs_cents                  BIGINT      NOT NULL DEFAULT 0,
            gain_pct                        DOUBLE PRECISION,
            total_change_eur_cents          BIGINT      NOT NULL DEFAULT 0,
            total_change_pct                DOUBLE PRECISION,
            source                          TEXT,
            coverage_ratio                  DOUBLE PRECISION,
            day_change_native               DOUBLE PRECISION,
            day_change_eur                  DOUBLE PRECISION,
            day_change_pct                  DOUBLE PRECISION,
            day_change_source               TEXT,
            day_change_coverage             DOUBLE PRECISION,
            last_price_native_raw           BIGINT,
            last_close_native_raw           BIGINT,
            provenance                      JSONB,
            created_at                      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at                      TIMESTAMPTZ,
            UNIQUE (metric_run_uuid, portfolio_uuid, security_uuid)
        )
    """)

    _exec("""
        CREATE INDEX idx_security_metrics_security
        ON security_metrics (security_uuid)
    """)

    _exec("""
        CREATE INDEX idx_security_metrics_portfolio
        ON security_metrics (portfolio_uuid)
    """)

    _exec("""
        CREATE INDEX idx_security_metrics_run
        ON security_metrics (metric_run_uuid)
    """)

    # ------------------------------------------------------------------
    # 17. Snapshot tables
    # ------------------------------------------------------------------
    _exec("""
        CREATE TABLE portfolio_snapshots (
            id                          INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            metric_run_uuid             TEXT        NOT NULL REFERENCES metric_runs(run_uuid) ON DELETE CASCADE,
            portfolio_uuid              TEXT        NOT NULL REFERENCES portfolios(uuid) ON DELETE CASCADE,
            snapshot_at                 TIMESTAMPTZ NOT NULL,
            name                        TEXT        NOT NULL,
            currency_code               TEXT        NOT NULL DEFAULT 'EUR',
            current_value               DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            purchase_sum                DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            gain_abs                    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            gain_pct                    DOUBLE PRECISION,
            total_change_eur            DOUBLE PRECISION,
            total_change_pct            DOUBLE PRECISION,
            position_count              INTEGER     NOT NULL DEFAULT 0,
            missing_value_positions     INTEGER     NOT NULL DEFAULT 0,
            has_current_value           BOOLEAN     NOT NULL DEFAULT TRUE,
            coverage_ratio              DOUBLE PRECISION,
            performance_source          TEXT,
            performance_provenance      TEXT,
            provenance                  JSONB,
            payload                     JSONB,
            created_at                  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at                  TIMESTAMPTZ,
            UNIQUE (metric_run_uuid, portfolio_uuid)
        )
    """)

    _exec("""
        CREATE INDEX idx_portfolio_snapshots_run
        ON portfolio_snapshots (metric_run_uuid)
    """)

    _exec("""
        CREATE INDEX idx_portfolio_snapshots_portfolio
        ON portfolio_snapshots (portfolio_uuid)
    """)

    _exec("""
        CREATE TABLE account_snapshots (
            id                  INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            metric_run_uuid     TEXT        NOT NULL REFERENCES metric_runs(run_uuid) ON DELETE CASCADE,
            account_uuid        TEXT        NOT NULL REFERENCES accounts(uuid) ON DELETE CASCADE,
            snapshot_at         TIMESTAMPTZ NOT NULL,
            name                TEXT        NOT NULL,
            currency_code       TEXT        NOT NULL,
            orig_balance        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            balance             DOUBLE PRECISION,
            fx_unavailable      BOOLEAN     NOT NULL DEFAULT FALSE,
            fx_rate             BIGINT,     -- FX rate, 10^8 scaled (Decision 3)
            fx_rate_source      TEXT,
            fx_rate_timestamp   TIMESTAMPTZ,
            coverage_ratio      DOUBLE PRECISION,
            provenance          JSONB,
            payload             JSONB,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMPTZ,
            UNIQUE (metric_run_uuid, account_uuid)
        )
    """)

    _exec("""
        CREATE INDEX idx_account_snapshots_run
        ON account_snapshots (metric_run_uuid)
    """)

    _exec("""
        CREATE INDEX idx_account_snapshots_account
        ON account_snapshots (account_uuid)
    """)

    _exec("""
        CREATE TABLE daily_wealth (
            date                TEXT    NOT NULL,
            scope_uuid          TEXT    NOT NULL,
            scope_type          TEXT    NOT NULL,
            total_wealth_cents  BIGINT  NOT NULL,
            total_invested_cents BIGINT NOT NULL,
            PRIMARY KEY (date, scope_uuid, scope_type)
        )
    """)


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------

def downgrade() -> None:
    # Drop in reverse dependency order
    tables = [
        # Snapshots & wealth
        "daily_wealth",
        "account_snapshots",
        "portfolio_snapshots",
        # Metrics
        "security_metrics",
        "account_metrics",
        "portfolio_metrics",
        "metric_runs",
        # Staging
        "stg_historical_prices",
        "stg_transaction_units",
        "stg_transactions",
        "stg_securities",
        "stg_portfolios",
        "stg_accounts",
        "stg_metadata",
        # Metadata / reference
        "metadata",
        "price_history_queue",
        "fx_rates",
        "exchange_rates",
        "exchange_rate_series",
        "client_properties",
        # Settings / config
        "settings_configuration_sets",
        "settings_attribute_types",
        "settings_bookmarks",
        # Dashboard
        "widget_configuration",
        "dashboard_widgets",
        "dashboard_columns",
        "dashboard_configuration",
        "dashboards",
        # Taxonomy
        "taxonomy_assignments",
        "taxonomy_classifications",
        "taxonomy_dimensions",
        "taxonomies",
        # Watchlists
        "watchlist_securities",
        "watchlists",
        # Plans
        "plan_transactions",
        "plan_attributes",
        "plans",
        # Canonical transactions
        "transaction_units",
        "transactions",
        # Canonical portfolios
        "portfolio_securities",
        "portfolio_attributes",
        "portfolios",
        # Canonical securities
        "historical_prices",
        "securities",
        # Canonical accounts
        "account_attributes",
        "accounts",
    ]
    for table in tables:
        _exec(f"DROP TABLE IF EXISTS {table} CASCADE")
