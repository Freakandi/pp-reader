"""Ingestion pipeline: writes parsed portfolio data to PostgreSQL staging tables.

Entry point: ``ingest(conn, parsed, *, file_path=None) -> str``

Design notes:
- All writes occur inside a single ``BEGIN/COMMIT`` transaction via asyncpg.
- Staging tables are cleared (child-first) before each run to ensure a clean slate.
- Historical prices are bulk-inserted via ``copy_records_to_table()`` for performance.
- FX rate resolution (amount_eur_cents, fx_rate_used) is NOT performed here.
  That is Phase 5 (enrichment). The columns are left NULL at ingestion time.
- All numeric values are stored in PP wire format: no scaling or conversion applied
  except for ``fx_rate_to_base`` (float → BIGINT at 10^8) and epoch-day dates
  (int → datetime.date via ``epoch_day_to_date``).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

    from app.models.parsed import ParsedClient

from app.models.constants import EIGHT_DECIMAL_SCALE
from app.models.domain import epoch_day_to_date
from app.db.queries.ingestion import (
    CLEAR_STAGING_TABLES,
    HISTORICAL_PRICES_COLUMNS,
    INSERT_ACCOUNT,
    INSERT_METADATA,
    INSERT_PORTFOLIO,
    INSERT_SECURITY,
    INSERT_TRANSACTION,
    INSERT_TRANSACTION_UNIT,
)

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def _json_or_none(value: Any) -> str | None:
    """Return a compact JSON string for a non-empty mapping, else None."""
    if not value:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Metadata / extra-sections serialization
# ---------------------------------------------------------------------------


def _build_metadata_blob(
    parsed: ParsedClient,
    properties: dict[str, str],
) -> dict[str, Any] | None:
    """Build the JSON blob stored in ``stg_metadata.properties``.

    Serializes watchlists, plans, taxonomies, dashboards, settings, and the
    raw ``properties`` map from the portfolio file.  Returns ``None`` when
    there is nothing to store.
    """
    payload: dict[str, Any] = {}

    if properties:
        payload["properties"] = {str(k): str(v) for k, v in properties.items()}

    # Watchlists
    watchlists = [
        {"name": wl.name, "securities": list(wl.securities)}
        for wl in parsed.watchlists
        if wl.name
    ]
    if watchlists:
        payload["watchlists"] = watchlists

    # Investment plans
    plans = [
        {
            "name": plan.name,
            "note": plan.note,
            "security": plan.security,
            "portfolio": plan.portfolio,
            "account": plan.account,
            "attributes": dict(plan.attributes),
            "auto_generate": plan.auto_generate,
            "date": plan.date,
            "interval": plan.interval,
            "amount": plan.amount,
            "fees": plan.fees,
            "transactions": list(plan.transactions),
            "taxes": plan.taxes,
            "plan_type": plan.plan_type,
        }
        for plan in parsed.plans
        if plan.name
    ]
    if plans:
        payload["plans"] = plans

    # Taxonomies
    taxonomies = [
        {
            "id": tx.id,
            "name": tx.name,
            "source": tx.source,
            "dimensions": list(tx.dimensions),
            "classifications": [
                {
                    "id": cls.id,
                    "name": cls.name,
                    "parent_id": cls.parent_id,
                    "note": cls.note,
                    "color": cls.color,
                    "weight": cls.weight,
                    "rank": cls.rank,
                    "data": dict(cls.data),
                    "assignments": [
                        {
                            "investment_vehicle": asgn.investment_vehicle,
                            "weight": asgn.weight,
                            "rank": asgn.rank,
                            "data": dict(asgn.data),
                        }
                        for asgn in cls.assignments
                    ],
                }
                for cls in tx.classifications
            ],
        }
        for tx in parsed.taxonomies
        if tx.id
    ]
    if taxonomies:
        payload["taxonomies"] = taxonomies

    # Dashboards
    dashboards = [
        {
            "name": db.name,
            "configuration": dict(db.configuration),
            "columns": [
                {
                    "weight": col.weight,
                    "widgets": [
                        {
                            "type": w.type,
                            "label": w.label,
                            "configuration": dict(w.configuration),
                        }
                        for w in col.widgets
                    ],
                }
                for col in db.columns
            ],
            "dashboard_id": db.dashboard_id,
        }
        for db in parsed.dashboards
        if db.name
    ]
    if dashboards:
        payload["dashboards"] = dashboards

    # Settings
    if parsed.settings:
        s = parsed.settings
        s_blob: dict[str, Any] = {}

        bookmarks = [
            {"label": b.label, "pattern": b.pattern}
            for b in s.bookmarks
            if b.label
        ]
        if bookmarks:
            s_blob["bookmarks"] = bookmarks

        attr_types = [
            {
                "id": a.id,
                "name": a.name,
                "column_label": a.column_label,
                "source": a.source,
                "target": a.target,
                "type": a.type,
                "converter_class": a.converter_class,
                "properties": dict(a.properties),
            }
            for a in s.attribute_types
            if a.id
        ]
        if attr_types:
            s_blob["attribute_types"] = attr_types

        config_sets = [
            {"key": c.key, "uuid": c.uuid, "name": c.name, "data": c.data}
            for c in s.configuration_sets
            if c.key
        ]
        if config_sets:
            s_blob["configuration_sets"] = config_sets

        if s_blob:
            payload["settings"] = s_blob

    return payload if payload else None


# ---------------------------------------------------------------------------
# Main ingestion entry point
# ---------------------------------------------------------------------------


async def ingest(
    conn: asyncpg.Connection,
    parsed: ParsedClient,
    *,
    file_path: str | None = None,
) -> str:
    """Write parsed portfolio data into PostgreSQL staging tables.

    Clears all staging tables and reloads them within a single transaction.
    Returns the ``run_id`` (32-char hex UUID) assigned to this ingestion run.

    Parameters
    ----------
    conn:
        An active asyncpg connection.  The caller is responsible for managing
        the connection lifecycle (acquire from pool, release after return).
    parsed:
        The ``ParsedClient`` produced by ``pipeline.parser.parse_portfolio_file``.
    file_path:
        Original ``.portfolio`` file path stored in metadata for traceability.
    """
    run_id = uuid.uuid4().hex
    parsed_at = datetime.now(tz=UTC)

    async with conn.transaction():
        # ── 1. Clear staging tables (child-first order) ──────────────────────
        for table in CLEAR_STAGING_TABLES:
            await conn.execute(f"DELETE FROM {table}")

        # ── 2. Accounts ──────────────────────────────────────────────────────
        account_rows = [
            (
                acc.uuid,
                acc.name,
                acc.currency_code,
                acc.note,
                acc.is_retired,
                _json_or_none(acc.attributes),
                acc.updated_at,
            )
            for acc in parsed.accounts
        ]
        if account_rows:
            await conn.executemany(INSERT_ACCOUNT, account_rows)

        # ── 3. Portfolios ────────────────────────────────────────────────────
        portfolio_rows = [
            (
                p.uuid,
                p.name,
                p.note,
                p.reference_account,
                p.is_retired,
                _json_or_none(p.attributes),
                p.updated_at,
            )
            for p in parsed.portfolios
        ]
        if portfolio_rows:
            await conn.executemany(INSERT_PORTFOLIO, portfolio_rows)

        # ── 4. Securities + collect price payloads ───────────────────────────
        security_rows = []
        price_payload: list[tuple[str, list]] = []

        for sec in parsed.securities:
            latest = sec.latest
            security_rows.append(
                (
                    sec.uuid,
                    sec.name,
                    sec.currency_code,
                    sec.target_currency_code,
                    sec.isin,
                    sec.ticker_symbol,
                    sec.wkn,
                    sec.note,
                    sec.online_id,
                    sec.feed,
                    sec.feed_url,
                    sec.latest_feed,
                    sec.latest_feed_url,
                    epoch_day_to_date(latest.date) if latest else None,
                    latest.close if latest else None,
                    latest.high if latest else None,
                    latest.low if latest else None,
                    latest.volume if latest else None,
                    sec.is_retired,
                    _json_or_none(sec.attributes),
                    _json_or_none(sec.properties),
                    sec.updated_at,
                )
            )

            if sec.prices:
                price_payload.append((sec.uuid, sec.prices))
            elif latest:
                # Fallback: treat latest-price-only as a single price point.
                price_payload.append((sec.uuid, [latest]))

        if security_rows:
            await conn.executemany(INSERT_SECURITY, security_rows)

        # ── 5. Transactions + collect unit payloads ──────────────────────────
        txn_rows = []
        unit_payload: list[tuple[str, list]] = []

        for txn in parsed.transactions:
            txn_rows.append(
                (
                    txn.uuid,
                    txn.type,
                    txn.account,
                    txn.portfolio,
                    txn.other_account,
                    txn.other_portfolio,
                    txn.other_uuid,
                    txn.other_updated_at,
                    # ParsedTransaction.date is datetime; stg column is DATE.
                    txn.date.date() if txn.date is not None else None,
                    txn.currency_code,
                    txn.amount,
                    None,  # amount_eur_cents — populated in Phase 5 (FX enrichment)
                    None,  # fx_rate_used     — populated in Phase 5
                    txn.shares,
                    txn.note,
                    txn.security,
                    txn.source,
                    txn.updated_at,
                )
            )
            if txn.units:
                unit_payload.append((txn.uuid, txn.units))

        if txn_rows:
            await conn.executemany(INSERT_TRANSACTION, txn_rows)

        # ── 6. Transaction units ─────────────────────────────────────────────
        unit_rows = []
        for txn_uuid, units in unit_payload:
            for idx, unit in enumerate(units):
                # fx_rate_to_base arrives as float from protobuf decimal;
                # store as BIGINT scaled at 10^8 per Decision 3.
                fx_rate_to_base_scaled = (
                    round(unit.fx_rate_to_base * EIGHT_DECIMAL_SCALE)
                    if unit.fx_rate_to_base is not None
                    else None
                )
                unit_rows.append(
                    (
                        txn_uuid,
                        idx,
                        unit.type,
                        unit.amount,
                        None,  # amount_eur_cents — Phase 5
                        None,  # fx_rate_used     — Phase 5
                        unit.currency_code,
                        unit.fx_amount,
                        unit.fx_currency_code,
                        fx_rate_to_base_scaled,
                    )
                )

        if unit_rows:
            await conn.executemany(INSERT_TRANSACTION_UNIT, unit_rows)

        # ── 7. Historical prices (bulk via copy_records_to_table) ─────────────
        price_records = [
            (
                security_uuid,
                epoch_day_to_date(price.date),
                price.close,
                price.high,
                price.low,
                price.volume,
            )
            for security_uuid, prices in price_payload
            for price in prices
        ]
        if price_records:
            await conn.copy_records_to_table(
                "stg_historical_prices",
                records=price_records,
                columns=HISTORICAL_PRICES_COLUMNS,
            )

        # ── 8. Metadata row (written last for deterministic row counts) ───────
        metadata_blob = _build_metadata_blob(parsed, dict(parsed.properties))
        await conn.execute(
            INSERT_METADATA,
            run_id,
            file_path,
            parsed_at,
            parsed.version,
            parsed.base_currency,
            _json_or_none(metadata_blob),
        )

    _LOGGER.info(
        "Ingested run_id=%s: %d accounts, %d portfolios, %d securities, "
        "%d transactions, %d units, %d prices",
        run_id,
        len(account_rows),
        len(portfolio_rows),
        len(security_rows),
        len(txn_rows),
        len(unit_rows),
        len(price_records),
    )

    return run_id
