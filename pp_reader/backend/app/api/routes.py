"""REST API route handlers for PP Reader.

Implements all endpoints declared in the frontend API client (frontend/src/api/client.ts).

Monetary conversion at the API boundary (Architecture Decision 3):
  - BIGINT cents  (÷ 100)  → EUR float
  - BIGINT 10^8   (÷ 10^8) → shares / price float
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from app.db.queries.api import (
    get_daily_wealth,
    get_dashboard_data,
    get_latest_completed_run,
    get_accounts_for_run,
    get_portfolios_for_run,
    get_positions_for_portfolio,
    get_security_snapshot,
    get_security_history,
    get_trades,
    get_status,
)
from app.dependencies import get_pool
from app.models.constants import EIGHT_DECIMAL_SCALE, MONETARY_SCALE, TransactionType

__all__ = ["router"]

_LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/api")

# ── Scale helpers ─────────────────────────────────────────────────────────

_CENTS: int = MONETARY_SCALE       # 100
_SCALE8: int = EIGHT_DECIMAL_SCALE  # 10^8


def _c(v: int | float | None) -> float | None:
    """Cents → EUR float; None propagates."""
    return None if v is None else float(v) / _CENTS


def _c0(v: int | float | None) -> float:
    """Cents → EUR float, default 0.0."""
    return 0.0 if v is None else float(v) / _CENTS


def _s(v: int | None) -> float | None:
    """10^8-scaled → float; None propagates."""
    return None if v is None else v / _SCALE8


# ── Range key helpers ─────────────────────────────────────────────────────

_RANGE_DAYS: dict[str, int | None] = {
    "1M": 30,
    "6M": 182,
    "1Y": 365,
    "5Y": 1825,
    "ALL": None,
}


def _range_start(range_key: str) -> str:
    days = _RANGE_DAYS.get(range_key)
    return "1900-01-01" if days is None else (date.today() - timedelta(days=days)).isoformat()


# ── Trade type names ──────────────────────────────────────────────────────

_TRADE_TYPE_NAMES: dict[int, str] = {
    TransactionType.BUY: "BUY",
    TransactionType.SELL: "SELL",
    TransactionType.INBOUND_DELIVERY: "INBOUND_DELIVERY",
    TransactionType.OUTBOUND_DELIVERY: "OUTBOUND_DELIVERY",
    TransactionType.DIVIDEND: "DIVIDEND",
    TransactionType.INTEREST: "INTEREST",
}

# ── Shared helper ─────────────────────────────────────────────────────────


async def _latest_run_uuid(pool: asyncpg.Pool) -> str | None:
    async with pool.acquire() as conn:
        row = await get_latest_completed_run(conn)
    return row["run_uuid"] if row else None


# ── Endpoints ─────────────────────────────────────────────────────────────


@router.get("/dashboard")
async def dashboard(pool: asyncpg.Pool = Depends(get_pool)) -> JSONResponse:
    """Aggregated dashboard metrics (total wealth, counts)."""
    async with pool.acquire() as conn:
        data = await get_dashboard_data(conn)

    last_updated = data.get("last_updated")
    return JSONResponse({
        "total_wealth": _c0(data.get("total_wealth_cents")),
        "last_updated": last_updated.isoformat() if last_updated else None,
        "portfolio_count": int(data.get("portfolio_count") or 0),
        "account_count": int(data.get("account_count") or 0),
    })


@router.get("/accounts")
async def accounts(pool: asyncpg.Pool = Depends(get_pool)) -> JSONResponse:
    """All active accounts with EUR-equivalent balances."""
    run_uuid = await _latest_run_uuid(pool)
    if run_uuid is None:
        return JSONResponse([])

    async with pool.acquire() as conn:
        rows = await get_accounts_for_run(conn, run_uuid)

    result: list[dict[str, Any]] = []
    for r in rows:
        balance = r.get("balance")
        result.append({
            "uuid": r["uuid"],
            "name": r["name"],
            "currency": r["currency"],
            "balance": float(balance) if balance is not None else 0.0,
            "is_deposit": bool(r.get("is_deposit", True)),
        })
    return JSONResponse(result)


@router.get("/portfolios")
async def portfolios(pool: asyncpg.Pool = Depends(get_pool)) -> JSONResponse:
    """All active portfolios with current metrics."""
    run_uuid = await _latest_run_uuid(pool)
    if run_uuid is None:
        return JSONResponse([])

    async with pool.acquire() as conn:
        rows = await get_portfolios_for_run(conn, run_uuid)

    result: list[dict[str, Any]] = []
    for r in rows:
        result.append({
            "uuid": r["uuid"],
            "name": r["name"],
            "currency": r["currency"],
            "current_value": float(r.get("current_value") or 0.0),
            "purchase_value": float(r.get("purchase_value") or 0.0),
            "gain_abs": float(r.get("gain_abs") or 0.0),
            "gain_pct": r.get("gain_pct"),
        })
    return JSONResponse(result)


@router.get("/portfolios/{uuid}/positions")
async def portfolio_positions(
    uuid: str,
    pool: asyncpg.Pool = Depends(get_pool),
) -> JSONResponse:
    """Security positions held in a specific portfolio."""
    run_uuid = await _latest_run_uuid(pool)
    if run_uuid is None:
        return JSONResponse([])

    async with pool.acquire() as conn:
        rows = await get_positions_for_portfolio(conn, run_uuid, uuid)

    result: list[dict[str, Any]] = []
    for r in rows:
        holdings_raw: int = r.get("holdings_raw") or 0
        holdings = holdings_raw / _SCALE8

        # Average price in security currency per share.
        purch_raw: int | None = r.get("purchase_security_value_raw")
        average_price: float | None = None
        if purch_raw is not None and holdings_raw > 0:
            average_price = (purch_raw / _SCALE8) / (holdings_raw / _SCALE8)

        result.append({
            "uuid": r["security_uuid"],
            "security_uuid": r["security_uuid"],
            "security_name": r["security_name"],
            "isin": r.get("isin"),
            "ticker": r.get("ticker"),
            "currency": r.get("currency") or "EUR",
            "current_holdings": holdings,
            "average_price": average_price,
            "purchase_value": _c(r.get("purchase_value_cents")),
            "current_value": _c(r.get("current_value_cents")),
            "gain_abs": _c(r.get("gain_abs_cents")),
            "gain_pct": r.get("gain_pct"),
            "day_change_abs": r.get("day_change_eur"),
            "day_change_pct": r.get("day_change_pct"),
            "fx_unavailable": bool(r.get("fx_unavailable", False)),
        })
    return JSONResponse(result)


@router.get("/securities/{uuid}")
async def security_detail(
    uuid: str,
    pool: asyncpg.Pool = Depends(get_pool),
) -> JSONResponse:
    """Full snapshot for a single security."""
    run_uuid = await _latest_run_uuid(pool)
    if run_uuid is None:
        raise HTTPException(status_code=404, detail="No metric run available")

    async with pool.acquire() as conn:
        row = await get_security_snapshot(conn, uuid, run_uuid)

    if row is None:
        raise HTTPException(status_code=404, detail="Security not found")

    holdings_raw: int = row.get("holdings_raw") or 0
    holdings = holdings_raw / _SCALE8

    # Average price from purchase_value_cents / holdings (EUR / share).
    purch_cents = row.get("purchase_value_cents")
    average_price: float | None = None
    if purch_cents is not None and holdings > 0:
        average_price = (purch_cents / _CENTS) / holdings

    last_price_date = row.get("last_price_date")

    return JSONResponse({
        "uuid": row["uuid"],
        "name": row["name"],
        "isin": row.get("isin"),
        "ticker": row.get("ticker"),
        "currency": row.get("currency") or "EUR",
        "latest_price": _s(row.get("last_price")),
        "latest_price_date": last_price_date.isoformat() if last_price_date else None,
        "current_holdings": holdings,
        "average_price": average_price,
        "purchase_value": _c(row.get("purchase_value_cents")),
        "current_value": _c(row.get("current_value_cents")),
        "gain_abs": _c(row.get("gain_abs_cents")),
        "gain_pct": row.get("gain_pct"),
        "day_change_abs": row.get("day_change_eur"),
        "day_change_pct": row.get("day_change_pct"),
        "fx_rate": None,
        "fx_unavailable": bool(row.get("fx_unavailable", False)),
    })


@router.get("/securities/{uuid}/history")
async def security_history(
    uuid: str,
    range: str = Query(default="1Y"),
    pool: asyncpg.Pool = Depends(get_pool),
) -> JSONResponse:
    """Historical price time series for a security."""
    if range not in _RANGE_DAYS:
        raise HTTPException(status_code=400, detail=f"Invalid range: {range!r}")

    from_date = _range_start(range)
    async with pool.acquire() as conn:
        rows = await get_security_history(conn, uuid, from_date)

    points = [
        {"date": r["date"], "value": r["close"] / _SCALE8}
        for r in rows
    ]
    return JSONResponse({"uuid": uuid, "range": range, "points": points})


@router.get("/wealth/daily")
async def wealth_daily(
    from_: str = Query(default="", alias="from"),
    to: str = Query(default=""),
    pool: asyncpg.Pool = Depends(get_pool),
) -> JSONResponse:
    """Daily total-wealth time series for the given date range."""
    from_date = from_ or (date.today() - timedelta(days=365)).isoformat()
    to_date = to or date.today().isoformat()

    async with pool.acquire() as conn:
        rows = await get_daily_wealth(conn, from_date, to_date)

    points = [
        {"date": r["date"], "value": r["total_wealth_cents"] / _CENTS}
        for r in rows
    ]
    return JSONResponse({"from": from_date, "to": to_date, "points": points})


@router.get("/trades")
async def trades(pool: asyncpg.Pool = Depends(get_pool)) -> JSONResponse:
    """All investment transactions (buys, sells, dividends, etc.)."""
    async with pool.acquire() as conn:
        rows = await get_trades(conn)

    result: list[dict[str, Any]] = []
    for r in rows:
        shares_raw: int = r.get("shares") or 0
        amount: int = r.get("amount") or 0
        fees_raw: int = r.get("fees") or 0

        holdings = shares_raw / _SCALE8
        value = amount / _CENTS
        fees = fees_raw / _CENTS
        price = (value / holdings) if holdings else 0.0

        result.append({
            "uuid": r["uuid"],
            "portfolio_uuid": r.get("portfolio_uuid") or "",
            "portfolio_name": r.get("portfolio_name") or "",
            "security_uuid": r.get("security_uuid") or "",
            "security_name": r.get("security_name") or "",
            "type": _TRADE_TYPE_NAMES.get(int(r.get("type", -1)), "UNKNOWN"),
            "date": r["date"],
            "shares": holdings,
            "price": price,
            "value": value,
            "fees": fees,
            "currency": r.get("currency") or "EUR",
        })
    return JSONResponse(result)


@router.get("/performance")
async def performance(
    from_: str = Query(default="", alias="from"),
    to: str = Query(default=""),
    pool: asyncpg.Pool = Depends(get_pool),
) -> JSONResponse:
    """Portfolio performance breakdown for the given date range.

    Returns gain_abs and gain_pct summed across all portfolios in the
    latest metric run.  TWR and IRR are not yet computed per date range
    and are returned as null.
    """
    from_date = from_ or (date.today() - timedelta(days=365)).isoformat()
    to_date = to or date.today().isoformat()

    run_uuid = await _latest_run_uuid(pool)
    if run_uuid is None:
        return JSONResponse(
            {"from": from_date, "to": to_date, "twr": None, "irr": None,
             "gain_abs": None, "gain_pct": None},
        )

    async with pool.acquire() as conn:
        rows = await get_portfolios_for_run(conn, run_uuid)

    total_gain_abs = sum(float(r.get("gain_abs") or 0.0) for r in rows)
    total_purchase = sum(float(r.get("purchase_value") or 0.0) for r in rows)
    gain_pct = (total_gain_abs / total_purchase) if total_purchase else None

    return JSONResponse({
        "from": from_date,
        "to": to_date,
        "twr": None,
        "irr": None,
        "gain_abs": total_gain_abs,
        "gain_pct": gain_pct,
    })


@router.get("/status")
async def app_status(pool: asyncpg.Pool = Depends(get_pool)) -> JSONResponse:
    """Application and pipeline status."""
    async with pool.acquire() as conn:
        data = await get_status(conn)

    last_update = data.get("last_file_update")
    pipeline_status = data.get("pipeline_status") or "idle"

    return JSONResponse({
        "last_file_update": last_update.isoformat() if last_update else None,
        "pipeline_status": pipeline_status,
        "version": "0.1.0",
    })


@router.get("/news-prompt")
async def news_prompt() -> JSONResponse:
    """Placeholder for AI news context prompt generation."""
    return JSONResponse({"prompt": ""})
