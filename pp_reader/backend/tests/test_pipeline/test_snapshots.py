"""Tests for the snapshot builder pipeline (Phase 8).

All tests use mock asyncpg connections — no live database required.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from app.pipeline.snapshots import (
    build_account_snapshots,
    build_daily_wealth,
    build_portfolio_snapshots,
    build_snapshots,
)


# ── Fixtures ──────────────────────────────────────────────────────────

def _make_conn(
    portfolio_metrics: list[dict] | None = None,
    account_metrics: list[dict] | None = None,
    security_metrics: list[dict] | None = None,
) -> AsyncMock:
    """Return a mock asyncpg connection with fetch/execute wired up.

    The DB query helpers do ``[dict(r) for r in rows]`` on the fetch result,
    so we return plain dicts — ``dict(d)`` on a dict is a no-op copy.
    """
    conn = AsyncMock()

    # Default empty lists
    _pm = portfolio_metrics or []
    _am = account_metrics or []
    _sm = security_metrics or []

    async def _fetch(sql, *args):
        sql_stripped = sql.strip()
        if "portfolio_metrics" in sql_stripped and "JOIN portfolios" in sql_stripped:
            return list(_pm)
        if "account_metrics" in sql_stripped and "JOIN accounts" in sql_stripped:
            return list(_am)
        if "security_metrics" in sql_stripped and "JOIN securities" in sql_stripped:
            return list(_sm)
        return []

    conn.fetch.side_effect = _fetch
    conn.execute = AsyncMock(return_value=None)
    return conn


def _pm_row(
    portfolio_uuid: str = "P1",
    name: str = "Test Portfolio",
    valuation_currency: str = "EUR",
    current_value_cents: int = 100,  # €1.00
    purchase_value_cents: int = 80,  # €0.80
    gain_abs_cents: int = 20,
    gain_pct: float = 0.25,
    total_change_eur_cents: int = 20,
    total_change_pct: float = 0.25,
    position_count: int = 2,
    missing_value_positions: int = 0,
    source: str = "metrics_engine",
    coverage_ratio: float = 1.0,
    provenance: dict | None = None,
) -> dict:
    return {
        "portfolio_uuid": portfolio_uuid,
        "name": name,
        "valuation_currency": valuation_currency,
        "current_value_cents": current_value_cents,
        "purchase_value_cents": purchase_value_cents,
        "gain_abs_cents": gain_abs_cents,
        "gain_pct": gain_pct,
        "total_change_eur_cents": total_change_eur_cents,
        "total_change_pct": total_change_pct,
        "position_count": position_count,
        "missing_value_positions": missing_value_positions,
        "source": source,
        "coverage_ratio": coverage_ratio,
        "provenance": json.dumps(provenance or {}),
    }


def _am_row(
    account_uuid: str = "A1",
    name: str = "Test Account",
    currency_code: str = "EUR",
    valuation_currency: str = "EUR",
    balance_native_cents: int = 500,  # €5.00
    balance_eur_cents: int | None = 500,
    fx_rate: int | None = None,
    fx_rate_source: str | None = None,
    fx_rate_timestamp: datetime | None = None,
    coverage_ratio: float = 1.0,
    provenance: dict | None = None,
) -> dict:
    return {
        "account_uuid": account_uuid,
        "name": name,
        "currency_code": currency_code,
        "valuation_currency": valuation_currency,
        "balance_native_cents": balance_native_cents,
        "balance_eur_cents": balance_eur_cents,
        "fx_rate": fx_rate,
        "fx_rate_source": fx_rate_source,
        "fx_rate_timestamp": fx_rate_timestamp,
        "coverage_ratio": coverage_ratio,
        "provenance": json.dumps(provenance or {}),
    }


def _sm_row(
    portfolio_uuid: str = "P1",
    security_uuid: str = "S1",
    name: str = "Apple Inc",
    isin: str = "US0378331005",
    ticker_symbol: str = "AAPL",
    security_currency_code: str = "USD",
    valuation_currency: str = "EUR",
    holdings_raw: int = 10 * 10**8,  # 10 shares
    current_value_cents: int = 80,   # €0.80
    purchase_value_cents: int = 60,  # €0.60
    gain_abs_cents: int = 20,
    gain_pct: float = 0.333,
    total_change_eur_cents: int = 20,
    total_change_pct: float = 0.333,
    coverage_ratio: float = 1.0,
    day_change_native: float | None = 1.5,
    day_change_eur: float | None = 1.3,
    day_change_pct: float | None = 0.012,
    provenance: dict | None = None,
) -> dict:
    return {
        "portfolio_uuid": portfolio_uuid,
        "security_uuid": security_uuid,
        "name": name,
        "isin": isin,
        "ticker_symbol": ticker_symbol,
        "security_currency_code": security_currency_code,
        "valuation_currency": valuation_currency,
        "holdings_raw": holdings_raw,
        "current_value_cents": current_value_cents,
        "purchase_value_cents": purchase_value_cents,
        "gain_abs_cents": gain_abs_cents,
        "gain_pct": gain_pct,
        "total_change_eur_cents": total_change_eur_cents,
        "total_change_pct": total_change_pct,
        "coverage_ratio": coverage_ratio,
        "day_change_native": day_change_native,
        "day_change_eur": day_change_eur,
        "day_change_pct": day_change_pct,
        "provenance": json.dumps(provenance or {
            "irr": 0.12,
            "fifo_cost": 60,
            "realized_gains_fifo": 0,
            "dividend_total": 2,
        }),
    }


# ── build_portfolio_snapshots ─────────────────────────────────────────

class TestBuildPortfolioSnapshots:
    @pytest.mark.asyncio
    async def test_empty_run(self):
        conn = _make_conn()
        result = await build_portfolio_snapshots(conn, "run-001")
        assert result == []
        # execute should NOT be called (no snapshots to write)
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_portfolio_snapshot(self):
        pm = _pm_row()
        sm = _sm_row()
        conn = _make_conn(portfolio_metrics=[pm], security_metrics=[sm])
        snap_at = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)

        result = await build_portfolio_snapshots(conn, "run-001", snapshot_at=snap_at)

        assert len(result) == 1
        snap = result[0]
        assert snap["portfolio_uuid"] == "P1"
        assert snap["name"] == "Test Portfolio"
        assert snap["current_value"] == pytest.approx(1.00)  # 100 cents → €1
        assert snap["purchase_sum"] == pytest.approx(0.80)
        assert snap["gain_abs"] == pytest.approx(0.20)
        assert snap["gain_pct"] == pytest.approx(0.25)
        assert snap["position_count"] == 2
        assert snap["coverage_ratio"] == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_upsert_called_with_correct_args(self):
        pm = _pm_row()
        conn = _make_conn(portfolio_metrics=[pm])
        snap_at = datetime(2026, 3, 15, tzinfo=timezone.utc)

        await build_portfolio_snapshots(conn, "run-001", snapshot_at=snap_at)

        assert conn.execute.call_count == 1
        call_args = conn.execute.call_args[0]
        # First arg is SQL, subsequent are positional params
        sql = call_args[0]
        assert "portfolio_snapshots" in sql
        # run_uuid is param[1]
        assert call_args[1] == "run-001"
        # portfolio_uuid is param[2]
        assert call_args[2] == "P1"
        # snapshot_at is param[3]
        assert call_args[3] == snap_at

    @pytest.mark.asyncio
    async def test_has_current_value_false_when_missing_positions(self):
        pm = _pm_row(
            current_value_cents=0,
            missing_value_positions=2,
        )
        conn = _make_conn(portfolio_metrics=[pm])
        await build_portfolio_snapshots(conn, "run-001")
        # has_current_value should be False — check the SQL execute call params
        call_args = conn.execute.call_args[0]
        # has_current_value is the 15th param (index 14 after SQL at index 0)
        has_current_value = call_args[14]
        assert has_current_value is False

    @pytest.mark.asyncio
    async def test_positions_in_payload(self):
        pm = _pm_row()
        sm = _sm_row()
        conn = _make_conn(portfolio_metrics=[pm], security_metrics=[sm])
        await build_portfolio_snapshots(conn, "run-001")

        call_args = conn.execute.call_args[0]
        # payload is the last param (index 18 after SQL at index 0)
        payload_json = call_args[18]
        payload = json.loads(payload_json)

        assert "positions" in payload
        assert len(payload["positions"]) == 1
        pos = payload["positions"][0]
        assert pos["security_uuid"] == "S1"
        assert pos["name"] == "Apple Inc"
        assert pos["ticker"] == "AAPL"
        assert pos["holdings"] == pytest.approx(10.0)  # 10 * 10^8 / 10^8
        assert pos["current_value"] == pytest.approx(0.80)   # 80 cents → €0.80
        assert pos["gain_abs"] == pytest.approx(0.20)          # 20 cents → €0.20
        assert pos["day_change_pct"] == pytest.approx(0.012)

    @pytest.mark.asyncio
    async def test_multiple_portfolios(self):
        pm1 = _pm_row(portfolio_uuid="P1", name="Portfolio A", current_value_cents=100_00)
        pm2 = _pm_row(portfolio_uuid="P2", name="Portfolio B", current_value_cents=200_00)
        conn = _make_conn(portfolio_metrics=[pm1, pm2])

        result = await build_portfolio_snapshots(conn, "run-002")

        assert len(result) == 2
        assert conn.execute.call_count == 2
        uuids = {s["portfolio_uuid"] for s in result}
        assert uuids == {"P1", "P2"}

    @pytest.mark.asyncio
    async def test_positions_separated_by_portfolio(self):
        """Positions must only appear in their own portfolio's payload."""
        pm1 = _pm_row(portfolio_uuid="P1", name="Portfolio A")
        pm2 = _pm_row(portfolio_uuid="P2", name="Portfolio B")
        sm1 = _sm_row(portfolio_uuid="P1", security_uuid="S1")
        sm2 = _sm_row(portfolio_uuid="P2", security_uuid="S2")
        conn = _make_conn(portfolio_metrics=[pm1, pm2], security_metrics=[sm1, sm2])

        await build_portfolio_snapshots(conn, "run-003")

        calls = conn.execute.call_args_list
        assert len(calls) == 2

        for c in calls:
            args = c[0]
            p_uuid = args[2]
            payload = json.loads(args[18])
            positions = payload["positions"]
            assert all(pos["security_uuid"] != "S2" for pos in positions) if p_uuid == "P1" else \
                   all(pos["security_uuid"] != "S1" for pos in positions)

    @pytest.mark.asyncio
    async def test_default_snapshot_at_is_recent(self):
        pm = _pm_row()
        conn = _make_conn(portfolio_metrics=[pm])
        before = datetime.now(tz=timezone.utc)
        await build_portfolio_snapshots(conn, "run-001")
        after = datetime.now(tz=timezone.utc)

        call_args = conn.execute.call_args[0]
        snap_at = call_args[3]
        assert before <= snap_at <= after


# ── build_account_snapshots ───────────────────────────────────────────

class TestBuildAccountSnapshots:
    @pytest.mark.asyncio
    async def test_empty_run(self):
        conn = _make_conn()
        result = await build_account_snapshots(conn, "run-001")
        assert result == []

    @pytest.mark.asyncio
    async def test_eur_account_no_fx(self):
        am = _am_row(currency_code="EUR", valuation_currency="EUR", balance_native_cents=500)
        conn = _make_conn(account_metrics=[am])
        snap_at = datetime(2026, 3, 15, tzinfo=timezone.utc)

        result = await build_account_snapshots(conn, "run-001", snapshot_at=snap_at)

        assert len(result) == 1
        snap = result[0]
        assert snap["account_uuid"] == "A1"
        assert snap["currency_code"] == "EUR"
        assert snap["orig_balance"] == pytest.approx(5.0)  # 500 cents → €5
        assert snap["balance"] == pytest.approx(5.0)
        assert snap["fx_unavailable"] is False

    @pytest.mark.asyncio
    async def test_foreign_currency_account_with_fx(self):
        am = _am_row(
            currency_code="USD",
            valuation_currency="EUR",
            balance_native_cents=1000,  # $10.00
            balance_eur_cents=920,      # €9.20
            fx_rate=92_000_000,         # 0.92 * 10^8
            fx_rate_source="ecb",
        )
        conn = _make_conn(account_metrics=[am])

        result = await build_account_snapshots(conn, "run-001")

        snap = result[0]
        assert snap["orig_balance"] == pytest.approx(10.0)  # 1000 cents → $10
        assert snap["balance"] == pytest.approx(9.20)       # 920 cents → €9.20
        assert snap["fx_unavailable"] is False
        assert snap["currency_code"] == "USD"

    @pytest.mark.asyncio
    async def test_foreign_currency_account_no_fx_available(self):
        am = _am_row(
            currency_code="JPY",
            valuation_currency="EUR",
            balance_native_cents=100000,
            balance_eur_cents=None,
            fx_rate=None,
        )
        conn = _make_conn(account_metrics=[am])

        result = await build_account_snapshots(conn, "run-001")

        snap = result[0]
        assert snap["balance"] is None
        assert snap["fx_unavailable"] is True

    @pytest.mark.asyncio
    async def test_upsert_called(self):
        am = _am_row()
        conn = _make_conn(account_metrics=[am])
        await build_account_snapshots(conn, "run-001")
        assert conn.execute.call_count == 1
        call_args = conn.execute.call_args[0]
        assert "account_snapshots" in call_args[0]

    @pytest.mark.asyncio
    async def test_multiple_accounts(self):
        ams = [
            _am_row(account_uuid="A1", name="Checking"),
            _am_row(account_uuid="A2", name="Savings"),
        ]
        conn = _make_conn(account_metrics=ams)
        result = await build_account_snapshots(conn, "run-001")
        assert len(result) == 2
        assert conn.execute.call_count == 2


# ── build_daily_wealth ────────────────────────────────────────────────

class TestBuildDailyWealth:
    @pytest.mark.asyncio
    async def test_empty_run(self):
        conn = _make_conn()
        result = await build_daily_wealth(conn, "run-001")
        # Only the "all" global row
        assert len(result) == 1
        assert result[0]["scope_type"] == "global"
        assert result[0]["total_wealth_cents"] == 0

    @pytest.mark.asyncio
    async def test_single_portfolio_wealth(self):
        pm = _pm_row(current_value_cents=10000, purchase_value_cents=8000)
        conn = _make_conn(portfolio_metrics=[pm])
        target_date = date(2026, 3, 15)

        result = await build_daily_wealth(conn, "run-001", snapshot_date=target_date)

        # Should have portfolio row + global row
        assert len(result) == 2
        portfolio_row = next(r for r in result if r["scope_type"] == "portfolio")
        global_row = next(r for r in result if r["scope_type"] == "global")

        assert portfolio_row["scope_uuid"] == "P1"
        assert portfolio_row["total_wealth_cents"] == 10000
        assert portfolio_row["total_invested_cents"] == 8000
        assert portfolio_row["date"] == "2026-03-15"

        assert global_row["scope_uuid"] == "all"
        assert global_row["total_wealth_cents"] == 10000  # same as portfolio (no accounts)
        assert global_row["total_invested_cents"] == 8000

    @pytest.mark.asyncio
    async def test_global_includes_account_cash(self):
        pm = _pm_row(current_value_cents=10000, purchase_value_cents=8000)
        am = _am_row(balance_native_cents=500, balance_eur_cents=500)
        conn = _make_conn(portfolio_metrics=[pm], account_metrics=[am])

        result = await build_daily_wealth(conn, "run-001")

        global_row = next(r for r in result if r["scope_type"] == "global")
        # Portfolio 10000 cents + account 500 cents = 10500 cents
        assert global_row["total_wealth_cents"] == 10500
        # Invested does NOT include account cash
        assert global_row["total_invested_cents"] == 8000

    @pytest.mark.asyncio
    async def test_global_excludes_account_with_no_eur_balance(self):
        """Accounts with no fx conversion contribute 0 to global wealth."""
        pm = _pm_row(current_value_cents=10000, purchase_value_cents=8000)
        am = _am_row(
            currency_code="JPY",
            balance_eur_cents=None,
        )
        conn = _make_conn(portfolio_metrics=[pm], account_metrics=[am])

        result = await build_daily_wealth(conn, "run-001")

        global_row = next(r for r in result if r["scope_type"] == "global")
        assert global_row["total_wealth_cents"] == 10000  # no account contribution

    @pytest.mark.asyncio
    async def test_multiple_portfolios_summed(self):
        pm1 = _pm_row(portfolio_uuid="P1", current_value_cents=5000, purchase_value_cents=4000)
        pm2 = _pm_row(portfolio_uuid="P2", current_value_cents=3000, purchase_value_cents=2500)
        conn = _make_conn(portfolio_metrics=[pm1, pm2])

        result = await build_daily_wealth(conn, "run-001")

        global_row = next(r for r in result if r["scope_type"] == "global")
        assert global_row["total_wealth_cents"] == 8000
        assert global_row["total_invested_cents"] == 6500

    @pytest.mark.asyncio
    async def test_upsert_called_for_each_row(self):
        pm1 = _pm_row(portfolio_uuid="P1")
        pm2 = _pm_row(portfolio_uuid="P2")
        conn = _make_conn(portfolio_metrics=[pm1, pm2])

        await build_daily_wealth(conn, "run-001")

        # 2 portfolio rows + 1 global row
        assert conn.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_default_date_is_today(self):
        pm = _pm_row()
        conn = _make_conn(portfolio_metrics=[pm])
        today = date.today()
        result = await build_daily_wealth(conn, "run-001")
        portfolio_row = next(r for r in result if r["scope_type"] == "portfolio")
        assert portfolio_row["date"] == str(today)


# ── build_snapshots (integration) ────────────────────────────────────

class TestBuildSnapshots:
    @pytest.mark.asyncio
    async def test_orchestrates_all_builders(self):
        pm = _pm_row()
        am = _am_row()
        sm = _sm_row()
        conn = _make_conn(portfolio_metrics=[pm], account_metrics=[am], security_metrics=[sm])
        snap_at = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
        snap_date = date(2026, 3, 15)

        result = await build_snapshots(
            conn, "run-001", snapshot_at=snap_at, snapshot_date=snap_date,
        )

        assert "portfolios" in result
        assert "accounts" in result
        assert "daily_wealth" in result
        assert len(result["portfolios"]) == 1
        assert len(result["accounts"]) == 1
        # 1 portfolio row + 1 global row
        assert len(result["daily_wealth"]) == 2

    @pytest.mark.asyncio
    async def test_empty_run_returns_empty_lists(self):
        conn = _make_conn()
        result = await build_snapshots(conn, "run-empty")
        assert result["portfolios"] == []
        assert result["accounts"] == []
        # global wealth row always present
        assert len(result["daily_wealth"]) == 1

    @pytest.mark.asyncio
    async def test_snapshot_date_derived_from_snapshot_at(self):
        """When snapshot_date is omitted, it defaults to snapshot_at.date()."""
        pm = _pm_row()
        conn = _make_conn(portfolio_metrics=[pm])
        snap_at = datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc)

        result = await build_snapshots(conn, "run-001", snapshot_at=snap_at)

        wealth_rows = result["daily_wealth"]
        portfolio_row = next(r for r in wealth_rows if r["scope_type"] == "portfolio")
        assert portfolio_row["date"] == "2026-06-01"
