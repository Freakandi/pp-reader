"""REST API and SSE endpoint tests."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# /healthz
# ---------------------------------------------------------------------------


def test_healthz_endpoint(client):
    """Test the /healthz health check endpoint."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# /api/dashboard
# ---------------------------------------------------------------------------


def test_dashboard_no_run(client):
    """Dashboard returns zero values when no metric run has completed."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value={
        "total_wealth_cents": 0,
        "portfolio_count": 0,
        "account_count": 0,
        "last_updated": None,
    })

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/dashboard")

    assert response.status_code == 200
    data = response.json()
    assert data["total_wealth"] == 0.0
    assert data["last_updated"] is None
    assert data["portfolio_count"] == 0
    assert data["account_count"] == 0


def test_dashboard_with_data(client):
    """Dashboard converts cents to EUR floats correctly."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value={
        "total_wealth_cents": 123456,   # €1234.56
        "portfolio_count": 2,
        "account_count": 3,
        "last_updated": _dt("2026-03-15T12:00:00"),
    })

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/dashboard")

    assert response.status_code == 200
    data = response.json()
    assert data["total_wealth"] == pytest.approx(1234.56)
    assert data["portfolio_count"] == 2
    assert data["account_count"] == 3
    assert data["last_updated"] is not None


# ---------------------------------------------------------------------------
# /api/accounts
# ---------------------------------------------------------------------------


def test_accounts_empty_when_no_run(client):
    """Accounts endpoint returns [] when no completed run exists."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)  # no completed run

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/accounts")

    assert response.status_code == 200
    assert response.json() == []


def test_accounts_returns_list(client):
    """Accounts endpoint returns correctly shaped account objects."""
    mock_conn = AsyncMock()

    async def fetchrow_side_effect(query, *args):
        # First call: get_latest_completed_run
        return {"run_uuid": "run-001"}

    async def fetch_side_effect(query, *args):
        return [
            {
                "uuid": "acc-1",
                "name": "Cash EUR",
                "currency": "EUR",
                "balance": 50000.0,   # already a float in account_snapshots
                "fx_unavailable": False,
                "is_deposit": True,
            }
        ]

    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_side_effect)
    mock_conn.fetch = AsyncMock(side_effect=fetch_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/accounts")

    assert response.status_code == 200
    accounts = response.json()
    assert len(accounts) == 1
    acc = accounts[0]
    assert acc["uuid"] == "acc-1"
    assert acc["name"] == "Cash EUR"
    assert acc["currency"] == "EUR"
    assert acc["balance"] == pytest.approx(50000.0)
    assert acc["is_deposit"] is True


# ---------------------------------------------------------------------------
# /api/portfolios
# ---------------------------------------------------------------------------


def test_portfolios_empty_when_no_run(client):
    """Portfolios endpoint returns [] when no completed run exists."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/portfolios")

    assert response.status_code == 200
    assert response.json() == []


def test_portfolios_returns_list(client):
    """Portfolios endpoint returns correctly shaped portfolio objects."""
    mock_conn = AsyncMock()

    async def fetchrow_side_effect(query, *args):
        return {"run_uuid": "run-001"}

    async def fetch_side_effect(query, *args):
        return [
            {
                "uuid": "port-1",
                "name": "Main Portfolio",
                "currency": "EUR",
                "current_value": 10000.0,
                "purchase_value": 8000.0,
                "gain_abs": 2000.0,
                "gain_pct": 0.25,
            }
        ]

    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_side_effect)
    mock_conn.fetch = AsyncMock(side_effect=fetch_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/portfolios")

    assert response.status_code == 200
    portfolios = response.json()
    assert len(portfolios) == 1
    p = portfolios[0]
    assert p["uuid"] == "port-1"
    assert p["name"] == "Main Portfolio"
    assert p["current_value"] == pytest.approx(10000.0)
    assert p["gain_pct"] == pytest.approx(0.25)


# ---------------------------------------------------------------------------
# /api/portfolios/{uuid}/positions
# ---------------------------------------------------------------------------


def test_positions_empty_when_no_run(client):
    """Positions endpoint returns [] when no completed run exists."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/portfolios/port-1/positions")

    assert response.status_code == 200
    assert response.json() == []


def test_positions_converts_bigint(client):
    """Positions endpoint converts 10^8-scaled BIGINT holdings correctly."""
    mock_conn = AsyncMock()

    async def fetchrow_side_effect(query, *args):
        return {"run_uuid": "run-001"}

    async def fetch_side_effect(query, *args):
        # 1.5 shares = 150_000_000 in 10^8 scale
        return [
            {
                "security_uuid": "sec-1",
                "security_name": "Apple Inc",
                "isin": "US0378331005",
                "ticker": "AAPL",
                "currency": "USD",
                "holdings_raw": 150_000_000,
                "current_value_cents": 22_500,      # €225.00
                "purchase_value_cents": 20_000,     # €200.00
                "purchase_security_value_raw": None,
                "gain_abs_cents": 2_500,            # €25.00
                "gain_pct": 0.125,
                "day_change_eur": 1.50,
                "day_change_pct": 0.0067,
                "coverage_ratio": 1.0,
                "fx_unavailable": False,
            }
        ]

    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_side_effect)
    mock_conn.fetch = AsyncMock(side_effect=fetch_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/portfolios/port-1/positions")

    assert response.status_code == 200
    positions = response.json()
    assert len(positions) == 1
    pos = positions[0]
    assert pos["security_uuid"] == "sec-1"
    assert pos["current_holdings"] == pytest.approx(1.5)
    assert pos["current_value"] == pytest.approx(225.0)
    assert pos["purchase_value"] == pytest.approx(200.0)
    assert pos["gain_abs"] == pytest.approx(25.0)
    assert pos["gain_pct"] == pytest.approx(0.125)


# ---------------------------------------------------------------------------
# /api/securities/{uuid}
# ---------------------------------------------------------------------------


def test_security_detail_not_found(client):
    """Security detail returns 404 when security doesn't exist."""
    mock_conn = AsyncMock()

    async def fetchrow_side_effect(query, *args):
        if "metric_runs" in query:
            return {"run_uuid": "run-001"}
        return None  # security not found

    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/securities/nonexistent-uuid")

    assert response.status_code == 404


def test_security_detail_returns_snapshot(client):
    """Security detail endpoint returns correctly shaped snapshot."""
    from datetime import date as dt_date
    mock_conn = AsyncMock()

    async def fetchrow_side_effect(query, *args):
        if "metric_runs" in query:
            return {"run_uuid": "run-001"}
        return {
            "uuid": "sec-1",
            "name": "Apple Inc",
            "isin": "US0378331005",
            "ticker": "AAPL",
            "currency": "USD",
            "last_price": 15_000_000_000,   # $150.00 in 10^8
            "last_price_date": dt_date(2026, 3, 14),
            "holdings_raw": 100_000_000,    # 1 share
            "current_value_cents": 15_000,  # €150.00
            "purchase_value_cents": 13_000, # €130.00
            "purchase_security_value_raw": None,
            "gain_abs_cents": 2_000,
            "gain_pct": 0.1538,
            "day_change_eur": 0.50,
            "day_change_pct": 0.0033,
            "coverage_ratio": 1.0,
            "fx_unavailable": False,
        }

    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/securities/sec-1")

    assert response.status_code == 200
    snap = response.json()
    assert snap["uuid"] == "sec-1"
    assert snap["name"] == "Apple Inc"
    assert snap["latest_price"] == pytest.approx(150.0)
    assert snap["current_holdings"] == pytest.approx(1.0)
    assert snap["current_value"] == pytest.approx(150.0)
    assert snap["gain_abs"] == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# /api/securities/{uuid}/history
# ---------------------------------------------------------------------------


def test_security_history_invalid_range(client):
    """History endpoint returns 400 for unknown range key."""
    response = client.get("/api/securities/sec-1/history?range=INVALID")
    assert response.status_code == 400


def test_security_history_returns_points(client):
    """History endpoint returns sorted price points."""
    mock_conn = AsyncMock()

    async def fetch_side_effect(query, *args):
        return [
            {"date": "2026-01-01", "close": 14_000_000_000},  # $140
            {"date": "2026-02-01", "close": 15_000_000_000},  # $150
        ]

    mock_conn.fetch = AsyncMock(side_effect=fetch_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/securities/sec-1/history?range=1Y")

    assert response.status_code == 200
    hist = response.json()
    assert hist["uuid"] == "sec-1"
    assert hist["range"] == "1Y"
    assert len(hist["points"]) == 2
    assert hist["points"][0]["value"] == pytest.approx(140.0)
    assert hist["points"][1]["value"] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# /api/wealth/daily
# ---------------------------------------------------------------------------


def test_wealth_daily_returns_series(client):
    """Wealth series endpoint returns points with correct EUR conversion."""
    mock_conn = AsyncMock()

    async def fetch_side_effect(query, *args):
        return [
            {"date": "2026-01-01", "total_wealth_cents": 100_000},  # €1000
            {"date": "2026-02-01", "total_wealth_cents": 110_000},  # €1100
        ]

    mock_conn.fetch = AsyncMock(side_effect=fetch_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/wealth/daily?from=2026-01-01&to=2026-03-01")

    assert response.status_code == 200
    series = response.json()
    assert series["from"] == "2026-01-01"
    assert series["to"] == "2026-03-01"
    assert len(series["points"]) == 2
    assert series["points"][0]["value"] == pytest.approx(1000.0)
    assert series["points"][1]["value"] == pytest.approx(1100.0)


# ---------------------------------------------------------------------------
# /api/trades
# ---------------------------------------------------------------------------


def test_trades_returns_list(client):
    """Trades endpoint converts raw BIGINT amounts to floats."""
    mock_conn = AsyncMock()

    async def fetch_side_effect(query, *args):
        return [
            {
                "uuid": "txn-1",
                "portfolio_uuid": "port-1",
                "portfolio_name": "Main",
                "security_uuid": "sec-1",
                "security_name": "Apple",
                "type": 0,              # BUY
                "date": "2026-01-15",
                "shares": 100_000_000,  # 1 share in 10^8
                "amount": 15_000,       # €150 in cents
                "currency": "EUR",
                "fees": 100,            # €1 in cents
            }
        ]

    mock_conn.fetch = AsyncMock(side_effect=fetch_side_effect)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/trades")

    assert response.status_code == 200
    trades = response.json()
    assert len(trades) == 1
    t = trades[0]
    assert t["uuid"] == "txn-1"
    assert t["type"] == "BUY"
    assert t["shares"] == pytest.approx(1.0)
    assert t["value"] == pytest.approx(150.0)
    assert t["fees"] == pytest.approx(1.0)
    assert t["price"] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# /api/performance
# ---------------------------------------------------------------------------


def test_performance_no_run(client):
    """Performance endpoint returns nulls when no data is available."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/performance?from=2026-01-01&to=2026-03-15")

    assert response.status_code == 200
    perf = response.json()
    assert perf["from"] == "2026-01-01"
    assert perf["to"] == "2026-03-15"
    assert perf["gain_abs"] is None
    assert perf["twr"] is None


# ---------------------------------------------------------------------------
# /api/status
# ---------------------------------------------------------------------------


def test_status_returns_idle_when_no_runs(client):
    """Status endpoint returns idle pipeline_status when no runs exist."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value={
        "last_file_update": None,
        "pipeline_status": None,
    })

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/status")

    assert response.status_code == 200
    status = response.json()
    assert status["pipeline_status"] == "idle"
    assert status["last_file_update"] is None
    assert status["version"] == "0.1.0"


def test_status_returns_completed(client):
    """Status endpoint reflects the latest pipeline run state."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value={
        "last_file_update": _dt("2026-03-15T10:00:00"),
        "pipeline_status": "completed",
    })

    with patch("app.db.pool.get_pool") as mock_pool:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.return_value = pool

        response = client.get("/api/status")

    assert response.status_code == 200
    status = response.json()
    assert status["pipeline_status"] == "completed"
    assert status["last_file_update"] is not None


# ---------------------------------------------------------------------------
# /api/news-prompt
# ---------------------------------------------------------------------------


def test_news_prompt_returns_placeholder(client):
    """News-prompt endpoint returns an empty prompt string."""
    response = client.get("/api/news-prompt")
    assert response.status_code == 200
    assert response.json() == {"prompt": ""}


# ---------------------------------------------------------------------------
# /api/events (SSE)
# ---------------------------------------------------------------------------


def test_sse_events_content_type(client):
    """SSE endpoint responds with text/event-stream content type and keep-alive comment."""
    from app.api.events import EventBus

    bus = EventBus()

    # Replace the infinite generator with a finite one so the test doesn't hang.
    async def _finite_stream(_bus: EventBus):
        yield ": connected\n\n"

    with patch("app.api.sse._event_stream", side_effect=_finite_stream):
        with patch("app.dependencies.get_event_bus", return_value=bus):
            response = client.get("/api/events")

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert ": connected" in response.text
