"""Shared test fixtures for backend tests."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def app():
    """Create a test FastAPI application instance with DB lifecycle mocked."""
    return create_app()


@pytest.fixture
def client(app):
    """Create a test client with DB migrations, pool, and scheduler mocked out."""
    with (
        patch("app.db.migrations.run_migrations", return_value=None),
        patch("app.db.pool.create_pool", new_callable=AsyncMock),
        patch("app.db.pool.close_pool", new_callable=AsyncMock),
        patch("app.db.pool.get_pool", return_value=AsyncMock()),
        patch(
            "app.pipeline.scheduler.PipelineScheduler.start",
            new_callable=AsyncMock,
        ),
        patch(
            "app.pipeline.scheduler.PipelineScheduler.stop",
            new_callable=AsyncMock,
        ),
    ):
        with TestClient(app) as c:
            yield c
