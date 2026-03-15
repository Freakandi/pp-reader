"""Programmatic Alembic runner — executes pending migrations at startup."""

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config

logger = logging.getLogger(__name__)

# alembic.ini lives at backend/alembic/alembic.ini
_ALEMBIC_INI = Path(__file__).parent.parent.parent / "alembic" / "alembic.ini"


def run_migrations() -> None:
    """Run all pending Alembic migrations (upgrade to head)."""
    logger.info("Running database migrations (alembic upgrade head)…")
    cfg = Config(str(_ALEMBIC_INI))
    command.upgrade(cfg, "head")
    logger.info("Database migrations complete.")
