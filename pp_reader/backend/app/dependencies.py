"""FastAPI dependency injection providers.

Module-level singletons are set by ``main.py`` during the application
lifespan.  Route handlers and other FastAPI dependencies should call
these functions via ``Depends()`` rather than importing singletons directly.

Usage::

    from fastapi import Depends
    from app.dependencies import get_event_bus, get_pool, get_scheduler

    @app.get("/api/status")
    async def status(bus: EventBus = Depends(get_event_bus)):
        ...
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

    from app.api.events import EventBus
    from app.pipeline.scheduler import PipelineScheduler

__all__ = ["get_event_bus", "get_pool", "get_scheduler"]

# Set by main.py lifespan — do not import or assign outside of that context.
_event_bus: EventBus | None = None
_scheduler: PipelineScheduler | None = None


def get_pool() -> asyncpg.Pool:
    """Return the active asyncpg connection pool.

    Raises :exc:`RuntimeError` if the pool has not been initialised.
    """
    from app.db.pool import get_pool as _get_pool

    return _get_pool()


def get_event_bus() -> EventBus:
    """Return the active :class:`~app.api.events.EventBus` singleton.

    Raises :exc:`RuntimeError` if the bus has not been initialised.
    """
    if _event_bus is None:
        raise RuntimeError("EventBus not initialized — lifespan not started")
    return _event_bus


def get_scheduler() -> PipelineScheduler:
    """Return the active :class:`~app.pipeline.scheduler.PipelineScheduler`.

    Raises :exc:`RuntimeError` if the scheduler has not been initialised.
    """
    if _scheduler is None:
        raise RuntimeError("PipelineScheduler not initialized — lifespan not started")
    return _scheduler
