"""Event bus — asyncio.Queue-backed pub/sub for SSE and WebSocket consumers.

Each subscriber gets its own bounded asyncio.Queue (max 100 items).
Slow consumers lose events rather than causing unbounded memory growth.

Event types (Architecture Decision 5):
  ``data-updated``     — canonical data changed; clients should refresh
  ``pipeline-status``  — progress update during a pipeline run
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

__all__ = ["EventBus"]

_LOGGER = logging.getLogger(__name__)

_SENTINEL: object = object()  # signals end-of-stream to _drain generators


class EventBus:
    """Publish/subscribe hub backed by per-subscriber :class:`asyncio.Queue` instances.

    Usage::

        bus = EventBus()

        # Publisher
        bus.publish("data-updated", {"trigger": "file_change"})

        # Subscriber (e.g. in an SSE handler)
        async with bus.subscribe() as stream:
            async for event in stream:
                # event = {"event": "data-updated", "data": {...}}
                yield event
    """

    def __init__(self) -> None:
        self._queues: list[asyncio.Queue[Any]] = []

    # ------------------------------------------------------------------
    # Publisher interface
    # ------------------------------------------------------------------

    def publish(self, event: str, data: dict[str, Any] | None = None) -> None:
        """Broadcast *event* with optional *data* to all active subscribers.

        Non-blocking.  Drops the event (with a warning) for any subscriber
        whose queue is already at capacity.
        """
        payload: dict[str, Any] = {"event": event, "data": data or {}}
        for q in list(self._queues):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                _LOGGER.warning("EventBus: queue full, dropping event=%s", event)

    # ------------------------------------------------------------------
    # Subscriber interface
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[AsyncGenerator[dict[str, Any], None]]:
        """Async context manager that yields an async event stream.

        The stream terminates automatically when the context block exits.

        Example::

            async with bus.subscribe() as stream:
                async for event in stream:
                    print(event["event"], event["data"])
        """
        q: asyncio.Queue[Any] = asyncio.Queue(maxsize=100)
        self._queues.append(q)
        try:
            yield self._drain(q)
        finally:
            self._queues.remove(q)
            # Signal the drain generator to stop iterating.
            try:
                q.put_nowait(_SENTINEL)
            except asyncio.QueueFull:
                pass  # generator will be GC'd anyway

    @staticmethod
    async def _drain(q: asyncio.Queue[Any]) -> AsyncGenerator[dict[str, Any], None]:
        """Yield events from *q* until the sentinel value is received."""
        while True:
            item = await q.get()
            if item is _SENTINEL:
                return
            yield item
