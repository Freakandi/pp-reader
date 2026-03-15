"""SSE (Server-Sent Events) endpoint for real-time pipeline and data events.

Architecture Decision 5: REST + SSE for real-time client updates.

Clients connect to GET /api/events and receive a stream formatted per the
SSE specification (https://html.spec.whatwg.org/multipage/server-sent-events.html):

    event: <event-type>\n
    data: <json-payload>\n
    \n

Event types:
  ``data-updated``     — canonical DB data changed; clients should re-fetch
  ``pipeline-status``  — progress update published during a pipeline run

The stream is backed by the :class:`~app.api.events.EventBus` subscriber
queue.  Slow clients silently lose events (queue max 100) rather than
causing memory growth.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from app.api.events import EventBus
from app.dependencies import get_event_bus

__all__ = ["sse_router"]

_LOGGER = logging.getLogger(__name__)

sse_router = APIRouter()


async def _event_stream(bus: EventBus) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted strings from the EventBus until the client disconnects."""
    # Immediately send an SSE comment so the browser/client knows the stream is live.
    yield ": connected\n\n"

    async with bus.subscribe() as stream:
        async for event in stream:
            event_type = event.get("event", "message")
            data = event.get("data", {})
            payload = json.dumps(data, default=str)
            yield f"event: {event_type}\ndata: {payload}\n\n"


@sse_router.get("/api/events")
async def sse_events(bus: EventBus = Depends(get_event_bus)) -> StreamingResponse:
    """Real-time event stream (Server-Sent Events).

    Connect once; the stream delivers ``data-updated`` and
    ``pipeline-status`` events as they are published by the pipeline
    scheduler.  The connection should be kept open by the client and
    will be closed when the server shuts down.
    """
    return StreamingResponse(
        _event_stream(bus),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
