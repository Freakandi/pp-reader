"""FastAPI application factory with lifespan — runs migrations and opens DB pool on startup."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import migrations, pool

# Frontend build output is placed at /app/static (one level above /app/backend)
_STATIC_DIR = Path(__file__).parent.parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: migrate DB, open pool, start scheduler on startup;
    stop scheduler and close pool on shutdown.
    """
    import app.dependencies as deps
    from app.api.events import EventBus
    from app.pipeline.scheduler import PipelineScheduler

    migrations.run_migrations()
    await pool.create_pool()

    # Create event bus and expose via DI.
    bus = EventBus()
    deps._event_bus = bus

    # Create and start the pipeline scheduler.
    scheduler = PipelineScheduler(
        portfolio_path=Path(settings.portfolio_path) if settings.portfolio_path else Path(""),
        pool=pool.get_pool(),
        event_bus=bus,
        poll_interval=settings.file_poll_interval,
        enrich_interval=settings.enrich_interval,
    )
    deps._scheduler = scheduler
    await scheduler.start()

    yield

    await scheduler.stop()
    deps._scheduler = None
    deps._event_bus = None
    await pool.close_pool()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    from app.api.routes import router
    from app.api.sse import sse_router

    app = FastAPI(
        title="PP Reader API",
        description="Portfolio Performance standalone reader API",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        """Health check endpoint."""
        return JSONResponse({"status": "ok"})

    app.include_router(router)
    app.include_router(sse_router)

    # Serve the Vite-built frontend as static files.
    # The SPA catch-all must come after all API routes.
    if _STATIC_DIR.is_dir():
        app.mount("/assets", StaticFiles(directory=str(_STATIC_DIR / "assets")), name="assets")

        @app.get("/{full_path:path}", include_in_schema=False)
        async def spa_fallback(full_path: str) -> FileResponse:
            """Return index.html for any unmatched path (SPA client-side routing)."""
            index = _STATIC_DIR / "index.html"
            return FileResponse(str(index))

    return app


if __name__ == "__main__":
    app = create_app()
    uvicorn.run(app, host=settings.host, port=settings.port)
