"""Pipeline scheduler — orchestrates all background pipeline stages.

Responsibilities:
  1. Watches a ``.portfolio`` file for mtime changes via :class:`FileWatcher`.
  2. On change: runs the full pipeline
     parse → ingest → sync → enrich (FX + prices) → metrics → snapshots.
  3. Runs a periodic enrichment cycle (live price refresh) on a configurable
     schedule, independent of file changes.
  4. Prevents overlapping runs with an :class:`asyncio.Lock`.
  5. Publishes ``pipeline-status`` and ``data-updated`` events at each stage
     via the :class:`~app.api.events.EventBus`.

Architecture notes:
  - Uses ``asyncio.TaskGroup`` (Python 3.11+) to run watch and enrich loops
    concurrently.  Both loops swallow non-cancellation exceptions internally
    so a transient error in one loop never brings down the other.
  - Stage functions are imported lazily (inside methods) to avoid circular
    imports at module load time.
  - The scheduler does **not** manage DB connections; it acquires them from
    the pool per stage to keep transactions short-lived.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

    from app.api.events import EventBus

__all__ = ["PipelineScheduler"]

_LOGGER = logging.getLogger(__name__)


class PipelineScheduler:
    """Background orchestrator for the PP Reader data pipeline.

    Parameters
    ----------
    portfolio_path:
        Filesystem path to the ``.portfolio`` binary file.
    pool:
        Active asyncpg connection pool.
    event_bus:
        :class:`~app.api.events.EventBus` for broadcasting status events.
    poll_interval:
        Seconds between file-mtime checks (default 60).
    enrich_interval:
        Seconds between periodic enrichment-only runs (default 3600).
    """

    def __init__(
        self,
        portfolio_path: Path,
        pool: asyncpg.Pool,
        event_bus: EventBus,
        poll_interval: int = 60,
        enrich_interval: int = 3600,
    ) -> None:
        self._path = portfolio_path
        self._pool = pool
        self._bus = event_bus
        self._poll_interval = poll_interval
        self._enrich_interval = enrich_interval
        self._run_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn background tasks and return immediately."""
        if self._task is not None and not self._task.done():
            _LOGGER.warning("PipelineScheduler: already running, ignoring start()")
            return
        self._task = asyncio.create_task(self._main(), name="pipeline-scheduler")
        _LOGGER.info("PipelineScheduler: started (path=%s)", self._path)

    async def stop(self) -> None:
        """Cancel background tasks and wait for them to finish."""
        if self._task is None or self._task.done():
            return
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None
        _LOGGER.info("PipelineScheduler: stopped")

    # ------------------------------------------------------------------
    # Public trigger (for manual / test use)
    # ------------------------------------------------------------------

    async def trigger(self, trigger: str = "manual") -> None:
        """Manually kick off a full pipeline run."""
        await self._run_full_pipeline(trigger=trigger)

    # ------------------------------------------------------------------
    # Internal — main task
    # ------------------------------------------------------------------

    async def _main(self) -> None:
        """Entry point for the background task.

        Creates a :class:`asyncio.TaskGroup` with two forever-running loops:
        the file-change watch loop and the periodic enrichment loop.
        """
        from app.pipeline.watcher import FileWatcher

        watcher = FileWatcher(
            path=self._path,
            bus=self._bus,
            poll_interval=self._poll_interval,
        )
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._watch_loop(watcher), name="file-watcher")
                tg.create_task(self._enrich_loop(), name="enrichment-schedule")
        except* asyncio.CancelledError:
            raise asyncio.CancelledError from None

    # ------------------------------------------------------------------
    # Internal — watch loop
    # ------------------------------------------------------------------

    async def _watch_loop(self, watcher: Any) -> None:
        """Poll for file changes and trigger the full pipeline on change."""
        _LOGGER.info("PipelineScheduler: watch loop started for %s", self._path)
        while True:
            try:
                if watcher.check():
                    await self._run_full_pipeline(trigger="file_change")
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("PipelineScheduler: error in watch loop")
            await asyncio.sleep(self._poll_interval)

    # ------------------------------------------------------------------
    # Internal — enrichment loop
    # ------------------------------------------------------------------

    async def _enrich_loop(self) -> None:
        """Periodically re-enrich with live prices and recompute metrics."""
        _LOGGER.info(
            "PipelineScheduler: enrichment loop started (interval=%ds)",
            self._enrich_interval,
        )
        while True:
            await asyncio.sleep(self._enrich_interval)
            try:
                await self._run_enrichment_only(trigger="schedule")
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("PipelineScheduler: error in enrichment loop")

    # ------------------------------------------------------------------
    # Internal — orchestrated run helpers
    # ------------------------------------------------------------------

    async def _run_full_pipeline(self, trigger: str = "manual") -> None:
        """Execute the full pipeline: parse→ingest→sync→enrich→metrics→snapshots."""
        if self._run_lock.locked():
            _LOGGER.warning(
                "PipelineScheduler: pipeline already running, skipping (trigger=%s)",
                trigger,
            )
            return

        async with self._run_lock:
            self._bus.publish("pipeline-status", {"status": "running", "trigger": trigger})
            _LOGGER.info("PipelineScheduler: full pipeline starting (trigger=%s)", trigger)
            try:
                await self._stage_parse_ingest()
                await self._stage_sync()
                await self._stage_enrich()
                await self._stage_metrics_and_snapshots(trigger=trigger)
                self._bus.publish("data-updated", {"trigger": trigger})
                _LOGGER.info("PipelineScheduler: full pipeline complete (trigger=%s)", trigger)
            except asyncio.CancelledError:
                self._bus.publish(
                    "pipeline-status", {"status": "cancelled", "trigger": trigger}
                )
                raise
            except Exception:
                _LOGGER.exception(
                    "PipelineScheduler: pipeline failed (trigger=%s)", trigger
                )
                self._bus.publish(
                    "pipeline-status", {"status": "error", "trigger": trigger}
                )

    async def _run_enrichment_only(self, trigger: str = "schedule") -> None:
        """Execute enrichment + metrics + snapshots (no file re-parse)."""
        if self._run_lock.locked():
            _LOGGER.warning(
                "PipelineScheduler: pipeline already running, skipping enrichment (trigger=%s)",
                trigger,
            )
            return

        async with self._run_lock:
            self._bus.publish("pipeline-status", {"status": "enriching", "trigger": trigger})
            _LOGGER.info("PipelineScheduler: enrichment-only run (trigger=%s)", trigger)
            try:
                await self._stage_enrich()
                await self._stage_metrics_and_snapshots(trigger=trigger)
                self._bus.publish("data-updated", {"trigger": trigger})
                _LOGGER.info(
                    "PipelineScheduler: enrichment-only complete (trigger=%s)", trigger
                )
            except asyncio.CancelledError:
                self._bus.publish(
                    "pipeline-status", {"status": "cancelled", "trigger": trigger}
                )
                raise
            except Exception:
                _LOGGER.exception(
                    "PipelineScheduler: enrichment-only failed (trigger=%s)", trigger
                )
                self._bus.publish(
                    "pipeline-status", {"status": "error", "trigger": trigger}
                )

    # ------------------------------------------------------------------
    # Internal — individual pipeline stages
    # ------------------------------------------------------------------

    async def _stage_parse_ingest(self) -> None:
        """Parse the .portfolio file and write to staging tables."""
        from app.pipeline.ingestion import ingest
        from app.pipeline.parser import parse_portfolio_file

        self._bus.publish("pipeline-status", {"status": "parsing"})
        _LOGGER.info("PipelineScheduler: parsing %s", self._path)
        parsed = await parse_portfolio_file(self._path)

        self._bus.publish("pipeline-status", {"status": "ingesting"})
        _LOGGER.info("PipelineScheduler: ingesting")
        async with self._pool.acquire() as conn:
            await ingest(conn, parsed, file_path=str(self._path))

    async def _stage_sync(self) -> None:
        """Promote staging tables to canonical tables."""
        from app.pipeline.sync import sync_to_canonical

        self._bus.publish("pipeline-status", {"status": "syncing"})
        _LOGGER.info("PipelineScheduler: syncing to canonical")
        async with self._pool.acquire() as conn:
            await sync_to_canonical(conn)

    async def _stage_enrich(self) -> None:
        """Fetch FX rates and enqueue / process historical price jobs."""
        from app.enrichment.fx import discover_active_currencies, ensure_fx_rates
        from app.enrichment.history import plan_history_jobs, process_pending_jobs

        self._bus.publish("pipeline-status", {"status": "enriching-fx"})
        _LOGGER.info("PipelineScheduler: enriching FX rates")
        async with self._pool.acquire() as conn:
            currencies = await discover_active_currencies(conn)
            if currencies:
                await ensure_fx_rates(conn, currencies, {date.today()})

        self._bus.publish("pipeline-status", {"status": "enriching-prices"})
        _LOGGER.info("PipelineScheduler: enriching historical prices")
        async with self._pool.acquire() as conn:
            await plan_history_jobs(conn)
            await process_pending_jobs(conn)

    async def _stage_metrics_and_snapshots(self, trigger: str) -> None:
        """Run the metrics engine then build denormalized snapshots."""
        from app.metrics.engine import run_metrics
        from app.pipeline.snapshots import build_snapshots

        self._bus.publish("pipeline-status", {"status": "calculating-metrics"})
        _LOGGER.info("PipelineScheduler: running metrics")
        async with self._pool.acquire() as conn:
            run_uuid = await run_metrics(conn, trigger=trigger)

        self._bus.publish("pipeline-status", {"status": "building-snapshots"})
        _LOGGER.info("PipelineScheduler: building snapshots (run=%s)", run_uuid)
        async with self._pool.acquire() as conn:
            await build_snapshots(conn, run_uuid)
