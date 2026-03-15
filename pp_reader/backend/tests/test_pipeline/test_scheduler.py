"""Tests for Phase 9 — Pipeline Orchestration.

Covers:
  - EventBus publish/subscribe mechanics
  - FileWatcher mtime-based change detection
  - PipelineScheduler orchestration (stages, events, error handling)

All tests use mocks — no live database or filesystem required.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.api.events import EventBus
from app.pipeline.watcher import FileWatcher


# ═══════════════════════════════════════════════════════════════════════
# EventBus tests
# ═══════════════════════════════════════════════════════════════════════


class TestEventBus:
    def test_publish_with_no_subscribers_does_not_raise(self) -> None:
        bus = EventBus()
        bus.publish("data-updated")  # should not raise

    async def test_subscriber_receives_published_event(self) -> None:
        bus = EventBus()
        received: list[dict] = []

        async with bus.subscribe() as stream:
            bus.publish("data-updated", {"trigger": "test"})
            event = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
            received.append(event)

        assert len(received) == 1
        assert received[0]["event"] == "data-updated"
        assert received[0]["data"] == {"trigger": "test"}

    async def test_multiple_subscribers_all_receive_event(self) -> None:
        bus = EventBus()
        results: list[dict] = []

        async with bus.subscribe() as s1, bus.subscribe() as s2:
            bus.publish("pipeline-status", {"status": "running"})
            e1 = await asyncio.wait_for(s1.__anext__(), timeout=1.0)
            e2 = await asyncio.wait_for(s2.__anext__(), timeout=1.0)
            results.extend([e1, e2])

        assert len(results) == 2
        assert all(r["event"] == "pipeline-status" for r in results)

    async def test_stream_terminates_after_context_exit(self) -> None:
        """Stream should stop (not block forever) once the context manager exits."""
        bus = EventBus()
        collected: list[dict] = []

        async with bus.subscribe() as stream:
            bus.publish("data-updated", {"x": 1})
            bus.publish("data-updated", {"x": 2})
            # Consume both events and wait for the sentinel.
            async for event in stream:
                collected.append(event)
                if len(collected) == 2:
                    break  # exit context → sentinel injected → stream ends

        assert len(collected) == 2

    async def test_queue_full_drops_event_without_raising(self) -> None:
        """A saturated subscriber queue should not propagate an exception."""
        bus = EventBus()
        # Publish 200 events — only 100 fit in the queue.
        async with bus.subscribe() as _stream:
            for i in range(200):
                bus.publish("data-updated", {"i": i})
            # No exception should have been raised.

    def test_publish_default_data_is_empty_dict(self) -> None:
        bus = EventBus()
        captured: list[dict] = []

        # Directly inspect the internal queue by adding a mock queue.
        q: asyncio.Queue = asyncio.Queue()
        bus._queues.append(q)
        bus.publish("data-updated")

        payload = q.get_nowait()
        assert payload == {"event": "data-updated", "data": {}}


# ═══════════════════════════════════════════════════════════════════════
# FileWatcher tests
# ═══════════════════════════════════════════════════════════════════════


class TestFileWatcher:
    def _make_watcher(self, path: Path = Path("/data/test.portfolio")) -> tuple[FileWatcher, EventBus]:
        bus = EventBus()
        watcher = FileWatcher(path=path, bus=bus, poll_interval=60)
        return watcher, bus

    def test_check_returns_false_when_file_missing(self) -> None:
        watcher, _ = self._make_watcher(Path("/nonexistent/file.portfolio"))
        assert watcher.check() is False

    def test_check_first_call_registers_baseline_without_triggering(self) -> None:
        watcher, bus = self._make_watcher()
        with patch("os.stat") as mock_stat:
            mock_stat.return_value = MagicMock(st_mtime=1_000_000.5)
            result = watcher.check()

        assert result is False
        assert watcher._last_mtime_minute == 1_000_000 // 60

    def test_check_returns_false_when_mtime_unchanged(self) -> None:
        watcher, _ = self._make_watcher()
        with patch("os.stat") as mock_stat:
            mock_stat.return_value = MagicMock(st_mtime=1_200_000.0)
            watcher.check()  # baseline
            result = watcher.check()  # same mtime

        assert result is False

    def test_check_returns_true_when_mtime_changes(self) -> None:
        watcher, bus = self._make_watcher()
        events_published: list[dict] = []
        orig_publish = bus.publish
        bus.publish = lambda e, d=None: events_published.append({"event": e, "data": d})

        with patch("os.stat") as mock_stat:
            mock_stat.return_value = MagicMock(st_mtime=1_200_000.0)
            watcher.check()  # baseline: minute = 20000

            mock_stat.return_value = MagicMock(st_mtime=1_200_060.0)
            result = watcher.check()  # changed: minute = 20001

        assert result is True
        assert len(events_published) == 1
        assert events_published[0]["event"] == "pipeline-status"
        assert events_published[0]["data"]["status"] == "file-changed"

        bus.publish = orig_publish

    def test_mtime_truncation_same_minute_no_trigger(self) -> None:
        """Two mtimes within the same minute must NOT trigger a change."""
        watcher, _ = self._make_watcher()
        with patch("os.stat") as mock_stat:
            mock_stat.return_value = MagicMock(st_mtime=1_200_000.0)
            watcher.check()  # baseline: minute 20000

            mock_stat.return_value = MagicMock(st_mtime=1_200_059.9)
            result = watcher.check()  # same minute: 20000

        assert result is False

    def test_poll_interval_stored_correctly(self) -> None:
        watcher, _ = self._make_watcher()
        assert watcher.poll_interval == 60


# ═══════════════════════════════════════════════════════════════════════
# PipelineScheduler tests
# ═══════════════════════════════════════════════════════════════════════


def _make_scheduler(
    portfolio_path: str = "/data/test.portfolio",
    poll_interval: int = 1,
    enrich_interval: int = 9999,
) -> tuple:
    """Return (scheduler, bus, mock_pool)."""
    from app.pipeline.scheduler import PipelineScheduler

    bus = EventBus()
    mock_pool = MagicMock()

    # pool.acquire() is used as an async context manager.
    mock_conn = AsyncMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    scheduler = PipelineScheduler(
        portfolio_path=Path(portfolio_path),
        pool=mock_pool,
        event_bus=bus,
        poll_interval=poll_interval,
        enrich_interval=enrich_interval,
    )
    return scheduler, bus, mock_pool, mock_conn


class TestPipelineScheduler:
    async def test_start_and_stop_lifecycle(self) -> None:
        scheduler, bus, _, _ = _make_scheduler()
        await scheduler.start()
        assert scheduler._task is not None
        assert not scheduler._task.done()
        await scheduler.stop()
        assert scheduler._task is None

    async def test_start_is_idempotent(self) -> None:
        scheduler, bus, _, _ = _make_scheduler()
        await scheduler.start()
        task1 = scheduler._task
        await scheduler.start()  # second call should be a no-op
        assert scheduler._task is task1
        await scheduler.stop()

    async def test_stop_when_not_started_does_not_raise(self) -> None:
        scheduler, _, _, _ = _make_scheduler()
        await scheduler.stop()  # should not raise

    async def test_run_full_pipeline_calls_all_stages(self) -> None:
        scheduler, bus, _, mock_conn = _make_scheduler()
        events_seen: list[str] = []

        orig_publish = bus.publish

        def _capture(event: str, data=None):
            events_seen.append(event)
            orig_publish(event, data)

        bus.publish = _capture

        with (
            patch("app.pipeline.parser.parse_portfolio_file", new_callable=AsyncMock) as p_parse,
            patch("app.pipeline.ingestion.ingest", new_callable=AsyncMock) as p_ingest,
            patch("app.pipeline.sync.sync_to_canonical", new_callable=AsyncMock) as p_sync,
            patch("app.enrichment.fx.discover_active_currencies", new_callable=AsyncMock, return_value=set()) as p_fx,
            patch("app.enrichment.history.plan_history_jobs", new_callable=AsyncMock) as p_plan,
            patch("app.enrichment.history.process_pending_jobs", new_callable=AsyncMock) as p_process,
            patch("app.metrics.engine.run_metrics", new_callable=AsyncMock, return_value="run-uuid-123") as p_metrics,
            patch("app.pipeline.snapshots.build_snapshots", new_callable=AsyncMock) as p_snaps,
        ):
            await scheduler._run_full_pipeline(trigger="test")

        p_parse.assert_awaited_once()
        p_ingest.assert_awaited_once()
        p_sync.assert_awaited_once()
        p_fx.assert_awaited_once()
        p_plan.assert_awaited_once()
        p_process.assert_awaited_once()
        p_metrics.assert_awaited_once()
        p_snaps.assert_awaited_once()

        # Verify event progression
        assert "pipeline-status" in events_seen
        assert "data-updated" in events_seen

    async def test_run_full_pipeline_publishes_error_on_failure(self) -> None:
        scheduler, bus, _, _ = _make_scheduler()
        events_seen: list[dict] = []
        bus.publish = lambda e, d=None: events_seen.append({"event": e, "data": d or {}})

        with patch(
            "app.pipeline.parser.parse_portfolio_file",
            new_callable=AsyncMock,
            side_effect=RuntimeError("parse failed"),
        ):
            await scheduler._run_full_pipeline(trigger="test")

        error_events = [
            e for e in events_seen
            if e["event"] == "pipeline-status" and e["data"].get("status") == "error"
        ]
        assert len(error_events) == 1

    async def test_concurrent_run_is_skipped(self) -> None:
        """A second run while the first is in progress should be a no-op."""
        scheduler, bus, _, mock_conn = _make_scheduler()
        run_count = 0

        original_lock = scheduler._run_lock

        async def _slow_parse(*_args, **_kwargs):
            nonlocal run_count
            run_count += 1
            await asyncio.sleep(0.1)

        with (
            patch("app.pipeline.parser.parse_portfolio_file", new_callable=AsyncMock, side_effect=_slow_parse),
            patch("app.pipeline.ingestion.ingest", new_callable=AsyncMock),
            patch("app.pipeline.sync.sync_to_canonical", new_callable=AsyncMock),
            patch("app.enrichment.fx.discover_active_currencies", new_callable=AsyncMock, return_value=set()),
            patch("app.enrichment.history.plan_history_jobs", new_callable=AsyncMock),
            patch("app.enrichment.history.process_pending_jobs", new_callable=AsyncMock),
            patch("app.metrics.engine.run_metrics", new_callable=AsyncMock, return_value="uuid"),
            patch("app.pipeline.snapshots.build_snapshots", new_callable=AsyncMock),
        ):
            # Launch two concurrent runs.
            await asyncio.gather(
                scheduler._run_full_pipeline(trigger="first"),
                scheduler._run_full_pipeline(trigger="second"),
            )

        assert run_count == 1  # only one parse should have run

    async def test_enrichment_only_run(self) -> None:
        scheduler, bus, _, mock_conn = _make_scheduler()
        events_seen: list[str] = []
        bus.publish = lambda e, d=None: events_seen.append(e)

        with (
            patch("app.enrichment.fx.discover_active_currencies", new_callable=AsyncMock, return_value=set()),
            patch("app.enrichment.history.plan_history_jobs", new_callable=AsyncMock),
            patch("app.enrichment.history.process_pending_jobs", new_callable=AsyncMock),
            patch("app.metrics.engine.run_metrics", new_callable=AsyncMock, return_value="uuid"),
            patch("app.pipeline.snapshots.build_snapshots", new_callable=AsyncMock),
        ):
            await scheduler._run_enrichment_only(trigger="schedule")

        assert "data-updated" in events_seen

    async def test_file_change_triggers_full_pipeline(self) -> None:
        """Watch loop should call _run_full_pipeline when watcher reports a change."""
        scheduler, bus, _, _ = _make_scheduler(poll_interval=1)

        pipeline_triggered = asyncio.Event()
        orig_run = scheduler._run_full_pipeline

        async def _mock_run(trigger: str = "manual") -> None:
            pipeline_triggered.set()

        scheduler._run_full_pipeline = _mock_run

        mock_watcher = MagicMock()
        # First call: change detected; subsequent calls: no change (prevents loop spin).
        mock_watcher.check.side_effect = [True, False, False, False, False]
        mock_watcher.poll_interval = 0  # no sleep between polls in test

        task = asyncio.create_task(scheduler._watch_loop(mock_watcher))
        await asyncio.wait_for(pipeline_triggered.wait(), timeout=2.0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert pipeline_triggered.is_set()
