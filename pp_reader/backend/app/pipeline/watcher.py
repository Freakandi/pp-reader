"""File watcher — polls a .portfolio file for modification-time changes.

Minute-granularity mtime truncation avoids spurious re-parses on network
filesystems (NAS) where subsecond mtime precision is unreliable.

Design:
  - ``FileWatcher.check()`` is a synchronous, side-effect-free probe.
    It returns ``True`` and publishes a ``pipeline-status`` event when a
    change is detected; ``False`` otherwise.
  - The caller (``PipelineScheduler``) drives the polling loop.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.api.events import EventBus

__all__ = ["FileWatcher"]

_LOGGER = logging.getLogger(__name__)


class FileWatcher:
    """Poll a ``.portfolio`` file for modification-time changes.

    Parameters
    ----------
    path:
        Filesystem path to the ``.portfolio`` binary file.
    bus:
        :class:`~app.api.events.EventBus` used to broadcast change events.
    poll_interval:
        Seconds between successive ``check()`` calls (used by the scheduler
        to space its sleep calls; not enforced here).
    """

    def __init__(
        self,
        path: Path,
        bus: EventBus,
        poll_interval: int = 60,
    ) -> None:
        self._path = path
        self._bus = bus
        self.poll_interval = poll_interval
        # Last seen mtime truncated to the nearest minute (None = not yet seen).
        self._last_mtime_minute: int | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self) -> bool:
        """Check whether the watched file has changed since the last call.

        On first call the current mtime is registered as the baseline and
        ``False`` is returned (no change triggered yet).

        Returns ``True`` and publishes a ``pipeline-status:file-changed``
        event when a change is detected.  Returns ``False`` when the file
        is absent or the mtime is unchanged.
        """
        current = self._read_mtime_minute()

        if current is None:
            # File does not exist (yet) — nothing to do.
            return False

        if self._last_mtime_minute is None:
            # First time we see the file — set baseline, do not trigger.
            self._last_mtime_minute = current
            _LOGGER.debug("FileWatcher: baseline mtime registered for %s", self._path)
            return False

        if current == self._last_mtime_minute:
            return False

        # File has changed.
        self._last_mtime_minute = current
        _LOGGER.info("FileWatcher: change detected in %s", self._path)
        self._bus.publish(
            "pipeline-status",
            {"status": "file-changed", "path": str(self._path)},
        )
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_mtime_minute(self) -> int | None:
        """Return ``st_mtime`` truncated to whole minutes, or ``None`` if missing."""
        try:
            raw_mtime: float = os.stat(self._path).st_mtime
            return int(raw_mtime) // 60
        except OSError:
            return None
