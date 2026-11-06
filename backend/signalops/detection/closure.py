"""Closing detection windows outside the sample path.

`WindowCloser` is the live closer: a quiet service has no next sample to close
its window, so a process closes windows by wall clock. It keeps no state: open
windows are rows, so a restart resumes from them and a crash mid-batch rolls the
batch back. Rows are locked with SKIP LOCKED, so closers do not block a detector
or each other.

`flush_replay` ends a replay: the last windows of a service have no later sample
to move its simulated clock past them. Run it after the detector has consumed
the replay.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Final

from signalops.detection.detector import DetectionPolicy, close_windows
from signalops.detection.rules import WindowClosure
from signalops.persistence.database import Database
from signalops.persistence.repositories.detection import DetectionRepository
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

CLOSE_BATCH_SIZE: Final = 100


class WindowCloser:
    def __init__(
        self,
        database: Database,
        policy: DetectionPolicy,
        *,
        interval_seconds: float = 5.0,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._database = database
        self._policy = policy
        self._interval = interval_seconds
        self._clock = clock

    async def run(self, stop: asyncio.Event) -> None:
        """Close due windows until `stop` is set; waits only after a batch that was not full."""
        logger.info("window closer started")
        while not stop.is_set():
            if await self.close_once() < CLOSE_BATCH_SIZE:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self._interval)
        logger.info("window closer stopped")

    async def close_once(self) -> int:
        """Close one batch of the oldest due live windows; returns how many closed."""
        before = self._clock() - self._policy.lateness
        async with self._database.transaction() as session:
            detection = DetectionRepository(session)
            due = await detection.due_live_windows(before, CLOSE_BATCH_SIZE)
            await close_windows(detection, self._policy, due, WindowClosure.WALL_CLOCK)
        if due:
            logger.info("detection windows closed", extra={"windows": len(due)})
        return len(due)


async def flush_replay(database: Database, scope: EvidenceScope, policy: DetectionPolicy) -> int:
    """Close every open window of a replay scope, oldest first; returns how many closed."""
    closed = 0
    while True:
        async with database.transaction() as session:
            detection = DetectionRepository(session)
            windows = await detection.open_scope_windows(scope, CLOSE_BATCH_SIZE)
            await close_windows(detection, policy, windows, WindowClosure.FINAL_FLUSH)
        closed += len(windows)
        if len(windows) < CLOSE_BATCH_SIZE:
            logger.info(
                "replay windows flushed",
                extra={"replay_scope": scope.replay_scope, "windows": closed},
            )
            return closed
