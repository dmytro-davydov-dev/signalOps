"""The investigation progress stream served as server-sent events.

PostgreSQL `progress_events` is the record; this module only replays it. A
stream starts after the client's cursor, sends each later event once and in ID
order, and reads again on a Redis wake-up or every catch-up interval, so a lost
wake-up delays an event by at most that interval. No transaction stays open
while it waits.

Cursor rules:

- no cursor, or 0: the whole retained history;
- a cursor naming an event of this investigation: the events after it;
- any other cursor (from another investigation, or an event retention removed):
  a `reset` event, then the whole retained history. The client discards what it
  held and reads the investigation again.

The stream ends after `investigation.ended`, which is always the last event, and
after its maximum lifetime; the client reconnects with `Last-Event-ID`.
"""

import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from signalops.contracts.progress import ProgressEventType, ProgressEventView
from signalops.wakeups import Wakeups

# Reconnect delay a client should use, in milliseconds.
RETRY_MS = 3000
PAGE_SIZE = 100


class ProgressSource(Protocol):
    async def known(self, cursor: int) -> bool:
        """Whether `cursor` names an event of this investigation."""
        ...

    async def after(self, cursor: int, limit: int) -> Sequence[ProgressEventView]:
        """Events after `cursor`, oldest first, at most `limit`."""
        ...

    async def terminal(self) -> bool:
        """Whether the investigation has ended."""
        ...


@dataclass(frozen=True)
class StreamLimits:
    catchup_seconds: float
    max_stream_seconds: float


def frame(event: ProgressEventView) -> str:
    data = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
    return f"id: {event.id}\nevent: progress\ndata: {data}\n\n"


RESET_FRAME = 'event: reset\ndata: {"reason":"unknown_cursor"}\n\n'
KEEPALIVE_FRAME = ": keepalive\n\n"


async def progress_frames(
    source: ProgressSource,
    wakeups: Wakeups,
    cursor: int,
    limits: StreamLimits,
    *,
    disconnected: Callable[[], Awaitable[bool]],
    clock: Callable[[], float] = time.monotonic,
) -> AsyncIterator[str]:
    """SSE frames for one connection, as described in the module docstring."""
    yield f"retry: {RETRY_MS}\n\n"
    if cursor > 0 and not await source.known(cursor):
        yield RESET_FRAME
        cursor = 0
    closes_at = clock() + limits.max_stream_seconds
    while True:
        if await disconnected():
            return
        while True:
            events = await source.after(cursor, PAGE_SIZE)
            for event in events:
                yield frame(event)
                cursor = event.id
                if event.event_type is ProgressEventType.INVESTIGATION_ENDED:
                    return
            if len(events) < PAGE_SIZE:
                break
        if await source.terminal():
            # Ended without an ending event, which the schema prevents; stop anyway.
            return
        left = closes_at - clock()
        if left <= 0:
            return
        if not await wakeups.wait(min(limits.catchup_seconds, left)):
            yield KEEPALIVE_FRAME
