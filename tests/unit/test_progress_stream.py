"""The progress stream's cursor, catch-up, and ending rules, with fake reads (P08-T03)."""

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest
from fastapi import HTTPException
from redis.exceptions import ConnectionError as RedisConnectionError

from signalops.api.investigations import _cursor
from signalops.api.progress_stream import (
    KEEPALIVE_FRAME,
    PAGE_SIZE,
    RESET_FRAME,
    StreamLimits,
    frame,
    progress_frames,
)
from signalops.contracts.progress import (
    InvestigationEnded,
    ProgressEventType,
    ProgressEventView,
    StepProgress,
)
from signalops.contracts.workflow import AgentRole, InvestigationStatus
from signalops.wakeups import RedisProgressNotifier, channel_for, subscribe

AT = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
LIMITS = StreamLimits(catchup_seconds=5.0, max_stream_seconds=300.0)


def started(event_id: int, step_key: str = "investigate") -> ProgressEventView:
    return ProgressEventView(
        id=event_id,
        event_type=ProgressEventType.STEP_STARTED,
        payload=StepProgress(step_key=step_key, agent_role=AgentRole.INVESTIGATOR, attempt=1),
        created_at=AT,
    )


def ended(event_id: int) -> ProgressEventView:
    return ProgressEventView(
        id=event_id,
        event_type=ProgressEventType.INVESTIGATION_ENDED,
        payload=InvestigationEnded(status=InvestigationStatus.FAILED),
        created_at=AT,
    )


@dataclass
class FakeSource:
    events: list[ProgressEventView] = field(default_factory=list)
    is_terminal: bool = False
    reads: list[int] = field(default_factory=list)

    async def known(self, cursor: int) -> bool:
        return any(event.id == cursor for event in self.events)

    async def after(self, cursor: int, limit: int) -> Sequence[ProgressEventView]:
        self.reads.append(cursor)
        return [event for event in self.events if event.id > cursor][:limit]

    async def terminal(self) -> bool:
        return self.is_terminal


@dataclass
class FakeClock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


@dataclass
class ScriptedWakeups:
    """Runs a step before each wait, then reports whether it woke the stream."""

    clock: FakeClock
    steps: list[tuple[bool, object]] = field(default_factory=list)
    waits: list[float] = field(default_factory=list)

    async def wait(self, seconds: float) -> bool:
        self.waits.append(seconds)
        if not self.steps:
            self.clock.now += 10_000
            return False
        woke, action = self.steps.pop(0)
        if callable(action):
            action()
        self.clock.now += 0.0 if woke else seconds
        return woke


async def never_disconnected() -> bool:
    return False


async def collect(
    source: FakeSource,
    cursor: int,
    wakeups: ScriptedWakeups,
    limits: StreamLimits = LIMITS,
) -> list[str]:
    return [
        chunk
        async for chunk in progress_frames(
            source, wakeups, cursor, limits, disconnected=never_disconnected, clock=wakeups.clock
        )
    ]


def ids(frames: list[str]) -> list[int]:
    found = []
    for chunk in frames:
        if chunk.startswith("id: "):
            found.append(int(chunk.split("\n")[0].removeprefix("id: ")))
    return found


async def test_a_first_connection_replays_the_history_then_ends_after_the_ending() -> None:
    source = FakeSource([started(3), started(7, "conclude"), ended(9)])
    frames = await collect(source, 0, ScriptedWakeups(FakeClock()))

    assert frames[0] == "retry: 3000\n\n"
    assert ids(frames) == [3, 7, 9]
    data = json.loads(frames[1].split("data: ")[1])
    assert data["payload"]["step_key"] == "investigate"
    assert frames[-1].startswith("id: 9\nevent: progress\n")


async def test_a_reconnect_replays_only_what_came_after_its_cursor_in_order() -> None:
    source = FakeSource([started(3), started(7, "conclude"), ended(9)])
    frames = await collect(source, 3, ScriptedWakeups(FakeClock()))

    assert RESET_FRAME not in frames
    assert ids(frames) == [7, 9]


async def test_an_unknown_cursor_resets_and_replays_everything() -> None:
    source = FakeSource([started(3), ended(9)])
    # 5 is no event of this investigation: another stream's ID, or one retention removed.
    frames = await collect(source, 5, ScriptedWakeups(FakeClock()))

    assert frames[1] == RESET_FRAME
    assert ids(frames) == [3, 9]


async def test_a_missed_wake_up_is_covered_by_the_catch_up_read() -> None:
    clock = FakeClock()
    source = FakeSource([started(3)])
    # The next event commits, but its wake-up never arrives: the wait times out.
    wakeups = ScriptedWakeups(clock, [(False, lambda: source.events.append(ended(4)))])
    frames = await collect(source, 0, wakeups)

    assert ids(frames) == [3, 4]
    assert KEEPALIVE_FRAME in frames
    assert wakeups.waits == [LIMITS.catchup_seconds]


async def test_a_wake_up_reads_at_once() -> None:
    clock = FakeClock()
    source = FakeSource([started(3)])
    wakeups = ScriptedWakeups(clock, [(True, lambda: source.events.append(ended(4)))])
    frames = await collect(source, 0, wakeups)

    assert ids(frames) == [3, 4]
    assert KEEPALIVE_FRAME not in frames
    assert source.reads == [0, 3]


async def test_a_spurious_wake_up_sends_nothing_twice() -> None:
    clock = FakeClock()
    source = FakeSource([started(3)])
    wakeups = ScriptedWakeups(clock, [(True, None), (True, lambda: source.events.append(ended(4)))])
    frames = await collect(source, 0, wakeups)

    assert ids(frames) == [3, 4]


async def test_a_long_backlog_is_read_in_pages_without_waiting() -> None:
    events = [started(n, f"step_{n}") for n in range(1, PAGE_SIZE + 6)]
    source = FakeSource([*events, ended(PAGE_SIZE + 6)])
    wakeups = ScriptedWakeups(FakeClock())
    frames = await collect(source, 0, wakeups)

    assert ids(frames) == list(range(1, PAGE_SIZE + 7))
    assert wakeups.waits == []


async def test_the_stream_closes_at_its_lifetime_so_the_client_reconnects() -> None:
    clock = FakeClock()
    source = FakeSource([started(3)])
    limits = StreamLimits(catchup_seconds=5.0, max_stream_seconds=12.0)
    wakeups = ScriptedWakeups(clock, [(False, None), (False, None), (False, None)])
    frames = await collect(source, 0, wakeups, limits)

    assert ids(frames) == [3]
    # Two full catch-ups, then the rest of the lifetime.
    assert wakeups.waits == [5.0, 5.0, 2.0]


async def test_an_ended_investigation_without_an_ending_event_still_closes() -> None:
    source = FakeSource([started(3)], is_terminal=True)
    wakeups = ScriptedWakeups(FakeClock())
    frames = await collect(source, 0, wakeups)

    assert ids(frames) == [3]
    assert wakeups.waits == []


async def test_a_disconnected_client_stops_the_reads() -> None:
    source = FakeSource([started(3)])

    async def disconnected() -> bool:
        return True

    frames = [
        chunk
        async for chunk in progress_frames(
            source, ScriptedWakeups(FakeClock()), 0, LIMITS, disconnected=disconnected
        )
    ]
    assert frames == ["retry: 3000\n\n"]
    assert source.reads == []


@pytest.mark.parametrize(
    ("header", "after", "expected"),
    [(None, None, 0), (None, 12, 12), ("40", 12, 40), (" 7 ", None, 7), ("0", None, 0)],
)
def test_last_event_id_wins_over_after(
    header: str | None, after: int | None, expected: int
) -> None:
    assert _cursor(header, after) == expected


@pytest.mark.parametrize("header", ["-1", "abc", "1.5", "", "٣", str(2**63)])
def test_a_malformed_last_event_id_is_rejected(header: str) -> None:
    with pytest.raises(HTTPException) as raised:
        _cursor(header, None)
    assert raised.value.status_code == 400


def test_frames_carry_the_durable_id_and_valid_json() -> None:
    chunk = frame(started(42))
    lines = chunk.rstrip("\n").split("\n")
    assert lines[0] == "id: 42"
    assert lines[1] == "event: progress"
    assert ProgressEventView.model_validate_json(lines[2].removeprefix("data: ")).id == 42


class FailingRedis:
    def __init__(self) -> None:
        self.published: list[str] = []

    async def publish(self, channel: str, message: str) -> int:
        self.published.append(channel)
        raise RedisConnectionError("down")


async def test_a_failed_wake_up_publish_is_swallowed() -> None:
    client = FailingRedis()
    investigation_id = uuid.uuid4()
    await RedisProgressNotifier(client)(frozenset({investigation_id}))  # type: ignore[arg-type]
    assert client.published == [channel_for(investigation_id)]


async def test_without_redis_a_stream_waits_out_each_catch_up() -> None:
    async with subscribe(None, uuid.uuid4()) as wakeups:
        assert await wakeups.wait(0.01) is False
