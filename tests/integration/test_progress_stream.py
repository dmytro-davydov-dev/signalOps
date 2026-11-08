"""The progress stream against Compose PostgreSQL and Redis (P08-T03).

Live tests drive `progress_frames` with the real database source and Redis
wake-ups, writing events from a second connection as a worker would. Route
tests use ended investigations, whose streams finish on their own.
"""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy import Engine

from signalops.agents.investigations import end_investigation
from signalops.api.app import create_app
from signalops.api.investigations import DatabaseProgressSource
from signalops.api.progress_stream import RESET_FRAME, StreamLimits, progress_frames
from signalops.config import Settings
from signalops.contracts.progress import ProgressEventType, StepProgress
from signalops.contracts.workflow import AgentRole, InvestigationStatus
from signalops.dependencies import Resources
from signalops.persistence import database as db
from signalops.persistence.repositories.jobs import JobRepository
from signalops.persistence.repositories.progress import ProgressRepository
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from signalops.wakeups import RedisProgressNotifier, subscribe
from tests.integration.job_support import add_job

pytestmark = pytest.mark.integration

TOKEN, OTHER_TOKEN = "operator-demo", "operator-other"
OTHER_WORKSPACE = uuid.UUID("3a7b2e4d-8f5c-4d6a-b2e9-7c1f0a3d5e28")


async def never() -> bool:
    return False


async def add_step(database: db.Database, investigation_id: uuid.UUID, key: str) -> None:
    async with database.transaction() as session:
        await ProgressRepository(session).add(
            investigation_id,
            ProgressEventType.STEP_STARTED,
            StepProgress(step_key=key, agent_role=AgentRole.INVESTIGATOR, attempt=1),
        )


async def end(database: db.Database, investigation_id: uuid.UUID) -> None:
    async with database.transaction() as session:
        await JobRepository(session).start_investigation(investigation_id)
        await end_investigation(session, investigation_id, InvestigationStatus.FAILED)


def event_ids(frames: list[str]) -> list[int]:
    return [int(f.split("\n")[0][4:]) for f in frames if f.startswith("id: ")]


def step_keys(frames: list[str]) -> list[str]:
    keys = []
    for chunk in frames:
        if chunk.startswith("id: "):
            payload = json.loads(chunk.split("data: ")[1])["payload"]
            keys.append(payload.get("step_key", "ended"))
    return keys


@pytest.fixture
async def redis(infra_settings: Settings) -> AsyncIterator[Redis]:
    client = Redis.from_url(str(infra_settings.redis_url))
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def notifying(seeded: tuple[Settings, Engine], redis: Redis) -> AsyncIterator[db.Database]:
    """A worker's database: each commit with progress publishes a wake-up."""
    engine = db.create_engine(seeded[0])
    try:
        yield db.Database.from_engine(engine, on_progress=RedisProgressNotifier(redis))
    finally:
        await engine.dispose()


async def read_until_ended(
    database: db.Database,
    redis: Redis | None,
    investigation_id: uuid.UUID,
    cursor: int,
    limits: StreamLimits,
    frames: list[str],
) -> None:
    source = DatabaseProgressSource(database, investigation_id)
    async with subscribe(redis, investigation_id) as wakeups:
        async for chunk in progress_frames(source, wakeups, cursor, limits, disconnected=never):
            frames.append(chunk)


async def test_a_wake_up_delivers_a_new_event_before_the_catch_up(
    database: db.Database, notifying: db.Database, redis: Redis
) -> None:
    investigation_id = await add_job(database)
    await add_step(notifying, investigation_id, "load_context")
    frames: list[str] = []
    # A catch-up this long would fail the test: only the wake-up can deliver in time.
    limits = StreamLimits(catchup_seconds=30.0, max_stream_seconds=60.0)
    reader = asyncio.create_task(
        read_until_ended(database, redis, investigation_id, 0, limits, frames)
    )
    await asyncio.sleep(0.5)
    assert step_keys(frames) == ["load_context"]

    started = time.monotonic()
    await add_step(notifying, investigation_id, "investigate")
    await end(notifying, investigation_id)
    await asyncio.wait_for(reader, timeout=10)

    assert time.monotonic() - started < 10
    assert step_keys(frames) == ["load_context", "investigate", "ended"]
    assert event_ids(frames) == sorted(event_ids(frames))


async def test_without_any_wake_up_the_catch_up_still_delivers(database: db.Database) -> None:
    investigation_id = await add_job(database)
    frames: list[str] = []
    limits = StreamLimits(catchup_seconds=0.5, max_stream_seconds=60.0)
    # No Redis at the reader and no hook at the writer: nothing ever wakes the stream.
    reader = asyncio.create_task(
        read_until_ended(database, None, investigation_id, 0, limits, frames)
    )
    await asyncio.sleep(0.2)
    await add_step(database, investigation_id, "investigate")
    await end(database, investigation_id)
    await asyncio.wait_for(reader, timeout=10)

    assert step_keys(frames) == ["investigate", "ended"]


async def test_a_reconnect_replays_the_missed_events_in_order(
    database: db.Database, notifying: db.Database, redis: Redis
) -> None:
    investigation_id = await add_job(database)
    await add_step(notifying, investigation_id, "load_context")
    first: list[str] = []
    limits = StreamLimits(catchup_seconds=0.5, max_stream_seconds=1.0)
    # The first connection reaches its lifetime and closes, as after a dropped network.
    await read_until_ended(database, redis, investigation_id, 0, limits, first)
    last_seen = event_ids(first)[-1]

    for key in ("investigate", "verify", "conclude"):
        await add_step(notifying, investigation_id, key)
    await end(notifying, investigation_id)

    second: list[str] = []
    await read_until_ended(database, redis, investigation_id, last_seen, limits, second)
    assert RESET_FRAME not in second
    assert step_keys(second) == ["investigate", "verify", "conclude", "ended"]
    assert event_ids(second) == sorted(event_ids(second))
    assert min(event_ids(second)) > last_seen


async def add_then_fail(database: db.Database, investigation_id: uuid.UUID) -> None:
    async with database.transaction() as session:
        await ProgressRepository(session).add(
            investigation_id,
            ProgressEventType.STEP_STARTED,
            StepProgress(step_key="x", agent_role=AgentRole.INVESTIGATOR, attempt=1),
        )
        raise RuntimeError("roll back")


async def test_a_rolled_back_write_publishes_no_wake_up(
    database: db.Database, redis: Redis, seeded: tuple[Settings, Engine]
) -> None:
    investigation_id = await add_job(database)
    heard: list[frozenset[uuid.UUID]] = []

    async def hook(ids: frozenset[uuid.UUID]) -> None:
        heard.append(ids)

    engine = db.create_engine(seeded[0])
    try:
        hooked = db.Database.from_engine(engine, on_progress=hook)
        with pytest.raises(RuntimeError):
            await add_then_fail(hooked, investigation_id)
        assert heard == []
        await add_step(hooked, investigation_id, "y")
        assert heard == [frozenset({investigation_id})]
    finally:
        await engine.dispose()


@pytest.fixture
def client(seeded: tuple[Settings, Engine]) -> Iterator[TestClient]:
    settings = seeded[0].model_copy(
        update={
            "operator_tokens": {
                DEMO_WORKSPACE_ID: SecretStr(TOKEN),
                OTHER_WORKSPACE: SecretStr(OTHER_TOKEN),
            }
        }
    )

    @asynccontextmanager
    async def resources(app_settings: Settings) -> AsyncIterator[Resources]:
        engine = db.create_engine(app_settings)
        try:
            yield Resources(checks=[], engine=engine, database=db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    with TestClient(create_app(settings, resource_factory=resources)) as test_client:
        yield test_client


def bearer(token: str = TOKEN, **extra: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", **extra}


def ended_investigation(client: TestClient, steps: tuple[str, ...]) -> uuid.UUID:
    async def make(database: db.Database) -> uuid.UUID:
        investigation_id = await add_job(database)
        for key in steps:
            await add_step(database, investigation_id, key)
        await end(database, investigation_id)
        return investigation_id

    async def call() -> uuid.UUID:
        engine = db.create_engine(client.app.state.settings)  # type: ignore[attr-defined]
        try:
            return await make(db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    assert client.portal is not None
    result: uuid.UUID = client.portal.call(call)
    return result


def frames_of(text: str) -> list[str]:
    return [f"{chunk}\n\n" for chunk in text.split("\n\n") if chunk]


def test_the_route_streams_the_history_and_honours_last_event_id(client: TestClient) -> None:
    investigation_id = ended_investigation(client, ("load_context", "investigate"))
    url = f"/v1/investigations/{investigation_id}/events"

    response = client.get(url, headers=bearer())
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    frames = frames_of(response.text)
    assert frames[0] == "retry: 3000\n\n"
    assert step_keys(frames) == ["load_context", "investigate", "ended"]

    first = event_ids(frames)[0]
    resumed = frames_of(client.get(url, headers=bearer(**{"Last-Event-ID": str(first)})).text)
    assert step_keys(resumed) == ["investigate", "ended"]

    by_query = frames_of(client.get(url, params={"after": first}, headers=bearer()).text)
    assert step_keys(by_query) == ["investigate", "ended"]


def test_a_cursor_from_another_investigation_resets_the_stream(client: TestClient) -> None:
    other = ended_investigation(client, ("load_context",))
    other_cursor = event_ids(
        frames_of(client.get(f"/v1/investigations/{other}/events", headers=bearer()).text)
    )[0]
    investigation_id = ended_investigation(client, ("investigate",))

    frames = frames_of(
        client.get(
            f"/v1/investigations/{investigation_id}/events",
            headers=bearer(**{"Last-Event-ID": str(other_cursor)}),
        ).text
    )
    assert frames[1] == RESET_FRAME
    assert step_keys(frames) == ["investigate", "ended"]


def test_the_route_is_scoped_and_validates_its_cursor(client: TestClient) -> None:
    investigation_id = ended_investigation(client, ())
    url = f"/v1/investigations/{investigation_id}/events"

    assert client.get(url).status_code == 401
    assert client.get(url, headers=bearer(OTHER_TOKEN)).status_code == 404
    replay = client.get(url, params={"replay_scope": f"replay:{uuid.uuid4()}"}, headers=bearer())
    assert replay.status_code == 404
    missing = client.get(f"/v1/investigations/{uuid.uuid4()}/events", headers=bearer())
    assert missing.status_code == 404
    bad = client.get(url, headers=bearer(**{"Last-Event-ID": "abc"}))
    assert bad.status_code == 400
    assert client.get(url, params={"after": -1}, headers=bearer()).status_code == 422
