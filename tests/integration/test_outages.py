"""Crash and outage checks against the running Compose stack (P11-T03).

These stop, kill, restart, and scale the shared Compose services, so they run
only with `SIGNALOPS_OUTAGE_CHECKS=1` (plus `SIGNALOPS_INTEGRATION=1` and the
`.env` variables), never in an ordinary integration run. They need the stack
up with `SIGNALOPS_INGEST_TOKENS` and `SIGNALOPS_OPERATOR_TOKENS` set and at
least one live incident (any earlier realtime demo run leaves one).

1. Signals: one generated dataset is posted in chunks while Kafka is stopped,
   PostgreSQL is stopped, Redis restarts, normalizer and detector are scaled to
   two members and back (rebalances), and the outbox publisher is killed. The
   first chunk is then posted again. After the pipeline drains, every accepted
   record must have its normalizer receipt or a quarantine event, and each
   must be stored once with one normalized event.
2. Investigation: a worker whose fake provider is throttled on every third
   call is killed mid-investigation while Redis restarts. Its lease must
   expire, another attempt must reuse completed steps, and the investigation
   must end with one job, one row per step, and the throttled calls recorded.
"""

import asyncio
import json
import os
import random
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient
from sqlalchemy import func, select

from signalops.adapters.ingestion import IngestPolicy, IngestSource, ingest_batch
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE
from signalops.contracts.evidence import SourceKind
from signalops.contracts.workflow import LlmCallStatus
from signalops.demo.generator import DemoRecord, Scenario, generate
from signalops.demo.poster import PostSummary, post_records, urllib_transport
from signalops.observability.snapshot import database_state, kafka_lag
from signalops.persistence import database as db
from signalops.persistence.models import (
    AgentStep,
    Incident,
    Investigation,
    Job,
    LlmCall,
    OutboxEvent,
    Signal,
)
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from signalops.streaming.producer import EventProducer
from signalops.streaming.reconcile import RecordKey, reconcile

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("SIGNALOPS_OUTAGE_CHECKS") != "1",
        reason="stops shared Compose services; set SIGNALOPS_OUTAGE_CHECKS=1 to run",
    ),
]

ROOT = Path(__file__).parents[2]
API = f"http://127.0.0.1:{os.environ.get('API_HOST_PORT', '8010')}"
DRAIN_TIMEOUT = timedelta(minutes=6)


def compose(*args: str, env: Mapping[str, str] | None = None) -> None:
    subprocess.run(  # noqa: S603 - fixed docker compose arguments
        ["docker", "compose", *args],  # noqa: S607
        cwd=ROOT,
        check=True,
        capture_output=True,
        timeout=300,
        env={**os.environ, **(env or {})},
    )


def log(message: str) -> None:
    print(f"{datetime.now(UTC).strftime('%H:%M:%S')} {message}", flush=True)


def _tokens(name: str) -> dict[uuid.UUID, str]:
    return {uuid.UUID(k): v for k, v in json.loads(os.environ[name]).items()}


class Poster:
    """Posts chunks and remembers every record the API accepted."""

    def __init__(self) -> None:
        self.tokens = _tokens("SIGNALOPS_INGEST_TOKENS")
        self.accepted: set[RecordKey] = set()
        self.accepted_responses = 0
        self.transport = urllib_transport(10)

    def post(self, records: Sequence[DemoRecord], *, attempts: int = 10) -> PostSummary:
        summary = post_records(
            records,
            api_url=API,
            tokens=self.tokens,
            transport=self.transport,
            batch_size=50,
            max_attempts=attempts,
            backoff_seconds=2.0,
            sleep=lambda seconds: time.sleep(min(seconds, 15)),
        )
        assert summary.rejected == 0, summary.rejected_reasons
        # `post_records` raises unless every record was accepted.
        self.accepted.update(RecordKey(r.source_id, r.source_event_id) for r in records)
        self.accepted_responses += summary.accepted
        return summary

    def post_during(
        self,
        records: Sequence[DemoRecord],
        down: Callable[[], None],
        up: Callable[[], None],
        hold_seconds: float,
    ) -> PostSummary:
        """Take a service down, post while it is down, bring it back; the poster retries."""
        result: list[PostSummary] = []
        down()
        thread = threading.Thread(target=lambda: result.append(self.post(records)))
        thread.start()
        try:
            time.sleep(hold_seconds)
        finally:
            up()
            thread.join(timeout=600)
        assert result, "posting did not finish"
        return result[0]


def _chunks(records: Sequence[DemoRecord], count: int) -> list[list[DemoRecord]]:
    size = -(-len(records) // count)
    return [list(records[i : i + size]) for i in range(0, len(records), size)]


async def _drained(database: db.Database, settings: Settings, keys: set[RecordKey]) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    deadline = time.monotonic() + DRAIN_TIMEOUT.total_seconds()
    while time.monotonic() < deadline:
        result = await reconcile(database, settings, keys, LIVE_REPLAY_SCOPE)
        lag = await asyncio.to_thread(kafka_lag, admin)
        async with database.transaction() as session:
            outbox = (await database_state(session)).outbox_pending
        if not result.missing and lag is not None and not sum(i.lag for i in lag) and not outbox:
            return
        await asyncio.sleep(5)
    raise AssertionError("pipeline did not drain")


@pytest.fixture
def stack() -> Iterator[None]:
    """Whatever a check stops or scales, leave every service running at one member."""
    try:
        yield
    finally:
        compose("up", "-d", "--no-build", "--scale", "normalizer=1", "--scale", "detector=1")


async def test_outages_lose_no_accepted_record_and_duplicate_nothing(
    infra_settings: Settings, stack: None
) -> None:
    engine = db.create_engine(infra_settings)
    database = db.Database.from_engine(engine)
    poster = Poster()
    seed = random.SystemRandom().randrange(1, 2**31)
    dataset = generate(
        Scenario.DOWNSTREAM_TIMEOUT,
        seed=seed,
        start=(datetime.now(UTC) - timedelta(minutes=12)).replace(microsecond=0),
        duration=timedelta(minutes=10),
    )
    chunks = _chunks(dataset.records, 8)
    log(f"seed {seed}: {len(dataset.records)} records in {len(chunks)} chunks")
    try:
        poster.post(chunks[0])
        log("baseline posted")

        summary = poster.post_during(
            chunks[1], lambda: compose("stop", "kafka"), lambda: compose("start", "kafka"), 20
        )
        log(f"kafka outage: accepted after {summary.retries} retries")
        assert summary.retries > 0

        summary = poster.post_during(
            chunks[2], lambda: compose("stop", "postgres"), lambda: compose("start", "postgres"), 10
        )
        log(f"postgres outage: accepted after {summary.retries} retries")
        assert summary.retries > 0
        # Consumers that stopped after their retries exhausted come back by restart policy;
        # `up` also restores any the outage left exited.
        compose("up", "-d", "--no-build")

        poster.post_during(
            chunks[3], lambda: compose("stop", "redis"), lambda: compose("start", "redis"), 5
        )
        log("redis restarted")

        compose("up", "-d", "--no-build", "--scale", "normalizer=2", "--scale", "detector=2")
        poster.post(chunks[4])
        compose("up", "-d", "--no-build", "--scale", "normalizer=1", "--scale", "detector=1")
        poster.post(chunks[5])
        log("normalizer and detector scaled to 2 and back")

        poster.post_during(
            chunks[6],
            lambda: compose("kill", "outbox-publisher"),
            lambda: compose("start", "outbox-publisher"),
            5,
        )
        poster.post(chunks[7])
        log("outbox publisher killed and restarted")

        # Physical duplicates: the first chunk again, with the same source event IDs.
        poster.post(chunks[0])
        assert poster.accepted_responses == len(dataset.records) + len(chunks[0])

        # A record the normalizer must quarantine: accepted for a source that does not exist.
        orphan = await _publish_orphan(infra_settings)
        log(f"orphan record published for unknown source {orphan.source_id}")

        await _drained(database, infra_settings, poster.accepted)
        quarantine = await reconcile(database, infra_settings, [orphan], LIVE_REPLAY_SCOPE)
        assert (quarantine.processed, len(quarantine.quarantined), quarantine.missing) == (0, 1, ())
        log(f"orphan reconciled as quarantined: {quarantine.quarantined[0]}")
        result = await reconcile(database, infra_settings, poster.accepted, LIVE_REPLAY_SCOPE)
        log(f"reconciled: {result}")
        assert result.accepted == len(dataset.records)
        assert result.missing == ()
        assert result.processed + len(result.quarantined) == result.accepted
        assert result.signals == result.processed

        signal_ids = await _signal_ids(database, poster.accepted)
        assert len(signal_ids) == result.processed
        async with database.transaction() as session:
            events = await session.execute(
                select(OutboxEvent.aggregate_id, func.count())
                .where(OutboxEvent.aggregate_id.in_(signal_ids))
                .group_by(OutboxEvent.aggregate_id)
            )
            per_signal = {aggregate: count for aggregate, count in events}
        assert set(per_signal) == set(signal_ids)
        assert set(per_signal.values()) == {1}
    finally:
        await engine.dispose()


async def _publish_orphan(settings: Settings) -> RecordKey:
    source = IngestSource(uuid.uuid4(), DEMO_WORKSPACE_ID, SourceKind.STRUCTURED_LOG, enabled=True)
    key = RecordKey(source.id, f"orphan-{uuid.uuid4().hex}")
    producer = EventProducer(settings, client_id="signalops-outage-check")
    try:
        response = await ingest_batch(
            [
                {
                    "source_event_id": key.source_event_id,
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "payload": {"service": "checkout", "level": "error", "message": "orphan"},
                }
            ],
            source=source,
            publisher=producer,
            policy=IngestPolicy.from_settings(settings),
            now=datetime.now(UTC),
            batch_id=uuid.uuid4(),
        )
    finally:
        producer.close()
    assert response.accepted == 1, response
    return key


async def _signal_ids(database: db.Database, keys: set[RecordKey]) -> list[uuid.UUID]:
    by_source: dict[uuid.UUID, list[str]] = {}
    for key in keys:
        by_source.setdefault(key.source_id, []).append(key.source_event_id)
    found: list[uuid.UUID] = []
    async with database.transaction() as session:
        for source_id, event_ids in by_source.items():
            found += list(
                await session.scalars(
                    select(Signal.id).where(
                        Signal.source_id == source_id,
                        Signal.replay_scope == LIVE_REPLAY_SCOPE,
                        Signal.source_event_id.in_(event_ids),
                    )
                )
            )
    return found


def _request(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    import urllib.request

    [token] = _tokens("SIGNALOPS_OPERATOR_TOKENS").values()
    request = urllib.request.Request(  # noqa: S310 - loopback API
        f"{API}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Idempotency-Key": f"outage-{uuid.uuid4()}",
        },
    )
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
        return json.loads(response.read())  # type: ignore[no-any-return]


async def test_a_killed_throttled_worker_resumes_through_a_redis_restart(
    infra_settings: Settings, stack: None
) -> None:
    engine = db.create_engine(infra_settings)
    database = db.Database.from_engine(engine)
    slow = {"SIGNALOPS_FAKE_MODEL_DELAY_SECONDS": "2", "SIGNALOPS_FAKE_MODEL_THROTTLE_EVERY": "3"}
    try:
        async with database.transaction() as session:
            incident_id = await session.scalar(
                select(Incident.id)
                .where(Incident.replay_scope == LIVE_REPLAY_SCOPE)
                .order_by(Incident.detected_at.desc())
                .limit(1)
            )
        assert incident_id is not None, "needs a live incident (run a realtime demo first)"
        compose("up", "-d", "--no-build", "investigation-worker", env=slow)
        accepted = _request("POST", f"/v1/incidents/{incident_id}/investigations", {})
        investigation_id = uuid.UUID(accepted["id"])
        log(f"investigation {investigation_id}")

        # Kill once a step has completed and a later one is running.
        for _ in range(120):
            async with database.transaction() as session:
                done = await session.scalar(
                    select(func.count()).where(
                        AgentStep.investigation_id == investigation_id,
                        AgentStep.completed_at.is_not(None),
                    )
                )
            if done and done >= 2:
                break
            await asyncio.sleep(0.5)
        compose("kill", "investigation-worker")
        compose("restart", "redis")
        log(f"worker killed after {done} completed steps; redis restarted")
        async with database.transaction() as session:
            before = {
                step.step_key: (step.attempt, step.completed_at)
                for step in await session.scalars(
                    select(AgentStep).where(
                        AgentStep.investigation_id == investigation_id,
                        AgentStep.completed_at.is_not(None),
                    )
                )
            }
        compose("up", "-d", "--no-build", "investigation-worker", env=slow)

        for _ in range(240):
            async with database.transaction() as session:
                row = await session.get_one(Investigation, investigation_id, populate_existing=True)
            if row.completed_at is not None:
                break
            await asyncio.sleep(1)
        log(f"ended {row.status}")
        async with database.transaction() as session:
            jobs = list(
                await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
            )
            steps = list(
                await session.scalars(
                    select(AgentStep).where(AgentStep.investigation_id == investigation_id)
                )
            )
            calls = list(
                await session.scalars(
                    select(LlmCall).where(LlmCall.agent_step_id.in_([s.id for s in steps]))
                )
            )
        assert row.status in {"completed", "insufficient_evidence"}
        [job] = jobs
        assert job.status == "succeeded"
        # The killed attempt's lease expired and the next claim took over (generation 2).
        # A later attempt can follow when throttling exhausts a call's retries.
        assert job.attempts >= 2
        assert job.lease_generation == job.attempts
        assert job.last_error is not None
        assert job.last_error["category"] in {"lease_expired", "transient"}
        abandoned = [c for c in calls if c.lease_generation == 1 and c.status == "unknown"]
        assert abandoned, "the killed worker's in-flight calls should stay unknown"
        assert {c.error_category for c in abandoned} == {"abandoned"}
        assert {c.reserved_microusd > 0 for c in abandoned} == {True}
        assert len({s.step_key for s in steps}) == len(steps)
        for step in steps:
            if step.step_key in before:
                # Completed before the kill: reused, not rerun.
                assert (step.attempt, step.completed_at) == before[step.step_key]
        throttled = [c for c in calls if c.error_category == "throttled"]
        assert throttled, "no call was throttled"
        assert {c.status for c in throttled} == {LlmCallStatus.FAILED}
        log(f"{len(calls)} calls, {len(throttled)} throttled, {len(before)} steps reused")
    finally:
        compose("up", "-d", "--no-build", "investigation-worker")
        await engine.dispose()
