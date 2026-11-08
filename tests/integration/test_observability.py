"""Observability against Compose PostgreSQL and Kafka (P11-T01).

A fault is followed from its ingestion batches to the investigation's model
calls, through the trace route and through log lines that carry the same IDs.
Slow model calls and a growing Kafka backlog move different numbers.
"""

import asyncio
import contextlib
import io
import json
import logging
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from pydantic import SecretStr
from sqlalchemy import Engine, delete, select

from signalops.agents.jobs import JobRunner
from signalops.api.app import create_app
from signalops.config import Settings
from signalops.contracts.common import replay_scope_for
from signalops.contracts.investigations import InvestigationTrace
from signalops.contracts.workflow import InvestigationStatus
from signalops.demo.generator import Scenario, generate
from signalops.dependencies import Resources
from signalops.gateway.fake import FakeProvider, FakeStep
from signalops.gateway.fake_roles import role_step
from signalops.gateway.types import ModelRequest
from signalops.logs import JsonFormatter
from signalops.observability.snapshot import database_state, kafka_lag
from signalops.persistence import database as db
from signalops.persistence.models import Investigation, Job, Signal
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.multi_agent_support import queued_v3, v3_engine
from tests.integration.test_investigator import JOBS
from tests.integration.test_replay import _replay

pytestmark = pytest.mark.integration

TOKEN = "operator-observability"  # noqa: S105 - test credential
MODEL_DELAY = 0.2


def _slow(request: ModelRequest) -> FakeStep:
    return role_step(request).model_copy(update={"delay_seconds": MODEL_DELAY})


@contextlib.contextmanager
def captured_logs() -> Iterator[list[dict[str, object]]]:
    """JSON log lines written while the block runs, as the processes write them."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    previous = root.level
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    lines: list[dict[str, object]] = []
    try:
        yield lines
    finally:
        root.removeHandler(handler)
        root.setLevel(previous)
        lines.extend(json.loads(line) for line in stream.getvalue().splitlines())


@asynccontextmanager
async def worker(database: db.Database) -> AsyncIterator[None]:
    engine = v3_engine(database, FakeProvider(responder=_slow))
    stop = asyncio.Event()
    task = asyncio.create_task(JobRunner(database, engine, JOBS, owner="observed").run(stop))
    try:
        yield
    finally:
        stop.set()
        await task


async def _queue(database: db.Database, scope: EvidenceScope, incident_id: uuid.UUID) -> uuid.UUID:
    """Queue a `v3` investigation as the only job, so the worker claims it next."""
    async with database.transaction() as session:
        await session.execute(delete(Job))
    return await queued_v3(database, scope, incident_id)


async def _wait_for_end(database: db.Database, investigation_id: uuid.UUID) -> Investigation:
    for _ in range(600):
        async with database.transaction() as session:
            row = await session.get_one(Investigation, investigation_id, populate_existing=True)
        if row.completed_at is not None:
            return row
        await asyncio.sleep(0.05)
    raise AssertionError("investigation did not end")


def _client(settings: Settings) -> TestClient:
    @asynccontextmanager
    async def resources(app_settings: Settings) -> AsyncIterator[Resources]:
        engine = db.create_engine(app_settings)
        try:
            yield Resources(checks=[], engine=engine, database=db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    configured = settings.model_copy(
        update={"operator_tokens": {DEMO_WORKSPACE_ID: SecretStr(TOKEN)}}
    )
    return TestClient(create_app(configured, resource_factory=resources))


def _model_seconds() -> tuple[float, float]:
    labels = {"provider": "fake", "outcome": "succeeded"}
    count = REGISTRY.get_sample_value("signalops_model_call_seconds_count", labels) or 0.0
    total = REGISTRY.get_sample_value("signalops_model_call_seconds_sum", labels) or 0.0
    return count, total


async def test_a_fault_is_followed_from_ingestion_batches_to_model_calls(
    database: db.Database, seeded: tuple[Settings, Engine]
) -> None:
    run_id = uuid.uuid4()
    report = await _replay(database, generate(Scenario.DOWNSTREAM_TIMEOUT, seed=8301), run_id)
    [incident_id] = report.incident_ids
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
    investigation_id = await _queue(database, scope, incident_id)
    calls_before, seconds_before = _model_seconds()

    with captured_logs() as lines:
        async with worker(database):
            ended = await _wait_for_end(database, investigation_id)
    # Without the graph and runbooks the fake verifier may abstain; either way the job succeeded.
    assert ended.status in {
        InvestigationStatus.COMPLETED,
        InvestigationStatus.INSUFFICIENT_EVIDENCE,
    }

    with _client(seeded[0]) as client:
        response = client.get(
            f"/v1/investigations/{investigation_id}/trace",
            params={"replay_scope": scope.replay_scope},
            headers={"Authorization": f"Bearer {TOKEN}", "X-Request-ID": "trace-check-1"},
        )
        assert response.status_code == 200, response.text
        assert response.headers["x-request-id"] == "trace-check-1"
        trace = InvestigationTrace.model_validate(response.json())
        other_scope = client.get(
            f"/v1/investigations/{investigation_id}/trace",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        assert other_scope.status_code == 404

    # Ingestion: the batches the replay posted are the ones behind the cited evidence.
    async with database.transaction() as session:
        provenance = Signal.attributes["provenance"]["correlation_id"].astext
        scoped_batches = set(
            await session.scalars(select(provenance).where(scope.clause(Signal)).distinct())
        )
    assert trace.incident_id == incident_id
    assert trace.evidence.signals > 0
    assert trace.evidence.ingestion_batches
    assert set(trace.evidence.ingestion_batches) <= scoped_batches
    assert trace.evidence.first_ingested_at is not None
    assert trace.evidence.first_ingested_at <= trace.created_at

    # Investigation: job, steps, and where the time went.
    assert trace.job is not None
    assert (trace.job.status, trace.job.attempts) == ("succeeded", 1)
    assert trace.log_fields["job_id"] == (str(trace.job.job_id),)
    model_calls = sum(step.model_calls for step in trace.steps)
    assert model_calls > 0
    assert trace.stages.model_s >= model_calls * MODEL_DELAY * 0.9
    assert trace.stages.running_s is not None
    # Summed call time: parallel specialists make it exceed the wall-clock run.
    assert trace.stages.model_s > trace.stages.tool_s
    # Replay incidents carry event-time detection, so wall-clock stages before the request are null.
    assert trace.stages.ingestion_to_detection_s is None

    # Metrics: every call observed once with its provider latency.
    calls_after, seconds_after = _model_seconds()
    assert calls_after - calls_before == model_calls
    assert seconds_after - seconds_before >= model_calls * MODEL_DELAY * 0.9

    # Logs: the worker's lines carry the IDs the trace names.
    settled = [line for line in lines if line["message"] == "model call settled"]
    tools = [line for line in lines if line["message"] == "tool call finished"]
    assert len(settled) == model_calls
    assert tools
    for line in (*settled, *tools):
        assert line["investigation_id"] == str(investigation_id)
        assert line["job_id"] == str(trace.job.job_id)
        assert line["lease_generation"] == str(trace.job.lease_generation)


def _topic(admin: AdminClient, name: str) -> None:
    [future] = admin.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1)]
    ).values()
    future.result(timeout=10)


def _produce(settings: Settings, topic: str, count: int) -> None:
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for index in range(count):
        producer.produce(topic, value=f"record-{index}".encode())
    assert producer.flush(10) == 0


def _commit(settings: Settings, group: str, topic: str, offset: int) -> None:
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": group})
    try:
        consumer.commit(offsets=[TopicPartition(topic, 0, offset)], asynchronous=False)
    finally:
        consumer.close()


def _lag_of(admin: AdminClient, group: str, topic: str) -> int:
    lag = kafka_lag(admin, {group: topic})
    assert lag is not None
    [item] = lag
    return item.lag


async def test_slow_model_calls_and_kafka_backlog_move_different_numbers(
    database: db.Database, seeded: tuple[Settings, Engine]
) -> None:
    settings = seeded[0]
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    suffix = uuid.uuid4().hex[:12]
    topic, group = f"test.observability.{suffix}", f"observability-{suffix}"
    _topic(admin, topic)
    try:
        # A backlog: 25 records, of which the group has committed 5.
        await asyncio.to_thread(_produce, settings, topic, 25)
        await asyncio.to_thread(_commit, settings, group, topic, 5)
        assert await asyncio.to_thread(_lag_of, admin, group, topic) == 20
        never_committed = await asyncio.to_thread(kafka_lag, admin, {f"{group}-new": topic})
        assert never_committed is not None
        assert [(i.lag, i.committed) for i in never_committed] == [(25, None)]

        # Slow model calls: in flight in PostgreSQL and timed by the gateway,
        # while the backlog neither grows nor drains.
        run_id = uuid.uuid4()
        report = await _replay(database, generate(Scenario.DOWNSTREAM_TIMEOUT, seed=8302), run_id)
        scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
        investigation_id = await _queue(database, scope, report.incident_ids[0])
        calls_before, seconds_before = _model_seconds()
        in_flight_seen = 0
        async with worker(database):
            for _ in range(200):
                async with database.transaction() as session:
                    state = await database_state(session)
                in_flight_seen = max(in_flight_seen, state.model_calls_in_flight)
                if in_flight_seen:
                    break
                await asyncio.sleep(0.02)
            await _wait_for_end(database, investigation_id)
        calls_after, seconds_after = _model_seconds()
        assert in_flight_seen >= 1
        assert calls_after > calls_before
        assert (seconds_after - seconds_before) / (calls_after - calls_before) >= MODEL_DELAY * 0.9
        assert await asyncio.to_thread(_lag_of, admin, group, topic) == 20

        # A growing backlog: lag rises; the model latency series does not move.
        await asyncio.to_thread(_produce, settings, topic, 15)
        assert await asyncio.to_thread(_lag_of, admin, group, topic) == 35
        assert _model_seconds() == (calls_after, seconds_after)
        async with database.transaction() as session:
            assert (await database_state(session)).model_calls_in_flight == 0
    finally:
        [future] = admin.delete_topics([topic]).values()
        future.result(timeout=10)
