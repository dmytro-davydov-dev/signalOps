"""Investigation requests and dispatch against Compose PostgreSQL and Kafka (P06-T01).

Requests go through the real app where the response matters, and through
`request_by_operator` directly where concurrent transactions matter. Dispatch
uses its own topic and consumer group, so Kafka redelivery is real.
"""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, func, select

from signalops.agents.investigations import (
    DISPATCHER_CONSUMER_GROUP,
    INVESTIGATION_EVENT_NAMESPACE,
    WORKFLOW_VERSION,
    Dispatcher,
    IdempotencyConflictError,
    IdempotentRequest,
    InvestigationPolicy,
    request_by_detector,
    request_by_operator,
    request_hash_for,
)
from signalops.api.app import create_app
from signalops.api.investigations import REQUEST_ROUTE
from signalops.config import Settings
from signalops.contracts.common import ContractError, replay_scope_for
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    InvestigationRequestedPayload,
    InvestigationTrigger,
    decode_event,
)
from signalops.contracts.investigations import InvestigationRequest
from signalops.contracts.workflow import IncidentSeverity, InvestigationStatus, JobStatus
from signalops.dependencies import Resources
from signalops.detection.incidents import Breach, IncidentPolicy, record_breach
from signalops.detection.rules import DEFAULT_RULES, Window
from signalops.persistence import database as db
from signalops.persistence.models import (
    IdempotencyKey,
    Incident,
    Investigation,
    Job,
    OutboxEvent,
)
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.detection import SeriesKey
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from signalops.streaming.consumer import (
    ConsumerRunner,
    ProcessOutcome,
    TransactionalProcessor,
    create_consumer,
)
from signalops.streaming.producer import EventProducer, OutgoingMessage
from signalops.streaming.retry import RetryPolicy
from signalops.streaming.topics import TopicSpec, ensure_topics, message_key

pytestmark = pytest.mark.integration

OTHER_WORKSPACE = uuid.UUID("2f6a1d3c-7e4b-4c59-a1d8-6b0e9f2c4d17")
TOKEN, OTHER_TOKEN = "operator-demo", "operator-other"
REPLAY = replay_scope_for(uuid.UUID("7d4c0e8a-1111-4a2b-9c3d-000000000003"))
LIVE = EvidenceScope(DEMO_WORKSPACE_ID)
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
WINDOW = timedelta(minutes=1)
POLICY = InvestigationPolicy()
POLL_DEADLINE_SECONDS = 30.0


def path(incident_id: uuid.UUID) -> str:
    return f"/v1/incidents/{incident_id}/investigations"


async def add_incident(database: db.Database, scope: EvidenceScope) -> uuid.UUID:
    async with database.transaction() as session:
        return await IncidentRepository(session).create(
            NewIncident(
                scope=scope,
                fingerprint=f"env:{uuid.uuid4().hex[:8]}",
                title="High error rate",
                severity=IncidentSeverity.HIGH,
                window=Window(T0, T0 + WINDOW),
                detected_at=T0 + WINDOW,
            )
        )


async def requests_of(database: db.Database, incident_id: uuid.UUID) -> list[Investigation]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(Investigation).where(Investigation.incident_id == incident_id)
        )
        return list(rows.all())


async def outbox_for(database: db.Database, investigation_id: uuid.UUID) -> list[OutboxEvent]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == investigation_id)
        )
        return list(rows.all())


async def jobs_for(database: db.Database, investigation_id: uuid.UUID) -> list[Job]:
    async with database.transaction() as session:
        rows = await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
        return list(rows.all())


@pytest.fixture(scope="module", autouse=True)
def other_workspace(seeded: tuple[Settings, Engine]) -> None:
    async def store() -> None:
        engine = db.create_engine(seeded[0])
        try:
            async with db.Database.from_engine(engine).transaction() as session:
                await CatalogRepository(session).ensure_workspace(OTHER_WORKSPACE, "other")
        finally:
            await engine.dispose()

    asyncio.run(store())


@dataclass
class Api:
    client: TestClient
    settings: Settings
    # Keys are per workspace and the module shares a database, so each test has its own.
    nonce: str = field(default_factory=lambda: uuid.uuid4().hex)

    def run(self, action: Any) -> Any:
        async def call() -> Any:
            engine = db.create_engine(self.settings)
            try:
                return await action(db.Database.from_engine(engine))
            finally:
                await engine.dispose()

        assert self.client.portal is not None
        return self.client.portal.call(call)

    def incident(self, scope: EvidenceScope = LIVE) -> uuid.UUID:
        result: uuid.UUID = self.run(lambda database: add_incident(database, scope))
        return result

    def post(
        self,
        incident_id: uuid.UUID,
        key: str | None,
        body: dict[str, Any] | None = None,
        token: str = TOKEN,
    ) -> Any:
        headers = {"Authorization": f"Bearer {token}"}
        if key is not None:
            headers["Idempotency-Key"] = key and f"{self.nonce}-{key}"
        return self.client.post(path(incident_id), headers=headers, json=body)


def api_for(settings: Settings, **overrides: Any) -> TestClient:
    settings = settings.model_copy(
        update={
            "operator_tokens": {
                DEMO_WORKSPACE_ID: SecretStr(TOKEN),
                OTHER_WORKSPACE: SecretStr(OTHER_TOKEN),
            },
            **overrides,
        }
    )

    @asynccontextmanager
    async def resources(app_settings: Settings) -> AsyncIterator[Resources]:
        engine = db.create_engine(app_settings)
        try:
            yield Resources(checks=[], engine=engine, database=db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    return TestClient(create_app(settings, resource_factory=resources))


@pytest.fixture
def api(seeded: tuple[Settings, Engine]) -> Iterator[Api]:
    with api_for(seeded[0]) as client:
        yield Api(client, seeded[0])


def test_a_repeated_request_returns_the_same_investigation(api: Api) -> None:
    incident_id = api.incident()
    first = api.post(incident_id, "key-1", {"max_steps": 5})
    second = api.post(incident_id, "key-1", {"max_steps": 5})

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert first.json()["status"] == "queued"
    assert first.json()["incident_id"] == str(incident_id)

    rows = api.run(lambda database: requests_of(database, incident_id))
    assert [str(row.id) for row in rows] == [first.json()["id"]]
    assert rows[0].request_key.startswith("operator:")
    assert rows[0].workflow_version == WORKFLOW_VERSION
    assert rows[0].evidence_cutoff_at == rows[0].created_at

    outbox = api.run(lambda database: outbox_for(database, rows[0].id))
    assert len(outbox) == 1
    assert outbox[0].topic == "investigations.requested.v1"
    assert outbox[0].message_key == str(rows[0].id)
    assert outbox[0].published_at is None
    envelope = decode_event(json.dumps(outbox[0].envelope).encode())
    assert isinstance(envelope.payload, InvestigationRequestedPayload)
    assert envelope.payload.trigger is InvestigationTrigger.OPERATOR
    assert envelope.payload.request_key == rows[0].request_key


def test_a_request_writes_no_job(api: Api) -> None:
    """The dispatcher owns the job, so the API needs no broker to acknowledge."""
    response = api.post(api.incident(), "key-1")
    assert response.status_code == 202
    investigation_id = uuid.UUID(response.json()["id"])
    assert api.run(lambda database: jobs_for(database, investigation_id)) == []


def test_the_canonical_hash_ignores_field_order_and_explicit_nulls(api: Api) -> None:
    incident_id = api.incident()
    headers = {"Authorization": f"Bearer {TOKEN}", "Idempotency-Key": api.nonce}
    first = api.client.post(path(incident_id), headers=headers, json={})
    second = api.client.post(
        path(incident_id),
        headers=headers | {"Content-Type": "application/json"},
        content=b'{"max_steps": null,  "budget_microusd":null}',
    )
    third = api.client.post(path(incident_id), headers=headers)
    assert first.status_code == second.status_code == third.status_code == 202
    assert first.json()["id"] == second.json()["id"] == third.json()["id"]


def test_key_reuse_with_another_request_conflicts(api: Api) -> None:
    incident_id, other_incident = api.incident(), api.incident()
    created = api.post(incident_id, "key-1", {"max_steps": 5})
    assert created.status_code == 202

    assert api.post(incident_id, "key-1", {"max_steps": 6}).status_code == 409
    assert api.post(incident_id, "key-1").status_code == 409
    # The hash carries the incident, because the route template does not.
    assert api.post(other_incident, "key-1", {"max_steps": 5}).status_code == 409

    assert len(api.run(lambda database: requests_of(database, incident_id))) == 1
    assert api.run(lambda database: requests_of(database, other_incident)) == []


def test_another_key_is_another_investigation(api: Api) -> None:
    incident_id = api.incident()
    first = api.post(incident_id, "key-1").json()["id"]
    second = api.post(incident_id, "key-2").json()["id"]
    assert first != second


def test_a_key_belongs_to_its_workspace(api: Api) -> None:
    ours, theirs = api.incident(), api.incident(EvidenceScope(OTHER_WORKSPACE))
    mine = api.post(ours, "shared-key", {"max_steps": 5})
    other = api.post(theirs, "shared-key", {"max_steps": 6}, token=OTHER_TOKEN)
    assert mine.status_code == other.status_code == 202
    assert mine.json()["id"] != other.json()["id"]


def test_requests_outside_the_live_scope_of_the_workspace_are_404(api: Api) -> None:
    replayed = api.incident(EvidenceScope(DEMO_WORKSPACE_ID, REPLAY))
    foreign = api.incident(EvidenceScope(OTHER_WORKSPACE))
    assert api.post(replayed, "key-1").status_code == 404
    assert api.post(foreign, "key-1").status_code == 404
    assert api.post(uuid.uuid4(), "key-1").status_code == 404
    assert api.run(lambda database: requests_of(database, replayed)) == []
    assert api.run(lambda database: requests_of(database, foreign)) == []


def test_a_key_and_a_token_are_required(api: Api) -> None:
    incident_id = api.incident()
    assert api.post(incident_id, None).status_code == 422
    assert api.post(incident_id, "").status_code == 422
    assert api.post(incident_id, "k" * 257).status_code == 422
    assert api.post(incident_id, "key-1", token="wrong").status_code == 401  # noqa: S106
    assert api.run(lambda database: requests_of(database, incident_id)) == []


def test_unknown_fields_are_rejected(api: Api) -> None:
    incident_id = api.incident()
    assert api.post(incident_id, "key-1", {"evidence_cutoff_at": "2030-01-01"}).status_code == 422


def test_a_request_can_lower_limits_but_not_raise_them(api: Api) -> None:
    settings = api.settings
    incident_id = api.incident()

    for body in (
        {"budget_microusd": settings.investigation_budget_microusd + 1},
        {"max_steps": settings.investigation_max_steps + 1},
        {"deadline_seconds": settings.investigation_deadline_seconds + 1},
    ):
        response = api.post(incident_id, f"key-{next(iter(body))}", body)
        assert response.status_code == 422, body
    assert api.run(lambda database: requests_of(database, incident_id)) == []

    assert (
        api.post(
            incident_id,
            "lower",
            {"budget_microusd": 100, "max_steps": 2, "deadline_seconds": 60},
        ).status_code
        == 202
    )
    assert api.post(incident_id, "defaults").status_code == 202
    rows = api.run(lambda database: requests_of(database, incident_id))
    limits = {(r.budget_microusd, r.max_steps, r.deadline_at - r.created_at) for r in rows}
    assert limits == {
        (100, 2, timedelta(seconds=60)),
        (
            settings.investigation_budget_microusd,
            settings.investigation_max_steps,
            timedelta(seconds=settings.investigation_deadline_seconds),
        ),
    }


def test_a_repeat_still_answers_after_the_limit_drops(
    api: Api, seeded: tuple[Settings, Engine]
) -> None:
    incident_id = api.incident()
    created = api.post(incident_id, "key-1", {"max_steps": 10})
    assert created.status_code == 202

    with api_for(seeded[0], investigation_max_steps=5) as lowered:
        repeated = replace(api, client=lowered).post(incident_id, "key-1", {"max_steps": 10})
        fresh = replace(api, client=lowered).post(incident_id, "key-2", {"max_steps": 10})
    assert repeated.status_code == 202
    assert repeated.json()["id"] == created.json()["id"]
    assert fresh.status_code == 422


def idempotent(incident_id: uuid.UUID, key: str, body: InvestigationRequest) -> IdempotentRequest:
    return IdempotentRequest(REQUEST_ROUTE, key, request_hash_for(incident_id, body))


async def post_directly(
    database: db.Database, incident_id: uuid.UUID, key: str, body: InvestigationRequest
) -> uuid.UUID:
    async with database.transaction() as session:
        row = await request_by_operator(
            session,
            LIVE,
            incident_id,
            body,
            idempotent(incident_id, key, body),
            POLICY,
            now=datetime.now(UTC),
        )
        assert row is not None
        return row.id


async def test_concurrent_requests_with_one_key_make_one_investigation(
    database: db.Database,
) -> None:
    incident_id = await add_incident(database, LIVE)
    body = InvestigationRequest()
    key = uuid.uuid4().hex
    ids = await asyncio.gather(*(post_directly(database, incident_id, key, body) for _ in range(8)))

    assert len(set(ids)) == 1
    assert len(await requests_of(database, incident_id)) == 1
    assert len(await outbox_for(database, ids[0])) == 1
    async with database.transaction() as session:
        keys = await session.scalar(
            select(func.count())
            .select_from(IdempotencyKey)
            .where(IdempotencyKey.investigation_id == ids[0])
        )
    assert keys == 1


async def test_concurrent_requests_with_different_bodies_leave_one_winner(
    database: db.Database,
) -> None:
    incident_id = await add_incident(database, LIVE)
    key = uuid.uuid4().hex
    results = await asyncio.gather(
        *(
            post_directly(database, incident_id, key, InvestigationRequest(max_steps=steps))
            for steps in range(1, 7)
        ),
        return_exceptions=True,
    )

    winners = [result for result in results if isinstance(result, uuid.UUID)]
    assert len(winners) == 1
    assert sum(isinstance(result, IdempotencyConflictError) for result in results) == 5
    assert [row.id for row in await requests_of(database, incident_id)] == winners


async def breach(
    database: db.Database, policy: IncidentPolicy, series: SeriesKey, window: Window
) -> None:
    async with database.transaction() as session:
        await record_breach(
            session, policy, Breach(series, window, DEFAULT_RULES[0], None, window.end)
        )


async def incidents_in(database: db.Database, scope: EvidenceScope) -> list[Incident]:
    async with database.transaction() as session:
        rows = await session.scalars(select(Incident).where(scope.clause(Incident)))
        return list(rows.all())


async def fresh_workspace(database: db.Database) -> tuple[uuid.UUID, uuid.UUID]:
    """A workspace of its own, so a test can find its incidents by scope."""
    workspace, service_id = uuid.uuid4(), uuid.uuid4()
    async with database.transaction() as session:
        catalog = CatalogRepository(session)
        await catalog.ensure_workspace(workspace, f"test-{workspace}")
        await catalog.ensure_service(
            service_id=service_id, workspace_id=workspace, name="checkout", environment="prod"
        )
    return workspace, service_id


async def test_a_new_live_incident_requests_one_investigation(database: db.Database) -> None:
    workspace, service_id = await fresh_workspace(database)
    scope = EvidenceScope(workspace)
    series = SeriesKey(workspace, service_id, scope.replay_scope, "trace.60s")
    policy = IncidentPolicy(investigations=POLICY)

    await breach(database, policy, series, Window(T0, T0 + WINDOW))
    await breach(database, policy, series, Window(T0 + WINDOW, T0 + 2 * WINDOW))

    incident = (await incidents_in(database, scope))[0]
    rows = await requests_of(database, incident.id)
    assert len(rows) == 1
    assert rows[0].request_key == f"detector:{incident.id}"
    assert rows[0].status == InvestigationStatus.QUEUED
    assert rows[0].budget_microusd == POLICY.budget_microusd
    outbox = await outbox_for(database, rows[0].id)
    assert len(outbox) == 1
    envelope = decode_event(json.dumps(outbox[0].envelope).encode())
    assert isinstance(envelope.payload, InvestigationRequestedPayload)
    assert envelope.payload.trigger is InvestigationTrigger.DETECTOR
    assert envelope.payload.incident_id == incident.id


async def test_the_detector_key_is_stable(database: db.Database) -> None:
    incident_id = await add_incident(database, LIVE)
    for _ in range(2):
        async with database.transaction() as session:
            await request_by_detector(session, LIVE, incident_id, POLICY, now=datetime.now(UTC))

    rows = await requests_of(database, incident_id)
    assert len(rows) == 1
    assert len(await outbox_for(database, rows[0].id)) == 1


async def test_replay_incidents_and_a_disabled_policy_request_nothing(
    database: db.Database,
) -> None:
    workspace, service_id = await fresh_workspace(database)
    replay = EvidenceScope(workspace, REPLAY)
    await breach(
        database,
        IncidentPolicy(investigations=POLICY),
        SeriesKey(workspace, service_id, REPLAY, "trace.60s"),
        Window(T0, T0 + WINDOW),
    )
    live = EvidenceScope(workspace)
    await breach(
        database,
        IncidentPolicy(),
        SeriesKey(workspace, service_id, live.replay_scope, "trace.60s"),
        Window(T0, T0 + WINDOW),
    )

    for scope in (replay, live):
        incident = (await incidents_in(database, scope))[0]
        assert await requests_of(database, incident.id) == []


async def test_a_failed_breach_transaction_leaves_neither_incident_nor_request(
    database: db.Database,
) -> None:
    workspace, service_id = await fresh_workspace(database)
    scope = EvidenceScope(workspace)
    series = SeriesKey(workspace, service_id, scope.replay_scope, "trace.60s")
    window = Window(T0, T0 + WINDOW)

    class CrashError(Exception):
        pass

    async def crash_before_commit() -> None:
        async with database.transaction() as session:
            await record_breach(
                session,
                IncidentPolicy(investigations=POLICY),
                Breach(series, window, DEFAULT_RULES[0], None, window.end),
            )
            raise CrashError

    with pytest.raises(CrashError):
        await crash_before_commit()

    assert await incidents_in(database, scope) == []


def requested(
    investigation: Investigation, event_id: uuid.UUID | None = None
) -> EventEnvelope[EventPayload]:
    return EventEnvelope[EventPayload](
        event_id=event_id
        or uuid.uuid5(INVESTIGATION_EVENT_NAMESPACE, f"{investigation.id}:requested"),
        event_type=EventType.INVESTIGATION_REQUESTED,
        schema_version=1,
        workspace_id=investigation.workspace_id,
        occurred_at=T0,
        ingested_at=T0,
        payload=InvestigationRequestedPayload(
            investigation_id=investigation.id,
            incident_id=investigation.incident_id,
            request_key=investigation.request_key,
            trigger=InvestigationTrigger.DETECTOR,
            workflow_version=investigation.workflow_version,
        ),
    )


async def detector_investigation(database: db.Database) -> Investigation:
    incident_id = await add_incident(database, LIVE)
    async with database.transaction() as session:
        await request_by_detector(session, LIVE, incident_id, POLICY, now=datetime.now(UTC))
    return (await requests_of(database, incident_id))[0]


@pytest.fixture
def topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.dispatch.{uuid.uuid4().hex[:8]}"
    ensure_topics(admin, (TopicSpec(name, partitions=1, retention_ms=60_000),))
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            future.result()


def publish(settings: Settings, topic: str, envelopes: list[EventEnvelope[EventPayload]]) -> None:
    producer = EventProducer(settings, client_id="test-dispatch-producer")
    messages = [OutgoingMessage(topic, message_key(e), e.encode(), e.event_id) for e in envelopes]
    assert all(result.delivered for result in producer.publish(messages))


def runner_for(
    settings: Settings, consumer: Consumer, database: db.Database, group: str, topic: str
) -> ConsumerRunner:
    runner = ConsumerRunner(
        consumer,
        TransactionalProcessor(database, group, Dispatcher()),
        consumer_group=group,
        failure_publisher=EventProducer(settings, client_id=f"{group}-failures"),
        retry_policy=RetryPolicy(5, base_delay_seconds=0.05, max_delay_seconds=0.2),
    )
    runner.subscribe([topic])
    return runner


async def next_outcome(runner: ConsumerRunner) -> ProcessOutcome:
    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    while time.monotonic() < deadline:
        outcome = await runner.run_once()
        if outcome is not None:
            return outcome
    raise AssertionError("no record within the poll deadline")


async def test_duplicate_kafka_requests_create_one_job(
    infra_settings: Settings, database: db.Database, topic: str
) -> None:
    group = f"{DISPATCHER_CONSUMER_GROUP}-{uuid.uuid4().hex[:8]}"
    investigation = await detector_investigation(database)
    envelope = requested(investigation)
    # A republished outbox row, and a distinct event for one request: the receipt
    # catches the first, and only the jobs unique constraint catches the second.
    publish(infra_settings, topic, [envelope, envelope, requested(investigation, uuid.uuid4())])

    consumer = create_consumer(infra_settings, group_id=group, client_id=f"{group}-member")
    try:
        runner = runner_for(infra_settings, consumer, database, group, topic)
        outcomes = [await next_outcome(runner) for _ in range(3)]
    finally:
        consumer.close()

    assert outcomes[1] is ProcessOutcome.DUPLICATE
    assert outcomes[0] is outcomes[2] is ProcessOutcome.APPLIED
    jobs = await jobs_for(database, investigation.id)
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.QUEUED
    assert (jobs[0].attempts, jobs[0].lease_generation, jobs[0].lease_owner) == (0, 0, None)


async def test_a_redelivered_request_after_a_lost_offset_finds_its_job(
    infra_settings: Settings, database: db.Database, topic: str
) -> None:
    group = f"{DISPATCHER_CONSUMER_GROUP}-{uuid.uuid4().hex[:8]}"
    investigation = await detector_investigation(database)
    publish(infra_settings, topic, [requested(investigation)])

    # The job and receipt commit, and the process dies before the offset does.
    processor = TransactionalProcessor(database, group, Dispatcher())
    assert await processor.process(requested(investigation)) is ProcessOutcome.APPLIED

    consumer = create_consumer(infra_settings, group_id=group, client_id=f"{group}-member")
    try:
        runner = runner_for(infra_settings, consumer, database, group, topic)
        assert await next_outcome(runner) is ProcessOutcome.DUPLICATE
    finally:
        consumer.close()
    assert len(await jobs_for(database, investigation.id)) == 1


async def test_a_finished_investigation_gets_no_job(database: db.Database) -> None:
    investigation = await detector_investigation(database)
    async with database.transaction() as session:
        row = await session.get_one(Investigation, investigation.id)
        row.status = InvestigationStatus.CANCELLED
        row.completed_at = datetime.now(UTC)

    async with database.transaction() as session:
        await Dispatcher()(session, requested(investigation))
    assert await jobs_for(database, investigation.id) == []


async def test_dispatcher_rejects_a_request_from_another_scope(database: db.Database) -> None:
    investigation = await detector_investigation(database)
    foreign = requested(investigation).model_copy(update={"workspace_id": OTHER_WORKSPACE})
    with pytest.raises(ContractError):
        async with database.transaction() as session:
            await Dispatcher()(session, foreign)
    assert await jobs_for(database, investigation.id) == []


async def test_dispatcher_rejects_a_missing_investigation(database: db.Database) -> None:
    investigation = await detector_investigation(database)
    payload = requested(investigation).payload.model_copy(update={"investigation_id": uuid.uuid4()})
    with pytest.raises(ContractError):
        async with database.transaction() as session:
            await Dispatcher()(
                session, requested(investigation).model_copy(update={"payload": payload})
            )
