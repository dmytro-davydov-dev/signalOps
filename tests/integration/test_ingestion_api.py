"""Batch ingestion against PostgreSQL and the Compose Kafka broker.

The real app, source lookup, and producer run end to end. Messages are
redirected to a scratch topic so nothing reaches signals.raw.v1, which
workers consume; topic routing itself is covered by unit tests.
"""

import asyncio
import dataclasses
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, text

from signalops.adapters.ingestion import REDACTED, DatabaseSourceLookup, raw_event_id
from signalops.api.app import create_app
from signalops.config import Settings
from signalops.contracts.events import RawSignalPayload, decode_event
from signalops.dependencies import Resources
from signalops.persistence.database import Database, create_engine
from signalops.persistence.seed import DEMO_SOURCES, DEMO_WORKSPACE_ID, seed_demo
from signalops.streaming.producer import DeliveryResult, EventProducer, OutgoingMessage
from signalops.streaming.topics import TopicSpec, ensure_topics

pytestmark = pytest.mark.integration

LOGS, TRACES, DEPLOYMENTS = (source.id for source in DEMO_SOURCES)
TOKENS = {LOGS: "logs-token", TRACES: "traces-token", DEPLOYMENTS: "deploy-token"}


@dataclasses.dataclass
class Redirected:
    producer: EventProducer
    topic: str

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        return self.producer.publish([dataclasses.replace(m, topic=self.topic) for m in messages])

    def close(self, timeout_seconds: float = 10.0) -> int:
        return self.producer.close(timeout_seconds)


@pytest.fixture(scope="module")
def seeded(migrated: tuple[Settings, Engine]) -> tuple[Settings, Engine]:
    settings, engine = migrated

    async def seed() -> None:
        async_engine = create_engine(settings)
        try:
            await seed_demo(Database.from_engine(async_engine))
        finally:
            await async_engine.dispose()

    asyncio.run(seed())
    return settings, engine


@pytest.fixture
def scratch_topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.ingest.{uuid.uuid4().hex[:8]}"
    ensure_topics(admin, (TopicSpec(name, partitions=3, retention_ms=60_000),))
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            future.result()


def make_client(settings: Settings, topic: str, *, bootstrap: str | None = None) -> TestClient:
    settings = settings.model_copy(
        update={
            "ingest_tokens": {sid: SecretStr(token) for sid, token in TOKENS.items()},
            "kafka_delivery_timeout_ms": 5_000 if bootstrap is None else 1_000,
            "kafka_bootstrap_servers": bootstrap or settings.kafka_bootstrap_servers,
        }
    )

    @asynccontextmanager
    async def resources(app_settings: Settings) -> AsyncIterator[Resources]:
        engine = create_engine(app_settings)
        producer = EventProducer(app_settings, client_id="test-ingest")
        try:
            yield Resources(
                checks=[],
                engine=engine,
                sources=DatabaseSourceLookup(Database.from_engine(engine)),
                ingest_publisher=Redirected(producer, topic),
            )
        finally:
            producer.close(5)
            await engine.dispose()

    return TestClient(create_app(settings, resource_factory=resources))


def read_topic(settings: Settings, topic: str) -> list[tuple[bytes, bytes]]:
    """(key, value) of every record on the topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    records: list[tuple[bytes, bytes]] = []
    try:
        for partition in range(3):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high <= low:
                continue
            consumer.assign([TopicPartition(topic, partition, low)])
            for _ in range(high - low):
                record = consumer.poll(10)
                assert record is not None
                assert record.error() is None
                key, value = record.key(), record.value()
                assert key is not None
                assert value is not None
                records.append((key, value))
    finally:
        consumer.close()
    return records


def batch(*ids: str) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "records": [
            {
                "source_event_id": source_event_id,
                "occurred_at": now,
                "payload": {
                    "service": "payment",
                    "level": "error",
                    "message": "bank-gateway timed out",
                    "attributes": {"password": "hunter2"},
                },
            }
            for source_event_id in ids
        ]
    }


def auth(source_id: uuid.UUID) -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKENS[source_id]}"}


def test_accepted_records_are_on_kafka_redacted(
    seeded: tuple[Settings, Engine], scratch_topic: str
) -> None:
    settings, _ = seeded
    with make_client(settings, scratch_topic) as client:
        response = client.post(f"/v1/ingest/{LOGS}", json=batch("a", "b", "c"), headers=auth(LOGS))

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 3
    accepted = {r["event_id"] for r in body["results"]}

    records = read_topic(settings, scratch_topic)
    envelopes = [decode_event(value) for _, value in records]
    assert {str(e.event_id) for e in envelopes} == accepted
    for (key, value), envelope in zip(records, envelopes, strict=True):
        assert key == f"{LOGS}:payment".encode()
        assert envelope.workspace_id == DEMO_WORKSPACE_ID
        assert isinstance(envelope.payload, RawSignalPayload)
        assert envelope.payload.record["attributes"] == {"password": REDACTED}
        assert b"hunter2" not in value


def test_broker_outage_accepts_nothing_and_a_retry_keeps_event_ids(
    seeded: tuple[Settings, Engine], scratch_topic: str
) -> None:
    settings, _ = seeded
    # Nothing listens on port 1: every delivery times out unacknowledged.
    with make_client(settings, scratch_topic, bootstrap="127.0.0.1:1") as client:
        outage = client.post(f"/v1/ingest/{LOGS}", json=batch("x", "y"), headers=auth(LOGS))

    assert outage.status_code == 503
    failed = outage.json()
    assert (failed["accepted"], failed["failed"]) == (0, 2)
    assert {r["status"] for r in failed["results"]} == {"failed"}
    assert read_topic(settings, scratch_topic) == []

    with make_client(settings, scratch_topic) as client:
        retry = client.post(f"/v1/ingest/{LOGS}", json=batch("x", "y"), headers=auth(LOGS))

    assert retry.status_code == 200
    expected = [str(raw_event_id(LOGS, "live", sid)) for sid in ("x", "y")]
    assert [r["event_id"] for r in failed["results"]] == expected
    assert [r["event_id"] for r in retry.json()["results"]] == expected
    assert sorted(
        str(decode_event(value).event_id) for _, value in read_topic(settings, scratch_topic)
    ) == sorted(expected)


def test_source_state_comes_from_the_catalog(
    seeded: tuple[Settings, Engine], scratch_topic: str
) -> None:
    settings, engine = seeded
    with engine.begin() as conn:
        conn.execute(text("UPDATE sources SET enabled = false WHERE id = :id"), {"id": DEPLOYMENTS})
        conn.execute(
            text('UPDATE sources SET config = \'{"redact_fields": "x"}\' WHERE id = :id'),
            {"id": TRACES},
        )
    try:
        with make_client(settings, scratch_topic) as client:
            disabled = client.post(
                f"/v1/ingest/{DEPLOYMENTS}", json=batch("d"), headers=auth(DEPLOYMENTS)
            )
            misconfigured = client.post(
                f"/v1/ingest/{TRACES}", json=batch("t"), headers=auth(TRACES)
            )
            cross_source = client.post(f"/v1/ingest/{LOGS}", json=batch("l"), headers=auth(TRACES))
    finally:
        with engine.begin() as conn:
            conn.execute(text("UPDATE sources SET enabled = true, config = '{}'"))

    assert disabled.status_code == 403
    assert misconfigured.status_code == 500
    assert cross_source.status_code == 401
    assert read_topic(settings, scratch_topic) == []
