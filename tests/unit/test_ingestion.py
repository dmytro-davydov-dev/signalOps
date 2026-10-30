import json
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from signalops.adapters.ingestion import (
    REDACTED,
    IngestSource,
    Redactor,
    ReplaceablePublisher,
    SourceConfigError,
    SourceTokens,
    raw_event_id,
    source_redact_fields,
)
from signalops.api.app import create_app
from signalops.config import Settings
from signalops.contracts.events import RawSignalPayload, decode_event
from signalops.contracts.evidence import SourceKind
from signalops.dependencies import Resources
from signalops.streaming.producer import (
    DeliveryError,
    DeliveryErrorCategory,
    DeliveryResult,
    OutgoingMessage,
    ProducerUnusableError,
)

WORKSPACE_ID = uuid.UUID("82631aa1-86f5-4d37-b39f-fac74ebbdac8")
SOURCE_ID = uuid.UUID("b6c72d79-7ec9-4df7-88d0-1d4332c88808")
OTHER_SOURCE_ID = uuid.UUID("1dee9492-8198-4ab9-af5e-9bcc709660fd")
TOKEN = "test-source-token"  # noqa: S105 - test fixture, not a credential.
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@dataclass
class FakePublisher:
    """Acknowledges every message unless `error` is set."""

    error: DeliveryError | None = None
    unusable: bool = False
    published: list[OutgoingMessage] = field(default_factory=list)
    closed: bool = False

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        if self.unusable:
            raise ProducerUnusableError("fatal earlier")
        if self.error is not None:
            return [DeliveryResult(message, error=self.error) for message in messages]
        self.published.extend(messages)
        start = len(self.published) - len(messages)
        return [
            DeliveryResult(message, partition=0, offset=start + i)
            for i, message in enumerate(messages)
        ]

    def close(self, timeout_seconds: float = 10.0) -> int:
        self.closed = True
        return 0


@dataclass
class FakeSources:
    sources: dict[uuid.UUID, IngestSource]
    error: Exception | None = None

    async def get(self, source_id: uuid.UUID) -> IngestSource | None:
        if self.error is not None:
            raise self.error
        return self.sources.get(source_id)


def demo_source(**changes: Any) -> IngestSource:
    values: dict[str, Any] = {
        "id": SOURCE_ID,
        "workspace_id": WORKSPACE_ID,
        "kind": SourceKind.STRUCTURED_LOG,
        "enabled": True,
    }
    return IngestSource(**(values | changes))


def make_client(
    publisher: FakePublisher | None = None,
    sources: FakeSources | None = None,
    **settings_changes: Any,
) -> TestClient:
    settings = Settings(
        database_url=SecretStr("postgresql+psycopg://unused@localhost/unused"),
        ingest_tokens={SOURCE_ID: SecretStr(TOKEN), OTHER_SOURCE_ID: SecretStr("other-token")},
        **settings_changes,
    )
    resolved_sources = sources or FakeSources({SOURCE_ID: demo_source()})

    @asynccontextmanager
    async def fake_resources(_: Settings) -> AsyncIterator[Resources]:
        yield Resources(
            checks=[],
            sources=resolved_sources,
            ingest_publisher=publisher if publisher is not None else FakePublisher(),
        )

    return TestClient(create_app(settings, resource_factory=fake_resources))


def now_text(offset: timedelta = timedelta()) -> str:
    return (datetime.now(UTC) + offset).isoformat()


def log_record(source_event_id: str, **payload: Any) -> dict[str, Any]:
    return {
        "source_event_id": source_event_id,
        "occurred_at": now_text(),
        "payload": {"service": "payment", "level": "error", "message": "timeout"} | payload,
    }


def post(client: TestClient, records: list[Any], headers: dict[str, str] = AUTH) -> Any:
    return client.post(f"/v1/ingest/{SOURCE_ID}", json={"records": records}, headers=headers)


def test_accepted_records_are_published_as_raw_envelopes() -> None:
    publisher = FakePublisher()
    with make_client(publisher) as client:
        response = post(client, [log_record("evt-1"), log_record("evt-2")])

    assert response.status_code == 200
    body = response.json()
    assert (body["accepted"], body["rejected"], body["failed"]) == (2, 0, 0)
    assert [r["status"] for r in body["results"]] == ["accepted", "accepted"]
    assert len(publisher.published) == 2

    message = publisher.published[0]
    envelope = decode_event(message.value)
    assert message.topic == "signals.raw.v1"
    assert message.key == f"{SOURCE_ID}:payment"
    assert envelope.event_id == raw_event_id(SOURCE_ID, "live", "evt-1")
    assert str(envelope.event_id) == body["results"][0]["event_id"]
    assert envelope.workspace_id == WORKSPACE_ID
    assert envelope.source_id == SOURCE_ID
    assert envelope.correlation_id == body["batch_id"]
    assert envelope.replay_run_id is None
    payload = envelope.payload
    assert isinstance(payload, RawSignalPayload)
    assert payload.source_kind is SourceKind.STRUCTURED_LOG
    assert payload.source_event_id == "evt-1"
    assert payload.record == {"service": "payment", "level": "error", "message": "timeout"}


def test_client_retry_reuses_the_event_id() -> None:
    publisher = FakePublisher()
    with make_client(publisher) as client:
        first = post(client, [log_record("evt-1")]).json()
        second = post(client, [log_record("evt-1", message="timeout again")]).json()

    assert first["batch_id"] != second["batch_id"]
    assert first["results"][0]["event_id"] == second["results"][0]["event_id"]
    assert len({m.event_id for m in publisher.published}) == 1


def test_event_id_depends_on_source_scope_and_source_event_id() -> None:
    base = raw_event_id(SOURCE_ID, "live", "evt-1")
    assert base == raw_event_id(SOURCE_ID, "live", "evt-1")
    assert base != raw_event_id(OTHER_SOURCE_ID, "live", "evt-1")
    assert base != raw_event_id(SOURCE_ID, f"replay:{uuid.uuid4()}", "evt-1")
    assert base != raw_event_id(SOURCE_ID, "live", "evt-2")


@pytest.mark.parametrize(
    ("category", "reason"),
    [
        (DeliveryErrorCategory.TIMEOUT, "ambiguous_timeout"),
        (DeliveryErrorCategory.TRANSIENT, "broker_unavailable"),
        (DeliveryErrorCategory.QUEUE_FULL, "broker_unavailable"),
        (DeliveryErrorCategory.REJECTED, "broker_unavailable"),
        (DeliveryErrorCategory.FATAL, "broker_unavailable"),
    ],
)
def test_unacknowledged_records_fail_with_503(category: DeliveryErrorCategory, reason: str) -> None:
    publisher = FakePublisher(error=DeliveryError(category, "_TRANSPORT: broker down"))
    with make_client(publisher) as client:
        response = post(client, [log_record("evt-1"), log_record("evt-2")])

    assert response.status_code == 503
    assert response.headers["Retry-After"]
    body = response.json()
    assert (body["accepted"], body["rejected"], body["failed"]) == (0, 0, 2)
    for result in body["results"]:
        assert result["status"] == "failed"
        assert result["reason"] == reason
        assert result["event_id"] == str(raw_event_id(SOURCE_ID, "live", result["source_event_id"]))


def test_unusable_producer_fails_every_record() -> None:
    with make_client(FakePublisher(unusable=True)) as client:
        response = post(client, [log_record("evt-1")])
    assert response.status_code == 503
    assert response.json()["results"][0]["status"] == "failed"


def test_rejections_are_reported_per_record_and_the_rest_proceed() -> None:
    publisher = FakePublisher()
    records = [
        log_record("ok-1"),
        {"source_event_id": "no-payload", "occurred_at": now_text()},
        log_record("naive") | {"occurred_at": "2026-10-30T10:00:00"},
        log_record("numeric") | {"occurred_at": 1_793_000_000},
        log_record("future") | {"occurred_at": now_text(timedelta(days=2))},
        log_record("ancient") | {"occurred_at": now_text(-timedelta(days=31))},
        log_record("big", filler="x" * 70_000),
        log_record("ok-1"),
        "not an object",
        log_record("extra") | {"source_id": str(SOURCE_ID)},
        log_record("ok-2"),
    ]
    with make_client(publisher) as client:
        response = post(client, records)

    assert response.status_code == 200
    body = response.json()
    outcomes = [
        (r["index"], r["source_event_id"], r["status"], r["reason"]) for r in body["results"]
    ]
    assert outcomes == [
        (0, "ok-1", "accepted", None),
        (1, "no-payload", "rejected", "invalid_record"),
        (2, "naive", "rejected", "invalid_record"),
        (3, "numeric", "rejected", "invalid_record"),
        (4, "future", "rejected", "timestamp_out_of_range"),
        (5, "ancient", "rejected", "timestamp_out_of_range"),
        (6, "big", "rejected", "oversized"),
        (7, "ok-1", "rejected", "duplicate_in_batch"),
        (8, None, "rejected", "invalid_record"),
        (9, "extra", "rejected", "invalid_record"),
        (10, "ok-2", "accepted", None),
    ]
    assert (body["accepted"], body["rejected"], body["failed"]) == (2, 9, 0)
    assert [m.event_id for m in publisher.published] == [
        raw_event_id(SOURCE_ID, "live", "ok-1"),
        raw_event_id(SOURCE_ID, "live", "ok-2"),
    ]


def test_rejection_detail_never_echoes_record_values() -> None:
    record = log_record("evt-1") | {"occurred_at": "secret-value-not-a-date"}
    with make_client() as client:
        response = post(client, [record])
    assert response.json()["results"][0]["reason"] == "invalid_record"
    assert "secret-value" not in response.text


def test_payloads_are_redacted_before_publishing() -> None:
    publisher = FakePublisher()
    source = demo_source(redact_fields=("customer_email",))
    record = log_record(
        "evt-1",
        Authorization="Bearer abc",
        attributes={
            "db": {"Password": "hunter2", "host": "orders-db"},
            "headers": [{"set-cookie": "sid=1"}, {"accept": "json"}],
            "customer email": "a@example.com",
            "api_key_hint": "kept",
        },
    )
    with make_client(publisher, FakeSources({SOURCE_ID: source})) as client:
        response = post(client, [record])

    assert response.status_code == 200
    published = decode_event(publisher.published[0].value).payload
    assert isinstance(published, RawSignalPayload)
    assert published.record["Authorization"] == REDACTED
    assert published.record["attributes"] == {
        "db": {"Password": REDACTED, "host": "orders-db"},
        "headers": [{"set-cookie": REDACTED}, {"accept": "json"}],
        "customer email": REDACTED,
        "api_key_hint": "kept",
    }
    for secret in ("abc", "hunter2", "sid=1", "a@example.com"):
        assert secret not in publisher.published[0].value.decode()


def test_redactor_counts_replacements() -> None:
    redacted, count = Redactor(["token"]).redact({"Token": "a", "nested": [{"TOKEN": "b"}], "n": 1})
    assert redacted == {"Token": REDACTED, "nested": [{"TOKEN": REDACTED}], "n": 1}
    assert count == 2


def test_service_hint_is_omitted_when_not_a_valid_name() -> None:
    publisher = FakePublisher()
    with make_client(publisher) as client:
        post(client, [log_record("a", service="bad name!"), log_record("b", service=7)])
    assert [m.key for m in publisher.published] == [str(SOURCE_ID), str(SOURCE_ID)]


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": f"Basic {TOKEN}"},
        {"Authorization": "Bearer wrong"},
        # A valid token for another source.
        {"Authorization": "Bearer other-token"},
    ],
)
def test_missing_or_wrong_credential_is_401(headers: dict[str, str]) -> None:
    publisher = FakePublisher()
    with make_client(publisher) as client:
        response = post(client, [log_record("evt-1")], headers=headers)
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert publisher.published == []


def test_source_without_a_configured_token_is_401() -> None:
    unconfigured = uuid.uuid4()
    with make_client() as client:
        response = client.post(
            f"/v1/ingest/{unconfigured}", json={"records": [log_record("e")]}, headers=AUTH
        )
    assert response.status_code == 401


def test_source_tokens_verify_only_their_own_source() -> None:
    tokens = SourceTokens({SOURCE_ID: SecretStr("a")})
    assert tokens.verify(SOURCE_ID, "a")
    assert not tokens.verify(SOURCE_ID, "b")
    assert not tokens.verify(OTHER_SOURCE_ID, "a")


@pytest.mark.parametrize(
    ("sources", "expected"),
    [
        (FakeSources({SOURCE_ID: demo_source(enabled=False)}), 403),
        (FakeSources({}), 404),
        (FakeSources({}, error=OperationalError("SELECT", {}, Exception("down"))), 503),
        (FakeSources({}, error=SourceConfigError("bad")), 500),
    ],
)
def test_source_state_errors(sources: FakeSources, expected: int) -> None:
    publisher = FakePublisher()
    with make_client(publisher, sources) as client:
        response = post(client, [log_record("evt-1")])
    assert response.status_code == expected
    assert publisher.published == []


def test_missing_ingestion_resources_is_503() -> None:
    settings = Settings(
        database_url=SecretStr("postgresql+psycopg://unused@localhost/unused"),
        ingest_tokens={SOURCE_ID: SecretStr(TOKEN)},
    )

    @asynccontextmanager
    async def bare(_: Settings) -> AsyncIterator[Resources]:
        yield Resources(checks=[])

    with TestClient(create_app(settings, resource_factory=bare)) as client:
        assert post(client, [log_record("evt-1")]).status_code == 503


def test_oversized_request_is_413_by_header_or_stream() -> None:
    publisher = FakePublisher()
    with make_client(publisher, ingest_max_request_bytes=64 * 1024) as client:
        records = [log_record(f"evt-{i}", filler="x" * 1000) for i in range(80)]
        response = post(client, records)
        assert response.status_code == 413

        def chunks() -> Any:
            yield b'{"records": ['
            for _ in range(80):
                yield json.dumps(log_record("e", filler="x" * 1000)).encode() + b","
            yield b"{}]}"

        streamed = client.post(
            f"/v1/ingest/{SOURCE_ID}",
            content=chunks(),
            headers=AUTH | {"Content-Type": "application/json"},
        )
        assert streamed.status_code == 413
    assert publisher.published == []


def test_too_many_records_is_413() -> None:
    with make_client(ingest_max_records=2) as client:
        response = post(client, [log_record(f"evt-{i}") for i in range(3)])
    assert response.status_code == 413


@pytest.mark.parametrize(
    "body",
    [b"not json", b"[]", b'{"records": []}', b'{"records": {}}', b'{"items": [{}]}'],
)
def test_body_that_is_not_a_batch_is_422(body: bytes) -> None:
    with make_client() as client:
        response = client.post(
            f"/v1/ingest/{SOURCE_ID}",
            content=body,
            headers=AUTH | {"Content-Type": "application/json"},
        )
    assert response.status_code == 422


def test_source_redact_fields_must_be_a_list_of_strings() -> None:
    assert source_redact_fields({}) == ()
    assert source_redact_fields({"redact_fields": ["a"]}) == ("a",)
    for bad in ("a", [1], {"a": 1}):
        with pytest.raises(SourceConfigError):
            source_redact_fields({"redact_fields": bad})


def test_replaceable_publisher_replaces_an_unusable_producer() -> None:
    made: list[FakePublisher] = []

    def factory() -> FakePublisher:
        made.append(FakePublisher(unusable=not made))
        return made[-1]

    publisher = ReplaceablePublisher(factory)
    message = OutgoingMessage("signals.raw.v1", "k", b"{}", uuid.uuid4())
    results = publisher.publish([message])

    assert [r.delivered for r in results] == [True]
    assert len(made) == 2
    assert made[0].closed
    assert made[1].published == [message]


def unidentified_log(**payload: Any) -> dict[str, Any]:
    record = log_record("unused", **payload)
    del record["source_event_id"]
    return record


def test_record_without_id_gets_a_derived_one() -> None:
    publisher = FakePublisher()
    record = unidentified_log()
    with make_client(publisher) as client:
        first = post(client, [record]).json()
        retried = post(client, [record]).json()

    [result] = first["results"]
    assert result["status"] == "accepted"
    assert result["source_event_id"].startswith("derived:structured_log:")
    assert retried["results"][0]["source_event_id"] == result["source_event_id"]
    assert retried["results"][0]["event_id"] == result["event_id"]
    payload = decode_event(publisher.published[0].value).payload
    assert isinstance(payload, RawSignalPayload)
    assert payload.source_event_id == result["source_event_id"]
    assert payload.source_event_id_derived is True


def test_sent_ids_are_not_marked_derived() -> None:
    publisher = FakePublisher()
    with make_client(publisher) as client:
        post(client, [log_record("evt-1")])

    payload = decode_event(publisher.published[0].value).payload
    assert isinstance(payload, RawSignalPayload)
    assert payload.source_event_id_derived is False


def test_derived_id_is_computed_after_redaction() -> None:
    publisher = FakePublisher()
    occurred_at = now_text()
    records = [
        unidentified_log(password=secret) | {"occurred_at": occurred_at}
        for secret in ("hunter2", "correct-horse")
    ]
    with make_client(publisher) as client:
        body = post(client, records).json()

    # The secret value does not feed the ID, so both redact to the same record.
    assert [(r["status"], r["reason"]) for r in body["results"]] == [
        ("accepted", None),
        ("rejected", "duplicate_in_batch"),
    ]


def test_record_without_id_or_fallback_fields_is_rejected() -> None:
    sources = FakeSources({SOURCE_ID: demo_source(kind=SourceKind.TRACE_SUMMARY)})
    record = {"occurred_at": now_text(), "payload": {"service": "payment", "operation": "GET /"}}
    with make_client(sources=sources) as client:
        body = post(client, [record]).json()

    [result] = body["results"]
    assert (result["status"], result["reason"]) == ("rejected", "missing_source_event_id")
    assert result["source_event_id"] is None
    assert "trace_id" in result["detail"]
