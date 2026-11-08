"""Source-facing batch ingestion: credentials, record checks, redaction, publication.

`ingest_batch` validates each record independently, redacts configured fields,
and publishes `signal.raw` envelopes to signals.raw.v1. A record is reported
accepted only when Kafka acknowledged it. Every other record is rejected (the
record is at fault) or failed (retry it unchanged).

The envelope event ID is derived from the source, replay scope, and
`source_event_id`, so a client retry, including one after an ambiguous
timeout, produces the same event ID and consumers deduplicate it. A record
sent without `source_event_id` gets a stable one derived from its content
(`adapters.quality.derive_source_event_id`) and is flagged downstream.
"""

import asyncio
import logging
import re
import threading
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from pydantic import JsonValue, TypeAdapter, ValidationError

from signalops.adapters.quality import MissingIdentityError, derive_source_event_id
from signalops.config import Settings
from signalops.contracts.common import (
    MAX_SOURCE_EVENT_ID_LENGTH,
    ContractError,
    Name,
    describe_validation_error,
    json_size,
    replay_scope_for,
)
from signalops.contracts.events import (
    MAX_RAW_RECORD_BYTES,
    EventEnvelope,
    EventType,
    RawSignalPayload,
)
from signalops.contracts.evidence import SourceKind
from signalops.contracts.ingestion import (
    IngestFailureReason,
    IngestRecord,
    IngestRecordResult,
    IngestRecordStatus,
    IngestRejectReason,
    IngestResponse,
)
from signalops.observability.metrics import INGEST_RECORDS
from signalops.persistence.database import Database
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.streaming.producer import (
    DeliveryError,
    DeliveryErrorCategory,
    DeliveryResult,
    OutgoingMessage,
    ProducerUnusableError,
)

logger = logging.getLogger(__name__)

# Fixed namespace for raw signal event IDs. Changing it changes every ID and
# defeats deduplication of retries that span the change.
RAW_EVENT_NAMESPACE = uuid.UUID("0b5d7c1e-2f43-4a8e-9c61-5e8f3d2a7b90")

REDACTED = "[REDACTED]"

_SERVICE_NAME = TypeAdapter[str](Name)


def raw_event_id(source_id: uuid.UUID, replay_scope: str, source_event_id: str) -> uuid.UUID:
    return uuid.uuid5(RAW_EVENT_NAMESPACE, f"{source_id}:{replay_scope}:{source_event_id}")


def normalize_field_name(name: str) -> str:
    return name.strip().lower().replace("-", "_").replace(" ", "_")


class Redactor:
    """Replaces the values of sensitive keys at any depth of a JSON document."""

    def __init__(self, fields: Iterable[str]) -> None:
        self._fields = frozenset(normalize_field_name(field) for field in fields)

    def redact(self, value: JsonValue) -> tuple[JsonValue, int]:
        """Return a redacted copy and the number of values replaced."""
        if isinstance(value, dict):
            count = 0
            result: dict[str, JsonValue] = {}
            for key, item in value.items():
                if normalize_field_name(key) in self._fields:
                    result[key] = REDACTED
                    count += 1
                else:
                    result[key], nested = self.redact(item)
                    count += nested
            return result, count
        if isinstance(value, list):
            items = [self.redact(item) for item in value]
            return [item for item, _ in items], sum(nested for _, nested in items)
        return value, 0


class SourceConfigError(Exception):
    """A source's stored configuration cannot be applied; fail closed."""


@dataclass(frozen=True)
class IngestSource:
    id: uuid.UUID
    workspace_id: uuid.UUID
    kind: SourceKind
    enabled: bool
    # Added to the process-wide redaction list for this source.
    redact_fields: tuple[str, ...] = ()


class TextRedactor:
    """Masks `key=value` pairs of sensitive keys in free text, which ingestion keeps as is."""

    def __init__(self, fields: tuple[str, ...]) -> None:
        self.fields = fields
        names = "|".join(sorted((re.escape(field) for field in fields), key=len, reverse=True))
        self._pairs = re.compile(rf"(?i)\b({names})\b(\s*[:=]\s*)(?:bearer\s+)?[^\s,;\"']+")
        self._bearer = re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+")

    def redact(self, text: str) -> str:
        masked = self._pairs.sub(rf"\1\2{REDACTED}", text)
        return self._bearer.sub(f"Bearer {REDACTED}", masked)


def source_redact_fields(config: Mapping[str, Any]) -> tuple[str, ...]:
    value = config.get("redact_fields", [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SourceConfigError("redact_fields must be a list of strings")
    return tuple(value)


def source_text_redactors(
    base: TextRedactor, sources: Iterable[Any]
) -> dict[uuid.UUID, TextRedactor]:
    """Per source, `base` plus the source's own `redact_fields`; raises SourceConfigError."""
    return {
        source.id: TextRedactor((*base.fields, *source_redact_fields(source.config)))
        for source in sources
    }


class SourceLookup(Protocol):
    async def get(self, source_id: uuid.UUID) -> IngestSource | None: ...


@dataclass(frozen=True)
class DatabaseSourceLookup:
    database: Database

    async def get(self, source_id: uuid.UUID) -> IngestSource | None:
        async with self.database.transaction() as session:
            source = await CatalogRepository(session).get_source(source_id)
        if source is None:
            return None
        return IngestSource(
            id=source.id,
            workspace_id=source.workspace_id,
            kind=SourceKind(source.kind),
            enabled=source.enabled,
            redact_fields=source_redact_fields(source.config),
        )


class Publisher(Protocol):
    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]: ...

    def close(self, timeout_seconds: float = 10.0) -> int: ...


class ReplaceablePublisher:
    """Replaces its producer after a fatal error instead of failing every later call.

    The batch that met the fatal error has already been reported as failed by
    the producer; only later batches use the replacement.
    """

    def __init__(self, factory: Callable[[], Publisher]) -> None:
        self._factory = factory
        self._lock = threading.Lock()
        self._producer = factory()

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        producer = self._producer
        try:
            return producer.publish(messages)
        except ProducerUnusableError:
            with self._lock:
                if self._producer is producer:
                    logger.warning("replacing kafka producer after a fatal error")
                    producer.close(0)
                    self._producer = self._factory()
                producer = self._producer
            return producer.publish(messages)

    def close(self, timeout_seconds: float = 10.0) -> int:
        return self._producer.close(timeout_seconds)


@dataclass(frozen=True)
class IngestPolicy:
    max_future: timedelta
    max_age: timedelta
    redact_fields: tuple[str, ...]

    @classmethod
    def from_settings(cls, settings: Settings) -> "IngestPolicy":
        return cls(
            max_future=timedelta(seconds=settings.ingest_max_future_seconds),
            max_age=timedelta(seconds=settings.ingest_max_age_seconds),
            redact_fields=tuple(settings.ingest_redact_fields),
        )


def _readable_source_event_id(item: object) -> str | None:
    if not isinstance(item, dict):
        return None
    value = item.get("source_event_id")
    if isinstance(value, str) and 1 <= len(value) <= MAX_SOURCE_EVENT_ID_LENGTH:
        return value
    return None


def _service_hint(payload: Mapping[str, JsonValue]) -> str | None:
    # Routing only; the normalizer resolves and flags identity.
    try:
        return _SERVICE_NAME.validate_python(payload.get("service"))
    except ValidationError:
        return None


def _rejected(
    index: int, source_event_id: str | None, reason: IngestRejectReason, detail: str
) -> IngestRecordResult:
    return IngestRecordResult(
        index=index,
        source_event_id=source_event_id,
        status=IngestRecordStatus.REJECTED,
        reason=reason,
        detail=detail,
    )


def _delivery_outcome(
    index: int, source_event_id: str, result: DeliveryResult
) -> IngestRecordResult:
    event_id = result.message.event_id
    if result.error is None:
        return IngestRecordResult(
            index=index,
            source_event_id=source_event_id,
            status=IngestRecordStatus.ACCEPTED,
            event_id=event_id,
        )
    if result.error.category is DeliveryErrorCategory.TIMEOUT:
        reason = IngestFailureReason.AMBIGUOUS_TIMEOUT
    else:
        reason = IngestFailureReason.BROKER_UNAVAILABLE
    return IngestRecordResult(
        index=index,
        source_event_id=source_event_id,
        status=IngestRecordStatus.FAILED,
        event_id=event_id,
        reason=reason,
        detail=result.error.detail,
    )


async def ingest_batch(
    items: Sequence[object],
    *,
    source: IngestSource,
    publisher: Publisher,
    policy: IngestPolicy,
    now: datetime,
    batch_id: uuid.UUID,
    replay_run_id: uuid.UUID | None = None,
) -> IngestResponse:
    """Validate, redact, and publish one batch; `now` is the receipt time.

    The HTTP API always ingests live. A replay passes its run ID (and a simulated
    `now`), which puts the records in their own replay scope and derives their event
    IDs within it, so republishing a replay is idempotent and two replays of the same
    dataset never share an ID.
    """
    replay_scope = replay_scope_for(replay_run_id)
    redactor = Redactor((*policy.redact_fields, *source.redact_fields))
    results: dict[int, IngestRecordResult] = {}
    pending: list[tuple[int, str, OutgoingMessage]] = []
    seen: set[str] = set()
    redacted_values = 0

    for index, item in enumerate(items):
        source_event_id = _readable_source_event_id(item)
        payload = item.get("payload") if isinstance(item, dict) else None
        if isinstance(payload, dict) and json_size(payload) > MAX_RAW_RECORD_BYTES:
            results[index] = _rejected(
                index,
                source_event_id,
                IngestRejectReason.OVERSIZED,
                f"payload exceeds {MAX_RAW_RECORD_BYTES} bytes when serialized",
            )
            continue
        try:
            record = IngestRecord.model_validate(item)
        except ValidationError as error:
            results[index] = _rejected(
                index,
                source_event_id,
                IngestRejectReason.INVALID_RECORD,
                describe_validation_error(error),
            )
            continue
        if not now - policy.max_age <= record.occurred_at <= now + policy.max_future:
            results[index] = _rejected(
                index,
                record.source_event_id,
                IngestRejectReason.TIMESTAMP_OUT_OF_RANGE,
                "occurred_at is outside the accepted range around receipt time",
            )
            continue

        redacted, count = redactor.redact(record.payload)
        assert isinstance(redacted, dict)  # noqa: S101 - redaction preserves the top-level type.
        derived = record.source_event_id is None
        if record.source_event_id is not None:
            source_event_id = record.source_event_id
        else:
            try:
                # From the redacted payload, so no secret value feeds the ID.
                source_event_id = derive_source_event_id(source.kind, record.occurred_at, redacted)
            except MissingIdentityError as error:
                results[index] = _rejected(
                    index, None, IngestRejectReason.MISSING_SOURCE_EVENT_ID, str(error)
                )
                continue
        if source_event_id in seen:
            results[index] = _rejected(
                index,
                source_event_id,
                IngestRejectReason.DUPLICATE_IN_BATCH,
                "source_event_id already appears earlier in this batch",
            )
            continue
        seen.add(source_event_id)
        redacted_values += count
        try:
            envelope = EventEnvelope[RawSignalPayload](
                event_id=raw_event_id(source.id, replay_scope, source_event_id),
                event_type=EventType.SIGNAL_RAW,
                schema_version=1,
                workspace_id=source.workspace_id,
                source_id=source.id,
                occurred_at=record.occurred_at,
                ingested_at=now,
                replay_run_id=replay_run_id,
                correlation_id=str(batch_id),
                payload=RawSignalPayload(
                    source_kind=source.kind,
                    source_event_id=source_event_id,
                    source_event_id_derived=derived,
                    service_hint=_service_hint(redacted),
                    record=redacted,
                ),
            )
            message = OutgoingMessage.from_envelope(envelope)  # type: ignore[arg-type]
        except ValidationError as error:
            results[index] = _rejected(
                index,
                source_event_id,
                IngestRejectReason.INVALID_RECORD,
                describe_validation_error(error),
            )
            continue
        except ContractError as error:
            results[index] = _rejected(
                index, source_event_id, IngestRejectReason.OVERSIZED, error.detail
            )
            continue
        pending.append((index, source_event_id, message))

    if pending:
        messages = [message for _, _, message in pending]
        try:
            deliveries = await asyncio.to_thread(publisher.publish, messages)
        except ProducerUnusableError as error:
            unusable = DeliveryError(DeliveryErrorCategory.FATAL, str(error))
            deliveries = [DeliveryResult(message, error=unusable) for message in messages]
        for (index, source_event_id, _), delivery in zip(pending, deliveries, strict=True):
            results[index] = _delivery_outcome(index, source_event_id, delivery)

    ordered = [results[index] for index in range(len(items))]
    counts = {status: 0 for status in IngestRecordStatus}
    for result in ordered:
        counts[result.status] += 1
    for status, count in counts.items():
        if count:
            INGEST_RECORDS.labels(source.kind.value, status.value).inc(count)
    logger.info(
        "ingest batch",
        extra={
            "batch_id": str(batch_id),
            "source_id": str(source.id),
            "records": len(items),
            "accepted": counts[IngestRecordStatus.ACCEPTED],
            "rejected": counts[IngestRecordStatus.REJECTED],
            "failed": counts[IngestRecordStatus.FAILED],
            "redacted_values": redacted_values,
        },
    )
    return IngestResponse(
        batch_id=batch_id,
        accepted=counts[IngestRecordStatus.ACCEPTED],
        rejected=counts[IngestRecordStatus.REJECTED],
        failed=counts[IngestRecordStatus.FAILED],
        results=ordered,
    )
