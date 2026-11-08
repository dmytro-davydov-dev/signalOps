"""Isolated replay of a dataset through the ingestion pipeline (P10-T02).

**Scope.** Each replay has its own run ID and so its own replay scope. Ingestion
derives event IDs from the source, the scope, and `source_event_id`, so the same
dataset replayed twice shares no ID, and republishing one replay is a no-op.

**Simulated time.** Records are ingested in order of event time, in ticks of
`tick`, each tick's batch received at the tick's end plus `ingest_delay`. So a
signal's `ingested_at` is at most `tick + ingest_delay` after its `occurred_at`,
and an investigation whose cutoff is the incident's detection time (event time,
ADR-016) sees what had arrived by then and nothing later. The detector closes
replay windows by its per-service simulated clock, and `flush_replay` closes the
rest at the end.

**Completion.** A replay is complete when PostgreSQL shows the work drained, never
after a fixed sleep: every accepted raw event has a normalizer receipt, every
normalized event of the scope is published from the outbox, and the detector and
the graph projector each hold a receipt for every one of them. Then the final
windows are flushed. A replay that does not drain before `timeout` is reported as
timed out with the raw events that were never normalized. The report reconciles
every count, so nothing accepted goes missing silently.

**Transport.** `KafkaTransport` publishes to `signals.raw.v1` for the running
pipeline services. `LocalTransport` runs the same processors in this process,
for tests and CI without the streaming services. Both are judged by the same
markers.
"""

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final, Protocol

from sqlalchemy import func, select

from signalops.adapters.ingestion import IngestPolicy, IngestSource, ingest_batch
from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP, Normalizer
from signalops.adapters.quality import TimingPolicy
from signalops.contracts.common import ContractModel, UtcDatetime, replay_scope_for
from signalops.contracts.evaluation import ReplayStatus
from signalops.contracts.events import EventType, decode_event
from signalops.contracts.ingestion import IngestRecordStatus
from signalops.demo.generator import DemoRecord
from signalops.detection.closure import flush_replay
from signalops.detection.detector import DETECTOR_CONSUMER_GROUP, DetectionPolicy, Detector
from signalops.detection.graph import GRAPH_CONSUMER_GROUP, GraphPolicy, GraphProjector
from signalops.persistence.database import Database
from signalops.persistence.models import ConsumerReceipt, Incident, OutboxEvent, Signal
from signalops.persistence.repositories.graph import GraphRepository, RunbookVersion
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.streaming.consumer import TransactionalProcessor
from signalops.streaming.producer import DeliveryResult, OutgoingMessage
from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE

logger = logging.getLogger(__name__)

NORMALIZED_TOPIC: Final = TOPIC_BY_EVENT_TYPE[EventType.SIGNAL_NORMALIZED]
DEFAULT_TICK: Final = timedelta(seconds=2)
DEFAULT_INGEST_DELAY: Final = timedelta(seconds=1)
MAX_UNRECONCILED_LISTED: Final = 20


class ReplayTransport(Protocol):
    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]: ...

    def close(self, timeout_seconds: float = 10.0) -> int: ...

    async def pump(self) -> None:
        """Advance work this transport runs itself; the Kafka pipeline runs elsewhere."""


@dataclass(frozen=True)
class ReplayTiming:
    tick: timedelta = DEFAULT_TICK
    ingest_delay: timedelta = DEFAULT_INGEST_DELAY
    timeout: timedelta = timedelta(minutes=5)
    poll_interval: timedelta = timedelta(milliseconds=500)


DEFAULT_TIMING: Final = ReplayTiming()


class PipelineMarkers(ContractModel):
    """Counts read from PostgreSQL that say how far the pipeline has got."""

    normalized: int
    signals_stored: int
    normalized_events: int
    normalized_published: int
    detected: int
    projected: int

    def drained(self, accepted: int) -> bool:
        events = self.normalized_events
        return (
            self.normalized == accepted
            and self.normalized_published == events
            and self.detected == events
            and self.projected == events
        )


class ReplayReport(ContractModel):
    replay_run_id: uuid.UUID
    replay_scope: str
    status: ReplayStatus
    records: int
    accepted: int
    rejected: int
    failed: int
    markers: PipelineMarkers
    windows_flushed: int
    incident_ids: tuple[uuid.UUID, ...]
    # Raw events accepted but never normalized; at most MAX_UNRECONCILED_LISTED.
    unreconciled_event_ids: tuple[uuid.UUID, ...]
    unreconciled_count: int
    simulated_start: UtcDatetime | None
    simulated_end: UtcDatetime | None
    started_at: UtcDatetime
    finished_at: UtcDatetime
    drain_seconds: float


@dataclass
class _Published:
    accepted: int = 0
    rejected: int = 0
    failed: int = 0
    raw_event_ids: list[uuid.UUID] = field(default_factory=list)


async def publish_dataset(
    records: Sequence[DemoRecord],
    *,
    replay_run_id: uuid.UUID,
    sources: Mapping[uuid.UUID, IngestSource],
    transport: ReplayTransport,
    policy: IngestPolicy,
    timing: ReplayTiming,
) -> _Published:
    """Ingest records tick by tick in event-time order, received at simulated times."""
    ticks: dict[tuple[datetime, uuid.UUID], list[DemoRecord]] = defaultdict(list)
    tick_seconds = timing.tick.total_seconds()
    for record in records:
        offset = record.occurred_at.timestamp() // tick_seconds * tick_seconds
        tick_start = datetime.fromtimestamp(offset, UTC)
        ticks[(tick_start, record.source_id)].append(record)
    published = _Published()
    for (tick_start, source_id), batch in sorted(ticks.items(), key=lambda item: item[0]):
        source = sources[source_id]
        items = []
        for record in batch:
            item = record.to_json()
            del item["source_id"]
            items.append(item)
        response = await ingest_batch(
            items,
            source=source,
            publisher=transport,
            policy=policy,
            now=tick_start + timing.tick + timing.ingest_delay,
            batch_id=uuid.uuid5(replay_run_id, f"{source_id}:{tick_start.isoformat()}"),
            replay_run_id=replay_run_id,
        )
        published.accepted += response.accepted
        published.rejected += response.rejected
        published.failed += response.failed
        published.raw_event_ids += [
            result.event_id
            for result in response.results
            if result.status is IngestRecordStatus.ACCEPTED and result.event_id is not None
        ]
    return published


async def read_markers(
    database: Database, scope: EvidenceScope, raw_event_ids: Collection[uuid.UUID]
) -> PipelineMarkers:
    async with database.transaction() as session:
        normalized = await session.scalar(
            select(func.count()).where(
                ConsumerReceipt.consumer_group == NORMALIZER_CONSUMER_GROUP,
                ConsumerReceipt.event_id.in_(list(raw_event_ids)),
            )
        )
        signals = select(Signal.id).where(scope.clause(Signal))
        stored = await session.scalar(select(func.count()).select_from(signals.subquery()))
        events = select(OutboxEvent.id, OutboxEvent.published_at).where(
            OutboxEvent.topic == NORMALIZED_TOPIC, OutboxEvent.aggregate_id.in_(signals)
        )
        rows = (await session.execute(events)).all()
        event_ids = [row.id for row in rows]

        async def receipts(group: str) -> int:
            if not event_ids:
                return 0
            count = await session.scalar(
                select(func.count()).where(
                    ConsumerReceipt.consumer_group == group,
                    ConsumerReceipt.event_id.in_(event_ids),
                )
            )
            return int(count or 0)

        return PipelineMarkers(
            normalized=int(normalized or 0),
            signals_stored=int(stored or 0),
            normalized_events=len(rows),
            normalized_published=sum(row.published_at is not None for row in rows),
            detected=await receipts(DETECTOR_CONSUMER_GROUP),
            projected=await receipts(GRAPH_CONSUMER_GROUP),
        )


async def unreconciled(database: Database, raw_event_ids: Collection[uuid.UUID]) -> list[uuid.UUID]:
    async with database.transaction() as session:
        done = set(
            await session.scalars(
                select(ConsumerReceipt.event_id).where(
                    ConsumerReceipt.consumer_group == NORMALIZER_CONSUMER_GROUP,
                    ConsumerReceipt.event_id.in_(list(raw_event_ids)),
                )
            )
        )
    return sorted(event_id for event_id in raw_event_ids if event_id not in done)


async def seed_runbooks(
    database: Database,
    scope: EvidenceScope,
    runbooks: Sequence[tuple[RunbookVersion, Sequence[uuid.UUID]]],
) -> None:
    """Give the replay scope the runbooks a live workspace would have."""
    async with database.transaction() as session:
        graph = GraphRepository(session)
        for runbook, service_ids in runbooks:
            await graph.ensure_runbook(scope, runbook, service_ids)


async def replay(
    database: Database,
    records: Sequence[DemoRecord],
    *,
    replay_run_id: uuid.UUID,
    workspace_id: uuid.UUID,
    sources: Mapping[uuid.UUID, IngestSource],
    transport: ReplayTransport,
    ingest_policy: IngestPolicy,
    detection_policy: DetectionPolicy,
    timing: ReplayTiming = DEFAULT_TIMING,
) -> ReplayReport:
    """Publish a dataset into its own scope, wait for the pipeline to drain, then flush."""
    started_at = datetime.now(UTC)
    scope = EvidenceScope(workspace_id, replay_scope_for(replay_run_id))
    published = await publish_dataset(
        records,
        replay_run_id=replay_run_id,
        sources=sources,
        transport=transport,
        policy=ingest_policy,
        timing=timing,
    )
    clock = time.monotonic()
    deadline = clock + timing.timeout.total_seconds()
    while True:
        await transport.pump()
        markers = await read_markers(database, scope, published.raw_event_ids)
        if markers.drained(published.accepted) or time.monotonic() >= deadline:
            break
        await asyncio.sleep(timing.poll_interval.total_seconds())
    drained = markers.drained(published.accepted)
    drain_seconds = time.monotonic() - clock
    flushed = await flush_replay(database, scope, detection_policy) if drained else 0
    async with database.transaction() as session:
        incident_ids = tuple(
            await session.scalars(
                select(Incident.id).where(scope.clause(Incident)).order_by(Incident.detected_at)
            )
        )
    missing = [] if drained else await unreconciled(database, published.raw_event_ids)
    report = ReplayReport(
        replay_run_id=replay_run_id,
        replay_scope=scope.replay_scope,
        status=ReplayStatus.COMPLETED if drained else ReplayStatus.TIMED_OUT,
        records=len(records),
        accepted=published.accepted,
        rejected=published.rejected,
        failed=published.failed,
        markers=markers,
        windows_flushed=flushed,
        incident_ids=incident_ids,
        unreconciled_event_ids=tuple(missing[:MAX_UNRECONCILED_LISTED]),
        unreconciled_count=len(missing),
        simulated_start=min((record.occurred_at for record in records), default=None),
        simulated_end=max((record.occurred_at for record in records), default=None),
        started_at=started_at,
        finished_at=datetime.now(UTC),
        drain_seconds=round(drain_seconds, 3),
    )
    logger.info(
        "replay finished",
        extra={
            "replay_scope": scope.replay_scope,
            "status": report.status.value,
            "accepted": report.accepted,
            "windows_flushed": flushed,
            "incidents": len(incident_ids),
            "drain_seconds": report.drain_seconds,
        },
    )
    return report


class KafkaTransport:
    """Publishes raw records to Kafka; the pipeline services do the rest."""

    def __init__(self, publisher: "ReplayPublisher") -> None:
        self._publisher = publisher

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        return self._publisher.publish(messages)

    def close(self, timeout_seconds: float = 10.0) -> int:
        return self._publisher.close(timeout_seconds)

    async def pump(self) -> None:
        return None


class ReplayPublisher(Protocol):
    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]: ...

    def close(self, timeout_seconds: float = 10.0) -> int: ...


class LocalTransport:
    """Runs the normalizer, detector, and graph projector in this process.

    Published raw records wait in memory until `pump`, which processes up to
    `batch` of them, then hands the scope's unpublished normalized outbox events
    to the detector and projector in event-time order and marks them published,
    as the outbox publisher would after acknowledgement.
    """

    def __init__(
        self,
        database: Database,
        scope: EvidenceScope,
        *,
        timing_policy: TimingPolicy,
        detection_policy: DetectionPolicy,
        graph_policy: GraphPolicy,
        batch: int = 10_000,
    ) -> None:
        self._database = database
        self._scope = scope
        self._batch = batch
        self._raw: list[bytes] = []
        self._normalizer = TransactionalProcessor(
            database, NORMALIZER_CONSUMER_GROUP, Normalizer(timing_policy)
        )
        self._detector = TransactionalProcessor(
            database, DETECTOR_CONSUMER_GROUP, Detector(detection_policy)
        )
        self._projector = TransactionalProcessor(
            database, GRAPH_CONSUMER_GROUP, GraphProjector(graph_policy)
        )

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        self._raw += [message.value for message in messages]
        return [DeliveryResult(message) for message in messages]

    def close(self, timeout_seconds: float = 10.0) -> int:
        return len(self._raw)

    async def pump(self) -> None:
        taken, self._raw = self._raw[: self._batch], self._raw[self._batch :]
        for value in taken:
            await self._normalizer.process(decode_event(value))
        async with self._database.transaction() as session:
            rows = list(
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.topic == NORMALIZED_TOPIC,
                        OutboxEvent.published_at.is_(None),
                        OutboxEvent.aggregate_id.in_(
                            select(Signal.id).where(self._scope.clause(Signal))
                        ),
                    )
                )
            )
        envelopes = sorted(
            (decode_event(json.dumps(row.envelope).encode()) for row in rows),
            key=lambda envelope: (envelope.occurred_at, envelope.event_id),
        )
        for envelope in envelopes:
            await self._detector.process(envelope)
            await self._projector.process(envelope)
        async with self._database.transaction() as session:
            await OutboxRepository(session).mark_published([row.id for row in rows])
