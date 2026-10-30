"""The normalizer's consumer effect: raw signal in, evidence row and outbox event out.

`Normalizer` runs inside `TransactionalProcessor`'s transaction, after the
consumer receipt. It validates the record with its source kind's adapter,
resolves service identity against the catalog, stores the signal (and, for a
resolved deployment, its deployment row), and stages `signal.normalized` in
the outbox, all in that one transaction.

Identity is resolved, never guessed:

- no service or environment claim: `missing_service`, degraded, no service;
- a claim with no catalog entry: `unknown_service`, degraded, no service;
- claims that disagree (`ReportedIdentity.conflicts`): `contradictory_identity`,
  quarantined, no service.

A deployment whose service is unresolved is stored as a signal only.

Records that fail their adapter contract raise `ContractError` and go to
processing.failed.v1; P04-T04 turns them into quarantined evidence instead.
"""

import uuid
from typing import Final

from pydantic import JsonValue, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.adapters.sources import ADAPTER_VERSION, AdaptedRecord, ReportedIdentity, adapt
from signalops.contracts.common import (
    ContractError,
    ContractErrorCategory,
    describe_validation_error,
)
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    NormalizedSignalPayload,
    RawSignalPayload,
)
from signalops.contracts.evidence import NormalizedSignal, QualityFlag, QualityStatus, SourceKind
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.reliability import OutboxRepository

# Fixed namespaces: derived IDs stay the same when a record is processed again,
# for example by a second consumer group or after a redrive.
SIGNAL_NAMESPACE: Final = uuid.UUID("6f1d2c8a-4b3e-4f59-a7d0-8e2b9c4f1a63")
DEPLOYMENT_NAMESPACE: Final = uuid.UUID("c2e8a4f6-1d7b-4c35-9e0a-3b5f7d9c1e28")
NORMALIZED_EVENT_NAMESPACE: Final = uuid.UUID("9a4c6e1f-8b2d-4a7e-b3c5-d1f0e2a4b6c8")

NORMALIZER_CONSUMER_GROUP: Final = "normalizer-v1"


def signal_id_for(source_id: uuid.UUID, replay_scope: str, source_event_id: str) -> uuid.UUID:
    return uuid.uuid5(SIGNAL_NAMESPACE, f"{source_id}:{replay_scope}:{source_event_id}")


def classify_identity(
    identity: ReportedIdentity, service_id: uuid.UUID | None
) -> tuple[QualityStatus, tuple[QualityFlag, ...]]:
    if identity.contradictory:
        return QualityStatus.QUARANTINED, (QualityFlag.CONTRADICTORY_IDENTITY,)
    if identity.service is None or identity.environment is None:
        return QualityStatus.DEGRADED, (QualityFlag.MISSING_SERVICE,)
    if service_id is None:
        return QualityStatus.DEGRADED, (QualityFlag.UNKNOWN_SERVICE,)
    return QualityStatus.ACCEPTED, ()


class Normalizer:
    """`EventHandler` for signals.raw.v1."""

    async def __call__(self, session: AsyncSession, envelope: EventEnvelope[EventPayload]) -> None:
        payload = envelope.payload
        if envelope.event_type is not EventType.SIGNAL_RAW or not isinstance(
            payload, RawSignalPayload
        ):
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                f"normalizer accepts {EventType.SIGNAL_RAW} only, not {envelope.event_type}",
                event_id=envelope.event_id,
            )
        # EventHeader guarantees source_id for source events.
        assert envelope.source_id is not None  # noqa: S101
        catalog = CatalogRepository(session)
        await self._check_source(catalog, envelope, payload)
        try:
            adapted = adapt(payload.source_kind, payload.record)
        except ContractError as error:
            raise ContractError(error.category, error.detail, event_id=envelope.event_id) from None

        service_id = await self._resolve(catalog, envelope, adapted.identity)
        status, flags = classify_identity(adapted.identity, service_id)
        signal = self._signal(envelope, payload, adapted, service_id, status, flags)

        evidence = EvidenceRepository(session)
        if not await evidence.insert_signal(signal):
            # Stored by an earlier delivery; its deployment and event were staged then.
            return
        if adapted.deployment is not None and signal.service_id is not None:
            await evidence.insert_deployment(
                deployment_id=uuid.uuid5(DEPLOYMENT_NAMESPACE, str(signal.id)),
                signal=signal,
                revision=adapted.deployment.revision,
                deployed_at=signal.occurred_at,
                attributes=_present(
                    {
                        "previous_revision": adapted.deployment.previous_revision,
                        "change_summary": adapted.deployment.change_summary,
                    }
                ),
            )
        await OutboxRepository(session).add(
            self._normalized_event(envelope, signal), aggregate_id=signal.id, aggregate_version=1
        )

    async def _check_source(
        self,
        catalog: CatalogRepository,
        envelope: EventEnvelope[EventPayload],
        payload: RawSignalPayload,
    ) -> None:
        assert envelope.source_id is not None  # noqa: S101
        source = await catalog.get_source(envelope.source_id)
        if source is None or source.workspace_id != envelope.workspace_id:
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                "source_id does not name a source in this workspace",
                event_id=envelope.event_id,
            )
        if SourceKind(source.kind) is not payload.source_kind:
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                f"source_kind {payload.source_kind} does not match the source's kind",
                event_id=envelope.event_id,
            )

    async def _resolve(
        self,
        catalog: CatalogRepository,
        envelope: EventEnvelope[EventPayload],
        identity: ReportedIdentity,
    ) -> uuid.UUID | None:
        if identity.contradictory or identity.service is None or identity.environment is None:
            return None
        return await catalog.find_service_id(
            workspace_id=envelope.workspace_id,
            name=identity.service,
            environment=identity.environment,
        )

    def _signal(
        self,
        envelope: EventEnvelope[EventPayload],
        payload: RawSignalPayload,
        adapted: AdaptedRecord,
        service_id: uuid.UUID | None,
        status: QualityStatus,
        flags: tuple[QualityFlag, ...],
    ) -> NormalizedSignal:
        assert envelope.source_id is not None  # noqa: S101
        identity = adapted.identity
        provenance: dict[str, JsonValue] = {
            "raw_event_id": str(envelope.event_id),
            "source_kind": payload.source_kind.value,
            "adapter_version": ADAPTER_VERSION,
            "reported_service": identity.service,
            "reported_environment": identity.environment,
        }
        if identity.conflicts:
            provenance["identity_conflicts"] = list(identity.conflicts)
        if envelope.correlation_id is not None:
            provenance["correlation_id"] = envelope.correlation_id
        try:
            return NormalizedSignal(
                id=signal_id_for(
                    envelope.source_id, envelope.replay_scope, payload.source_event_id
                ),
                workspace_id=envelope.workspace_id,
                source_id=envelope.source_id,
                service_id=service_id,
                source_event_id=payload.source_event_id,
                replay_scope=envelope.replay_scope,
                kind=adapted.kind,
                occurred_at=envelope.occurred_at,
                ingested_at=envelope.ingested_at,
                severity=adapted.severity,
                message=adapted.message,
                trace_id=adapted.trace_id,
                attributes={
                    "source": adapted.source_attributes,
                    "record": adapted.record_fields,
                    "provenance": provenance,
                },
                quality_status=status,
                quality_flags=flags,
            )
        except ValidationError as error:
            # For example, source attributes plus provenance over the size limit.
            raise ContractError(
                ContractErrorCategory.INVALID_PAYLOAD,
                f"normalized signal: {describe_validation_error(error)}",
                event_id=envelope.event_id,
            ) from None

    def _normalized_event(
        self, raw: EventEnvelope[EventPayload], signal: NormalizedSignal
    ) -> EventEnvelope[EventPayload]:
        return EventEnvelope[EventPayload](
            event_id=uuid.uuid5(NORMALIZED_EVENT_NAMESPACE, str(signal.id)),
            event_type=EventType.SIGNAL_NORMALIZED,
            schema_version=1,
            workspace_id=signal.workspace_id,
            source_id=signal.source_id,
            occurred_at=signal.occurred_at,
            ingested_at=signal.ingested_at,
            correlation_id=raw.correlation_id,
            causation_id=raw.event_id,
            trace_id=signal.trace_id,
            replay_run_id=raw.replay_run_id,
            payload=NormalizedSignalPayload(
                signal_id=signal.id,
                service_id=signal.service_id,
                kind=signal.kind,
                quality_status=signal.quality_status,
            ),
        )


def _present(values: dict[str, str | None]) -> dict[str, JsonValue]:
    return {key: value for key, value in values.items() if value is not None}
