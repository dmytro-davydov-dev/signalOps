"""The graph projector's consumer effect: normalized signal in, graph nodes and edges out.

`GraphProjector` runs inside `TransactionalProcessor`'s transaction under its
own consumer group, so it replays the topic independently of the detector
(ADR-018). Signals project in event time, so replay agrees with live.

Projected signals: `accepted`, with a resolved service. A degraded signal has
a skewed clock, and an edge's validity would inherit it.

A trace gives `DEPENDS_ON` from its service to each of `downstream_services`,
named in the caller's environment. A name the catalog does not know gets no
edge: a node needs a service row, and the normalizer owns that identity. Each
observation holds an edge valid for `dependency_ttl_seconds`, and overlapping
observations merge. A dependency nothing confirms expires on its own, and one
that returns after that is a new edge, with no background job involved.

A deployment gives `DEPLOYED_TO` from its node to the service, valid until the
service's next deployment.

An observed dependency is a call seen in a trace, not a claim about cause.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.config import Settings
from signalops.contracts.common import ContractError, ContractErrorCategory
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    NormalizedSignalPayload,
)
from signalops.contracts.evidence import QualityStatus, SignalKind
from signalops.contracts.graph import GraphRelation
from signalops.persistence.models import Deployment, Service, Signal
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.graph import EdgeWrite, GraphRepository
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

GRAPH_CONSUMER_GROUP: Final = "graph-projector-v1"

# A span in a trace and a deployment record are direct observations; the column
# leaves room for inferred relationships.
OBSERVED_CONFIDENCE: Final = 1.0


@dataclass(frozen=True)
class GraphPolicy:
    dependency_ttl_seconds: int = 7 * 24 * 60 * 60

    @classmethod
    def from_settings(cls, settings: Settings) -> "GraphPolicy":
        return cls(settings.graph_dependency_ttl_seconds)

    @property
    def dependency_ttl(self) -> timedelta:
        return timedelta(seconds=self.dependency_ttl_seconds)


def downstream_names(signal: Signal) -> frozenset[str]:
    """The distinct services a stored trace called; the normalizer wrote this shape."""
    record = signal.attributes.get("record")
    names = record.get("downstream_services") if isinstance(record, dict) else None
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise ContractError(
            ContractErrorCategory.INVALID_PAYLOAD,
            "trace signal lacks record.downstream_services",
        )
    return frozenset(names)


class GraphProjector:
    """`EventHandler` for signals.normalized.v1."""

    def __init__(self, policy: GraphPolicy) -> None:
        self._policy = policy

    async def __call__(self, session: AsyncSession, envelope: EventEnvelope[EventPayload]) -> None:
        payload = envelope.payload
        if envelope.event_type is not EventType.SIGNAL_NORMALIZED or not isinstance(
            payload, NormalizedSignalPayload
        ):
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                f"graph projector accepts {EventType.SIGNAL_NORMALIZED} only, "
                f"not {envelope.event_type}",
                event_id=envelope.event_id,
            )
        if (
            payload.kind not in (SignalKind.TRACE, SignalKind.DEPLOYMENT)
            or payload.service_id is None
            or payload.quality_status is not QualityStatus.ACCEPTED
        ):
            return

        scope = EvidenceScope(envelope.workspace_id, envelope.replay_scope)
        signal = await EvidenceRepository(session).get_signal(scope, payload.signal_id)
        if signal is None:
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                "signal_id does not name a stored signal in this workspace and replay scope",
                event_id=envelope.event_id,
            )
        if signal.service_id is None or signal.quality_status != QualityStatus.ACCEPTED:
            # The stored row is authoritative over the event's summary of it.
            return
        if payload.kind is SignalKind.DEPLOYMENT:
            await self._project_deployment(session, scope, signal, signal.service_id, envelope)
            return
        try:
            names = downstream_names(signal)
        except ContractError as error:
            raise ContractError(error.category, error.detail, event_id=envelope.event_id) from None
        await self._project_trace(session, scope, signal, signal.service_id, names)

    async def _project_trace(
        self,
        session: AsyncSession,
        scope: EvidenceScope,
        signal: Signal,
        service_id: uuid.UUID,
        names: frozenset[str],
    ) -> None:
        callees = await _resolve(session, scope, service_id, names)
        callees.discard(service_id)
        graph = GraphRepository(session)
        nodes = await graph.ensure_service_nodes(scope, {service_id, *callees})
        for callee in sorted(callees):
            await graph.observe(
                _edge(
                    scope,
                    signal,
                    nodes[service_id],
                    nodes[callee],
                    GraphRelation.DEPENDS_ON,
                    signal.occurred_at,
                ),
                signal.occurred_at + self._policy.dependency_ttl,
            )

    async def _project_deployment(
        self,
        session: AsyncSession,
        scope: EvidenceScope,
        signal: Signal,
        service_id: uuid.UUID,
        envelope: EventEnvelope[EventPayload],
    ) -> None:
        deployment = await session.scalar(
            select(Deployment).where(scope.clause(Deployment), Deployment.signal_id == signal.id)
        )
        if deployment is None:
            raise ContractError(
                ContractErrorCategory.INVALID_PAYLOAD,
                "accepted deployment signal has no deployment row",
                event_id=envelope.event_id,
            )
        graph = GraphRepository(session)
        nodes = await graph.ensure_service_nodes(scope, {service_id})
        await graph.record_deployment(
            _edge(
                scope,
                signal,
                await graph.ensure_deployment_node(scope, deployment.id),
                nodes[service_id],
                GraphRelation.DEPLOYED_TO,
                deployment.deployed_at,
            )
        )


async def _resolve(
    session: AsyncSession, scope: EvidenceScope, service_id: uuid.UUID, names: frozenset[str]
) -> set[uuid.UUID]:
    if not names:
        return set()
    caller = await session.execute(select(Service.environment).where(Service.id == service_id))
    rows = await session.execute(
        select(Service.name, Service.id).where(
            Service.workspace_id == scope.workspace_id,
            Service.environment == caller.scalar_one(),
            Service.name.in_(names),
        )
    )
    found = {name: found_id for name, found_id in rows}
    if unknown := sorted(names - found.keys()):
        logger.info(
            "downstream services not in the catalog",
            extra={"service_id": str(service_id), "unknown": unknown},
        )
    return set(found.values())


def _edge(
    scope: EvidenceScope,
    signal: Signal,
    from_node_id: uuid.UUID,
    to_node_id: uuid.UUID,
    relation: GraphRelation,
    start: datetime,
) -> EdgeWrite:
    return EdgeWrite(
        scope=scope,
        from_node_id=from_node_id,
        to_node_id=to_node_id,
        relation=relation,
        start=start,
        confidence=OBSERVED_CONFIDENCE,
        signal_id=signal.id,
    )
