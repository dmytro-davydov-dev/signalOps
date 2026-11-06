"""Temporal knowledge graph: node upserts, edge intervals, and bounded as-of reads.

Writes run in the projector's transaction. Edge changes for one relationship
take `_lock` first; edges cannot overlap, so the lock turns a concurrent
projection into a merge instead of a constraint failure.

Reads take the evidence cutoff as `as_of`, in event time, and answer as of that
instant. An edge's `valid_to` is never returned: a row valid at `as_of` ends
after it, so its end would disclose a later deployment or observation.
"""

import uuid
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Final

from sqlalchemy import ColumnElement, and_, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import array, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import Uuid

from signalops.contracts.graph import GraphNodeType, GraphRelation
from signalops.contracts.tools import (
    MAX_TOOL_ROWS,
    MAX_TRAVERSAL_HOPS,
    DependencyEntry,
    DeploymentEntry,
)
from signalops.persistence.models import Deployment, GraphEdge, GraphNode
from signalops.persistence.repositories.scope import EvidenceScope

# Paths a traversal may walk: the bound on its work, which `limit` on the result
# is not, since paths grow with fan-out per hop.
MAX_WALK_PATHS: Final = 1000


class GraphDirection(StrEnum):
    DOWNSTREAM = "downstream"
    UPSTREAM = "upstream"


@dataclass(frozen=True)
class DependencyWalk:
    items: tuple[DependencyEntry, ...]
    truncated: bool


@dataclass(frozen=True)
class EdgeWrite:
    """An edge's identity and the signal that supports it from `start`."""

    scope: EvidenceScope
    from_node_id: uuid.UUID
    to_node_id: uuid.UUID
    relation: GraphRelation
    start: datetime
    confidence: float
    signal_id: uuid.UUID

    def identity(self) -> ColumnElement[bool]:
        return and_(
            self.scope.clause(GraphEdge),
            GraphEdge.from_node_id == self.from_node_id,
            GraphEdge.to_node_id == self.to_node_id,
            GraphEdge.relation == self.relation,
        )

    def row(self, end: datetime | None, properties: dict[str, Any]) -> GraphEdge:
        return GraphEdge(
            workspace_id=self.scope.workspace_id,
            replay_scope=self.scope.replay_scope,
            from_node_id=self.from_node_id,
            to_node_id=self.to_node_id,
            relation=self.relation,
            valid_from=self.start,
            valid_to=end,
            confidence=self.confidence,
            provenance_signal_id=self.signal_id,
            properties=properties,
        )


@dataclass(frozen=True)
class Observed:
    """The evidence for one observed edge; `merge` is commutative and associative."""

    start: datetime
    end: datetime
    first_signal: uuid.UUID
    count: int
    last_at: datetime
    last_signal: uuid.UUID

    @classmethod
    def of_edge(cls, edge: GraphEdge) -> "Observed":
        if edge.valid_to is None or edge.provenance_signal_id is None:
            raise ValueError(f"observed edge {edge.id} has no end or no provenance")
        return cls(
            start=edge.valid_from,
            end=edge.valid_to,
            first_signal=edge.provenance_signal_id,
            count=edge.properties["observations"],
            last_at=datetime.fromisoformat(edge.properties["last_observed_at"]),
            last_signal=uuid.UUID(edge.properties["last_signal_id"]),
        )

    def merge(self, other: "Observed") -> "Observed":
        start, first_signal = min(
            (self.start, self.first_signal), (other.start, other.first_signal)
        )
        last_at, last_signal = max(
            (self.last_at, self.last_signal), (other.last_at, other.last_signal)
        )
        return Observed(
            start=start,
            end=max(self.end, other.end),
            first_signal=first_signal,
            count=self.count + other.count,
            last_at=last_at,
            last_signal=last_signal,
        )

    def properties(self) -> dict[str, Any]:
        return {
            "observations": self.count,
            "last_observed_at": self.last_at.isoformat(),
            "last_signal_id": str(self.last_signal),
        }


def _valid_at(as_of: datetime) -> ColumnElement[bool]:
    return and_(
        GraphEdge.valid_from <= as_of,
        or_(GraphEdge.valid_to.is_(None), GraphEdge.valid_to > as_of),
    )


class GraphRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def ensure_service_nodes(
        self, scope: EvidenceScope, service_ids: Collection[uuid.UUID]
    ) -> dict[uuid.UUID, uuid.UUID]:
        """Node IDs by service, created if absent; sorted, so writers cannot deadlock."""
        await self._session.execute(
            insert(GraphNode)
            .values(
                [
                    {
                        "workspace_id": scope.workspace_id,
                        "replay_scope": scope.replay_scope,
                        "node_type": GraphNodeType.SERVICE,
                        "external_key": str(service_id),
                        "service_id": service_id,
                    }
                    for service_id in sorted(service_ids)
                ]
            )
            .on_conflict_do_nothing(constraint="uq_graph_nodes_identity")
        )
        rows = await self._session.execute(
            select(GraphNode.service_id, GraphNode.id).where(
                scope.clause(GraphNode), GraphNode.service_id.in_(service_ids)
            )
        )
        return {service_id: node_id for service_id, node_id in rows if service_id}

    async def ensure_deployment_node(
        self, scope: EvidenceScope, deployment_id: uuid.UUID
    ) -> uuid.UUID:
        await self._session.execute(
            insert(GraphNode)
            .values(
                workspace_id=scope.workspace_id,
                replay_scope=scope.replay_scope,
                node_type=GraphNodeType.DEPLOYMENT,
                external_key=str(deployment_id),
                deployment_id=deployment_id,
            )
            .on_conflict_do_nothing(constraint="uq_graph_nodes_identity")
        )
        rows = await self._session.execute(
            select(GraphNode.id).where(
                scope.clause(GraphNode), GraphNode.deployment_id == deployment_id
            )
        )
        return rows.scalar_one()

    async def _lock(self, scope: EvidenceScope, key: str) -> None:
        name = f"graph:{scope.workspace_id}:{scope.replay_scope}:{key}"
        await self._session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(name, 0)))
        )

    async def observe(self, write: EdgeWrite, end: datetime) -> None:
        """Add an observation valid until `end`, merging every edge it touches into one.

        The result is the union of the intervals, so it does not depend on the
        order observations arrive in. The provenance column stays the earliest
        signal, which a read as of any instant in the edge may cite.
        """
        await self._lock(write.scope, f"{write.relation}:{write.from_node_id}:{write.to_node_id}")
        touching = list(
            await self._session.scalars(
                select(GraphEdge)
                .where(
                    write.identity(),
                    GraphEdge.valid_from <= end,
                    GraphEdge.valid_to >= write.start,
                )
                .order_by(GraphEdge.valid_from, GraphEdge.id)
                .execution_options(populate_existing=True)
            )
        )
        observed = Observed(
            start=write.start,
            end=end,
            first_signal=write.signal_id,
            count=1,
            last_at=write.start,
            last_signal=write.signal_id,
        )
        for edge in touching:
            observed = observed.merge(Observed.of_edge(edge))
        if not touching:
            self._session.add(write.row(end, observed.properties()))
            await self._session.flush()
            return

        keeper, *absorbed = touching
        # Flushed first: the unit of work deletes last, and the keeper would overlap them.
        for edge in absorbed:
            await self._session.delete(edge)
        await self._session.flush()
        keeper.valid_from = observed.start
        keeper.valid_to = observed.end
        keeper.provenance_signal_id = observed.first_signal
        keeper.properties = observed.properties()
        await self._session.flush()

    async def record_deployment(self, write: EdgeWrite) -> None:
        """Add a deployment edge, valid until the service's next deployment.

        An earlier edge still open at this time now ends here, and this one ends
        at the next deployment already known, so the intervals agree whatever
        order deployments arrive in. Deployments at one instant share the slot.
        """
        await self._lock(write.scope, f"{write.relation}:{write.to_node_id}")
        existing = await self._session.scalar(select(GraphEdge.id).where(write.identity()))
        if existing is not None:
            return
        into_service = and_(
            write.scope.clause(GraphEdge),
            GraphEdge.to_node_id == write.to_node_id,
            GraphEdge.relation == write.relation,
        )
        following = await self._session.scalar(
            select(func.min(GraphEdge.valid_from)).where(
                into_service, GraphEdge.valid_from > write.start
            )
        )
        await self._session.execute(
            update(GraphEdge)
            .where(
                into_service,
                GraphEdge.valid_from < write.start,
                or_(GraphEdge.valid_to.is_(None), GraphEdge.valid_to > write.start),
            )
            .values(valid_to=write.start)
        )
        self._session.add(write.row(following, {}))
        await self._session.flush()

    async def dependencies(
        self,
        scope: EvidenceScope,
        service_id: uuid.UUID,
        *,
        as_of: datetime,
        direction: GraphDirection = GraphDirection.DOWNSTREAM,
        max_hops: int = MAX_TRAVERSAL_HOPS,
        limit: int = MAX_TOOL_ROWS,
    ) -> DependencyWalk:
        """`DEPENDS_ON` edges reachable from the service as of a time, nearest first.

        Each path holds its nodes and may not revisit one, so a cycle ends; each
        edge reports its shortest distance. `truncated` means more edges may exist.
        """
        if not 1 <= max_hops <= MAX_TRAVERSAL_HOPS:
            raise ValueError(f"max_hops must be between 1 and {MAX_TRAVERSAL_HOPS}")
        if not 1 <= limit <= MAX_TOOL_ROWS:
            raise ValueError(f"limit must be between 1 and {MAX_TOOL_ROWS}")
        start = await self._session.scalar(
            select(GraphNode.id).where(scope.clause(GraphNode), GraphNode.service_id == service_id)
        )
        if start is None:
            return DependencyWalk((), truncated=False)

        paths = await self._walk(scope, start, as_of, direction, max_hops)
        nearest: dict[uuid.UUID, int] = {}
        for edge_id, hops in paths[:MAX_WALK_PATHS]:
            nearest[edge_id] = min(hops, nearest.get(edge_id, hops))
        ranked = sorted(nearest, key=lambda edge_id: (nearest[edge_id], edge_id))
        return DependencyWalk(
            await self._entries(scope, ranked[:limit], nearest),
            truncated=len(paths) > MAX_WALK_PATHS or len(ranked) > limit,
        )

    async def _walk(
        self,
        scope: EvidenceScope,
        start: uuid.UUID,
        as_of: datetime,
        direction: GraphDirection,
        max_hops: int,
    ) -> list[tuple[uuid.UUID, int]]:
        """(edge, hops) per path from a node, at most one row past `MAX_WALK_PATHS`."""
        downstream = direction is GraphDirection.DOWNSTREAM
        near = GraphEdge.from_node_id if downstream else GraphEdge.to_node_id
        far = GraphEdge.to_node_id if downstream else GraphEdge.from_node_id
        usable = and_(
            scope.clause(GraphEdge),
            GraphEdge.relation == GraphRelation.DEPENDS_ON,
            _valid_at(as_of),
        )
        walk = (
            select(
                GraphEdge.id.label("edge_id"),
                far.label("node_id"),
                literal(1).label("hops"),
                array[Any]([literal(start, Uuid()), far], type_=Uuid()).label("path"),
            )
            .where(usable, near == start)
            .cte("walk", recursive=True)
        )
        walk = walk.union_all(
            select(
                GraphEdge.id,
                far,
                walk.c.hops + 1,
                walk.c.path.op("||")(far),
            )
            .join_from(walk, GraphEdge, near == walk.c.node_id)
            .where(usable, walk.c.hops < max_hops, far != func.all(walk.c.path))
        )
        # No ORDER BY, so PostgreSQL stops the recursion once the limit is read.
        rows = await self._session.execute(
            select(walk.c.edge_id, walk.c.hops).limit(MAX_WALK_PATHS + 1)
        )
        return [(edge_id, hops) for edge_id, hops in rows]

    async def _entries(
        self, scope: EvidenceScope, edge_ids: Sequence[uuid.UUID], hops: dict[uuid.UUID, int]
    ) -> tuple[DependencyEntry, ...]:
        source = GraphNode.__table__.alias("source")
        target = GraphNode.__table__.alias("target")
        rows = await self._session.execute(
            select(GraphEdge, source.c.service_id, target.c.service_id)
            .join(source, source.c.id == GraphEdge.from_node_id)
            .join(target, target.c.id == GraphEdge.to_node_id)
            .where(scope.clause(GraphEdge), GraphEdge.id.in_(edge_ids))
        )
        by_id = {edge.id: (edge, first, second) for edge, first, second in rows}
        entries: list[DependencyEntry] = []
        for edge_id in edge_ids:
            edge, from_service, to_service = by_id[edge_id]
            entries.append(
                DependencyEntry(
                    from_service_id=from_service,
                    to_service_id=to_service,
                    relation=GraphRelation(edge.relation),
                    valid_from=edge.valid_from,
                    confidence=edge.confidence,
                    hops=hops[edge_id],
                    provenance_signal_id=edge.provenance_signal_id,
                )
            )
        return tuple(entries)

    async def deployments_as_of(
        self, scope: EvidenceScope, service_ids: Collection[uuid.UUID], *, as_of: datetime
    ) -> list[DeploymentEntry]:
        """What each service was running at a time, by service and time."""
        rows = await self._session.scalars(
            select(Deployment)
            .join(GraphNode, GraphNode.deployment_id == Deployment.id)
            .join(GraphEdge, GraphEdge.from_node_id == GraphNode.id)
            .where(
                scope.clause(Deployment),
                scope.clause(GraphEdge),
                Deployment.service_id.in_(service_ids),
                GraphEdge.relation == GraphRelation.DEPLOYED_TO,
                _valid_at(as_of),
            )
            .order_by(Deployment.service_id, Deployment.deployed_at, Deployment.id)
        )
        return [
            DeploymentEntry(
                deployment_id=deployment.id,
                signal_id=deployment.signal_id,
                service_id=deployment.service_id,
                revision=deployment.revision,
                previous_revision=deployment.attributes.get("previous_revision"),
                deployed_at=deployment.deployed_at,
            )
            for deployment in rows
        ]
