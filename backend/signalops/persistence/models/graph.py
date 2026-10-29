"""Temporal knowledge graph: typed nodes and time-bounded edges with provenance.

Nodes and edges carry `workspace_id` and `replay_scope`, and edges reference both
endpoints through `(workspace_id, replay_scope, id)`, so a traversal never leaves
its workspace or replay run. Entity nodes reference their service, deployment, or
incident row through a typed foreign key, and their `external_key` is that row's
ID, so each entity has at most one node per replay scope.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Double,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from signalops.contracts.graph import GraphNodeType, GraphRelation
from signalops.persistence.base import Base
from signalops.persistence.models.common import (
    MAX_REPLAY_SCOPE_LENGTH,
    created_at,
    json_attributes,
    json_object,
    length_between,
    one_of,
    replay_scope,
    uuid_pk,
)

MAX_EXTERNAL_KEY_LENGTH = 512

# Exactly the reference column matching node_type is set, and it is the external key.
ENTITY_REFERENCE = """CASE node_type
    WHEN 'service' THEN num_nonnulls(deployment_id, incident_id) = 0
        AND external_key IS NOT DISTINCT FROM service_id::text
    WHEN 'deployment' THEN num_nonnulls(service_id, incident_id) = 0
        AND external_key IS NOT DISTINCT FROM deployment_id::text
    WHEN 'incident' THEN num_nonnulls(service_id, deployment_id) = 0
        AND external_key IS NOT DISTINCT FROM incident_id::text
    ELSE num_nonnulls(service_id, deployment_id, incident_id) = 0
END"""


class GraphNode(Base):
    __tablename__ = "graph_nodes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "deployment_id"],
            ["deployments.workspace_id", "deployments.replay_scope", "deployments.id"],
        ),
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "incident_id"],
            ["incidents.workspace_id", "incidents.replay_scope", "incidents.id"],
        ),
        # Explicit name: the conventional one reaches PostgreSQL's 63-character limit.
        UniqueConstraint(
            "workspace_id",
            "replay_scope",
            "node_type",
            "external_key",
            name="uq_graph_nodes_identity",
        ),
        # Target for edges, whose endpoints must share the edge's workspace and replay scope.
        UniqueConstraint("workspace_id", "replay_scope", "id"),
        CheckConstraint(
            length_between("replay_scope", 1, MAX_REPLAY_SCOPE_LENGTH),
            name="replay_scope_length",
        ),
        CheckConstraint(one_of("node_type", GraphNodeType), name="node_type"),
        CheckConstraint(
            length_between("external_key", 1, MAX_EXTERNAL_KEY_LENGTH),
            name="external_key_length",
        ),
        CheckConstraint(ENTITY_REFERENCE, name="entity_reference"),
        CheckConstraint(json_object("properties"), name="properties_object"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"))
    replay_scope: Mapped[str] = replay_scope()
    node_type: Mapped[str] = mapped_column(Text)
    external_key: Mapped[str] = mapped_column(Text)
    # MATCH SIMPLE: the NULL references of other node types skip their checks.
    service_id: Mapped[uuid.UUID | None]
    deployment_id: Mapped[uuid.UUID | None]
    incident_id: Mapped[uuid.UUID | None]
    properties: Mapped[dict[str, Any]] = json_attributes()
    created_at: Mapped[datetime] = created_at()


class GraphEdge(Base):
    __tablename__ = "graph_edges"
    __table_args__ = (
        # Explicit names: the conventional ones would be identical for both endpoints.
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "from_node_id"],
            ["graph_nodes.workspace_id", "graph_nodes.replay_scope", "graph_nodes.id"],
            name="fk_graph_edges_from_node",
        ),
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "to_node_id"],
            ["graph_nodes.workspace_id", "graph_nodes.replay_scope", "graph_nodes.id"],
            name="fk_graph_edges_to_node",
        ),
        # MATCH SIMPLE: a NULL provenance_signal_id skips this check.
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "provenance_signal_id"],
            ["signals.workspace_id", "signals.replay_scope", "signals.id"],
        ),
        CheckConstraint(one_of("relation", GraphRelation), name="relation"),
        # Longer cycles are prevented by the bounded traversal, not the schema.
        CheckConstraint("from_node_id <> to_node_id", name="no_self_loop"),
        CheckConstraint("valid_to IS NULL OR valid_to > valid_from", name="validity_interval"),
        CheckConstraint("confidence BETWEEN 0 AND 1", name="confidence_range"),
        CheckConstraint(json_object("properties"), name="properties_object"),
        Index("ix_graph_edges_from_node_id_relation", "from_node_id", "relation"),
        Index("ix_graph_edges_to_node_id_relation", "to_node_id", "relation"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID]
    replay_scope: Mapped[str] = mapped_column(Text)
    from_node_id: Mapped[uuid.UUID]
    to_node_id: Mapped[uuid.UUID]
    relation: Mapped[str] = mapped_column(Text)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    confidence: Mapped[float] = mapped_column(Double)
    provenance_signal_id: Mapped[uuid.UUID | None]
    properties: Mapped[dict[str, Any]] = json_attributes()
    created_at: Mapped[datetime] = created_at()
