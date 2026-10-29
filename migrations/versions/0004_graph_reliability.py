"""Graph and reliability tables: graph_nodes, graph_edges, consumer_receipts, outbox_events.

Graph nodes and edges carry workspace_id and replay_scope; edges reference both endpoints
through (workspace_id, replay_scope, id), so dangling or cross-scope edges are rejected.
Entity nodes reference their service, deployment, or incident through a typed foreign key.
Pending outbox rows are found through a partial created_at index WHERE published_at IS NULL.

Revision ID: 0004_graph_reliability
Revises: 0003_incident_workflow
Create Date: 2026-10-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_graph_reliability"
down_revision: str | None = "0003_incident_workflow"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

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


def upgrade() -> None:
    # Target for graph nodes, which must share the deployment's replay scope.
    op.create_unique_constraint(
        op.f("uq_deployments_workspace_id_replay_scope_id"),
        "deployments",
        ["workspace_id", "replay_scope", "id"],
    )
    op.create_table(
        "consumer_receipts",
        sa.Column("consumer_group", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(consumer_group) BETWEEN 1 AND 256",
            name=op.f("ck_consumer_receipts_consumer_group_length"),
        ),
        sa.PrimaryKeyConstraint("consumer_group", "event_id", name=op.f("pk_consumer_receipts")),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_version", sa.BigInteger(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("message_key", sa.Text(), nullable=False),
        sa.Column("envelope", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.CheckConstraint(
            "jsonb_typeof(envelope) = 'object'", name=op.f("ck_outbox_events_envelope_object")
        ),
        sa.CheckConstraint(
            "aggregate_version >= 1", name=op.f("ck_outbox_events_aggregate_version_positive")
        ),
        sa.CheckConstraint("attempts >= 0", name=op.f("ck_outbox_events_attempts_nonnegative")),
        sa.CheckConstraint(
            "char_length(message_key) > 0", name=op.f("ck_outbox_events_message_key_nonempty")
        ),
        sa.CheckConstraint(
            "char_length(topic) BETWEEN 1 AND 256", name=op.f("ck_outbox_events_topic_length")
        ),
        sa.CheckConstraint(
            "published_at IS NULL OR published_at >= created_at",
            name=op.f("ck_outbox_events_published_after_created"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_events")),
        sa.UniqueConstraint(
            "aggregate_id",
            "aggregate_version",
            "topic",
            name=op.f("uq_outbox_events_aggregate_id_aggregate_version_topic"),
        ),
    )
    op.create_index(
        "ix_outbox_events_created_at",
        "outbox_events",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "graph_nodes",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), server_default="live", nullable=False),
        sa.Column("node_type", sa.Text(), nullable=False),
        sa.Column("external_key", sa.Text(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=True),
        sa.Column("deployment_id", sa.Uuid(), nullable=True),
        sa.Column("incident_id", sa.Uuid(), nullable=True),
        sa.Column(
            "properties",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            ENTITY_REFERENCE,
            name=op.f("ck_graph_nodes_entity_reference"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(properties) = 'object'", name=op.f("ck_graph_nodes_properties_object")
        ),
        sa.CheckConstraint(
            "node_type IN ('service', 'deployment', 'incident', 'runbook')",
            name=op.f("ck_graph_nodes_node_type"),
        ),
        sa.CheckConstraint(
            "char_length(external_key) BETWEEN 1 AND 512",
            name=op.f("ck_graph_nodes_external_key_length"),
        ),
        sa.CheckConstraint(
            "char_length(replay_scope) BETWEEN 1 AND 128",
            name=op.f("ck_graph_nodes_replay_scope_length"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "deployment_id"],
            ["deployments.workspace_id", "deployments.replay_scope", "deployments.id"],
            name=op.f("fk_graph_nodes_workspace_id_deployments"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "incident_id"],
            ["incidents.workspace_id", "incidents.replay_scope", "incidents.id"],
            name=op.f("fk_graph_nodes_workspace_id_incidents"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_graph_nodes_workspace_id_services"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspaces.id"], name=op.f("fk_graph_nodes_workspace_id_workspaces")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_graph_nodes")),
        sa.UniqueConstraint(
            "workspace_id",
            "replay_scope",
            "id",
            name=op.f("uq_graph_nodes_workspace_id_replay_scope_id"),
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "replay_scope",
            "node_type",
            "external_key",
            name="uq_graph_nodes_identity",
        ),
    )
    op.create_table(
        "graph_edges",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), nullable=False),
        sa.Column("from_node_id", sa.Uuid(), nullable=False),
        sa.Column("to_node_id", sa.Uuid(), nullable=False),
        sa.Column("relation", sa.Text(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("confidence", sa.Double(), nullable=False),
        sa.Column("provenance_signal_id", sa.Uuid(), nullable=True),
        sa.Column(
            "properties",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(properties) = 'object'", name=op.f("ck_graph_edges_properties_object")
        ),
        sa.CheckConstraint(
            "relation IN ('DEPENDS_ON', 'DEPLOYED_TO', 'AFFECTS', 'DOCUMENTED_BY')",
            name=op.f("ck_graph_edges_relation"),
        ),
        sa.CheckConstraint(
            "confidence BETWEEN 0 AND 1", name=op.f("ck_graph_edges_confidence_range")
        ),
        sa.CheckConstraint("from_node_id <> to_node_id", name=op.f("ck_graph_edges_no_self_loop")),
        sa.CheckConstraint(
            "valid_to IS NULL OR valid_to > valid_from",
            name=op.f("ck_graph_edges_validity_interval"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "from_node_id"],
            ["graph_nodes.workspace_id", "graph_nodes.replay_scope", "graph_nodes.id"],
            name="fk_graph_edges_from_node",
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "provenance_signal_id"],
            ["signals.workspace_id", "signals.replay_scope", "signals.id"],
            name=op.f("fk_graph_edges_workspace_id_signals"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "to_node_id"],
            ["graph_nodes.workspace_id", "graph_nodes.replay_scope", "graph_nodes.id"],
            name="fk_graph_edges_to_node",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_graph_edges")),
    )
    op.create_index(
        "ix_graph_edges_from_node_id_relation",
        "graph_edges",
        ["from_node_id", "relation"],
        unique=False,
    )
    op.create_index(
        "ix_graph_edges_to_node_id_relation",
        "graph_edges",
        ["to_node_id", "relation"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_graph_edges_to_node_id_relation", table_name="graph_edges")
    op.drop_index("ix_graph_edges_from_node_id_relation", table_name="graph_edges")
    op.drop_table("graph_edges")
    op.drop_table("graph_nodes")
    op.drop_index(
        "ix_outbox_events_created_at",
        table_name="outbox_events",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox_events")
    op.drop_table("consumer_receipts")
    op.drop_constraint(
        op.f("uq_deployments_workspace_id_replay_scope_id"), "deployments", type_="unique"
    )
