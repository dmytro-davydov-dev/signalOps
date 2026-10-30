"""Baseline indexes for foreign keys and workspace-scoped incident listing.

PostgreSQL does not index the referencing side of a foreign key. These cover the
references no existing index leads with, so reverse lookups and parent-side key
checks avoid sequential scans, plus incident listing within a workspace and
replay scope and investigation lookup by incident.

Revision ID: 0006_baseline_indexes
Revises: 0005_diagnosis_status_mapping
Create Date: 2026-10-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_baseline_indexes"
down_revision: str | None = "0005_diagnosis_status_mapping"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

GRAPH_NODE_REFERENCES = ("service_id", "deployment_id", "incident_id")


def upgrade() -> None:
    op.create_index(
        "ix_incidents_workspace_id_replay_scope_detected_at",
        "incidents",
        ["workspace_id", "replay_scope", sa.text("detected_at DESC")],
    )
    op.create_index("ix_incident_services_service_id", "incident_services", ["service_id"])
    op.create_index("ix_incident_signals_signal_id", "incident_signals", ["signal_id"])
    op.create_index(
        "ix_investigations_incident_id_created_at",
        "investigations",
        ["incident_id", sa.text("created_at DESC")],
    )
    for column in GRAPH_NODE_REFERENCES:
        op.create_index(
            f"ix_graph_nodes_{column}",
            "graph_nodes",
            [column],
            postgresql_where=sa.text(f"{column} IS NOT NULL"),
        )
    op.create_index(
        "ix_graph_edges_provenance_signal_id",
        "graph_edges",
        ["provenance_signal_id"],
        postgresql_where=sa.text("provenance_signal_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_graph_edges_provenance_signal_id", table_name="graph_edges")
    for column in reversed(GRAPH_NODE_REFERENCES):
        op.drop_index(f"ix_graph_nodes_{column}", table_name="graph_nodes")
    op.drop_index("ix_investigations_incident_id_created_at", table_name="investigations")
    op.drop_index("ix_incident_signals_signal_id", table_name="incident_signals")
    op.drop_index("ix_incident_services_service_id", table_name="incident_services")
    op.drop_index("ix_incidents_workspace_id_replay_scope_detected_at", table_name="incidents")
