"""Link triggers to incidents and track service recovery.

`detection_triggers.incident_id` records which incident a trigger became part
of. Triggers from before this revision stay null: no incident was created for
them. `incident_services.healthy_through` holds the end of the latest window
that showed the service healthy. An index on the fingerprint's most recent
incidents serves the detector's correlation lookup.

Revision ID: 0011_incident_correlation
Revises: 0010_window_closure
Create Date: 2026-11-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011_incident_correlation"
down_revision: str | None = "0010_window_closure"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "incident_services",
        sa.Column("healthy_through", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column("detection_triggers", sa.Column("incident_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        op.f("fk_detection_triggers_workspace_id_incidents"),
        "detection_triggers",
        "incidents",
        ["workspace_id", "replay_scope", "incident_id"],
        ["workspace_id", "replay_scope", "id"],
    )
    op.create_index(
        "ix_detection_triggers_incident_id",
        "detection_triggers",
        ["incident_id"],
        postgresql_where=sa.text("incident_id IS NOT NULL"),
    )
    op.create_index(
        "ix_incidents_fingerprint_last_seen_at",
        "incidents",
        ["workspace_id", "replay_scope", "fingerprint", sa.text("last_seen_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_incidents_fingerprint_last_seen_at", table_name="incidents")
    op.drop_index(
        "ix_detection_triggers_incident_id",
        table_name="detection_triggers",
        postgresql_where=sa.text("incident_id IS NOT NULL"),
    )
    op.drop_constraint(
        op.f("fk_detection_triggers_workspace_id_incidents"),
        "detection_triggers",
        type_="foreignkey",
    )
    op.drop_column("detection_triggers", "incident_id")
    op.drop_column("incident_services", "healthy_through")
