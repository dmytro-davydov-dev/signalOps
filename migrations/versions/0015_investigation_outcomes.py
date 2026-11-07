"""Allow one ending per investigation and settle investigations left running.

Before this revision a job's success did not end its investigation, so a finished
workflow left it `running`. Those rows get the outcome their `conclude` step
stored, with the `investigation.completed` row they never wrote. New code ends
both in one transaction, so no row can reach this state again.

The downgrade drops only the index: the settled outcomes are correct at any
revision.

Revision ID: 0015_investigation_outcomes
Revises: 0014_investigation_cancellation
Create Date: 2026-11-07
"""

import json
import uuid
from collections.abc import Sequence
from datetime import UTC

import sqlalchemy as sa
from alembic import op

revision: str = "0015_investigation_outcomes"
down_revision: str | None = "0014_investigation_cancellation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Hardcoded like other migration values: these must not move with the code.
EVENT_NAMESPACE = uuid.UUID("c1f0a6d4-8b2e-4f37-9e15-2a7d6b3c9e01")
INVESTIGATION_STATUS_BY_DIAGNOSIS = {
    "diagnosed": "completed",
    "insufficient_evidence": "insufficient_evidence",
}

STRANDED = sa.text(
    """
    SELECT i.id, i.workspace_id, i.incident_id, i.workflow_version,
           step.output, step.completed_at
    FROM investigations AS i
    JOIN jobs AS job ON job.investigation_id = i.id
    JOIN agent_steps AS step
      ON step.investigation_id = i.id
     AND step.step_key = 'conclude'
     AND step.status = 'completed'
    WHERE i.status = 'running'
      AND i.replay_scope = 'live'
      AND job.status = 'succeeded'
    FOR UPDATE OF i
    """
)
SETTLE = sa.text(
    """
    UPDATE investigations
    SET status = :status, diagnosis = CAST(:diagnosis AS jsonb),
        completed_at = :at, version = version + 1
    WHERE id = :id
    RETURNING version
    """
)
ADD_PROGRESS = sa.text(
    """
    INSERT INTO progress_events (investigation_id, event_type, payload)
    VALUES (:id, 'investigation.ended', CAST(:payload AS jsonb))
    """
)
ADD_OUTBOX = sa.text(
    """
    INSERT INTO outbox_events (id, aggregate_id, aggregate_version, topic, message_key, envelope)
    VALUES (:event_id, :id, :version, 'investigations.completed.v1', :key,
            CAST(:envelope AS jsonb))
    """
)


def _settle(connection: sa.Connection, row: sa.RowMapping) -> None:
    diagnosis_status = row["output"]["status"]
    status = INVESTIGATION_STATUS_BY_DIAGNOSIS[diagnosis_status]
    investigation_id = str(row["id"])
    at = row["completed_at"]
    version = connection.execute(
        SETTLE,
        {"id": row["id"], "status": status, "diagnosis": json.dumps(row["output"]), "at": at},
    ).scalar_one()
    connection.execute(
        ADD_PROGRESS,
        {
            "id": row["id"],
            "payload": json.dumps({"status": status, "diagnosis_status": diagnosis_status}),
        },
    )
    event_id = uuid.uuid5(EVENT_NAMESPACE, f"{investigation_id}:completed")
    moment = at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    envelope = {
        "event_id": str(event_id),
        "event_type": "investigation.completed",
        "schema_version": 1,
        "workspace_id": str(row["workspace_id"]),
        "occurred_at": moment,
        "ingested_at": moment,
        "source_id": None,
        "trace_id": None,
        "correlation_id": None,
        "causation_id": None,
        "replay_run_id": None,
        "payload": {
            "investigation_id": investigation_id,
            "incident_id": str(row["incident_id"]),
            "status": status,
            "diagnosis_status": diagnosis_status,
            "workflow_version": row["workflow_version"],
        },
    }
    connection.execute(
        ADD_OUTBOX,
        {
            "event_id": event_id,
            "id": row["id"],
            "version": version,
            "key": investigation_id,
            "envelope": json.dumps(envelope),
        },
    )


def upgrade() -> None:
    op.create_index(
        "uq_progress_events_investigation_ended",
        "progress_events",
        ["investigation_id"],
        unique=True,
        postgresql_where=sa.text("event_type = 'investigation.ended'"),
    )
    connection = op.get_bind()
    for row in connection.execute(STRANDED).mappings().all():
        _settle(connection, row)


def downgrade() -> None:
    op.drop_index(
        "uq_progress_events_investigation_ended",
        table_name="progress_events",
        postgresql_where=sa.text("event_type = 'investigation.ended'"),
    )
