"""Outbox retry and quarantine state.

`next_attempt_at` defers a row after a transient delivery failure; `quarantined_at`
stops retries after a permanent rejection until an operator redrives the row;
`last_error` holds the latest delivery error category and reason. A partial index
on pending rows by (aggregate_id, aggregate_version) lets the publisher hold back
an aggregate's later versions while an earlier one is deferred or quarantined.

Revision ID: 0007_outbox_retry
Revises: 0006_baseline_indexes
Create Date: 2026-10-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_outbox_retry"
down_revision: str | None = "0006_baseline_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox_events", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "outbox_events", sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("outbox_events", sa.Column("last_error", sa.Text(), nullable=True))
    op.create_check_constraint(
        op.f("ck_outbox_events_published_not_quarantined"),
        "outbox_events",
        "published_at IS NULL OR quarantined_at IS NULL",
    )
    op.create_check_constraint(
        op.f("ck_outbox_events_last_error_length"),
        "outbox_events",
        "last_error IS NULL OR char_length(last_error) BETWEEN 1 AND 2048",
    )
    op.create_index(
        "ix_outbox_events_pending_aggregate_id_aggregate_version",
        "outbox_events",
        ["aggregate_id", "aggregate_version"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_pending_aggregate_id_aggregate_version", table_name="outbox_events"
    )
    op.drop_constraint(op.f("ck_outbox_events_last_error_length"), "outbox_events", type_="check")
    op.drop_constraint(
        op.f("ck_outbox_events_published_not_quarantined"), "outbox_events", type_="check"
    )
    op.drop_column("outbox_events", "last_error")
    op.drop_column("outbox_events", "quarantined_at")
    op.drop_column("outbox_events", "next_attempt_at")
