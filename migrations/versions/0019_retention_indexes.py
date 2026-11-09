"""Retention cleanup: signal store time, and indexes for receipts, outbox, and windows.

`signals.stored_at` defaults to `now()`, a stable expression, so adding it does
not rewrite the table; existing rows read the migration time.

Revision ID: 0019_retention_indexes
Revises: 0018_evaluation_audits
Create Date: 2026-11-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019_retention_indexes"
down_revision: str | None = "0018_evaluation_audits"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "signals",
        sa.Column(
            "stored_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index(op.f("ix_signals_stored_at"), "signals", ["stored_at"], postgresql_using="brin")
    op.create_index(
        op.f("ix_consumer_receipts_consumer_group_processed_at"),
        "consumer_receipts",
        ["consumer_group", "processed_at"],
    )
    op.create_index(
        op.f("ix_outbox_events_published_at"),
        "outbox_events",
        ["published_at"],
        postgresql_where=sa.text("published_at IS NOT NULL"),
    )
    op.create_index(
        op.f("ix_signal_aggregates_closed_window_end"),
        "signal_aggregates",
        ["window_end"],
        postgresql_where=sa.text("closed_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_signal_aggregates_closed_window_end"), table_name="signal_aggregates")
    op.drop_index(op.f("ix_outbox_events_published_at"), table_name="outbox_events")
    op.drop_index(
        op.f("ix_consumer_receipts_consumer_group_processed_at"), table_name="consumer_receipts"
    )
    op.drop_index(op.f("ix_signals_stored_at"), table_name="signals")
    op.drop_column("signals", "stored_at")
