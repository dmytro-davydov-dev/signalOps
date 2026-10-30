"""Signal indexes for scoped evidence queries.

Evidence pages filter by workspace and replay scope, optionally by service, and
order by (occurred_at, id) for keyset pagination; quality summaries count per
source by ingestion time. The plan's service and source indexes lacked
`replay_scope` and the ID tiebreaker, so they are replaced rather than kept
alongside. Columns are ascending: descending pages scan the index backward, and
row comparisons on (occurred_at, id) stay index conditions.

Revision ID: 0008_evidence_query_indexes
Revises: 0007_outbox_retry
Create Date: 2026-10-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008_evidence_query_indexes"
down_revision: str | None = "0007_outbox_retry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_signals_workspace_id_replay_scope_occurred_at",
        "signals",
        ["workspace_id", "replay_scope", "occurred_at", "id"],
    )
    op.drop_index("ix_signals_service_id_occurred_at", table_name="signals")
    op.create_index(
        "ix_signals_service_id_replay_scope_occurred_at",
        "signals",
        ["service_id", "replay_scope", "occurred_at", "id"],
    )
    op.drop_index("ix_signals_source_id_ingested_at", table_name="signals")
    op.create_index(
        "ix_signals_source_id_replay_scope_ingested_at",
        "signals",
        ["source_id", "replay_scope", "ingested_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_signals_source_id_replay_scope_ingested_at", table_name="signals")
    op.create_index(
        "ix_signals_source_id_ingested_at", "signals", ["source_id", sa.text("ingested_at DESC")]
    )
    op.drop_index("ix_signals_service_id_replay_scope_occurred_at", table_name="signals")
    op.create_index(
        "ix_signals_service_id_occurred_at", "signals", ["service_id", sa.text("occurred_at DESC")]
    )
    op.drop_index("ix_signals_workspace_id_replay_scope_occurred_at", table_name="signals")
