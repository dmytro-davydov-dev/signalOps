"""Record cancellation requests on investigations.

The request is durable and separate from `status`, because a running
investigation stays `running` until the worker holding its job ends it.

Revision ID: 0014_investigation_cancellation
Revises: 0013_idempotency_keys
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_investigation_cancellation"
down_revision: str | None = "0013_idempotency_keys"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "investigations",
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("investigations", "cancel_requested_at")
