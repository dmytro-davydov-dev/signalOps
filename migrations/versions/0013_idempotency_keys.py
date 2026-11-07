"""Store idempotency keys for investigation requests.

A row maps a workspace, route, and caller-chosen key to the canonical request
hash and the investigation that request created, so a repeat returns it and a
key reused for another request is a conflict.

Revision ID: 0013_idempotency_keys
Revises: 0012_graph_projection
Create Date: 2026-11-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013_idempotency_keys"
down_revision: str | None = "0012_graph_projection"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "idempotency_keys",
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("route", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("investigation_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "char_length(key) BETWEEN 1 AND 256", name=op.f("ck_idempotency_keys_key_length")
        ),
        sa.CheckConstraint(
            "char_length(request_hash) = 64", name=op.f("ck_idempotency_keys_request_hash_length")
        ),
        sa.CheckConstraint(
            "char_length(route) > 0", name=op.f("ck_idempotency_keys_route_nonempty")
        ),
        sa.ForeignKeyConstraint(
            ["investigation_id"],
            ["investigations.id"],
            name=op.f("fk_idempotency_keys_investigation_id_investigations"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id"],
            ["workspaces.id"],
            name=op.f("fk_idempotency_keys_workspace_id_workspaces"),
        ),
        sa.PrimaryKeyConstraint("workspace_id", "route", "key", name=op.f("pk_idempotency_keys")),
    )
    op.create_index(
        op.f("ix_idempotency_keys_investigation_id"), "idempotency_keys", ["investigation_id"]
    )


def downgrade() -> None:
    op.drop_table("idempotency_keys")
