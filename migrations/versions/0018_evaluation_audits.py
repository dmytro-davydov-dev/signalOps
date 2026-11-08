"""Add human audits of cited evidence to the evaluation schema.

Migration 0017's default privileges give the evaluator role access and the
runtime role none, so this table needs no grant of its own.

Revision ID: 0018_evaluation_audits
Revises: 0017_evaluation
Create Date: 2026-11-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018_evaluation_audits"
down_revision: str | None = "0017_evaluation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "audits",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("case_id", sa.Uuid(), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("supported", sa.Boolean(), nullable=False),
        sa.Column("auditor", sa.Text(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(auditor) BETWEEN 1 AND 128", name=op.f("ck_audits_auditor_length")
        ),
        sa.CheckConstraint(
            "note IS NULL OR char_length(note) BETWEEN 1 AND 2000",
            name=op.f("ck_audits_note_length"),
        ),
        sa.ForeignKeyConstraint(
            ["case_id"], ["evaluation.cases.id"], name=op.f("fk_audits_case_id_cases")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audits")),
        sa.UniqueConstraint(
            "case_id", "signal_id", "auditor", name=op.f("uq_audits_case_id_signal_id_auditor")
        ),
        schema="evaluation",
    )


def downgrade() -> None:
    op.drop_table("audits", schema="evaluation")
