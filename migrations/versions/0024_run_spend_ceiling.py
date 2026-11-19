"""Run-level spend ceiling and approval reference on evaluation runs (P13-T06).

Nullable columns with no default do not rewrite the table. Covered by migration
0017's default privileges like the rest of `evaluation.runs`.

Revision ID: 0024_run_spend_ceiling
Revises: 0023_stage_runs
Create Date: 2026-11-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024_run_spend_ceiling"
down_revision: str | None = "0023_stage_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runs", sa.Column("max_spend_microusd", sa.BigInteger(), nullable=True), schema="evaluation"
    )
    op.add_column("runs", sa.Column("approval_ref", sa.Text(), nullable=True), schema="evaluation")
    op.create_check_constraint(
        op.f("ck_runs_max_spend_positive"),
        "runs",
        "max_spend_microusd IS NULL OR max_spend_microusd > 0",
        schema="evaluation",
    )
    op.create_check_constraint(
        op.f("ck_runs_approval_ref_length"),
        "runs",
        "approval_ref IS NULL OR char_length(approval_ref) BETWEEN 1 AND 256",
        schema="evaluation",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_runs_approval_ref_length"), "runs", schema="evaluation", type_="check"
    )
    op.drop_constraint(
        op.f("ck_runs_max_spend_positive"), "runs", schema="evaluation", type_="check"
    )
    op.drop_column("runs", "approval_ref", schema="evaluation")
    op.drop_column("runs", "max_spend_microusd", schema="evaluation")
