"""Stage scores per evaluation case (P13-T02).

`evaluation.cases` is covered by migration 0017's default privileges: the evaluator
role reads and writes it, the runtime role has nothing. Adding a nullable column
with no default does not rewrite the table.

Revision ID: 0021_stage_scores
Revises: 0020_catalog_v2
Create Date: 2026-11-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0021_stage_scores"
down_revision: str | None = "0020_catalog_v2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "cases",
        sa.Column("stage_scores", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema="evaluation",
    )
    op.create_check_constraint(
        op.f("ck_cases_stage_scores_object"),
        "cases",
        "stage_scores IS NULL OR jsonb_typeof(stage_scores) = 'object'",
        schema="evaluation",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_cases_stage_scores_object"), "cases", schema="evaluation", type_="check"
    )
    op.drop_column("cases", "stage_scores", schema="evaluation")
