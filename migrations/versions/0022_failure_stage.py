"""First failing stage and limit flag per evaluation case (P13-T03).

Covered by migration 0017's default privileges like the rest of `evaluation.cases`.
Nullable columns with no default do not rewrite the table.

Revision ID: 0022_failure_stage
Revises: 0021_stage_scores
Create Date: 2026-11-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0022_failure_stage"
down_revision: str | None = "0021_stage_scores"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STAGES = (
    "harness_failure",
    "detection_miss",
    "no_lead",
    "lead_lost_in_synthesis",
    "verifier_false_reject",
    "verifier_false_accept",
    "conclude_dropped",
    "wrong_failure_type",
    "false_diagnosis_on_underdetermined",
    "rule_error",
    "unattributed",
)


def upgrade() -> None:
    for column in (
        sa.Column("failure_stage", sa.Text(), nullable=True),
        sa.Column("failure_origin", sa.Text(), nullable=True),
        sa.Column("limit_hit", sa.Boolean(), nullable=True),
    ):
        op.add_column("cases", column, schema="evaluation")
    listed = ", ".join(f"'{stage}'" for stage in STAGES)
    for name, condition in (
        ("ck_cases_failure_stage", f"failure_stage IS NULL OR failure_stage IN ({listed})"),
        (
            "ck_cases_failure_origin_length",
            "failure_origin IS NULL OR char_length(failure_origin) BETWEEN 1 AND 128",
        ),
        ("ck_cases_failure_stage_limit_hit", "(failure_stage IS NULL) = (limit_hit IS NULL)"),
    ):
        op.create_check_constraint(op.f(name), "cases", condition, schema="evaluation")


def downgrade() -> None:
    for name in (
        "ck_cases_failure_stage_limit_hit",
        "ck_cases_failure_origin_length",
        "ck_cases_failure_stage",
    ):
        op.drop_constraint(op.f(name), "cases", schema="evaluation", type_="check")
    for column in ("limit_hit", "failure_origin", "failure_stage"):
        op.drop_column("cases", column, schema="evaluation")
