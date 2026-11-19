"""Component evaluation runs and cases (P13-T04).

Two tables in the `evaluation` schema, so migration 0017's default privileges give
the evaluator role access and the runtime role none, like the rest of ground truth.

Revision ID: 0023_stage_runs
Revises: 0022_failure_stage
Create Date: 2026-11-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0023_stage_runs"
down_revision: str | None = "0022_failure_stage"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSONB = postgresql.JSONB(astext_type=sa.Text())


def _now() -> sa.Column[sa.DateTime]:
    return sa.Column(
        "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def upgrade() -> None:
    op.create_table(
        "stage_runs",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("corpus_version", sa.Text(), nullable=False),
        sa.Column("corpus_hash", sa.Text(), nullable=False),
        sa.Column("trials", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), server_default="running", nullable=False),
        sa.Column("model_config", JSONB, nullable=False),
        sa.Column("limits", JSONB, nullable=False),
        sa.Column("metrics", JSONB, nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        _now(),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("role IN ('synthesizer', 'verifier')", name=op.f("ck_stage_runs_role")),
        sa.CheckConstraint(
            "char_length(corpus_version) > 0", name=op.f("ck_stage_runs_corpus_version_nonempty")
        ),
        sa.CheckConstraint(
            "corpus_hash ~ '^[0-9a-f]{64}$'", name=op.f("ck_stage_runs_corpus_hash_sha256")
        ),
        sa.CheckConstraint("trials BETWEEN 1 AND 20", name=op.f("ck_stage_runs_trials_range")),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed')",
            name=op.f("ck_stage_runs_status"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(model_config) = 'object'", name=op.f("ck_stage_runs_model_config_object")
        ),
        sa.CheckConstraint(
            "jsonb_typeof(limits) = 'object'", name=op.f("ck_stage_runs_limits_object")
        ),
        sa.CheckConstraint(
            "metrics IS NULL OR jsonb_typeof(metrics) = 'object'",
            name=op.f("ck_stage_runs_metrics_object"),
        ),
        sa.CheckConstraint(
            "error IS NULL OR char_length(error) BETWEEN 1 AND 2048",
            name=op.f("ck_stage_runs_error_length"),
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed')) = (completed_at IS NOT NULL)",
            name=op.f("ck_stage_runs_completed_at_status"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspaces.id"], name=op.f("fk_stage_runs_workspace_id_workspaces")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stage_runs")),
        schema="evaluation",
    )
    op.create_index(
        "ix_stage_runs_workspace_id_created_at",
        "stage_runs",
        ["workspace_id", "created_at"],
        schema="evaluation",
    )
    op.create_table(
        "stage_cases",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("stage_run_id", sa.Uuid(), nullable=False),
        sa.Column("case_key", sa.Text(), nullable=False),
        sa.Column("defect", sa.Text(), nullable=False),
        sa.Column("trial", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("investigation_id", sa.Uuid(), nullable=True),
        sa.Column("expected", JSONB, nullable=False),
        sa.Column("output", JSONB, nullable=True),
        sa.Column("scores", JSONB, nullable=True),
        sa.Column("investigation_status", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        _now(),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "char_length(case_key) BETWEEN 1 AND 256", name=op.f("ck_stage_cases_case_key_length")
        ),
        sa.CheckConstraint("char_length(defect) > 0", name=op.f("ck_stage_cases_defect_nonempty")),
        sa.CheckConstraint("trial >= 1", name=op.f("ck_stage_cases_trial_positive")),
        sa.CheckConstraint(
            "status IN ('pending', 'investigating', 'scored', 'failed')",
            name=op.f("ck_stage_cases_status"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(expected) = 'object'", name=op.f("ck_stage_cases_expected_object")
        ),
        sa.CheckConstraint(
            "output IS NULL OR jsonb_typeof(output) = 'object'",
            name=op.f("ck_stage_cases_output_object"),
        ),
        sa.CheckConstraint(
            "scores IS NULL OR jsonb_typeof(scores) = 'object'",
            name=op.f("ck_stage_cases_scores_object"),
        ),
        sa.CheckConstraint(
            "error IS NULL OR char_length(error) BETWEEN 1 AND 2048",
            name=op.f("ck_stage_cases_error_length"),
        ),
        sa.CheckConstraint(
            "(status IN ('scored', 'failed')) = (completed_at IS NOT NULL)",
            name=op.f("ck_stage_cases_completed_at_status"),
        ),
        sa.ForeignKeyConstraint(
            ["stage_run_id"],
            ["evaluation.stage_runs.id"],
            name=op.f("fk_stage_cases_stage_run_id_stage_runs"),
        ),
        sa.ForeignKeyConstraint(
            ["investigation_id"],
            ["investigations.id"],
            name=op.f("fk_stage_cases_investigation_id_investigations"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stage_cases")),
        sa.UniqueConstraint(
            "stage_run_id",
            "case_key",
            "trial",
            name=op.f("uq_stage_cases_stage_run_id_case_key_trial"),
        ),
        schema="evaluation",
    )
    op.create_index(
        "ix_stage_cases_investigation_id", "stage_cases", ["investigation_id"], schema="evaluation"
    )


def downgrade() -> None:
    op.drop_index("ix_stage_cases_investigation_id", "stage_cases", schema="evaluation")
    op.drop_table("stage_cases", schema="evaluation")
    op.drop_index("ix_stage_runs_workspace_id_created_at", "stage_runs", schema="evaluation")
    op.drop_table("stage_runs", schema="evaluation")
