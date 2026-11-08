"""Add the evaluation schema and the runtime and evaluator database roles.

Ground truth lives in schema `evaluation`. Two group roles divide access:
`signalops_runtime` (API, consumers, workers, and so the agent tools) may read and
write the application tables in `public` and has no privilege at all on
`evaluation`; `signalops_evaluator` has the same on `public` plus the evaluation
tables. Default privileges extend both to tables later revisions create, so a new
table never needs a grant. Login roles are created outside migrations by
`signalops migrate` from configured passwords (`persistence/roles.py`).

Roles are cluster-wide, so the upgrade creates them only when missing and the
downgrade revokes this database's grants but leaves the roles in place.

Revision ID: 0017_evaluation
Revises: 0016_llm_call_accounting
Create Date: 2026-11-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0017_evaluation"
down_revision: str | None = "0016_llm_call_accounting"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


RUNTIME = "signalops_runtime"
EVALUATOR = "signalops_evaluator"
BOTH = f"{RUNTIME}, {EVALUATOR}"
DML = "SELECT, INSERT, UPDATE, DELETE"


def _create_group_role(name: str) -> None:
    # A concurrent migration in another database may create it first.
    op.execute(
        f"""
        DO $$ BEGIN
            CREATE ROLE {name} NOLOGIN;
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
        """
    )


def upgrade() -> None:
    _create_group_role(RUNTIME)
    _create_group_role(EVALUATOR)
    op.execute(f"GRANT USAGE ON SCHEMA public TO {BOTH}")
    op.execute(f"GRANT {DML} ON ALL TABLES IN SCHEMA public TO {BOTH}")
    op.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {BOTH}")
    op.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT {DML} ON TABLES TO {BOTH}")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO {BOTH}"
    )
    # Only migrations move the schema version.
    op.execute(f"REVOKE INSERT, UPDATE, DELETE ON alembic_version FROM {BOTH}")

    op.execute("CREATE SCHEMA evaluation")
    op.execute("REVOKE ALL ON SCHEMA evaluation FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA evaluation TO {EVALUATOR}")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA evaluation GRANT {DML} ON TABLES TO {EVALUATOR}"
    )

    op.create_table(
        "scenarios",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("version", sa.Text(), nullable=False),
        sa.Column("family", sa.Text(), nullable=False),
        sa.Column("split", sa.Text(), nullable=False),
        sa.Column("synthetic", sa.Boolean(), nullable=False),
        sa.Column("dataset_ref", sa.Text(), nullable=False),
        sa.Column("dataset_hash", sa.Text(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("injection_spec", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("expected_outcome", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "dataset_hash ~ '^[0-9a-f]{64}$'", name=op.f("ck_scenarios_dataset_hash_sha256")
        ),
        sa.CheckConstraint(
            "family IN ('downstream_timeout', 'connection_pool_exhaustion', "
            "'deployment_regression', 'memory_pressure', 'healthy_noise', 'mixed', "
            "'insufficient_evidence')",
            name=op.f("ck_scenarios_family"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(expected_outcome) = 'object'",
            name=op.f("ck_scenarios_expected_outcome_object"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(injection_spec) = 'object'",
            name=op.f("ck_scenarios_injection_spec_object"),
        ),
        sa.CheckConstraint("split IN ('development', 'held_out')", name=op.f("ck_scenarios_split")),
        sa.CheckConstraint(
            "char_length(dataset_ref) > 0", name=op.f("ck_scenarios_dataset_ref_nonempty")
        ),
        sa.CheckConstraint(
            "char_length(name) BETWEEN 1 AND 128", name=op.f("ck_scenarios_name_length")
        ),
        sa.CheckConstraint("char_length(version) > 0", name=op.f("ck_scenarios_version_nonempty")),
        sa.CheckConstraint("record_count >= 0", name=op.f("ck_scenarios_record_count_nonnegative")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_scenarios")),
        sa.UniqueConstraint("name", "version", name=op.f("uq_scenarios_name_version")),
        schema="evaluation",
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("split", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="queued", nullable=False),
        sa.Column("variants", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trials", sa.Integer(), nullable=False),
        sa.Column("seed", sa.BigInteger(), nullable=False),
        sa.Column("dataset_hash", sa.Text(), nullable=False),
        sa.Column("workflow_versions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("prompt_versions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("model_config", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("pricing_config", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("limits", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("metrics", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed')) = (completed_at IS NOT NULL)",
            name=op.f("ck_runs_completed_at_status"),
        ),
        sa.CheckConstraint(
            "dataset_hash ~ '^[0-9a-f]{64}$'", name=op.f("ck_runs_dataset_hash_sha256")
        ),
        sa.CheckConstraint("jsonb_typeof(limits) = 'object'", name=op.f("ck_runs_limits_object")),
        sa.CheckConstraint(
            "jsonb_typeof(model_config) = 'object'", name=op.f("ck_runs_model_config_object")
        ),
        sa.CheckConstraint(
            "jsonb_typeof(pricing_config) = 'object'", name=op.f("ck_runs_pricing_config_object")
        ),
        sa.CheckConstraint(
            "jsonb_typeof(prompt_versions) = 'object'", name=op.f("ck_runs_prompt_versions_object")
        ),
        sa.CheckConstraint("jsonb_typeof(variants) = 'array'", name=op.f("ck_runs_variants_array")),
        sa.CheckConstraint(
            "jsonb_typeof(workflow_versions) = 'object'",
            name=op.f("ck_runs_workflow_versions_object"),
        ),
        sa.CheckConstraint(
            "metrics IS NULL OR jsonb_typeof(metrics) = 'object'",
            name=op.f("ck_runs_metrics_object"),
        ),
        sa.CheckConstraint("split IN ('development', 'held_out')", name=op.f("ck_runs_split")),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed')", name=op.f("ck_runs_status")
        ),
        sa.CheckConstraint(
            "(lease_owner IS NULL) = (lease_expires_at IS NULL)",
            name=op.f("ck_runs_lease_complete"),
        ),
        sa.CheckConstraint(
            "char_length(label) BETWEEN 1 AND 256", name=op.f("ck_runs_label_length")
        ),
        sa.CheckConstraint(
            "error IS NULL OR char_length(error) BETWEEN 1 AND 2048",
            name=op.f("ck_runs_error_length"),
        ),
        sa.CheckConstraint("trials BETWEEN 1 AND 20", name=op.f("ck_runs_trials_range")),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspaces.id"], name=op.f("fk_runs_workspace_id_workspaces")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runs")),
        schema="evaluation",
    )
    op.create_index(
        "ix_runs_status_created_at",
        "runs",
        ["status", "created_at"],
        unique=False,
        schema="evaluation",
    )
    op.create_index(
        "ix_runs_workspace_id_created_at",
        "runs",
        ["workspace_id", "created_at"],
        unique=False,
        schema="evaluation",
    )
    op.create_table(
        "replays",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("scenario_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("report", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "report IS NULL OR jsonb_typeof(report) = 'object'",
            name=op.f("ck_replays_report_object"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'completed', 'timed_out', 'failed')",
            name=op.f("ck_replays_status"),
        ),
        sa.CheckConstraint(
            "char_length(replay_scope) > 0", name=op.f("ck_replays_replay_scope_nonempty")
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["evaluation.runs.id"], name=op.f("fk_replays_run_id_runs")
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["evaluation.scenarios.id"],
            name=op.f("fk_replays_scenario_id_scenarios"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_replays")),
        sa.UniqueConstraint("replay_scope", name=op.f("uq_replays_replay_scope")),
        sa.UniqueConstraint("run_id", "scenario_id", name=op.f("uq_replays_run_id_scenario_id")),
        schema="evaluation",
    )
    op.create_index(
        "ix_replays_scenario_id", "replays", ["scenario_id"], unique=False, schema="evaluation"
    )
    op.create_table(
        "cases",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("replay_id", sa.Uuid(), nullable=False),
        sa.Column("scenario_id", sa.Uuid(), nullable=False),
        sa.Column("case_key", sa.Text(), nullable=False),
        sa.Column("variant", sa.Text(), nullable=False),
        sa.Column("trial", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("investigation_id", sa.Uuid(), nullable=True),
        sa.Column("expected_outcome", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("observed_outcome", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("scores", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(status IN ('scored', 'failed')) = (completed_at IS NOT NULL)",
            name=op.f("ck_cases_completed_at_status"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(expected_outcome) = 'object'",
            name=op.f("ck_cases_expected_outcome_object"),
        ),
        sa.CheckConstraint(
            "observed_outcome IS NULL OR jsonb_typeof(observed_outcome) = 'object'",
            name=op.f("ck_cases_observed_outcome_object"),
        ),
        sa.CheckConstraint(
            "scores IS NULL OR jsonb_typeof(scores) = 'object'", name=op.f("ck_cases_scores_object")
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'investigating', 'scored', 'failed')",
            name=op.f("ck_cases_status"),
        ),
        sa.CheckConstraint(
            "variant IN ('deterministic', 'single_agent', 'multi_agent')",
            name=op.f("ck_cases_variant"),
        ),
        sa.CheckConstraint(
            "char_length(case_key) BETWEEN 1 AND 256", name=op.f("ck_cases_case_key_length")
        ),
        sa.CheckConstraint(
            "error IS NULL OR char_length(error) BETWEEN 1 AND 2048",
            name=op.f("ck_cases_error_length"),
        ),
        sa.CheckConstraint("trial >= 1", name=op.f("ck_cases_trial_positive")),
        sa.ForeignKeyConstraint(
            ["investigation_id"],
            ["investigations.id"],
            name=op.f("fk_cases_investigation_id_investigations"),
        ),
        sa.ForeignKeyConstraint(
            ["replay_id"], ["evaluation.replays.id"], name=op.f("fk_cases_replay_id_replays")
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["evaluation.runs.id"], name=op.f("fk_cases_run_id_runs")
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["evaluation.scenarios.id"],
            name=op.f("fk_cases_scenario_id_scenarios"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_cases")),
        sa.UniqueConstraint("run_id", "case_key", name=op.f("uq_cases_run_id_case_key")),
        schema="evaluation",
    )
    op.create_index(
        "ix_cases_investigation_id",
        "cases",
        ["investigation_id"],
        unique=False,
        schema="evaluation",
    )
    op.create_index("ix_cases_replay_id", "cases", ["replay_id"], unique=False, schema="evaluation")
    op.create_index(
        "ix_cases_scenario_id", "cases", ["scenario_id"], unique=False, schema="evaluation"
    )


def downgrade() -> None:
    op.drop_index("ix_cases_scenario_id", table_name="cases", schema="evaluation")
    op.drop_index("ix_cases_replay_id", table_name="cases", schema="evaluation")
    op.drop_index("ix_cases_investigation_id", table_name="cases", schema="evaluation")
    op.drop_table("cases", schema="evaluation")
    op.drop_index("ix_replays_scenario_id", table_name="replays", schema="evaluation")
    op.drop_table("replays", schema="evaluation")
    op.drop_index("ix_runs_workspace_id_created_at", table_name="runs", schema="evaluation")
    op.drop_index("ix_runs_status_created_at", table_name="runs", schema="evaluation")
    op.drop_table("runs", schema="evaluation")
    op.drop_table("scenarios", schema="evaluation")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA evaluation REVOKE {DML} ON TABLES FROM {EVALUATOR}"
    )
    op.execute("DROP SCHEMA evaluation")
    op.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE {DML} ON TABLES FROM {BOTH}")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE USAGE, SELECT ON SEQUENCES FROM {BOTH}"  # noqa: S608 - constants
    )
    op.execute(f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {BOTH}")
    op.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {BOTH}")
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {BOTH}")
