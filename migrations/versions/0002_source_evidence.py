"""Source and evidence tables: workspaces, sources, services, signals, deployments, aggregates.

Workspace-owned references are composite foreign keys that include workspace_id.
Signal identity is UNIQUE(source_id, replay_scope, source_event_id), so one fixture
can be loaded into several replay scopes but not twice into the same one.

Revision ID: 0002_source_evidence
Revises: 0001_baseline
Create Date: 2026-10-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_source_evidence"
down_revision: str | None = "0001_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workspaces",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("char_length(name) > 0", name=op.f("ck_workspaces_name_nonempty")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workspaces")),
    )
    op.create_table(
        "services",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column(
            "attributes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(attributes) = 'object'", name=op.f("ck_services_attributes_object")
        ),
        sa.CheckConstraint(
            "char_length(environment) > 0", name=op.f("ck_services_environment_nonempty")
        ),
        sa.CheckConstraint("char_length(name) > 0", name=op.f("ck_services_name_nonempty")),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspaces.id"], name=op.f("fk_services_workspace_id_workspaces")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_services")),
        sa.UniqueConstraint("workspace_id", "id", name=op.f("uq_services_workspace_id_id")),
        sa.UniqueConstraint(
            "workspace_id",
            "name",
            "environment",
            name=op.f("uq_services_workspace_id_name_environment"),
        ),
    )
    op.create_table(
        "sources",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column(
            "config",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(config) = 'object'", name=op.f("ck_sources_config_object")
        ),
        sa.CheckConstraint(
            "kind IN ('structured_log', 'trace_summary', 'deployment')",
            name=op.f("ck_sources_kind"),
        ),
        sa.CheckConstraint("char_length(name) > 0", name=op.f("ck_sources_name_nonempty")),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspaces.id"], name=op.f("fk_sources_workspace_id_workspaces")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sources")),
        sa.UniqueConstraint("workspace_id", "id", name=op.f("uq_sources_workspace_id_id")),
        sa.UniqueConstraint("workspace_id", "name", name=op.f("uq_sources_workspace_id_name")),
    )
    op.create_table(
        "signal_aggregates",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), server_default="live", nullable=False),
        sa.Column("metric_name", sa.Text(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sample_count", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("error_count", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("value_sum", sa.Double(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "histogram",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(histogram) = 'object'", name=op.f("ck_signal_aggregates_histogram_object")
        ),
        sa.CheckConstraint(
            "char_length(metric_name) > 0", name=op.f("ck_signal_aggregates_metric_name_nonempty")
        ),
        sa.CheckConstraint(
            "0 <= error_count AND error_count <= sample_count",
            name=op.f("ck_signal_aggregates_counts_range"),
        ),
        sa.CheckConstraint(
            "window_end > window_start", name=op.f("ck_signal_aggregates_window_order")
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_signal_aggregates_workspace_id_services"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_signal_aggregates")),
        sa.UniqueConstraint(
            "service_id",
            "replay_scope",
            "metric_name",
            "window_start",
            name=op.f("uq_signal_aggregates_window"),
        ),
    )
    op.create_table(
        "signals",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=True),
        sa.Column("source_event_id", sa.Text(), nullable=False),
        sa.Column("replay_scope", sa.Text(), server_default="live", nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("trace_id", sa.Text(), nullable=True),
        sa.Column(
            "attributes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("quality_status", sa.Text(), nullable=False),
        sa.Column(
            "quality_flags",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(attributes) = 'object'", name=op.f("ck_signals_attributes_object")
        ),
        sa.CheckConstraint(
            "jsonb_typeof(quality_flags) = 'array'", name=op.f("ck_signals_quality_flags_array")
        ),
        sa.CheckConstraint("kind IN ('log', 'trace', 'deployment')", name=op.f("ck_signals_kind")),
        sa.CheckConstraint(
            "quality_status IN ('accepted', 'degraded', 'quarantined')",
            name=op.f("ck_signals_quality_status"),
        ),
        sa.CheckConstraint(
            "severity IN ('debug', 'info', 'warning', 'error', 'critical')",
            name=op.f("ck_signals_severity"),
        ),
        sa.CheckConstraint(
            "char_length(replay_scope) BETWEEN 1 AND 128",
            name=op.f("ck_signals_replay_scope_length"),
        ),
        sa.CheckConstraint(
            "char_length(source_event_id) BETWEEN 1 AND 512",
            name=op.f("ck_signals_source_event_id_length"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_signals_workspace_id_services"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "source_id"],
            ["sources.workspace_id", "sources.id"],
            name=op.f("fk_signals_workspace_id_sources"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_signals")),
        sa.UniqueConstraint(
            "source_id",
            "replay_scope",
            "source_event_id",
            name=op.f("uq_signals_source_id_replay_scope_source_event_id"),
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "replay_scope",
            "service_id",
            "id",
            name=op.f("uq_signals_workspace_id_replay_scope_service_id_id"),
        ),
    )
    op.create_index(
        "ix_signals_service_id_occurred_at",
        "signals",
        ["service_id", sa.literal_column("occurred_at DESC")],
        unique=False,
    )
    op.create_index(
        "ix_signals_source_id_ingested_at",
        "signals",
        ["source_id", sa.literal_column("ingested_at DESC")],
        unique=False,
    )
    op.create_index(
        "ix_signals_trace_id",
        "signals",
        ["trace_id"],
        unique=False,
        postgresql_where=sa.text("trace_id IS NOT NULL"),
    )
    op.create_table(
        "deployments",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), server_default="live", nullable=False),
        sa.Column("revision", sa.Text(), nullable=False),
        sa.Column("deployed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "attributes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(attributes) = 'object'", name=op.f("ck_deployments_attributes_object")
        ),
        sa.CheckConstraint(
            "char_length(revision) > 0", name=op.f("ck_deployments_revision_nonempty")
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "service_id", "signal_id"],
            ["signals.workspace_id", "signals.replay_scope", "signals.service_id", "signals.id"],
            name=op.f("fk_deployments_workspace_id_signals"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_deployments_workspace_id_services"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_deployments")),
        sa.UniqueConstraint("signal_id", name=op.f("uq_deployments_signal_id")),
    )
    op.create_index(
        "ix_deployments_service_id_replay_scope_deployed_at",
        "deployments",
        ["service_id", "replay_scope", sa.literal_column("deployed_at DESC")],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_deployments_service_id_replay_scope_deployed_at", table_name="deployments")
    op.drop_table("deployments")
    op.drop_index(
        "ix_signals_trace_id",
        table_name="signals",
        postgresql_where=sa.text("trace_id IS NOT NULL"),
    )
    op.drop_index("ix_signals_source_id_ingested_at", table_name="signals")
    op.drop_index("ix_signals_service_id_occurred_at", table_name="signals")
    op.drop_table("signals")
    op.drop_table("signal_aggregates")
    op.drop_table("sources")
    op.drop_table("services")
    op.drop_table("workspaces")
