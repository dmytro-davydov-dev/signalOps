"""Detector samples and rule triggers.

`detection_samples` is the per-signal business key that keeps a redelivered
signal from being counted twice in `signal_aggregates`; it cascades with its
signal. `detection_triggers` records rule threshold crossings per service
window, unique per (service, replay scope, rule, window start).

Revision ID: 0009_detection_state
Revises: 0008_evidence_query_indexes
Create Date: 2026-10-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009_detection_state"
down_revision: str | None = "0008_evidence_query_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "detection_triggers",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), server_default="live", nullable=False),
        sa.Column("rule_id", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.Integer(), nullable=False),
        sa.Column("metric_name", sa.Text(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed", sa.Double(), nullable=False),
        sa.Column("threshold", sa.Double(), nullable=False),
        sa.Column("sample_count", sa.BigInteger(), nullable=False),
        sa.Column("error_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "triggered_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "0 <= error_count AND error_count <= sample_count",
            name=op.f("ck_detection_triggers_counts_range"),
        ),
        sa.CheckConstraint(
            "char_length(metric_name) > 0", name=op.f("ck_detection_triggers_metric_name_nonempty")
        ),
        sa.CheckConstraint(
            "char_length(replay_scope) BETWEEN 1 AND 128",
            name=op.f("ck_detection_triggers_replay_scope_length"),
        ),
        sa.CheckConstraint(
            "char_length(rule_id) > 0", name=op.f("ck_detection_triggers_rule_id_nonempty")
        ),
        sa.CheckConstraint(
            "rule_version >= 1", name=op.f("ck_detection_triggers_rule_version_positive")
        ),
        sa.CheckConstraint(
            "window_end > window_start", name=op.f("ck_detection_triggers_window_order")
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_detection_triggers_workspace_id_services"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_detection_triggers")),
        sa.UniqueConstraint(
            "service_id",
            "replay_scope",
            "rule_id",
            "window_start",
            name=op.f("uq_detection_triggers_window"),
        ),
    )
    op.create_table(
        "detection_samples",
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("metric_name", sa.Text(), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), server_default="live", nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_error", sa.Boolean(), nullable=False),
        sa.Column("value", sa.Double(), nullable=True),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(metric_name) > 0", name=op.f("ck_detection_samples_metric_name_nonempty")
        ),
        sa.CheckConstraint(
            "value IS NULL OR value >= 0", name=op.f("ck_detection_samples_value_nonnegative")
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["signals.id"],
            name=op.f("fk_detection_samples_signal_id_signals"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_detection_samples_workspace_id_services"),
        ),
        sa.PrimaryKeyConstraint("signal_id", "metric_name", name=op.f("pk_detection_samples")),
    )
    op.create_index(
        "ix_detection_samples_window",
        "detection_samples",
        ["service_id", "replay_scope", "metric_name", "window_start"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_detection_samples_window", table_name="detection_samples")
    op.drop_table("detection_samples")
    op.drop_table("detection_triggers")
