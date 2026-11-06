"""Detection window closure and late samples.

`signal_aggregates` gains `late_count` and closure metadata (`closed_at`,
`closed_by`), with a partial index for finding open windows.
`detection_samples.late` marks samples past their window's lateness deadline.
`detection_clocks` holds each service's simulated clock per replay scope.

Windows counted before this revision are closed as `backfill`: their rules ran
on every sample, so closing them must not evaluate them again.

Revision ID: 0010_window_closure
Revises: 0009_detection_state
Create Date: 2026-11-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_window_closure"
down_revision: str | None = "0009_detection_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "signal_aggregates",
        sa.Column("late_count", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "signal_aggregates", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("signal_aggregates", sa.Column("closed_by", sa.Text(), nullable=True))
    op.execute("UPDATE signal_aggregates SET closed_at = now(), closed_by = 'backfill'")
    op.create_check_constraint(
        op.f("ck_signal_aggregates_late_count_nonnegative"), "signal_aggregates", "late_count >= 0"
    )
    op.create_check_constraint(
        op.f("ck_signal_aggregates_closure_complete"),
        "signal_aggregates",
        "(closed_at IS NULL) = (closed_by IS NULL)",
    )
    op.create_check_constraint(
        op.f("ck_signal_aggregates_closed_by_known"),
        "signal_aggregates",
        "closed_by IN ('wall_clock', 'simulated_clock', 'final_flush', 'backfill')",
    )
    op.create_index(
        "ix_signal_aggregates_open",
        "signal_aggregates",
        ["replay_scope", "window_end"],
        unique=False,
        postgresql_where=sa.text("closed_at IS NULL"),
    )
    op.add_column(
        "detection_samples",
        sa.Column("late", sa.Boolean(), server_default=sa.false(), nullable=False),
    )
    op.create_table(
        "detection_clocks",
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("clock_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "char_length(replay_scope) BETWEEN 1 AND 128",
            name=op.f("ck_detection_clocks_replay_scope_length"),
        ),
        sa.CheckConstraint("replay_scope <> 'live'", name=op.f("ck_detection_clocks_replay_only")),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_detection_clocks_workspace_id_services"),
        ),
        sa.PrimaryKeyConstraint("service_id", "replay_scope", name=op.f("pk_detection_clocks")),
    )


def downgrade() -> None:
    op.drop_table("detection_clocks")
    op.drop_column("detection_samples", "late")
    op.drop_index(
        "ix_signal_aggregates_open",
        table_name="signal_aggregates",
        postgresql_where=sa.text("closed_at IS NULL"),
    )
    op.drop_constraint(
        op.f("ck_signal_aggregates_closed_by_known"), "signal_aggregates", type_="check"
    )
    op.drop_constraint(
        op.f("ck_signal_aggregates_closure_complete"), "signal_aggregates", type_="check"
    )
    op.drop_constraint(
        op.f("ck_signal_aggregates_late_count_nonnegative"), "signal_aggregates", type_="check"
    )
    op.drop_column("signal_aggregates", "closed_by")
    op.drop_column("signal_aggregates", "closed_at")
    op.drop_column("signal_aggregates", "late_count")
