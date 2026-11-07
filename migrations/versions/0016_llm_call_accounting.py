"""Record what prices, cache use, and lease each model call settled with.

Revision ID: 0016_llm_call_accounting
Revises: 0015_investigation_outcomes
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016_llm_call_accounting"
down_revision: str | None = "0015_investigation_outcomes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # No row has an amount priced by a known version; the default stands in for any.
    op.add_column(
        "llm_calls",
        sa.Column("pricing_version", sa.Text(), server_default="unversioned", nullable=False),
    )
    op.alter_column("llm_calls", "pricing_version", server_default=None)
    op.add_column(
        "llm_calls",
        sa.Column("lease_generation", sa.BigInteger(), server_default="0", nullable=False),
    )
    op.add_column("llm_calls", sa.Column("cache_read_tokens", sa.Integer(), nullable=True))
    op.add_column("llm_calls", sa.Column("cache_write_tokens", sa.Integer(), nullable=True))
    op.add_column("llm_calls", sa.Column("error_category", sa.Text(), nullable=True))
    op.add_column("llm_calls", sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True))
    op.create_check_constraint(
        op.f("ck_llm_calls_pricing_version_nonempty"),
        "llm_calls",
        "char_length(pricing_version) > 0",
    )
    op.create_check_constraint(
        op.f("ck_llm_calls_cache_read_tokens_nonnegative"),
        "llm_calls",
        "cache_read_tokens IS NULL OR cache_read_tokens >= 0",
    )
    op.create_check_constraint(
        op.f("ck_llm_calls_cache_write_tokens_nonnegative"),
        "llm_calls",
        "cache_write_tokens IS NULL OR cache_write_tokens >= 0",
    )
    op.create_check_constraint(
        op.f("ck_llm_calls_lease_generation_nonnegative"),
        "llm_calls",
        "lease_generation >= 0",
    )
    op.create_check_constraint(
        op.f("ck_llm_calls_completed_at_status"),
        "llm_calls",
        "(status = 'pending') = (completed_at IS NULL)",
    )


def downgrade() -> None:
    for name in (
        "completed_at_status",
        "lease_generation_nonnegative",
        "cache_write_tokens_nonnegative",
        "cache_read_tokens_nonnegative",
        "pricing_version_nonempty",
    ):
        op.drop_constraint(op.f(f"ck_llm_calls_{name}"), "llm_calls", type_="check")
    for column in (
        "completed_at",
        "error_category",
        "cache_write_tokens",
        "cache_read_tokens",
        "lease_generation",
        "pricing_version",
    ):
        op.drop_column("llm_calls", column)
