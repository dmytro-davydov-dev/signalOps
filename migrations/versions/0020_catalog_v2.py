"""Catalog v2: allow the `adversarial` scenario family.

Stage labels live inside `scenarios.expected_outcome` (JSONB), so they need no
column. Version 1 rows are unchanged.

Revision ID: 0020_catalog_v2
Revises: 0019_retention_indexes
Create Date: 2026-11-19
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0020_catalog_v2"
down_revision: str | None = "0019_retention_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

V1_FAMILIES = (
    "'downstream_timeout', 'connection_pool_exhaustion', 'deployment_regression', "
    "'memory_pressure', 'healthy_noise', 'mixed', 'insufficient_evidence'"
)


def _family_check(families: str) -> None:
    op.drop_constraint(op.f("ck_scenarios_family"), "scenarios", schema="evaluation", type_="check")
    op.create_check_constraint(
        op.f("ck_scenarios_family"), "scenarios", f"family IN ({families})", schema="evaluation"
    )


def upgrade() -> None:
    _family_check(f"{V1_FAMILIES}, 'adversarial'")


def downgrade() -> None:
    _family_check(V1_FAMILIES)
