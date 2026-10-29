"""Baseline: empty schema. Phase 2 adds the domain tables on top of this revision.

Revision ID: 0001_baseline
Revises:
Create Date: 2026-10-29
"""

from collections.abc import Sequence

revision: str = "0001_baseline"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
