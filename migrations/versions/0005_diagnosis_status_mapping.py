"""Tie the stored diagnosis to the investigation status.

completed requires a diagnosed diagnosis, insufficient_evidence requires an
insufficient_evidence diagnosis, and every other status requires no diagnosis
(contracts.investigations.DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS). Replaces
the 0003 rule, which only limited which statuses could carry a diagnosis.

Revision ID: 0005_diagnosis_status_mapping
Revises: 0004_graph_reliability
Create Date: 2026-10-29
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005_diagnosis_status_mapping"
down_revision: str | None = "0004_graph_reliability"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MAPPED_RULE = (
    "(status = 'completed' AND diagnosis ->> 'status' IS NOT DISTINCT FROM 'diagnosed')"
    " OR (status = 'insufficient_evidence'"
    " AND diagnosis ->> 'status' IS NOT DISTINCT FROM 'insufficient_evidence')"
    " OR (NOT (status IN ('completed', 'insufficient_evidence')) AND diagnosis IS NULL)"
)
PREVIOUS_RULE = "diagnosis IS NULL OR status IN ('completed', 'insufficient_evidence')"


def upgrade() -> None:
    op.drop_constraint(op.f("ck_investigations_diagnosis_status"), "investigations", type_="check")
    op.create_check_constraint(
        op.f("ck_investigations_diagnosis_status"), "investigations", MAPPED_RULE
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_investigations_diagnosis_status"), "investigations", type_="check")
    op.create_check_constraint(
        op.f("ck_investigations_diagnosis_status"), "investigations", PREVIOUS_RULE
    )
