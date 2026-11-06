"""Keep one relationship's edges from overlapping in time.

The graph projector coalesces repeated observations into one edge per
validity interval. `ex_graph_edges_validity` makes that a database guarantee:
two rows for the same endpoints and relation cannot cover the same instant, so
an as-of traversal finds at most one. `btree_gist` provides the equality
operators a GiST constraint needs for UUID and text columns.

The extension stays on downgrade: another object may have come to use it.

Revision ID: 0012_graph_projection
Revises: 0011_incident_correlation
Create Date: 2026-11-06
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0012_graph_projection"
down_revision: str | None = "0011_incident_correlation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.execute(
        """
        ALTER TABLE graph_edges ADD CONSTRAINT ex_graph_edges_validity
        EXCLUDE USING gist (
            workspace_id WITH =,
            replay_scope WITH =,
            from_node_id WITH =,
            to_node_id WITH =,
            relation WITH =,
            tstzrange(valid_from, valid_to) WITH &&
        )
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE graph_edges DROP CONSTRAINT ex_graph_edges_validity")
