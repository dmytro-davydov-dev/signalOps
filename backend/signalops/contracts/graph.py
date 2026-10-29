"""Temporal knowledge graph enumerations shared by contracts and persistence CHECK constraints.

Adding a member requires a migration that widens the matching CHECK constraint.
"""

from enum import StrEnum


class GraphNodeType(StrEnum):
    SERVICE = "service"
    DEPLOYMENT = "deployment"
    INCIDENT = "incident"
    # Not a database entity; identified by its external key alone.
    RUNBOOK = "runbook"


class GraphRelation(StrEnum):
    DEPENDS_ON = "DEPENDS_ON"
    DEPLOYED_TO = "DEPLOYED_TO"
    # A correlation recorded by detection; not a causal claim.
    AFFECTS = "AFFECTS"
    DOCUMENTED_BY = "DOCUMENTED_BY"
