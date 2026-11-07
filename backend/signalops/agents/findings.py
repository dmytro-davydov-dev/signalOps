"""Code's check of what an agent submits, against PostgreSQL rather than any cache.

Validation has already fixed the shape, the confidence bounds, and at least one
citation for a diagnosed result. This checks facts the contract cannot: the named
service exists in the workspace, and every cited signal exists in the investigation's
scope and was known by its cutoff. The returned message names counts and fields only,
and goes back to the model.
"""

from signalops.agents.read_cache import ToolScope
from signalops.contracts.steps import ProposedDiagnosis
from signalops.persistence.database import Database
from signalops.persistence.repositories.evidence import EvidenceRepository


async def verify_findings(
    database: Database, scope: ToolScope, proposal: ProposedDiagnosis
) -> str | None:
    """Why the proposal cannot stand, or None when it can."""
    cause = proposal.root_cause
    if cause is None:
        return None
    async with database.transaction() as session:
        evidence = EvidenceRepository(session)
        if not await evidence.has_service(scope.evidence.workspace_id, cause.service_id):
            return "root_cause.service_id is not a service in this workspace"
        unverified = await evidence.missing_or_future(
            scope.evidence, cause.evidence_signal_ids, known_by=scope.cutoff
        )
    if unverified:
        return (
            f"{len(unverified)} of evidence_signal_ids are not signals known by the cutoff. "
            "Cite only IDs read from tool results."
        )
    return None
