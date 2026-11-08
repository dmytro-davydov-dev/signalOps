"""The incident brief both model-backed workflows start from."""

from typing import Final

from signalops.agents.jobs import PermanentJobError
from signalops.agents.nodes import Node, NodeContext
from signalops.contracts.steps import (
    MAX_BRIEF_EVIDENCE,
    MAX_BRIEF_SERVICES,
    IncidentBrief,
    ServiceBrief,
)
from signalops.contracts.workflow import AgentRole
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.incidents import IncidentRepository


async def load_incident(context: NodeContext) -> IncidentBrief:
    run = context.run
    cutoff = run.evidence_cutoff_at
    async with context.database.transaction() as session:
        incidents = IncidentRepository(session)
        incident = await incidents.get(run.scope, run.incident_id)
        if incident is None:
            raise PermanentJobError("the incident is outside the investigation's scope")
        services = await CatalogRepository(session).list_services(
            run.scope.workspace_id, limit=MAX_BRIEF_SERVICES
        )
        cited = await incidents.evidence_known_by(
            run.scope, run.incident_id, known_by=cutoff, limit=MAX_BRIEF_EVIDENCE
        )
    return IncidentBrief(
        incident_id=incident.id,
        title=incident.title,
        detected_at=incident.detected_at,
        evidence_cutoff_at=cutoff,
        window_start=incident.correlation_window_start,
        services=tuple(ServiceBrief(service_id=row.id, name=row.name) for row in services),
        evidence_signal_ids=tuple(cited),
    )


LOAD_INCIDENT: Final = Node("load_context", AgentRole.COORDINATOR, IncidentBrief, load_incident)
