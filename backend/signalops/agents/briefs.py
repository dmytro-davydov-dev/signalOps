"""The incident brief both model-backed workflows start from."""

import uuid
from datetime import datetime
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
from signalops.persistence.database import Database
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.incidents import IncidentRepository
from signalops.persistence.repositories.scope import EvidenceScope


async def incident_brief(
    database: Database, scope: EvidenceScope, incident_id: uuid.UUID, cutoff: datetime
) -> IncidentBrief | None:
    """Fields fixed at detection, the workspace's services, and evidence known by the cutoff."""
    async with database.transaction() as session:
        incidents = IncidentRepository(session)
        incident = await incidents.get(scope, incident_id)
        if incident is None:
            return None
        services = await CatalogRepository(session).list_services(
            scope.workspace_id, limit=MAX_BRIEF_SERVICES
        )
        cited = await incidents.evidence_known_by(
            scope, incident_id, known_by=cutoff, limit=MAX_BRIEF_EVIDENCE
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


async def load_incident(context: NodeContext) -> IncidentBrief:
    run = context.run
    brief = await incident_brief(
        context.database, run.scope, run.incident_id, run.evidence_cutoff_at
    )
    if brief is None:
        raise PermanentJobError("the incident is outside the investigation's scope")
    return brief


LOAD_INCIDENT: Final = Node("load_context", AgentRole.COORDINATOR, IncidentBrief, load_incident)
