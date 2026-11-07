"""Queued investigations for tests that claim jobs."""

import uuid
from datetime import UTC, datetime, timedelta

from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.workflow import IncidentSeverity
from signalops.detection.rules import Window
from signalops.persistence import database as db
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID

LIVE = EvidenceScope(DEMO_WORKSPACE_ID)
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
ABSTENTION = Diagnosis(
    status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
    summary="No diagnosis",
    root_cause=None,
    missing_evidence=("Nothing found",),
    workflow_version="v1",
)


async def add_job(
    database: db.Database, *, workflow_version: str = "v1", max_steps: int = 5
) -> uuid.UUID:
    """A queued investigation with its job; returns the investigation."""
    now = datetime.now(UTC)
    async with database.transaction() as session:
        incident_id = await IncidentRepository(session).create(
            NewIncident(
                scope=LIVE,
                fingerprint=f"env:{uuid.uuid4().hex[:8]}",
                title="High error rate",
                severity=IncidentSeverity.HIGH,
                window=Window(T0, T0 + timedelta(minutes=1)),
                detected_at=T0 + timedelta(minutes=1),
            )
        )
        repository = InvestigationRepository(session)
        investigation = await repository.create(
            NewInvestigation(
                scope=LIVE,
                incident_id=incident_id,
                request_key=f"test:{uuid.uuid4()}",
                workflow_version=workflow_version,
                budget_microusd=1_000,
                max_steps=max_steps,
                created_at=now,
                deadline_at=now + timedelta(minutes=10),
            )
        )
        assert investigation is not None
        assert await repository.enqueue(investigation.id)
        return investigation.id
