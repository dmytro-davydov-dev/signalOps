"""Read one investigation's path from ingestion through its workflow steps."""

import uuid
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.common import LIVE_REPLAY_SCOPE
from signalops.contracts.investigations import (
    MAX_TRACE_BATCHES,
    InvestigationTrace,
    TraceEvidence,
    TraceJob,
    TraceStages,
    TraceStep,
)
from signalops.contracts.workflow import InvestigationStatus
from signalops.persistence.models import (
    AgentStep,
    Incident,
    IncidentSignal,
    Investigation,
    Job,
    LlmCall,
    Signal,
    ToolCall,
)
from signalops.persistence.repositories.scope import EvidenceScope


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds(), 3)


class TraceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def investigation(
        self, scope: EvidenceScope, investigation_id: uuid.UUID
    ) -> InvestigationTrace | None:
        row = (
            await self._session.execute(
                select(Investigation, Incident)
                .join(Incident, Incident.id == Investigation.incident_id)
                .where(scope.clause(Investigation), Investigation.id == investigation_id)
            )
        ).one_or_none()
        if row is None:
            return None
        investigation, incident = row
        evidence = await self._evidence(incident.id)
        job = await self._job(investigation.id)
        steps = await self._steps(investigation.id)
        live = investigation.replay_scope == LIVE_REPLAY_SCOPE
        started = min((s.started_at for s in steps if s.started_at), default=None)
        return InvestigationTrace(
            investigation_id=investigation.id,
            incident_id=incident.id,
            replay_scope=investigation.replay_scope,
            status=InvestigationStatus(investigation.status),
            workflow_version=investigation.workflow_version,
            incident_detected_at=incident.detected_at,
            created_at=investigation.created_at,
            completed_at=investigation.completed_at,
            evidence=evidence,
            job=job,
            steps=tuple(steps),
            stages=TraceStages(
                ingestion_to_detection_s=(
                    _seconds(evidence.first_ingested_at, incident.detected_at) if live else None
                ),
                detection_to_request_s=(
                    _seconds(incident.detected_at, investigation.created_at) if live else None
                ),
                request_to_first_step_s=_seconds(investigation.created_at, started),
                running_s=_seconds(started, investigation.completed_at),
                model_s=round(sum(s.model_ms for s in steps) / 1000, 3),
                tool_s=round(sum(s.tool_ms for s in steps) / 1000, 3),
            ),
            log_fields={
                "correlation_id": evidence.ingestion_batches,
                "incident_id": (str(incident.id),),
                "investigation_id": (str(investigation.id),),
                "job_id": (str(job.job_id),) if job is not None else (),
            },
        )

    async def _evidence(self, incident_id: uuid.UUID) -> TraceEvidence:
        cited = select(IncidentSignal.signal_id).where(IncidentSignal.incident_id == incident_id)
        count, first_occurred, first_ingested, last_ingested = (
            await self._session.execute(
                select(
                    func.count(),
                    func.min(Signal.occurred_at),
                    func.min(Signal.ingested_at),
                    func.max(Signal.ingested_at),
                ).where(Signal.id.in_(cited))
            )
        ).one()
        batch = Signal.attributes["provenance"]["correlation_id"].astext
        batches = await self._session.scalars(
            select(batch)
            .where(Signal.id.in_(cited), batch.is_not(None))
            .group_by(batch)
            .order_by(func.min(Signal.ingested_at))
            .limit(MAX_TRACE_BATCHES)
        )
        return TraceEvidence(
            signals=count,
            first_occurred_at=first_occurred,
            first_ingested_at=first_ingested,
            last_ingested_at=last_ingested,
            ingestion_batches=tuple(batches),
        )

    async def _job(self, investigation_id: uuid.UUID) -> TraceJob | None:
        job = await self._session.scalar(
            select(Job).where(Job.investigation_id == investigation_id)
        )
        if job is None:
            return None
        category = (job.last_error or {}).get("category")
        return TraceJob(
            job_id=job.id,
            status=job.status,
            attempts=job.attempts,
            lease_generation=job.lease_generation,
            last_error_category=str(category) if category is not None else None,
        )

    async def _steps(self, investigation_id: uuid.UUID) -> list[TraceStep]:
        mine = select(AgentStep.id).where(AgentStep.investigation_id == investigation_id)
        model = (
            select(
                LlmCall.agent_step_id,
                func.count().label("calls"),
                func.coalesce(func.sum(LlmCall.latency_ms), 0).label("ms"),
            )
            .where(LlmCall.agent_step_id.in_(mine))
            .group_by(LlmCall.agent_step_id)
            .subquery()
        )
        tools = (
            select(
                ToolCall.agent_step_id,
                func.count().label("calls"),
                func.coalesce(func.sum(ToolCall.duration_ms), 0).label("ms"),
            )
            .where(ToolCall.agent_step_id.in_(mine))
            .group_by(ToolCall.agent_step_id)
            .subquery()
        )
        rows = await self._session.execute(
            select(
                AgentStep,
                func.coalesce(model.c.calls, 0),
                func.coalesce(model.c.ms, 0),
                func.coalesce(tools.c.calls, 0),
                func.coalesce(tools.c.ms, 0),
            )
            .outerjoin(model, model.c.agent_step_id == AgentStep.id)
            .outerjoin(tools, tools.c.agent_step_id == AgentStep.id)
            .where(AgentStep.investigation_id == investigation_id)
            .order_by(AgentStep.created_at, AgentStep.step_key)
        )
        return [
            TraceStep(
                step_key=step.step_key,
                agent_role=step.agent_role,
                status=step.status,
                attempt=step.attempt,
                lease_generation=step.lease_generation,
                started_at=step.started_at,
                completed_at=step.completed_at,
                model_calls=model_calls,
                model_ms=int(model_ms),
                tool_calls=tool_calls,
                tool_ms=int(tool_ms),
            )
            for step, model_calls, model_ms, tool_calls, tool_ms in rows
        ]
