"""Evaluation scenarios, runs, replays, and cases (schema `evaluation`).

Only a session on the evaluator's connection can use this repository: the runtime
role has no privilege on the schema, and PostgreSQL refuses its queries.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.evaluation import (
    AuditRecord,
    CaseStatus,
    ReplayStatus,
    RunStatus,
    ScenarioDefinition,
    ScenarioSplit,
    SystemVariant,
)
from signalops.persistence.models import (
    EvaluationAudit,
    EvaluationCase,
    EvaluationReplay,
    EvaluationRun,
    EvaluationScenario,
)


class ScenarioConflictError(Exception):
    """A stored scenario version differs from its definition; versions are immutable."""


@dataclass(frozen=True)
class NewRun:
    workspace_id: uuid.UUID
    label: str
    split: ScenarioSplit
    variants: Sequence[SystemVariant]
    trials: int
    seed: int
    dataset_hash: str
    workflow_versions: dict[str, Any]
    prompt_versions: dict[str, Any]
    model_config: dict[str, Any]
    pricing_config: dict[str, Any]
    limits: dict[str, Any]


class EvaluationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def register_scenario(self, definition: ScenarioDefinition) -> EvaluationScenario:
        """Store a scenario version, or return the stored one when it is identical."""
        values = definition.model_dump(mode="json")
        await self._session.execute(
            insert(EvaluationScenario)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["name", "version"])
        )
        stored = await self._session.scalar(
            select(EvaluationScenario).where(
                EvaluationScenario.name == definition.name,
                EvaluationScenario.version == definition.version,
            )
        )
        if stored is None:
            raise ScenarioConflictError(f"{definition.name}@{definition.version} vanished")
        changed = [field for field, value in values.items() if getattr(stored, field) != value]
        if changed:
            raise ScenarioConflictError(
                f"{definition.name}@{definition.version} is stored with different "
                f"{', '.join(sorted(changed))}; register a new version instead"
            )
        return stored

    async def scenarios(self, split: ScenarioSplit | None = None) -> list[EvaluationScenario]:
        statement = select(EvaluationScenario)
        if split is not None:
            statement = statement.where(EvaluationScenario.split == split)
        result = await self._session.scalars(
            statement.order_by(EvaluationScenario.name, EvaluationScenario.version)
        )
        return list(result)

    async def create_run(self, new: NewRun) -> EvaluationRun:
        run = EvaluationRun(
            workspace_id=new.workspace_id,
            label=new.label,
            split=new.split,
            status=RunStatus.QUEUED,
            variants=[variant.value for variant in new.variants],
            trials=new.trials,
            seed=new.seed,
            dataset_hash=new.dataset_hash,
            workflow_versions=new.workflow_versions,
            prompt_versions=new.prompt_versions,
            model_config=new.model_config,
            pricing_config=new.pricing_config,
            limits=new.limits,
        )
        self._session.add(run)
        await self._session.flush()
        return run

    async def run(
        self, run_id: uuid.UUID, workspace_id: uuid.UUID | None = None
    ) -> EvaluationRun | None:
        statement = select(EvaluationRun).where(EvaluationRun.id == run_id)
        if workspace_id is not None:
            statement = statement.where(EvaluationRun.workspace_id == workspace_id)
        result: EvaluationRun | None = await self._session.scalar(statement)
        return result

    async def runs(self, workspace_id: uuid.UUID, *, limit: int) -> list[EvaluationRun]:
        result = await self._session.scalars(
            select(EvaluationRun)
            .where(EvaluationRun.workspace_id == workspace_id)
            .order_by(EvaluationRun.created_at.desc(), EvaluationRun.id.desc())
            .limit(limit)
        )
        return list(result)

    async def claim_run(
        self, owner: str, *, now: datetime, lease: timedelta
    ) -> EvaluationRun | None:
        """Take the oldest queued run, or a running one whose runner stopped renewing."""
        candidate = await self._session.scalar(
            select(EvaluationRun)
            .where(
                or_(
                    EvaluationRun.status == RunStatus.QUEUED,
                    (EvaluationRun.status == RunStatus.RUNNING)
                    & (EvaluationRun.lease_expires_at < now),
                )
            )
            .order_by(EvaluationRun.created_at, EvaluationRun.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if candidate is None:
            return None
        candidate.status = RunStatus.RUNNING
        candidate.lease_owner = owner
        candidate.lease_expires_at = now + lease
        candidate.started_at = candidate.started_at or now
        await self._session.flush()
        return candidate

    async def renew_lease(
        self, run_id: uuid.UUID, owner: str, *, now: datetime, lease: timedelta
    ) -> bool:
        """Extend a held lease; False when another runner took the run."""
        result = await self._session.execute(
            update(EvaluationRun)
            .where(
                EvaluationRun.id == run_id,
                EvaluationRun.lease_owner == owner,
                EvaluationRun.status == RunStatus.RUNNING,
            )
            .values(lease_expires_at=now + lease)
            .returning(EvaluationRun.id)
        )
        return result.scalar() is not None

    async def finish_run(
        self,
        run_id: uuid.UUID,
        owner: str,
        *,
        status: RunStatus,
        now: datetime,
        metrics: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> bool:
        result = await self._session.execute(
            update(EvaluationRun)
            .where(
                EvaluationRun.id == run_id,
                EvaluationRun.lease_owner == owner,
                EvaluationRun.status == RunStatus.RUNNING,
            )
            .values(
                status=status,
                completed_at=now,
                metrics=metrics,
                error=error,
                lease_owner=None,
                lease_expires_at=None,
            )
            .returning(EvaluationRun.id)
        )
        return result.scalar() is not None

    async def ensure_replay(self, run_id: uuid.UUID, scenario_id: uuid.UUID) -> EvaluationReplay:
        """The run's replay of a scenario, created once with its own replay scope."""
        replay_id = uuid.uuid4()
        await self._session.execute(
            insert(EvaluationReplay)
            .values(
                id=replay_id,
                run_id=run_id,
                scenario_id=scenario_id,
                replay_scope=f"replay:{replay_id}",
            )
            .on_conflict_do_nothing(index_elements=["run_id", "scenario_id"])
        )
        replay = await self._session.scalar(
            select(EvaluationReplay).where(
                EvaluationReplay.run_id == run_id, EvaluationReplay.scenario_id == scenario_id
            )
        )
        if replay is None:
            raise LookupError("replay vanished")
        return replay

    async def finish_replay(
        self, replay_id: uuid.UUID, *, status: ReplayStatus, report: dict[str, Any], now: datetime
    ) -> None:
        await self._session.execute(
            update(EvaluationReplay)
            .where(EvaluationReplay.id == replay_id)
            .values(status=status, report=report, completed_at=now)
        )

    async def ensure_case(
        self,
        replay: EvaluationReplay,
        *,
        case_key: str,
        variant: SystemVariant,
        trial: int,
        expected_outcome: dict[str, Any],
    ) -> EvaluationCase:
        await self._session.execute(
            insert(EvaluationCase)
            .values(
                run_id=replay.run_id,
                replay_id=replay.id,
                scenario_id=replay.scenario_id,
                case_key=case_key,
                variant=variant,
                trial=trial,
                expected_outcome=expected_outcome,
            )
            .on_conflict_do_nothing(index_elements=["run_id", "case_key"])
        )
        case = await self._session.scalar(
            select(EvaluationCase).where(
                EvaluationCase.run_id == replay.run_id, EvaluationCase.case_key == case_key
            )
        )
        if case is None:
            raise LookupError("case vanished")
        return case

    async def start_case(self, case_id: uuid.UUID, investigation_id: uuid.UUID | None) -> None:
        await self._session.execute(
            update(EvaluationCase)
            .where(EvaluationCase.id == case_id, EvaluationCase.status == CaseStatus.PENDING)
            .values(status=CaseStatus.INVESTIGATING, investigation_id=investigation_id)
        )

    async def finish_case(
        self,
        case_id: uuid.UUID,
        *,
        status: CaseStatus,
        observed: dict[str, Any] | None,
        scores: dict[str, Any] | None,
        now: datetime,
        error: str | None = None,
    ) -> None:
        await self._session.execute(
            update(EvaluationCase)
            .where(EvaluationCase.id == case_id)
            .values(
                status=status,
                observed_outcome=observed,
                scores=scores,
                error=error,
                completed_at=now,
            )
        )

    async def cases(self, run_id: uuid.UUID) -> list[EvaluationCase]:
        result = await self._session.scalars(
            select(EvaluationCase)
            .where(EvaluationCase.run_id == run_id)
            .order_by(EvaluationCase.case_key)
        )
        return list(result)

    async def case(self, run_id: uuid.UUID, case_id: uuid.UUID) -> EvaluationCase | None:
        result: EvaluationCase | None = await self._session.scalar(
            select(EvaluationCase).where(
                EvaluationCase.run_id == run_id, EvaluationCase.id == case_id
            )
        )
        return result

    async def replays(self, run_id: uuid.UUID) -> list[EvaluationReplay]:
        result = await self._session.scalars(
            select(EvaluationReplay).where(EvaluationReplay.run_id == run_id)
        )
        return list(result)

    async def scenario(self, scenario_id: uuid.UUID) -> EvaluationScenario | None:
        return await self._session.get(EvaluationScenario, scenario_id)

    async def case_counts(self, run_id: uuid.UUID) -> dict[str, int]:
        rows = await self._session.execute(
            select(EvaluationCase.status, func.count())
            .where(EvaluationCase.run_id == run_id)
            .group_by(EvaluationCase.status)
        )
        return {str(status): int(count) for status, count in rows.tuples()}

    async def record_detection(self, replay_id: uuid.UUID, detection: dict[str, Any]) -> None:
        """Add a replay's detection score to its report."""
        replay = await self._session.get_one(EvaluationReplay, replay_id)
        replay.report = {**(replay.report or {}), "detection": detection}
        await self._session.flush()

    async def record_audit(self, case_id: uuid.UUID, record: AuditRecord) -> EvaluationAudit:
        """Store an auditor's judgement of a citation; a later one from them replaces it."""
        values = record.model_dump(mode="json")
        statement = insert(EvaluationAudit).values(case_id=case_id, **values)
        await self._session.execute(
            statement.on_conflict_do_update(
                index_elements=["case_id", "signal_id", "auditor"],
                set_={"supported": statement.excluded.supported, "note": statement.excluded.note},
            )
        )
        stored = await self._session.scalar(
            select(EvaluationAudit).where(
                EvaluationAudit.case_id == case_id,
                EvaluationAudit.signal_id == record.signal_id,
                EvaluationAudit.auditor == record.auditor,
            )
        )
        if stored is None:
            raise LookupError("audit vanished")
        return stored

    async def audits(self, run_id: uuid.UUID) -> list[EvaluationAudit]:
        result = await self._session.scalars(
            select(EvaluationAudit)
            .join(EvaluationCase, EvaluationCase.id == EvaluationAudit.case_id)
            .where(EvaluationCase.run_id == run_id)
            .order_by(EvaluationAudit.created_at)
        )
        return list(result)
