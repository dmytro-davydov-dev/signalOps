"""Queue and execute evaluation runs (P10-T04).

**Equal constraints.** Every variant investigates the same incident in the same
replay scope, with the same evidence cutoff (the incident's event-time detection),
the same tools and their bounds, and the same budget, step limit, and deadline,
all recorded on the run. A variant cannot win by spending more than another was
allowed. The deterministic variant runs once per scenario, because repeating it
changes nothing; the agent variants run `trials` times each.

**Durability.** A run holds a lease that the runner renews. Replays derive their
IDs from the replay scope and investigations from the case key, so a runner that
takes over an expired run repeats nothing already done: finished replays and
cases are skipped, and an investigation already requested is waited for, not
requested again.

**Connections.** The runner uses the evaluator's connection, the only one that
can read ground truth. Investigations run in the ordinary workers, which cannot.
"""

import asyncio
import logging
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Self

from pydantic import Field, model_validator
from sqlalchemy import select

from signalops.adapters.ingestion import IngestPolicy, IngestSource
from signalops.agents.deterministic import WORKFLOW_D1
from signalops.agents.investigator import PROMPT_VERSION as INVESTIGATOR_PROMPT
from signalops.agents.multi_agent import (
    DEPENDENCY,
    DEPLOYMENT,
    SYNTHESIZER_PROMPT_VERSION,
    TELEMETRY,
    VERIFIER_PROMPT_VERSION,
    WORKFLOW_V3,
)
from signalops.config import Settings
from signalops.contracts.common import ContractModel, require_unique
from signalops.contracts.diagnosis import Diagnosis
from signalops.contracts.evaluation import (
    TERMINAL_CASE_STATUSES,
    CaseStatus,
    ExpectedOutcome,
    ObservedOutcome,
    ReplayStatus,
    RunStatus,
    ScenarioSplit,
    SystemVariant,
)
from signalops.contracts.workflow import TERMINAL_INVESTIGATION_STATUSES
from signalops.detection.detector import DetectionPolicy
from signalops.evaluation.catalog import ENTRIES, CatalogEntry, definition_of, split_hash
from signalops.evaluation.metrics import summarize
from signalops.evaluation.replay import (
    DEFAULT_TIMING,
    ReplayReport,
    ReplayTiming,
    ReplayTransport,
    replay,
    seed_runbooks,
)
from signalops.evaluation.scoring import score_case
from signalops.gateway.pricing import PRICES, PRICING_VERSION
from signalops.persistence.database import Database
from signalops.persistence.models import (
    EvaluationCase,
    EvaluationReplay,
    EvaluationRun,
    EvaluationScenario,
    Incident,
    Investigation,
    Signal,
)
from signalops.persistence.repositories.evaluation import EvaluationRepository, NewRun
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.llm_calls import LlmCallRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_RUNBOOKS, DEMO_SERVICES

logger = logging.getLogger(__name__)

VARIANT_WORKFLOWS: Final = {
    SystemVariant.DETERMINISTIC: WORKFLOW_D1,
    SystemVariant.SINGLE_AGENT: "v2",
    SystemVariant.MULTI_AGENT: WORKFLOW_V3,
}
VARIANT_PROMPTS: Final = {
    SystemVariant.DETERMINISTIC: [],
    SystemVariant.SINGLE_AGENT: [INVESTIGATOR_PROMPT],
    SystemVariant.MULTI_AGENT: [
        TELEMETRY.prompt_version,
        DEPLOYMENT.prompt_version,
        DEPENDENCY.prompt_version,
        SYNTHESIZER_PROMPT_VERSION,
        VERIFIER_PROMPT_VERSION,
    ],
}
CATALOG: Final = {(entry.name, entry.version): entry for entry in ENTRIES}


class RunRequest(ContractModel):
    label: str = Field(min_length=1, max_length=256)
    split: ScenarioSplit = ScenarioSplit.HELD_OUT
    variants: tuple[SystemVariant, ...] = Field(
        default=tuple(SystemVariant), min_length=1, max_length=3
    )
    trials: int = Field(default=3, ge=1, le=20)
    seed: int = Field(default=0, ge=0, le=2**31 - 1)
    # Equal for every variant and case. Defaults to the investigation settings.
    budget_microusd: int | None = Field(default=None, ge=1, le=1_000_000_000)
    max_steps: int | None = Field(default=None, ge=10, le=200)
    deadline_seconds: int | None = Field(default=None, ge=60, le=24 * 60 * 60)
    # A subset of the split's scenario names; all of them when empty.
    scenarios: tuple[str, ...] = Field(default=(), max_length=50)

    @model_validator(mode="after")
    def _check(self) -> Self:
        require_unique(self.variants, "variants")
        require_unique(self.scenarios, "scenarios")
        return self


class UnknownScenarioError(ValueError):
    """A requested scenario is not in the catalog for that split."""


def model_config_of(settings: Settings) -> dict[str, Any]:
    return {
        "provider": settings.model_provider,
        "model": settings.resolved_model(),
        "max_output_tokens": settings.model_max_output_tokens,
        "effort": settings.model_effort,
        "max_attempts": settings.model_max_attempts,
        "request_timeout_seconds": settings.model_request_timeout_seconds,
        "agent_max_tool_calls": settings.agent_max_tool_calls,
        "agent_max_repairs": settings.agent_max_repairs,
        # Fake replies follow fixed rules: results show the harness, not model quality.
        "measures_model_quality": settings.model_provider != "fake",
    }


def pricing_of(settings: Settings) -> dict[str, Any]:
    price = PRICES.get((settings.model_provider, settings.resolved_model()))
    return {
        "version": PRICING_VERSION,
        "unit": "microUSD per million tokens",
        "price": None if price is None else vars(price),
    }


def selected_entries(request: RunRequest) -> list[CatalogEntry]:
    chosen = [entry for entry in ENTRIES if entry.split is request.split]
    if request.scenarios:
        names = {entry.name for entry in chosen}
        unknown = sorted(set(request.scenarios) - names)
        if unknown:
            raise UnknownScenarioError(f"not in the {request.split} split: {', '.join(unknown)}")
        chosen = [entry for entry in chosen if entry.name in request.scenarios]
    return chosen


async def queue_run(
    database: Database, request: RunRequest, *, workspace_id: uuid.UUID, settings: Settings
) -> EvaluationRun:
    """Register the scenarios and store a queued run with everything needed to repeat it."""
    definitions = [definition_of(entry) for entry in selected_entries(request)]
    limits = {
        "budget_microusd": request.budget_microusd or settings.investigation_budget_microusd,
        "max_steps": request.max_steps or settings.investigation_max_steps,
        "deadline_seconds": request.deadline_seconds or settings.investigation_deadline_seconds,
        "scenarios": [definition.name for definition in definitions],
        "deterministic_trials": 1,
    }
    async with database.transaction() as session:
        repository = EvaluationRepository(session)
        for definition in definitions:
            await repository.register_scenario(definition)
        return await repository.create_run(
            NewRun(
                workspace_id=workspace_id,
                label=request.label,
                split=request.split,
                variants=request.variants,
                trials=request.trials,
                seed=request.seed,
                dataset_hash=split_hash(definitions),
                workflow_versions={v.value: VARIANT_WORKFLOWS[v] for v in request.variants},
                prompt_versions={v.value: VARIANT_PROMPTS[v] for v in request.variants},
                model_config=model_config_of(settings),
                pricing_config=pricing_of(settings),
                limits=limits,
            )
        )


@dataclass(frozen=True)
class RunnerContext:
    database: Database
    sources: Mapping[uuid.UUID, IngestSource]
    ingest_policy: IngestPolicy
    detection_policy: DetectionPolicy
    transport_for: Callable[[EvidenceScope], ReplayTransport]
    replay_timing: ReplayTiming = DEFAULT_TIMING
    lease: timedelta = timedelta(minutes=2)
    poll_interval: timedelta = timedelta(seconds=1)
    # Longest wait for one scenario's investigations beyond their own deadline.
    investigation_grace: timedelta = timedelta(minutes=2)


class LeaseLost(Exception):
    """Another runner took the run."""


class RunExecutor:
    def __init__(self, context: RunnerContext, owner: str) -> None:
        self._context = context
        self._owner = owner

    @property
    def _db(self) -> Database:
        return self._context.database

    async def claim_and_execute(self) -> uuid.UUID | None:
        """Execute the next claimable run; None when there is none."""
        async with self._db.transaction() as session:
            run = await EvaluationRepository(session).claim_run(
                self._owner, now=datetime.now(UTC), lease=self._context.lease
            )
        if run is None:
            return None
        await self.execute(run.id)
        return run.id

    async def renew(self, run_id: uuid.UUID) -> None:
        async with self._db.transaction() as session:
            held = await EvaluationRepository(session).renew_lease(
                run_id, self._owner, now=datetime.now(UTC), lease=self._context.lease
            )
        if not held:
            raise LeaseLost(str(run_id))

    async def execute(self, run_id: uuid.UUID) -> RunStatus:
        """Execute a run this runner holds; end it completed, or failed with the reason."""
        try:
            await self._execute(run_id)
        except LeaseLost:
            logger.warning("evaluation run lease lost", extra={"run_id": str(run_id)})
            raise
        except Exception as error:
            logger.exception("evaluation run failed", extra={"run_id": str(run_id)})
            await self._finish(run_id, RunStatus.FAILED, error=f"{type(error).__name__}: {error}")
            return RunStatus.FAILED
        return RunStatus.COMPLETED

    async def _finish(
        self, run_id: uuid.UUID, status: RunStatus, *, error: str | None = None
    ) -> None:
        async with self._db.transaction() as session:
            repository = EvaluationRepository(session)
            run = await repository.run(run_id)
            cases = await repository.cases(run_id)
            replays = await repository.replays(run_id)
            metrics = summarize(run, cases, replays) if run is not None else None
            await repository.finish_run(
                run_id,
                self._owner,
                status=status,
                now=datetime.now(UTC),
                metrics=metrics,
                error=None if error is None else error[:2000],
            )

    async def _execute(self, run_id: uuid.UUID) -> None:
        async with self._db.transaction() as session:
            run = await session.get_one(EvaluationRun, run_id)
            names = list(run.limits.get("scenarios", []))
            scenarios = list(
                await session.scalars(
                    select(EvaluationScenario)
                    .where(EvaluationScenario.name.in_(names))
                    .order_by(EvaluationScenario.name)
                )
            )
        if len(scenarios) != len(names):
            raise LookupError("a run's scenario is missing from evaluation.scenarios")
        for scenario in scenarios:
            await self.renew(run_id)
            await self._scenario(run, scenario)
        await self._finish(run_id, RunStatus.COMPLETED)

    async def _scenario(self, run: EvaluationRun, scenario: EvaluationScenario) -> None:
        entry = CATALOG.get((scenario.name, scenario.version))
        if entry is None:
            raise LookupError(f"{scenario.name}@{scenario.version} is not in this catalog")
        async with self._db.transaction() as session:
            replay_row = await EvaluationRepository(session).ensure_replay(run.id, scenario.id)
        scope = EvidenceScope(run.workspace_id, replay_row.replay_scope)
        if replay_row.status == ReplayStatus.PENDING:
            dataset = entry.dataset()
            if dataset.labels.dataset_hash != scenario.dataset_hash:
                raise ValueError(f"{scenario.name} no longer regenerates to its dataset hash")
            report = await self._replay(scope, replay_row, dataset.records)
            replay_row.status = report.status
        expected = ExpectedOutcome.model_validate(scenario.expected_outcome)
        incidents = await self._incidents(scope)
        cases: list[EvaluationCase] = []
        for variant in (SystemVariant(value) for value in run.variants):
            trials = 1 if variant is SystemVariant.DETERMINISTIC else run.trials
            for trial in range(1, trials + 1):
                async with self._db.transaction() as session:
                    case = await EvaluationRepository(session).ensure_case(
                        replay_row,
                        case_key=f"{scenario.name}@{scenario.version}/{variant}/{trial}",
                        variant=variant,
                        trial=trial,
                        expected_outcome=expected.summary().model_dump(mode="json"),
                    )
                if case.status not in TERMINAL_CASE_STATUSES:
                    cases.append(case)
        if replay_row.status != ReplayStatus.COMPLETED:
            for case in cases:
                await self._fail(case, "the replay did not drain before its timeout")
            return
        waiting = []
        for case in cases:
            if not incidents:
                observed = ObservedOutcome()
                await self._score(case, expected, observed)
                continue
            investigation_id = await self._request(run, case, scope, incidents[0])
            waiting.append((case, investigation_id))
        await self._await(run, scope, expected, incidents, waiting)

    async def _replay(
        self, scope: EvidenceScope, row: EvaluationReplay, records: Sequence[Any]
    ) -> ReplayReport:
        services = {service.name: service.id for service in DEMO_SERVICES}
        await seed_runbooks(
            self._db,
            scope,
            [(runbook, [services[name] for name in names]) for runbook, names in DEMO_RUNBOOKS],
        )
        transport = self._context.transport_for(scope)
        try:
            report = await replay(
                self._db,
                records,
                replay_run_id=row.id,
                workspace_id=scope.workspace_id,
                sources=self._context.sources,
                transport=transport,
                ingest_policy=self._context.ingest_policy,
                detection_policy=self._context.detection_policy,
                timing=self._context.replay_timing,
            )
        finally:
            transport.close()
        async with self._db.transaction() as session:
            await EvaluationRepository(session).finish_replay(
                row.id,
                status=report.status,
                report=report.model_dump(mode="json"),
                now=datetime.now(UTC),
            )
        return report

    async def _incidents(self, scope: EvidenceScope) -> list[Incident]:
        async with self._db.transaction() as session:
            rows = await session.scalars(
                select(Incident)
                .where(scope.clause(Incident))
                .order_by(Incident.detected_at, Incident.id)
            )
            return list(rows)

    async def _request(
        self, run: EvaluationRun, case: EvaluationCase, scope: EvidenceScope, incident: Incident
    ) -> uuid.UUID:
        request_key = f"eval:{run.id}:{case.case_key}"
        now = datetime.now(UTC)
        async with self._db.transaction() as session:
            repository = InvestigationRepository(session)
            created = await repository.create(
                NewInvestigation(
                    scope=scope,
                    incident_id=incident.id,
                    request_key=request_key,
                    workflow_version=VARIANT_WORKFLOWS[SystemVariant(case.variant)],
                    budget_microusd=int(run.limits["budget_microusd"]),
                    max_steps=int(run.limits["max_steps"]),
                    created_at=now,
                    deadline_at=now + timedelta(seconds=int(run.limits["deadline_seconds"])),
                    evidence_cutoff_at=incident.detected_at,
                )
            )
            if created is not None:
                await repository.enqueue(created.id)
                investigation_id = created.id
            else:
                investigation_id = (
                    await session.execute(
                        select(Investigation.id).where(Investigation.request_key == request_key)
                    )
                ).scalar_one()
            await EvaluationRepository(session).start_case(case.id, investigation_id)
        return investigation_id

    async def _await(
        self,
        run: EvaluationRun,
        scope: EvidenceScope,
        expected: ExpectedOutcome,
        incidents: list[Incident],
        waiting: list[tuple[EvaluationCase, uuid.UUID]],
    ) -> None:
        limit = timedelta(seconds=int(run.limits["deadline_seconds"]))
        deadline = time.monotonic() + (limit + self._context.investigation_grace).total_seconds()
        pending = dict(waiting)
        while pending:
            await self.renew(run.id)
            for case, investigation_id in list(pending.items()):
                observed = await observe(self._db, scope, incidents, investigation_id)
                if observed.investigation_status in TERMINAL_INVESTIGATION_STATUSES:
                    await self._score(case, expected, observed)
                    del pending[case]
            if not pending:
                return
            if time.monotonic() >= deadline:
                for case in pending:
                    await self._fail(case, "the investigation did not end before the wait limit")
                return
            await asyncio.sleep(self._context.poll_interval.total_seconds())

    async def _score(
        self, case: EvaluationCase, expected: ExpectedOutcome, observed: ObservedOutcome
    ) -> None:
        scores = score_case(expected, observed)
        async with self._db.transaction() as session:
            await EvaluationRepository(session).finish_case(
                case.id,
                status=CaseStatus.SCORED,
                observed=observed.model_dump(mode="json"),
                scores=scores.model_dump(mode="json"),
                now=datetime.now(UTC),
            )

    async def _fail(self, case: EvaluationCase, reason: str) -> None:
        async with self._db.transaction() as session:
            await EvaluationRepository(session).finish_case(
                case.id,
                status=CaseStatus.FAILED,
                observed=None,
                scores=None,
                now=datetime.now(UTC),
                error=reason,
            )


async def observe(
    database: Database,
    scope: EvidenceScope,
    incidents: list[Incident],
    investigation_id: uuid.UUID,
) -> ObservedOutcome:
    """What the system stored for an investigation, read from runtime tables."""
    async with database.transaction() as session:
        row = await session.get_one(Investigation, investigation_id, populate_existing=True)
        usage = await LlmCallRepository(session).usage_of(investigation_id)
        diagnosis = None if row.diagnosis is None else Diagnosis.model_validate(row.diagnosis)
        cause = diagnosis.root_cause if diagnosis is not None else None
        cited = cause.evidence_signal_ids if cause is not None else ()
        event_ids: tuple[str, ...] = ()
        if cited:
            rows = await session.execute(
                select(Signal.id, Signal.source_event_id).where(
                    scope.clause(Signal), Signal.id.in_(cited)
                )
            )
            found = {signal_id: event_id for signal_id, event_id in rows}
            event_ids = tuple(found[signal_id] for signal_id in cited if signal_id in found)
    return ObservedOutcome(
        incident_ids=tuple(incident.id for incident in incidents),
        investigation_id=investigation_id,
        investigation_status=row.status,
        diagnosis_status=None if diagnosis is None else diagnosis.status.value,
        root_cause_service=None if cause is None else cause.service_name,
        failure_type=None if cause is None else cause.failure_type,
        cited_signal_ids=cited,
        cited_event_ids=event_ids,
        detected_at=incidents[0].detected_at if incidents else None,
        started_at=row.created_at,
        completed_at=row.completed_at,
        spent_microusd=row.spent_microusd,
        unknown_reserved_microusd=usage.unknown_reserved_microusd,
        model_calls=usage.model_calls,
    )
