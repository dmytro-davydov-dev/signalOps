"""Evaluation runs: start, list, read, drill into a case, and record audits (P10-T06).

These routes use the evaluator's connection, the only one that can read ground
truth; without `SIGNALOPS_EVALUATION_DATABASE_URL` they answer 503. Starting a
run only queues it: `signalops evaluation-worker` replays and investigates, never
the request handler. A run belongs to the operator token's workspace.
"""

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Path, Query, Request, status
from sqlalchemy import select

from signalops.api.auth import OperatorWorkspace
from signalops.config import Settings
from signalops.contracts.evaluation import (
    AuditRecord,
    CaseScores,
    ExpectedOutcome,
    ObservedOutcome,
)
from signalops.contracts.evaluation_api import (
    AuditView,
    CitedSignal,
    EvaluationCaseDetail,
    EvaluationCaseRow,
    EvaluationRunDetail,
    EvaluationRunList,
    EvaluationRunSummary,
)
from signalops.dependencies import Resources
from signalops.evaluation.runner import RunRequest, UnknownScenarioError, queue_run
from signalops.persistence.database import Database
from signalops.persistence.models import (
    EvaluationAudit,
    EvaluationCase,
    EvaluationReplay,
    EvaluationRun,
    EvaluationScenario,
    Service,
    Signal,
)
from signalops.persistence.repositories.evaluation import EvaluationRepository

router = APIRouter(prefix="/v1", tags=["evaluation"])

MAX_RUNS = 100
MESSAGE_EXCERPT = 500
NOT_FOUND: dict[int | str, dict[str, Any]] = {404: {"description": "No such run or case"}}


def evaluation_database(request: Request) -> Database:
    resources: Resources = request.app.state.resources
    if resources.evaluation is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "evaluation is not configured on this API"
        )
    return resources.evaluation


def _summary(run: EvaluationRun, counts: dict[str, int]) -> EvaluationRunSummary:
    return EvaluationRunSummary(
        id=run.id,
        label=run.label,
        split=run.split,  # type: ignore[arg-type]
        status=run.status,  # type: ignore[arg-type]
        variants=tuple(run.variants),  # type: ignore[arg-type]
        trials=run.trials,
        dataset_hash=run.dataset_hash,
        created_at=run.created_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
        error=run.error,
        case_counts=counts,
    )


def _row(
    case: EvaluationCase, scenario: EvaluationScenario, replay: EvaluationReplay
) -> dict[str, Any]:
    return {
        "id": case.id,
        "case_key": case.case_key,
        "scenario_name": scenario.name,
        "scenario_version": scenario.version,
        "family": scenario.family,
        "split": scenario.split,
        "synthetic": scenario.synthetic,
        "variant": case.variant,
        "trial": case.trial,
        "status": case.status,
        "replay_scope": replay.replay_scope,
        "investigation_id": case.investigation_id,
        "expected": ExpectedOutcome.model_validate(case.expected_outcome),
        "observed": (
            None
            if case.observed_outcome is None
            else ObservedOutcome.model_validate(case.observed_outcome)
        ),
        "scores": None if case.scores is None else CaseScores.model_validate(case.scores),
        "error": case.error,
    }


async def _owned_run(
    database: Database, run_id: uuid.UUID, workspace_id: uuid.UUID
) -> EvaluationRun:
    async with database.transaction() as session:
        run = await EvaluationRepository(session).run(run_id, workspace_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "evaluation run not found")
    return run


@router.post("/evaluation-runs", status_code=status.HTTP_202_ACCEPTED)
async def start_run(
    request: Request, workspace_id: OperatorWorkspace, body: RunRequest
) -> EvaluationRunSummary:
    """Queue a run of a scenario split with each variant; a worker executes it."""
    database = evaluation_database(request)
    settings: Settings = request.app.state.settings
    try:
        run = await queue_run(database, body, workspace_id=workspace_id, settings=settings)
    except UnknownScenarioError as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)) from error
    return _summary(run, {})


@router.get("/evaluation-runs")
async def list_runs(
    request: Request,
    workspace_id: OperatorWorkspace,
    limit: Annotated[int, Query(ge=1, le=MAX_RUNS)] = 20,
) -> EvaluationRunList:
    database = evaluation_database(request)
    async with database.transaction() as session:
        repository = EvaluationRepository(session)
        runs = await repository.runs(workspace_id, limit=limit)
        items = [_summary(run, await repository.case_counts(run.id)) for run in runs]
    return EvaluationRunList(items=tuple(items))


@router.get("/evaluation-runs/{run_id}", responses=NOT_FOUND)
async def get_run(
    request: Request, workspace_id: OperatorWorkspace, run_id: Annotated[uuid.UUID, Path()]
) -> EvaluationRunDetail:
    """A run's configuration, aggregate metrics, and every case's outcome."""
    database = evaluation_database(request)
    run = await _owned_run(database, run_id, workspace_id)
    async with database.transaction() as session:
        repository = EvaluationRepository(session)
        rows = (
            await session.execute(
                select(EvaluationCase, EvaluationScenario, EvaluationReplay)
                .join(EvaluationScenario, EvaluationScenario.id == EvaluationCase.scenario_id)
                .join(EvaluationReplay, EvaluationReplay.id == EvaluationCase.replay_id)
                .where(EvaluationCase.run_id == run.id)
                .order_by(EvaluationCase.case_key)
            )
        ).all()
        counts = await repository.case_counts(run.id)
    summary = _summary(run, counts)
    return EvaluationRunDetail(
        **summary.model_dump(),
        seed=run.seed,
        workflow_versions=run.workflow_versions,
        prompt_versions=run.prompt_versions,
        model_settings=run.model_config,
        pricing_config=run.pricing_config,
        limits=run.limits,
        metrics=run.metrics,
        cases=tuple(
            EvaluationCaseRow(**_row(case, scenario, replay)) for case, scenario, replay in rows
        ),
    )


async def _case_detail(
    database: Database, run: EvaluationRun, case_id: uuid.UUID
) -> EvaluationCaseDetail:
    async with database.transaction() as session:
        found = (
            await session.execute(
                select(EvaluationCase, EvaluationScenario, EvaluationReplay)
                .join(EvaluationScenario, EvaluationScenario.id == EvaluationCase.scenario_id)
                .join(EvaluationReplay, EvaluationReplay.id == EvaluationCase.replay_id)
                .where(EvaluationCase.run_id == run.id, EvaluationCase.id == case_id)
            )
        ).first()
        if found is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "evaluation case not found")
        case, scenario, replay = found
        row = _row(case, scenario, replay)
        observed: ObservedOutcome | None = row["observed"]
        cited = observed.cited_signal_ids if observed is not None else ()
        supporting = set(scenario.expected_outcome.get("supporting_event_ids", []))
        signals = {
            signal.id: (signal, name)
            for signal, name in (
                await session.execute(
                    select(Signal, Service.name)
                    .outerjoin(Service, Service.id == Signal.service_id)
                    .where(
                        Signal.workspace_id == run.workspace_id,
                        Signal.replay_scope == replay.replay_scope,
                        Signal.id.in_(cited),
                    )
                )
            ).all()
        }
        audits = list(
            await session.scalars(
                select(EvaluationAudit)
                .where(EvaluationAudit.case_id == case.id)
                .order_by(EvaluationAudit.created_at)
            )
        )
    citations = tuple(
        CitedSignal(
            signal_id=signal.id,
            source_event_id=signal.source_event_id,
            service_name=name,
            kind=signal.kind,
            severity=signal.severity,
            occurred_at=signal.occurred_at,
            message=signal.message[:MESSAGE_EXCERPT],
            fault_evidence=signal.source_event_id in supporting,
        )
        for signal_id in cited
        if signal_id in signals
        for signal, name in (signals[signal_id],)
    )
    return EvaluationCaseDetail(
        **row,
        replay_report=replay.report,
        citations=citations,
        audits=tuple(_audit_view(audit) for audit in audits),
    )


def _audit_view(audit: EvaluationAudit) -> AuditView:
    return AuditView(
        signal_id=audit.signal_id,
        supported=audit.supported,
        auditor=audit.auditor,
        note=audit.note,
        created_at=audit.created_at,
    )


@router.get("/evaluation-runs/{run_id}/cases/{case_id}", responses=NOT_FOUND)
async def get_case(
    request: Request,
    workspace_id: OperatorWorkspace,
    run_id: Annotated[uuid.UUID, Path()],
    case_id: Annotated[uuid.UUID, Path()],
) -> EvaluationCaseDetail:
    """One case: expected and observed outcomes, scores, the replay, and each citation."""
    database = evaluation_database(request)
    run = await _owned_run(database, run_id, workspace_id)
    return await _case_detail(database, run, case_id)


@router.post(
    "/evaluation-runs/{run_id}/cases/{case_id}/audits",
    status_code=status.HTTP_201_CREATED,
    responses={**NOT_FOUND, 422: {"description": "The signal is not cited by this case"}},
)
async def record_audit(
    request: Request,
    workspace_id: OperatorWorkspace,
    run_id: Annotated[uuid.UUID, Path()],
    case_id: Annotated[uuid.UUID, Path()],
    body: AuditRecord,
) -> AuditView:
    """Record a person's judgement of one citation; reported apart from automatic scores."""
    database = evaluation_database(request)
    run = await _owned_run(database, run_id, workspace_id)
    detail = await _case_detail(database, run, case_id)
    if body.signal_id not in {citation.signal_id for citation in detail.citations}:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "the signal is not cited by this case"
        )
    async with database.transaction() as session:
        audit = await EvaluationRepository(session).record_audit(case_id, body)
    return _audit_view(audit)
