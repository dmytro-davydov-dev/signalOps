"""Component evaluation: build the stage corpus, run one role on it, and score (P13-T04).

**Corpus.** `build_cases` takes a replayed catalog scenario and plants defects into
briefs built by the workflow's own code (`synthesis_brief`, `verification_brief`):

- Verifier: the leading hypothesis is the true cause citing fault records of its
  service (`none`), citing records of its service that the fault did not produce
  (`non_fault_citations`), or citing records of services the fault never touched
  (`other_service_citations`). Fault records of other affected services are left out:
  they are symptoms that name the cause, so they are not a clean defect.
- Synthesizer: the true cause in one role's findings only (`lead_in_one_role`), a
  second role naming another service with real confidence (`roles_in_conflict`), or
  only weak findings (`weak_findings`).

Each case's expected handling is recorded beside its brief, never inside it.

**Runs.** Every case and trial is a holder investigation in its own evaluation
replay scope (`agents/components.py`): the brief is its first, evaluator-written
step, and an ordinary worker runs the role through `AgentLoop` and the gateway, so
reservations, accounting, deadlines, and limits apply unchanged. A case whose
budget cannot fit a call ends `budget_exceeded` and is recorded as failed, alone.

**Scores.** Verdict accuracy, false accept rate (defect cases accepted), false reject
rate (correct cases not accepted), per-citation agreement, leading-cause accuracy,
contradiction recall, and abstention correctness, each with a Wilson interval. On
the fake provider they show the plumbing only (`measures_model_quality: false`).
"""

import asyncio
import hashlib
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import select

from signalops.agents.briefs import incident_brief
from signalops.agents.components import (
    BRIEF_STEP,
    SYNTHESIS_STEP,
    VERIFICATION_STEP,
    WORKFLOW_S1_SYNTHESIZER,
    WORKFLOW_S1_VERIFIER,
)
from signalops.agents.multi_agent import synthesis_brief, verification_brief
from signalops.agents.read_cache import ToolScope
from signalops.agents.tools import TextRedactor
from signalops.contracts.checkpoints import StepCheckpoint
from signalops.contracts.common import replay_scope_for
from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evaluation import CaseStatus, ExpectedOutcome, RunStatus
from signalops.contracts.evaluation_components import (
    STAGE_CORPUS_VERSION,
    ComponentCaseScores,
    ComponentRole,
    CorpusCase,
    Defect,
    Expectation,
    ExpectedHandling,
)
from signalops.contracts.roles import (
    AgentEnding,
    Finding,
    Hypothesis,
    Provenance,
    SpecialistFindings,
    SpecialistReport,
    SynthesisOutput,
    SynthesisReport,
    Verdict,
    VerificationOutput,
)
from signalops.contracts.steps import IncidentBrief
from signalops.contracts.workflow import (
    TERMINAL_INVESTIGATION_STATUSES,
    AgentRole,
    IncidentSeverity,
    StepStatus,
)
from signalops.detection.rules import Window
from signalops.evaluation.catalog import definition_of
from signalops.evaluation.report import interval
from signalops.persistence.database import Database
from signalops.persistence.models import (
    AgentStep,
    EvaluationStageCase,
    EvaluationStageRun,
    Incident,
    Investigation,
    Service,
    Signal,
)
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.scope import EvidenceScope

if TYPE_CHECKING:
    from signalops.evaluation.runner import RunnerContext

CITATIONS: Final = 5
STRONG: Final = 0.7
CONFLICTING: Final = 0.6
WEAK: Final = 0.2
WORKFLOW_OF: Final = {
    ComponentRole.SYNTHESIZER: WORKFLOW_S1_SYNTHESIZER,
    ComponentRole.VERIFIER: WORKFLOW_S1_VERIFIER,
}
ROLE_STEP: Final = {
    ComponentRole.SYNTHESIZER: SYNTHESIS_STEP,
    ComponentRole.VERIFIER: VERIFICATION_STEP,
}
# The brief step, the role, and the conclusion.
COMPONENT_STEPS: Final = 3


class CorpusError(ValueError):
    """A replayed scenario lacks what a corpus case needs."""


# --- Building the corpus ---


@dataclass(frozen=True)
class ScenarioSignals:
    """A replayed scenario's signals known by the cutoff, sorted into what cases cite."""

    brief: IncidentBrief
    names: dict[uuid.UUID, str]
    root_service: uuid.UUID
    root_fault: list[uuid.UUID]
    # Records of services the fault did not affect.
    unrelated: list[uuid.UUID]
    # Records of the cause's service that the fault did not produce.
    noise: list[uuid.UUID]
    deployments: list[tuple[uuid.UUID, uuid.UUID]]


async def scenario_signals(
    database: Database, scope: EvidenceScope, expected: ExpectedOutcome, redactor: TextRedactor
) -> ScenarioSignals:
    async with database.transaction() as session:
        incident = await session.scalar(
            select(Incident).where(scope.clause(Incident)).order_by(Incident.detected_at)
        )
        if incident is None:
            raise CorpusError("the scenario opened no incident")
        cutoff = incident.detected_at
        names = dict(
            (
                await session.execute(
                    select(Service.id, Service.name).where(
                        Service.workspace_id == scope.workspace_id
                    )
                )
            ).all()
        )
        rows = list(
            await session.scalars(
                select(Signal)
                .where(scope.clause(Signal), Signal.ingested_at <= cutoff)
                .order_by(Signal.occurred_at, Signal.id)
            )
        )
    brief = await incident_brief(database, scope, incident.id, cutoff)
    if brief is None:
        raise CorpusError("the incident is outside its scope")
    by_name = {name: service_id for service_id, name in names.items()}
    root_name = expected.root_cause_service or ""
    root = by_name[root_name]
    supporting = set(expected.supporting_event_ids)
    root_fault = [r.id for r in rows if r.source_event_id in supporting and r.service_id == root]
    # Records of the cause's service that the fault did not produce, warnings and errors first.
    noise = sorted(
        (r for r in rows if r.source_event_id not in supporting and r.service_id == root),
        key=lambda r: r.severity not in ("warning", "error"),
    )
    # Records of services the fault never touched: not even symptoms of it.
    affected = {by_name[name] for name in expected.affected_services if name in by_name}
    unrelated = sorted(
        (r for r in rows if r.service_id is not None and r.service_id not in affected),
        key=lambda r: r.severity not in ("warning", "error"),
    )
    deployments = [
        (r.id, r.service_id)
        for r in rows
        if r.kind == "deployment" and r.service_id is not None and r.service_id != root
    ]
    if not (root_fault and unrelated and noise and deployments):
        raise CorpusError("the scenario lacks fault, unrelated, noise, or deployment records")
    return ScenarioSignals(
        brief,
        names,
        root,
        root_fault,
        [r.id for r in unrelated],
        [r.id for r in noise],
        [(signal, service) for signal, service in deployments if service is not None],
    )


def _provenance(role: AgentRole) -> Provenance:
    return Provenance(role=role, prompt_version="corpus-v1", ended_by=AgentEnding.SUBMITTED)


def _findings(role: AgentRole, *found: Finding) -> SpecialistFindings:
    report = SpecialistReport(
        findings=found, missing_evidence=() if found else ("Nothing relevant was found",)
    )
    return SpecialistFindings(role=role, report=report, provenance=_provenance(role))


def _finding(
    service: uuid.UUID | None, failure: str | None, confidence: float, ids: Sequence[uuid.UUID]
) -> Finding:
    return Finding(
        claim="Records from this service show the failure." if service else "Structure only.",
        service_id=service,
        failure_type=failure if service else None,
        confidence=confidence,
        uncertainty="Built for the component corpus from replayed records.",
        evidence_signal_ids=tuple(ids)[:CITATIONS],
    )


async def build_cases(
    database: Database,
    scope: EvidenceScope,
    source: str,
    source_version: str,
    expected: ExpectedOutcome,
    redactor: TextRedactor,
) -> list[CorpusCase]:
    """Every planted-defect case for one replayed, diagnosable scenario."""
    if not expected.diagnosable or expected.failure_type is None:
        raise CorpusError("component cases need a diagnosable scenario")
    found = await scenario_signals(database, scope, expected, redactor)
    tag = source.split("/")[0]
    root, failure = found.root_service, expected.failure_type
    root_name = found.names[root]
    cases = []

    async def verifier(defect: Defect, citations: list[uuid.UUID], supports: bool) -> None:
        cited = tuple(citations[:CITATIONS])
        hypothesis = Hypothesis(
            service_id=root,
            failure_type=failure,
            confidence=STRONG,
            evidence_signal_ids=cited,
            supporting_roles=(AgentRole.TELEMETRY_INVESTIGATOR,),
            assessment="The leading cause, as the corpus plants it.",
        )
        synthesis = SynthesisOutput(
            report=SynthesisReport(
                status=DiagnosisStatus.DIAGNOSED,
                summary="One leading hypothesis to verify.",
                hypotheses=(hypothesis,),
            ),
            provenance=_provenance(AgentRole.SYNTHESIZER),
        )
        brief = await verification_brief(
            database,
            ToolScope(scope, found.brief.evidence_cutoff_at),
            redactor,
            found.brief,
            synthesis,
            cited,
            follow_up_allowed=False,
            follow_up=None,
        )
        cases.append(
            CorpusCase(
                key=f"verifier-{tag}-{defect.value.replace('_', '-')}",
                role=ComponentRole.VERIFIER,
                defect=defect,
                source=source,
                source_version=source_version,
                brief=brief.model_dump(mode="json"),
                expected=ExpectedHandling(
                    kind=Expectation.VERDICT,
                    verdict=Verdict.SUPPORTED if supports else Verdict.UNSUPPORTED,
                    citation_supports=dict.fromkeys(cited, supports),
                ),
            )
        )

    await verifier(Defect.NONE, found.root_fault, True)
    await verifier(Defect.NON_FAULT_CITATIONS, found.noise, False)
    await verifier(Defect.OTHER_SERVICE_CITATIONS, found.unrelated, False)

    def synthesizer(
        defect: Defect, findings: list[SpecialistFindings], expected_handling: ExpectedHandling
    ) -> None:
        brief = synthesis_brief(found.brief, findings)
        cases.append(
            CorpusCase(
                key=f"synthesizer-{tag}-{defect.value.replace('_', '-')}",
                role=ComponentRole.SYNTHESIZER,
                defect=defect,
                source=source,
                source_version=source_version,
                brief=brief.model_dump(mode="json"),
                expected=expected_handling,
            )
        )

    structure = _findings(
        AgentRole.DEPENDENCY_INVESTIGATOR, _finding(None, None, 0.5, found.noise[-1:])
    )
    lead = _finding(root, failure, STRONG, found.root_fault)
    synthesizer(
        Defect.LEAD_IN_ONE_ROLE,
        [
            _findings(AgentRole.TELEMETRY_INVESTIGATOR, lead),
            _findings(AgentRole.DEPLOYMENT_INVESTIGATOR),
            structure,
        ],
        ExpectedHandling(
            kind=Expectation.LEADING, leading_service=root_name, leading_failure_type=failure
        ),
    )
    deployment, deployed = found.deployments[-1]
    synthesizer(
        Defect.ROLES_IN_CONFLICT,
        [
            _findings(AgentRole.TELEMETRY_INVESTIGATOR, lead),
            _findings(
                AgentRole.DEPLOYMENT_INVESTIGATOR,
                _finding(deployed, "deployment_regression", CONFLICTING, [deployment]),
            ),
            structure,
        ],
        ExpectedHandling(
            kind=Expectation.CONTRADICTION,
            contradiction_service=found.names[deployed],
            contradiction_signal_ids=(deployment,),
        ),
    )
    synthesizer(
        Defect.WEAK_FINDINGS,
        [
            _findings(
                AgentRole.TELEMETRY_INVESTIGATOR,
                _finding(root, "elevated_errors", WEAK, found.noise[:2]),
            ),
            _findings(
                AgentRole.DEPLOYMENT_INVESTIGATOR,
                _finding(deployed, "deployment_regression", WEAK, [deployment]),
            ),
            structure,
        ],
        ExpectedHandling(kind=Expectation.ABSTENTION),
    )
    return cases


def corpus_files(cases: Sequence[CorpusCase]) -> dict[str, str]:
    return {
        f"{case.role.value}/{case.key}.json": case.model_dump_json(indent=2) + "\n"
        for case in cases
    }


def corpus_dir(root: Path, version: str = STAGE_CORPUS_VERSION) -> Path:
    return root / "fixtures" / "evaluation" / f"v{version}" / "stage-corpus"


def load_corpus(directory: Path, role: ComponentRole) -> tuple[list[CorpusCase], str]:
    """The role's corpus cases, sorted by key, and a SHA-256 over their files."""
    paths = sorted((directory / role.value).glob("*.json"))
    if not paths:
        raise CorpusError(f"no {role} cases under {directory}")
    digest = hashlib.sha256()
    cases = []
    for path in paths:
        content = path.read_bytes()
        digest.update(path.name.encode() + b"\0" + content)
        cases.append(CorpusCase.model_validate_json(content))
    return cases, digest.hexdigest()


CORPUS_SOURCES: Final = (
    "downstream-timeout/dev",
    "pool-exhaustion/dev",
    "memory-pressure/dev",
)


async def build_corpus(
    context: "RunnerContext", workspace_id: uuid.UUID, redactor: TextRedactor
) -> list[CorpusCase]:
    """Replay each source scenario of catalog v2 into a fresh scope and build its cases."""
    from signalops.evaluation.catalog import CATALOG_V2, entries
    from signalops.evaluation.runner import replay_into

    chosen = {entry.name: entry for entry in entries(version=CATALOG_V2)}
    cases: list[CorpusCase] = []
    for name in CORPUS_SOURCES:
        entry = chosen[name]
        dataset = entry.dataset()
        replay_id = uuid.uuid4()
        scope = EvidenceScope(workspace_id, replay_scope_for(replay_id))
        report = await replay_into(context, scope, replay_id, dataset.records)
        if report.status != "completed":
            raise CorpusError(f"{name} did not replay: {report.status}")
        expected = definition_of(entry, dataset).expected_outcome
        cases += await build_cases(
            context.database, scope, entry.name, entry.version, expected, redactor
        )
    return cases


# --- Scoring ---


def score_output(case: CorpusCase, output: dict[str, Any]) -> ComponentCaseScores:
    expected = case.expected
    if case.role is ComponentRole.VERIFIER:
        verification = VerificationOutput.model_validate(output).report
        checks = {check.signal_id: check.supports for check in verification.citation_checks}
        agreeing = sum(
            checks.get(signal_id) == supports
            for signal_id, supports in expected.citation_supports.items()
        )
        return ComponentCaseScores(
            correct=verification.verdict is expected.verdict,
            accepted=verification.verdict is Verdict.SUPPORTED,
            citations_expected=len(expected.citation_supports),
            citations_agreeing=agreeing,
        )
    synthesis = SynthesisOutput.model_validate(output).report
    brief = case.synthesis_brief()
    names = {service.service_id: service.name for service in brief.services}
    leading = synthesis.leading
    leading_name = names.get(leading.service_id) if leading else None
    abstained = synthesis.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE
    listed = None
    if expected.kind is Expectation.CONTRADICTION:
        target = expected.contradiction_service or ""
        planted = set(expected.contradiction_signal_ids)
        listed = any(
            target in item.description or planted & set(item.evidence_signal_ids)
            for item in synthesis.contradictions
        )
    correct = {
        Expectation.LEADING: leading is not None
        and leading_name == expected.leading_service
        and leading.failure_type == expected.leading_failure_type,
        Expectation.CONTRADICTION: bool(listed),
        Expectation.ABSTENTION: abstained,
        Expectation.VERDICT: False,
    }[expected.kind]
    return ComponentCaseScores(
        correct=correct,
        leading_service=leading_name,
        contradiction_listed=listed,
        abstained=abstained,
    )


def summarize_components(
    role: ComponentRole, rows: Sequence[EvaluationStageCase], measures_model_quality: bool
) -> dict[str, Any]:
    scored = [
        (row, ComponentCaseScores.model_validate(row.scores))
        for row in rows
        if row.status == CaseStatus.SCORED and row.scores
    ]
    by_defect: dict[str, Any] = {}
    for defect in sorted({row.defect for row in rows}):
        of = [score for row, score in scored if row.defect == defect]
        total = [row for row in rows if row.defect == defect]
        by_defect[defect] = interval(sum(score.correct for score in of), len(total))
    summary: dict[str, Any] = {
        "role": role.value,
        "cases": len(rows),
        "scored": len(scored),
        "failed": sum(row.status == CaseStatus.FAILED for row in rows),
        "failed_by_status": _counts(
            row.investigation_status or "unknown" for row in rows if row.status == CaseStatus.FAILED
        ),
        "correct": interval(sum(score.correct for _, score in scored), len(rows)),
        "by_defect": by_defect,
        "measures_model_quality": measures_model_quality,
    }
    if role is ComponentRole.VERIFIER:
        defects = [score for row, score in scored if row.defect != Defect.NONE]
        clean = [score for row, score in scored if row.defect == Defect.NONE]
        summary |= {
            "verdict_accuracy": interval(sum(s.correct for _, s in scored), len(scored)),
            "false_accept_rate": interval(sum(bool(s.accepted) for s in defects), len(defects)),
            "false_reject_rate": interval(sum(not s.accepted for s in clean), len(clean)),
            "citation_agreement": interval(
                sum(s.citations_agreeing for _, s in scored),
                sum(s.citations_expected for _, s in scored),
            ),
        }
    else:

        def scored_for(defect: Defect) -> list[ComponentCaseScores]:
            return [score for row, score in scored if row.defect == defect]

        summary |= {
            "leading_accuracy": interval(
                sum(s.correct for s in scored_for(Defect.LEAD_IN_ONE_ROLE)),
                len(scored_for(Defect.LEAD_IN_ONE_ROLE)),
            ),
            "contradiction_recall": interval(
                sum(s.correct for s in scored_for(Defect.ROLES_IN_CONFLICT)),
                len(scored_for(Defect.ROLES_IN_CONFLICT)),
            ),
            "abstention_correctness": interval(
                sum(s.correct for s in scored_for(Defect.WEAK_FINDINGS)),
                len(scored_for(Defect.WEAK_FINDINGS)),
            ),
        }
    return summary


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


# --- Running ---


@dataclass(frozen=True)
class ComponentLimits:
    budget_microusd: int
    deadline: timedelta = timedelta(minutes=10)
    max_steps: int = 10


async def start_run(
    database: Database,
    *,
    workspace_id: uuid.UUID,
    role: ComponentRole,
    corpus_hash: str,
    trials: int,
    model_config: dict[str, Any],
    limits: ComponentLimits,
) -> uuid.UUID:
    async with database.transaction() as session:
        run = EvaluationStageRun(
            workspace_id=workspace_id,
            role=role.value,
            corpus_version=STAGE_CORPUS_VERSION,
            corpus_hash=corpus_hash,
            trials=trials,
            status=RunStatus.RUNNING,
            model_config=model_config,
            limits={
                "budget_microusd": limits.budget_microusd,
                "deadline_seconds": int(limits.deadline.total_seconds()),
                "max_steps": limits.max_steps,
            },
        )
        session.add(run)
        await session.flush()
        return run.id


async def queue_case(
    database: Database,
    run_id: uuid.UUID,
    workspace_id: uuid.UUID,
    case: CorpusCase,
    trial: int,
    limits: ComponentLimits,
) -> uuid.UUID:
    """A holder incident and investigation in a fresh replay scope, the brief as step one."""
    workflow = WORKFLOW_OF[case.role]
    incident = (
        case.synthesis_brief().incident
        if case.role is ComponentRole.SYNTHESIZER
        else case.verification_brief().incident
    )
    scope = EvidenceScope(workspace_id, replay_scope_for(uuid.uuid4()))
    now = datetime.now(UTC)
    async with database.transaction() as session:
        incident_id = await IncidentRepository(session).create(
            NewIncident(
                scope=scope,
                fingerprint=f"component:{case.key}:{trial}",
                title=incident.title,
                severity=IncidentSeverity.MEDIUM,
                window=Window(incident.window_start or incident.detected_at, incident.detected_at),
                detected_at=incident.detected_at,
            )
        )
        repository = InvestigationRepository(session)
        investigation = await repository.create(
            NewInvestigation(
                scope=scope,
                incident_id=incident_id,
                request_key=f"component:{run_id}:{case.key}:{trial}",
                workflow_version=workflow,
                budget_microusd=limits.budget_microusd,
                max_steps=limits.max_steps,
                created_at=now,
                deadline_at=now + limits.deadline,
                evidence_cutoff_at=incident.evidence_cutoff_at,
            )
        )
        if investigation is None:
            raise CorpusError(f"{case.key} trial {trial} was already queued")
        checkpoint = StepCheckpoint(
            workflow_version=workflow,
            step_key=BRIEF_STEP,
            agent_role=AgentRole.COORDINATOR,
            attempt=1,
            lease_generation=0,
        )
        session.add(
            AgentStep(
                investigation_id=investigation.id,
                step_key=BRIEF_STEP,
                agent_role=AgentRole.COORDINATOR,
                status=StepStatus.COMPLETED,
                attempt=1,
                input={"written_by": "evaluator"},
                output=case.brief,
                checkpoint=checkpoint.model_dump(mode="json"),
                lease_generation=0,
                started_at=now,
                completed_at=now,
            )
        )
        await session.flush()
        await repository.enqueue(investigation.id)
        session.add(
            EvaluationStageCase(
                stage_run_id=run_id,
                case_key=case.key,
                defect=case.defect.value,
                trial=trial,
                status=CaseStatus.INVESTIGATING,
                investigation_id=investigation.id,
                expected=case.expected.model_dump(mode="json"),
            )
        )
    return investigation.id


async def collect(
    database: Database,
    run_id: uuid.UUID,
    cases: dict[str, CorpusCase],
    *,
    poll_interval: timedelta = timedelta(seconds=1),
    wait_limit: timedelta = timedelta(minutes=15),
) -> None:
    """Wait for every holder investigation, then score its role step or record the failure."""
    deadline = time.monotonic() + wait_limit.total_seconds()
    while True:
        async with database.transaction() as session:
            pending = list(
                await session.scalars(
                    select(EvaluationStageCase).where(
                        EvaluationStageCase.stage_run_id == run_id,
                        EvaluationStageCase.status == CaseStatus.INVESTIGATING,
                    )
                )
            )
        for row in pending:
            await _finish_case(
                database, row, cases[row.case_key], late=time.monotonic() >= deadline
            )
        if not pending:
            return
        if time.monotonic() >= deadline:
            return
        await asyncio.sleep(poll_interval.total_seconds())


async def _finish_case(
    database: Database, row: EvaluationStageCase, case: CorpusCase, *, late: bool
) -> None:
    async with database.transaction() as session:
        investigation = await session.get_one(Investigation, row.investigation_id)
        status = investigation.status
        if status not in TERMINAL_INVESTIGATION_STATUSES and not late:
            return
        step = await session.scalar(
            select(AgentStep).where(
                AgentStep.investigation_id == row.investigation_id,
                AgentStep.step_key == ROLE_STEP[ComponentRole(case.role)],
                AgentStep.status == StepStatus.COMPLETED,
            )
        )
        stored = await session.get_one(EvaluationStageCase, row.id)
        stored.investigation_status = status
        stored.completed_at = datetime.now(UTC)
        if step is None or step.output is None:
            stored.status = CaseStatus.FAILED
            stored.error = (
                f"the role produced no output (investigation {status})"
                if not late or status in TERMINAL_INVESTIGATION_STATUSES
                else "the case did not end before the wait limit"
            )
            return
        stored.status = CaseStatus.SCORED
        stored.output = step.output
        stored.scores = score_output(case, step.output).model_dump(mode="json")


async def finish_run(
    database: Database, run_id: uuid.UUID, role: ComponentRole, measures_model_quality: bool
) -> dict[str, Any]:
    async with database.transaction() as session:
        rows = list(
            await session.scalars(
                select(EvaluationStageCase)
                .where(EvaluationStageCase.stage_run_id == run_id)
                .order_by(EvaluationStageCase.case_key, EvaluationStageCase.trial)
            )
        )
        metrics = summarize_components(role, rows, measures_model_quality)
        run = await session.get_one(EvaluationStageRun, run_id)
        run.metrics = metrics
        run.status = RunStatus.COMPLETED
        run.completed_at = datetime.now(UTC)
    return metrics


async def run_components(
    database: Database,
    *,
    workspace_id: uuid.UUID,
    role: ComponentRole,
    cases: Sequence[CorpusCase],
    corpus_hash: str,
    trials: int,
    model_config: dict[str, Any],
    limits: ComponentLimits,
    budget_for: Callable[[CorpusCase, int], int | None] = lambda _case, _trial: None,
    poll_interval: timedelta = timedelta(seconds=1),
    wait_limit: timedelta = timedelta(minutes=15),
) -> uuid.UUID:
    """Queue every case and trial, wait for the workers, score, and store the metrics."""
    run_id = await start_run(
        database,
        workspace_id=workspace_id,
        role=role,
        corpus_hash=corpus_hash,
        trials=trials,
        model_config=model_config,
        limits=limits,
    )
    for case in cases:
        for trial in range(1, trials + 1):
            budget = budget_for(case, trial)
            chosen = (
                limits
                if budget is None
                else ComponentLimits(budget, limits.deadline, limits.max_steps)
            )
            await queue_case(database, run_id, workspace_id, case, trial, chosen)
    await collect(
        database,
        run_id,
        {case.key: case for case in cases},
        poll_interval=poll_interval,
        wait_limit=wait_limit,
    )
    await finish_run(database, run_id, role, bool(model_config.get("measures_model_quality")))
    return run_id
