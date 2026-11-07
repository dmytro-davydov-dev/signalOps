"""The first agent end to end: real evidence, real detector, fake model, durable workflow."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import delete, select, update

from signalops.agents.investigations import cancel_investigation
from signalops.agents.investigator import AgentLimits, AgentRuntime
from signalops.agents.jobs import JobPolicy, JobRunner, claim_job
from signalops.agents.tools import TextRedactor, ToolExecutor
from signalops.agents.workflow import (
    INVESTIGATE_AGENT,
    NodeContext,
    WorkflowEngine,
    WorkflowRun,
    conclude_findings,
)
from signalops.contracts.common import replay_scope_for
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.evidence import NormalizedSignal, QualityStatus, Severity, SignalKind
from signalops.contracts.steps import SUBMIT_TOOL, ProposedDiagnosis
from signalops.contracts.workflow import (
    CostBasis,
    IncidentSeverity,
    InvestigationStatus,
    LlmCallStatus,
    StepStatus,
    ToolCallStatus,
    ToolName,
)
from signalops.demo.generator import Scenario
from signalops.detection.rules import Window
from signalops.gateway.errors import ProviderErrorCategory
from signalops.gateway.fake import FAKE_MODEL, FakeAnswer, FakeFailure, FakeProvider, FakeStep
from signalops.gateway.fake_investigator import investigator_step
from signalops.gateway.gateway import GatewayPolicy, ModelGateway
from signalops.gateway.pricing import maximum_cost, price_of
from signalops.gateway.types import StopReason, TextBlock, ToolUseBlock
from signalops.persistence import database as db
from signalops.persistence.models import (
    AgentStep,
    Incident,
    Investigation,
    Job,
    LlmCall,
    Signal,
    ToolCall,
)
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID
from signalops.streaming.retry import RetryPolicy
from tests.integration.job_support import LIVE, add_job
from tests.integration.test_detector import _flush, _normalized_demo, _process

pytestmark = pytest.mark.integration

SERVICE_IDS = {service.name: service.id for service in DEMO_SERVICES}
JOBS = JobPolicy(
    lease_seconds=30,
    poll_interval_seconds=0.02,
    retry=RetryPolicy(base_delay_seconds=0.001, max_delay_seconds=0.001),
)
RETRIES = GatewayPolicy(retry=RetryPolicy(2, 0.001, 0.001), request_timeout_seconds=5)
LIMITS = AgentLimits(max_output_tokens=1_000, effort=None, max_repairs=2, max_tool_calls=4)
BUDGET = 5_000_000
ABSTAIN: dict[str, Any] = {
    "status": "insufficient_evidence",
    "summary": "Nothing shows a cause.",
    "missing_evidence": ["Traces for the window"],
}


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


def engine_with(
    database: db.Database, provider: FakeProvider, limits: AgentLimits = LIMITS
) -> WorkflowEngine:
    runtime = AgentRuntime(
        gateway=ModelGateway(database, provider, policy=RETRIES),
        tools=ToolExecutor(database, TextRedactor(("password",))),
        model=FAKE_MODEL,
        limits=limits,
    )
    return WorkflowEngine(database, runtime=runtime)


async def run_once(database: db.Database, engine: WorkflowEngine) -> None:
    lease = await claim_job(database, "worker-a", JOBS)
    assert lease is not None
    await JobRunner(database, engine, JOBS, owner=lease.owner).execute(lease, asyncio.Event())


async def replayed_incident(
    database: db.Database, scenario: Scenario, seed: int
) -> tuple[EvidenceScope, uuid.UUID | None]:
    start = (datetime.now(UTC) - timedelta(minutes=11)).replace(second=0, microsecond=0)
    run_id = uuid.uuid4()
    normalized = await _normalized_demo(
        database, start, timedelta(minutes=10), seed=seed, run_id=run_id, scenario=scenario
    )
    await _process(database, normalized)
    await _flush(database, run_id)
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
    async with database.transaction() as session:
        incident_id = await session.scalar(select(Incident.id).where(scope.clause(Incident)))
    return scope, incident_id


async def queued(
    database: db.Database,
    scope: EvidenceScope,
    incident_id: uuid.UUID,
    *,
    max_steps: int = 10,
    budget: int = BUDGET,
) -> uuid.UUID:
    now = datetime.now(UTC)
    async with database.transaction() as session:
        repository = InvestigationRepository(session)
        investigation = await repository.create(
            NewInvestigation(
                scope=scope,
                incident_id=incident_id,
                request_key=f"test:{uuid.uuid4()}",
                workflow_version="v2",
                budget_microusd=budget,
                max_steps=max_steps,
                created_at=now,
                deadline_at=now + timedelta(minutes=10),
            )
        )
        assert investigation is not None
        assert await repository.enqueue(investigation.id)
        return investigation.id


async def new_job(database: db.Database, *, max_steps: int = 10, budget: int = BUDGET) -> uuid.UUID:
    """A queued v2 investigation of an incident with no evidence behind it."""
    investigation_id = await add_job(database, workflow_version="v2", max_steps=max_steps)
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == investigation_id)
            .values(budget_microusd=budget)
        )
    return investigation_id


async def row_of(database: db.Database, investigation_id: uuid.UUID) -> Investigation:
    async with database.transaction() as session:
        return await session.get_one(Investigation, investigation_id)


async def diagnosis_of(database: db.Database, investigation_id: uuid.UUID) -> Diagnosis:
    row = await row_of(database, investigation_id)
    return Diagnosis.model_validate(row.diagnosis)


async def llm_calls_of(database: db.Database, investigation_id: uuid.UUID) -> list[LlmCall]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(LlmCall)
            .join(AgentStep, AgentStep.id == LlmCall.agent_step_id)
            .where(AgentStep.investigation_id == investigation_id)
            .order_by(LlmCall.attempt)
        )
        return list(rows)


async def tool_calls_of(database: db.Database, investigation_id: uuid.UUID) -> list[ToolCall]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(ToolCall)
            .join(AgentStep, AgentStep.id == ToolCall.agent_step_id)
            .where(AgentStep.investigation_id == investigation_id)
        )
        return list(rows)


def use(name: str, arguments: dict[str, Any], block_id: str = "toolu_1") -> ToolUseBlock:
    return ToolUseBlock(id=block_id, name=name, input=arguments)


def turn(*blocks: ToolUseBlock | TextBlock) -> FakeAnswer:
    has_tool = any(isinstance(block, ToolUseBlock) for block in blocks)
    return FakeAnswer(
        content=blocks, stop_reason=StopReason.TOOL_USE if has_tool else StopReason.END_TURN
    )


async def test_the_timeout_fixture_names_the_faulty_service_with_existing_evidence(
    database: db.Database,
) -> None:
    scope, incident_id = await replayed_incident(database, Scenario.DOWNSTREAM_TIMEOUT, 7301)
    assert incident_id is not None
    investigation_id = await queued(database, scope, incident_id)

    await run_once(database, engine_with(database, FakeProvider(responder=investigator_step)))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    diagnosis = Diagnosis.model_validate(row.diagnosis)
    assert diagnosis.status is DiagnosisStatus.DIAGNOSED
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.service_name == "bank-gateway"
    assert diagnosis.root_cause.service_id == SERVICE_IDS["bank-gateway"]
    assert diagnosis.workflow_version == "v2"
    cited = diagnosis.root_cause.evidence_signal_ids
    async with database.transaction() as session:
        stored = list(
            await session.scalars(select(Signal).where(scope.clause(Signal), Signal.id.in_(cited)))
        )
    assert len(stored) == len(cited)
    assert all(signal.occurred_at <= row.evidence_cutoff_at for signal in stored)

    calls = await llm_calls_of(database, investigation_id)
    assert [call.status for call in calls] == [LlmCallStatus.SUCCEEDED] * len(calls)
    assert {call.prompt_version for call in calls} == {"investigator-v1"}
    assert {call.cost_basis for call in calls} == {CostBasis.ESTIMATED}
    assert row.reserved_microusd == 0
    assert row.spent_microusd == sum(call.cost_microusd or 0 for call in calls) > 0
    tools = await tool_calls_of(database, investigation_id)
    assert {call.tool_name for call in tools} == {
        ToolName.GET_TRACE_SUMMARY,
        ToolName.QUERY_LOGS,
    }
    assert {call.status for call in tools} == {ToolCallStatus.SUCCEEDED}


async def test_underdetermined_input_abstains_instead_of_inventing_a_cause(
    database: db.Database,
) -> None:
    investigation_id = await new_job(database)

    await run_once(database, engine_with(database, FakeProvider(responder=investigator_step)))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    diagnosis = await diagnosis_of(database, investigation_id)
    assert diagnosis.root_cause is None
    assert diagnosis.missing_evidence


async def test_healthy_traffic_with_noisy_warnings_abstains(database: db.Database) -> None:
    scope, incident_id = await replayed_incident(database, Scenario.HEALTHY, 7302)
    assert incident_id is None
    async with database.transaction() as session:
        incident_id = await IncidentRepository(session).create(
            NewIncident(
                scope=scope,
                fingerprint="env:demo",
                title="Requested by an operator",
                severity=IncidentSeverity.LOW,
                window=Window(datetime.now(UTC) - timedelta(minutes=10), datetime.now(UTC)),
                detected_at=datetime.now(UTC),
            )
        )
    investigation_id = await queued(database, scope, incident_id)

    await run_once(database, engine_with(database, FakeProvider(responder=investigator_step)))

    diagnosis = await diagnosis_of(database, investigation_id)
    assert diagnosis.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE


async def test_invalid_findings_are_repaired_within_the_bound(database: db.Database) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted(
        [
            turn(use(SUBMIT_TOOL, {"status": "diagnosed", "summary": "It was payment."})),
            turn(use(SUBMIT_TOOL, ABSTAIN, "toolu_2")),
        ]
    )

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == 2
    last = provider.requests[1].messages[-1]
    assert "root_cause" in str(last.content) or "Invalid findings" in str(last.content)
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    assert (await diagnosis_of(database, investigation_id)).missing_evidence == (
        "Traces for the window",
    )


async def test_repeatedly_invalid_output_abstains_and_stops_calling(database: db.Database) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([turn(use(SUBMIT_TOOL, {"status": "maybe"}))])

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == LIMITS.max_repairs + 1
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE


async def test_a_reply_with_no_tool_call_is_sent_back(database: db.Database) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted(
        [turn(TextBlock(text="I think it was payment.")), turn(use(SUBMIT_TOOL, ABSTAIN))]
    )

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == 2
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE


async def test_a_cut_off_reply_is_dropped_and_never_acted_on(database: db.Database) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted(
        [
            FakeAnswer(
                content=(use(ToolName.QUERY_LOGS.value, {"limit": 1}),),
                stop_reason=StopReason.MAX_TOKENS,
            ),
            turn(use(SUBMIT_TOOL, ABSTAIN)),
        ]
    )

    await run_once(database, engine_with(database, provider))

    assert await tool_calls_of(database, investigation_id) == []
    assert all(
        not isinstance(block, ToolUseBlock)
        for message in provider.requests[1].messages
        for block in message.content
    )


async def test_a_refusal_abstains(database: db.Database) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([FakeAnswer(content=(), stop_reason=StopReason.REFUSAL)])

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == 1
    diagnosis = await diagnosis_of(database, investigation_id)
    assert diagnosis.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE
    assert "declined" in diagnosis.missing_evidence[0]


async def test_the_step_limit_abstains_rather_than_failing(database: db.Database) -> None:
    investigation_id = await new_job(database, max_steps=5)
    provider = FakeProvider.scripted([turn(use(ToolName.QUERY_LOGS.value, {"limit": 1}))])

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == 2
    diagnosis = await diagnosis_of(database, investigation_id)
    assert "step limit" in diagnosis.missing_evidence[0]


async def test_a_step_budget_of_no_turns_abstains_without_a_call(database: db.Database) -> None:
    investigation_id = await new_job(database, max_steps=3)
    provider = FakeProvider.scripted([turn(use(SUBMIT_TOOL, ABSTAIN))])

    await run_once(database, engine_with(database, provider))

    assert provider.requests == []
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE


async def test_output_tokens_and_tools_are_set_on_every_call(database: db.Database) -> None:
    await new_job(database)
    provider = FakeProvider.scripted([turn(use(SUBMIT_TOOL, ABSTAIN))])

    await run_once(database, engine_with(database, provider))

    [request] = provider.requests
    assert request.max_output_tokens == LIMITS.max_output_tokens
    assert {tool.name for tool in request.tools} == {
        *(tool.value for tool in ToolName),
        SUBMIT_TOOL,
    }


async def test_tool_calls_are_capped_per_turn_and_per_step(database: db.Database) -> None:
    investigation_id = await new_job(database)
    many = tuple(
        use(ToolName.QUERY_LOGS.value, {"limit": index + 1}, f"toolu_{index}") for index in range(7)
    )
    provider = FakeProvider.scripted([turn(*many), turn(use(SUBMIT_TOOL, ABSTAIN, "toolu_end"))])

    await run_once(database, engine_with(database, provider))

    assert len(await tool_calls_of(database, investigation_id)) == LIMITS.max_tool_calls
    results = provider.requests[1].messages[-1].content
    assert len(results) == len(many)


async def test_an_unknown_tool_is_an_error_result_for_the_model(database: db.Database) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted(
        [turn(use("run_shell", {"command": "ls"})), turn(use(SUBMIT_TOOL, ABSTAIN, "toolu_2"))]
    )

    await run_once(database, engine_with(database, provider))

    assert await tool_calls_of(database, investigation_id) == []
    assert "no such tool" in str(provider.requests[1].messages[-1].content)


async def test_a_cancellation_between_turns_stops_the_next_call(database: db.Database) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted(
        [
            turn(use(ToolName.QUERY_LOGS.value, {"limit": 1})),
            turn(use(SUBMIT_TOOL, ABSTAIN, "toolu_2")),
        ]
    )
    original = provider.complete

    async def cancelling(request: Any) -> Any:
        response = await original(request)
        async with database.transaction() as session:
            await cancel_investigation(session, LIVE, investigation_id)
        return response

    provider.complete = cancelling  # type: ignore[method-assign]

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == 1
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.CANCELLED


async def test_budget_exhausted_between_turns_ends_budget_exceeded(database: db.Database) -> None:
    first = turn(use(ToolName.QUERY_LOGS.value, {"limit": 1}))
    probe = FakeProvider.scripted([first])
    await new_job(database)
    await run_once(database, engine_with(database, probe))
    async with database.transaction() as session:
        await session.execute(delete(Job))
    amount = await reserved_for_first_call(probe)

    investigation_id = await new_job(database, budget=amount)
    provider = FakeProvider.scripted([first])

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == 1
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.BUDGET_EXCEEDED
    assert row.spent_microusd > 0
    assert row.diagnosis is None


async def reserved_for_first_call(provider: FakeProvider) -> int:
    return maximum_cost(provider.requests[0], price_of("fake", FAKE_MODEL))


async def test_a_failed_turn_retries_the_node_and_reuses_load_context(
    database: db.Database,
) -> None:
    investigation_id = await new_job(database)
    steps: list[FakeStep] = [
        FakeFailure(fail=ProviderErrorCategory.UNAVAILABLE),
        FakeFailure(fail=ProviderErrorCategory.UNAVAILABLE),
        turn(use(SUBMIT_TOOL, ABSTAIN)),
    ]
    provider = FakeProvider.scripted(steps)
    engine = engine_with(database, provider)

    await run_once(database, engine)
    await asyncio.sleep(0.01)
    await run_once(database, engine)

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    calls = await llm_calls_of(database, investigation_id)
    assert [call.status for call in calls] == [
        LlmCallStatus.UNKNOWN,
        LlmCallStatus.UNKNOWN,
        LlmCallStatus.SUCCEEDED,
    ]
    assert [call.attempt for call in calls] == [1, 2, 3]
    assert row.reserved_microusd == sum(
        call.reserved_microusd for call in calls if call.status == LlmCallStatus.UNKNOWN
    )
    async with database.transaction() as session:
        steps_stored = {
            step.step_key: step
            for step in await session.scalars(
                select(AgentStep).where(AgentStep.investigation_id == investigation_id)
            )
        }
    assert steps_stored["load_context"].attempt == 1
    assert steps_stored["investigate"].attempt == 2
    assert steps_stored["investigate"].status == StepStatus.COMPLETED


def cited(*signal_ids: uuid.UUID, service: uuid.UUID = SERVICE_IDS["payment"]) -> dict[str, Any]:
    return {
        "status": "diagnosed",
        "summary": "Payment failed.",
        "root_cause": {
            "service_id": str(service),
            "failure_type": "elevated_errors",
            "confidence": 0.6,
            "evidence_signal_ids": [str(signal_id) for signal_id in signal_ids],
        },
    }


async def evidence(
    database: db.Database,
    *,
    at: datetime,
    scope: EvidenceScope = LIVE,
    ingested: datetime | None = None,
) -> uuid.UUID:
    source = DEMO_SOURCES[0].id
    signal = NormalizedSignal(
        id=uuid.uuid4(),
        workspace_id=scope.workspace_id,
        source_id=source,
        service_id=SERVICE_IDS["payment"],
        source_event_id=f"finding-{uuid.uuid4()}",
        replay_scope=scope.replay_scope,
        kind=SignalKind.LOG,
        occurred_at=at,
        ingested_at=ingested or at,
        severity=Severity.ERROR,
        message="charge failed",
        attributes={},
        quality_status=QualityStatus.ACCEPTED,
        quality_flags=(),
    )
    async with database.transaction() as session:
        assert await EvidenceRepository(session).insert_signal(signal)
    return signal.id


async def test_a_supported_citation_becomes_a_diagnosis(database: db.Database) -> None:
    signal_id = await evidence(database, at=datetime.now(UTC) - timedelta(minutes=1))
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([turn(use(SUBMIT_TOOL, cited(signal_id)))])

    await run_once(database, engine_with(database, provider))

    diagnosis = await diagnosis_of(database, investigation_id)
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.service_name == "payment"
    assert diagnosis.root_cause.evidence_signal_ids == (signal_id,)


@pytest.mark.parametrize("problem", ["absent", "other_run", "after_cutoff", "arrived_late"])
async def test_an_unsupported_citation_is_refused_then_abstains(
    database: db.Database, problem: str
) -> None:
    now = datetime.now(UTC)
    if problem == "absent":
        signal_id = uuid.uuid4()
    elif problem == "other_run":
        replay = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(uuid.uuid4()))
        signal_id = await evidence(database, at=now - timedelta(minutes=1), scope=replay)
    elif problem == "after_cutoff":
        signal_id = await evidence(database, at=now + timedelta(minutes=5))
    else:
        signal_id = await evidence(
            database, at=now - timedelta(minutes=1), ingested=now + timedelta(minutes=5)
        )
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([turn(use(SUBMIT_TOOL, cited(signal_id)))])

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == LIMITS.max_repairs + 1
    assert "not signals known by the cutoff" in str(provider.requests[1].messages[-1].content)
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE


async def test_a_service_outside_the_workspace_is_refused(database: db.Database) -> None:
    signal_id = await evidence(database, at=datetime.now(UTC) - timedelta(minutes=1))
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted(
        [turn(use(SUBMIT_TOOL, cited(signal_id, service=uuid.uuid4())))]
    )

    await run_once(database, engine_with(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE


@pytest.mark.parametrize(
    "change",
    [
        {"evidence_signal_ids": []},
        {"confidence": 1.5},
        {"confidence": -0.1},
    ],
)
async def test_missing_citations_and_bad_confidence_fail_validation(
    database: db.Database, change: dict[str, Any]
) -> None:
    signal_id = await evidence(database, at=datetime.now(UTC) - timedelta(minutes=1))
    investigation_id = await new_job(database)
    proposal = cited(signal_id)
    proposal["root_cause"] |= change
    provider = FakeProvider.scripted([turn(use(SUBMIT_TOOL, proposal))])

    await run_once(database, engine_with(database, provider))

    assert "Invalid findings" in str(provider.requests[1].messages[-1].content)
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE


async def test_a_stored_proposal_is_verified_again_at_the_conclusion(
    database: db.Database,
) -> None:
    investigation_id = await new_job(database)
    forged = ProposedDiagnosis.model_validate(cited(uuid.uuid4()))
    context = NodeContext(database, await run_of(database, investigation_id))
    context.keep(INVESTIGATE_AGENT, forged)

    diagnosis = await conclude_findings(context)

    assert diagnosis.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE
    assert diagnosis.root_cause is None


async def run_of(database: db.Database, investigation_id: uuid.UUID) -> WorkflowRun:
    return WorkflowRun.of(await row_of(database, investigation_id))


async def test_a_worker_with_no_runtime_fails_rather_than_guessing(database: db.Database) -> None:
    investigation_id = await new_job(database)

    await run_once(database, WorkflowEngine(database))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.FAILED
