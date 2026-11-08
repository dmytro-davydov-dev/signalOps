"""Contradictions stay visible, and the verifier gets at most one follow-up round."""

import json
from typing import Any

import pytest
from sqlalchemy import delete

from signalops.agents import multi_agent
from signalops.agents.investigator import AgentRuntime
from signalops.agents.multi_agent import Room, room_left
from signalops.agents.tools import TextRedactor, ToolExecutor
from signalops.agents.workflow import WorkflowEngine
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.roles import (
    AgentEnding,
    FollowUpOutput,
    SynthesisOutput,
    Verdict,
    VerificationBrief,
    VerificationOutput,
)
from signalops.contracts.workflow import AgentRole, InvestigationStatus
from signalops.gateway.fake import FAKE_MODEL, FakeAnswer, FakeProvider
from signalops.gateway.fake_roles import synthesizer_step, verifier_step
from signalops.gateway.gateway import GatewayPolicy, ModelGateway
from signalops.gateway.types import ModelRequest, StopReason, ToolUseBlock
from signalops.persistence import database as db
from signalops.persistence.models import Job
from signalops.streaming.retry import RetryPolicy
from tests.integration.multi_agent_support import (
    V3_LIMITS,
    brief_of,
    queued_v3,
    roles_with,
    steps_by_key,
    timeout_incident,
    v3_engine,
)
from tests.integration.test_investigator import SERVICE_IDS, row_of, run_once

pytestmark = pytest.mark.integration

ASK_AGAIN = {"role": "telemetry_investigator", "question": "Look again at the leading service."}


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


def _input_of(answer: FakeAnswer) -> dict[str, Any]:
    block = answer.content[0]
    assert isinstance(block, ToolUseBlock)
    loaded: dict[str, Any] = json.loads(json.dumps(block.input))
    return loaded


def _submit(name: str, payload: dict[str, Any]) -> FakeAnswer:
    block = ToolUseBlock(id="toolu_v", name=name, input=payload)
    return FakeAnswer(content=(block,), stop_reason=StopReason.TOOL_USE)


def asking_verifier(*, always: bool) -> Any:
    """Asks for a follow-up whenever offered; with `always`, also when it is not."""

    def respond(request: ModelRequest) -> FakeAnswer:
        brief = VerificationBrief.model_validate(brief_of(request))
        honest = _input_of(verifier_step(request))
        if brief.follow_up_allowed or always:
            return _submit(
                "submit_verification",
                {**honest, "verdict": "needs_follow_up", "follow_up": ASK_AGAIN},
            )
        return _submit("submit_verification", honest)

    return respond


def verifier_requests(provider: FakeProvider) -> list[ModelRequest]:
    return [r for r in provider.requests if r.prompt_version == "verifier-v2"]


async def test_specialists_that_disagree_leave_a_visible_contradiction(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9301)
    investigation_id = await queued_v3(database, scope, incident_id)

    def silent_synthesizer(request: ModelRequest) -> FakeAnswer:
        # A synthesizer that merges the disagreement away.
        return _submit(
            "submit_synthesis", {**_input_of(synthesizer_step(request)), "contradictions": []}
        )

    provider = FakeProvider(responder=roles_with({"synthesizer-v1": silent_synthesizer}))
    await run_once(database, v3_engine(database, provider))

    diagnosis = Diagnosis.model_validate((await row_of(database, investigation_id)).diagnosis)
    assert diagnosis.status is DiagnosisStatus.DIAGNOSED
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.service_id == SERVICE_IDS["bank-gateway"]
    [conflict] = diagnosis.contradictions
    assert conflict.roles == (
        AgentRole.TELEMETRY_INVESTIGATOR,
        AgentRole.DEPLOYMENT_INVESTIGATOR,
    )
    assert "bank-gateway" in conflict.description
    assert "inventory" in conflict.description
    assert conflict.evidence_signal_ids
    steps = await steps_by_key(database, investigation_id)
    synthesis = SynthesisOutput.model_validate(steps["synthesize"].output)
    assert synthesis.report.contradictions == ()
    assert synthesis.detected_contradictions == diagnosis.contradictions
    # The verifier saw it too.
    [request] = verifier_requests(provider)
    assert VerificationBrief.model_validate(brief_of(request)).contradictions


async def test_an_abstention_still_shows_the_contradiction(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9302)
    investigation_id = await queued_v3(database, scope, incident_id)

    def rejecting(request: ModelRequest) -> FakeAnswer:
        honest = _input_of(verifier_step(request))
        for check in honest["citation_checks"]:
            check["supports"] = False
        return _submit(
            "submit_verification",
            {**honest, "verdict": "unsupported", "missing_evidence": ["Nothing holds up"]},
        )

    provider = FakeProvider(responder=roles_with({"verifier-v2": rejecting}))
    await run_once(database, v3_engine(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    diagnosis = Diagnosis.model_validate(row.diagnosis)
    assert diagnosis.root_cause is None
    assert diagnosis.contradictions
    # Rejected without asking for a follow-up, so none ran.
    steps = await steps_by_key(database, investigation_id)
    assert FollowUpOutput.model_validate(steps["follow_up"].output).ran is False


async def test_one_follow_up_round_runs_then_a_final_verdict(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9303)
    investigation_id = await queued_v3(database, scope, incident_id)
    provider = FakeProvider(responder=roles_with({"verifier-v2": asking_verifier(always=False)}))

    await run_once(database, v3_engine(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    steps = await steps_by_key(database, investigation_id)
    first = VerificationOutput.model_validate(steps["verify"].output)
    assert first.follow_up_allowed
    assert first.report.verdict is Verdict.NEEDS_FOLLOW_UP
    round_ = FollowUpOutput.model_validate(steps["follow_up"].output)
    assert round_.ran
    assert round_.findings is not None
    assert round_.findings.role is AgentRole.TELEMETRY_INVESTIGATOR
    assert round_.findings.provenance.ended_by is AgentEnding.SUBMITTED
    second = VerificationOutput.model_validate(steps["reverify"].output)
    assert not second.follow_up_allowed
    assert second.report.verdict is Verdict.SUPPORTED

    # The follow-up specialist got its question; the second verifier got its findings.
    telemetry = [r for r in provider.requests if r.prompt_version == "telemetry-v1"]
    questions = [brief_of(r)["question"] for r in telemetry if r.messages[0] is r.messages[-1]]
    assert questions == [None, ASK_AGAIN["question"]]
    first_brief, second_brief = (
        VerificationBrief.model_validate(brief_of(r)) for r in verifier_requests(provider)
    )
    assert first_brief.follow_up_allowed
    assert first_brief.follow_up is None
    assert not second_brief.follow_up_allowed
    assert second_brief.follow_up is not None
    diagnosis = Diagnosis.model_validate(row.diagnosis)
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.service_id == SERVICE_IDS["bank-gateway"]
    assert "One follow-up round ran before the final verdict" in diagnosis.limitations


async def test_a_verifier_that_always_asks_cannot_start_a_second_round(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9304)
    investigation_id = await queued_v3(database, scope, incident_id)
    provider = FakeProvider(responder=roles_with({"verifier-v2": asking_verifier(always=True)}))

    await run_once(database, v3_engine(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    steps = await steps_by_key(database, investigation_id)
    assert FollowUpOutput.model_validate(steps["follow_up"].output).ran
    second = VerificationOutput.model_validate(steps["reverify"].output)
    # Its second request for a round was refused until its repairs ran out.
    assert second.provenance.ended_by is AgentEnding.REPAIR_LIMIT
    assert second.report.verdict is Verdict.UNSUPPORTED
    # One first verification, then 1 + max_repairs tries at the final one.
    assert len(verifier_requests(provider)) == 1 + 1 + V3_LIMITS.max_repairs
    assert "not available" in str(verifier_requests(provider)[-1].messages[-1])
    telemetry_rounds = [
        r for r in provider.requests if r.prompt_version == "telemetry-v1" and len(r.messages) == 1
    ]
    assert len(telemetry_rounds) == 2  # the branch and the one follow-up


async def test_no_round_is_offered_when_the_deadline_is_too_close(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9305)
    from datetime import timedelta

    # A round needs two calls at the 60-second timeout: 120 seconds. Only 90 are left.
    investigation_id = await queued_v3(database, scope, incident_id, deadline=timedelta(seconds=90))
    provider = FakeProvider(responder=roles_with({"verifier-v2": asking_verifier(always=False)}))
    slow = GatewayPolicy(retry=RetryPolicy(2, 0.001, 0.001), request_timeout_seconds=60)
    runtime = AgentRuntime(
        gateway=ModelGateway(database, provider, policy=slow),
        tools=ToolExecutor(database, TextRedactor(("password",))),
        model=FAKE_MODEL,
        limits=V3_LIMITS,
    )

    await run_once(database, WorkflowEngine(database, runtime=runtime))

    steps = await steps_by_key(database, investigation_id)
    first = VerificationOutput.model_validate(steps["verify"].output)
    assert not first.follow_up_allowed
    assert first.report.verdict is Verdict.SUPPORTED
    assert FollowUpOutput.model_validate(steps["follow_up"].output).reason == (
        "The verifier gave a final verdict"
    )
    [request] = verifier_requests(provider)
    assert not VerificationBrief.model_validate(brief_of(request)).follow_up_allowed
    assert (await row_of(database, investigation_id)).status == InvestigationStatus.COMPLETED


async def test_a_round_that_no_longer_fits_does_not_run(
    database: db.Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    scope, incident_id = await timeout_incident(database, 9306)
    investigation_id = await queued_v3(database, scope, incident_id)
    real = multi_agent.room_left
    reads = {"count": 0}

    async def shrinking(database_: db.Database, investigation_id_: Any) -> Room:
        reads["count"] += 1
        room = await real(database_, investigation_id_)
        # Room at the verification; none by the time the round would start.
        return room if reads["count"] == 1 else Room(0, room.seconds)

    monkeypatch.setattr(multi_agent, "room_left", shrinking)
    provider = FakeProvider(responder=roles_with({"verifier-v2": asking_verifier(always=False)}))
    await run_once(database, v3_engine(database, provider))

    steps = await steps_by_key(database, investigation_id)
    round_ = FollowUpOutput.model_validate(steps["follow_up"].output)
    assert not round_.ran
    assert round_.reason is not None
    assert "no room" in round_.reason
    assert VerificationOutput.model_validate(steps["reverify"].output).provenance.ended_by is (
        AgentEnding.SKIPPED
    )
    row = await row_of(database, investigation_id)
    # Asked for a round that could not run: no final verdict, so it abstains.
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    assert row.diagnosis is not None
    assert any("no room" in item for item in row.diagnosis["missing_evidence"])


async def test_room_left_reads_the_budget_and_deadline_from_postgresql(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9307)
    investigation_id = await queued_v3(database, scope, incident_id, budget=1_234_567)

    room = await room_left(database, investigation_id)

    assert room.budget_microusd == 1_234_567
    assert 500 < room.seconds <= 600
