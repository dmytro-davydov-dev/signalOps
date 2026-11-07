"""What failed model calls leave behind.

Several tests here assert accounting for calls that time out. A provider may have run
and billed such a call before the client gave up, and nothing on this side can tell,
so these tests assert conservative records. They do not show that no call happened.
"""

import asyncio
import json
import logging
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import anthropic
import httpx2
import pytest
from sqlalchemy import delete, select, update

from signalops.agents.investigations import cancel_investigation
from signalops.agents.investigator import AgentLimits, AgentRuntime
from signalops.agents.jobs import JobPolicy, JobRunner, claim_job
from signalops.agents.tools import TextRedactor, ToolExecutor
from signalops.agents.workflow import WorkflowEngine
from signalops.contracts.diagnosis import Diagnosis
from signalops.contracts.evidence import NormalizedSignal, QualityStatus, Severity, SignalKind
from signalops.contracts.steps import SUBMIT_TOOL
from signalops.contracts.workflow import (
    CostBasis,
    InvestigationStatus,
    JobStatus,
    LlmCallStatus,
    StepStatus,
    ToolName,
)
from signalops.gateway.anthropic_provider import AnthropicProvider
from signalops.gateway.errors import ProviderErrorCategory
from signalops.gateway.fake import FAKE_MODEL, FakeAnswer, FakeFailure, FakeProvider, FakeStep
from signalops.gateway.gateway import GatewayPolicy, ModelGateway
from signalops.gateway.pricing import PRICES, ModelPrice
from signalops.gateway.provider import ModelProvider
from signalops.gateway.types import StopReason, ToolUseBlock
from signalops.logs import JsonFormatter, configure_logging
from signalops.persistence import database as db
from signalops.persistence.models import (
    AgentStep,
    Investigation,
    Job,
    LlmCall,
    ProgressEvent,
    ToolCall,
)
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.jobs import JobLease
from signalops.persistence.repositories.llm_calls import LlmCallRepository, UsageTotals
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES
from signalops.streaming.retry import RetryPolicy
from tests.integration.job_support import LIVE, add_job

pytestmark = pytest.mark.integration

SECRET = "hunter2-do-not-log"  # noqa: S105 - a marker to search logs for
API_KEY = "sk-test-credential-do-not-log"
EVIDENCE_TEXT = "customer jane@example.com card declined"
JOBS = JobPolicy(
    lease_seconds=30,
    poll_interval_seconds=0.02,
    retry=RetryPolicy(base_delay_seconds=0.001, max_delay_seconds=0.001),
)
RETRIES = GatewayPolicy(retry=RetryPolicy(2, 0.001, 0.001), request_timeout_seconds=5)
LIMITS = AgentLimits(max_output_tokens=1_000, effort=None, max_repairs=2, max_tool_calls=4)
ABSTAIN: dict[str, Any] = {
    "status": "insufficient_evidence",
    "summary": "Nothing shows a cause.",
    "missing_evidence": ["Traces for the window"],
}
READ_LOGS = FakeAnswer(
    content=(ToolUseBlock(id="toolu_1", name=ToolName.QUERY_LOGS.value, input={"limit": 5}),),
    stop_reason=StopReason.TOOL_USE,
)
SUBMIT = FakeAnswer(
    content=(ToolUseBlock(id="toolu_2", name=SUBMIT_TOOL, input=ABSTAIN),),
    stop_reason=StopReason.TOOL_USE,
)
TIMEOUT = FakeFailure(fail=ProviderErrorCategory.TIMEOUT)


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


@pytest.fixture
def captured() -> Iterator[list[logging.LogRecord]]:
    """Records from every logger at DEBUG, through the production configuration."""
    records: list[logging.LogRecord] = []

    class Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    configure_logging("DEBUG")
    root.handlers[:] = [Keep()]
    yield records
    root.handlers[:] = handlers
    root.setLevel(level)


def rendered(records: list[logging.LogRecord]) -> str:
    formatter = JsonFormatter()
    return "\n".join(formatter.format(record) for record in records)


def engine_with(
    database: db.Database, provider: ModelProvider, *, model: str = FAKE_MODEL
) -> WorkflowEngine:
    runtime = AgentRuntime(
        gateway=ModelGateway(database, provider, policy=RETRIES),
        tools=ToolExecutor(database, TextRedactor(("password",))),
        model=model,
        limits=LIMITS,
    )
    return WorkflowEngine(database, runtime=runtime)


async def new_job(database: db.Database, *, budget: int = 5_000_000) -> uuid.UUID:
    investigation_id = await add_job(database, workflow_version="v2", max_steps=10)
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == investigation_id)
            .values(budget_microusd=budget)
        )
    return investigation_id


async def run_once(database: db.Database, engine: WorkflowEngine, policy: JobPolicy = JOBS) -> None:
    lease = await claim_job(database, "worker-a", policy)
    assert lease is not None
    await JobRunner(database, engine, policy, owner=lease.owner).execute(lease, asyncio.Event())


async def row_of(database: db.Database, investigation_id: uuid.UUID) -> Investigation:
    async with database.transaction() as session:
        return await session.get_one(Investigation, investigation_id)


async def calls_of(database: db.Database, investigation_id: uuid.UUID) -> list[LlmCall]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(LlmCall)
            .join(AgentStep, AgentStep.id == LlmCall.agent_step_id)
            .where(AgentStep.investigation_id == investigation_id)
            .order_by(LlmCall.attempt)
        )
        return list(rows)


async def store_sensitive_log(database: db.Database) -> None:
    at = datetime.now(UTC) - timedelta(minutes=1)
    signal = NormalizedSignal(
        id=uuid.uuid4(),
        workspace_id=LIVE.workspace_id,
        source_id=DEMO_SOURCES[0].id,
        service_id=DEMO_SERVICES[0].id,
        source_event_id=f"failure-{uuid.uuid4()}",
        replay_scope="live",
        kind=SignalKind.LOG,
        occurred_at=at,
        ingested_at=at,
        severity=Severity.ERROR,
        message=f"{EVIDENCE_TEXT} password={SECRET}",
        attributes={},
        quality_status=QualityStatus.ACCEPTED,
        quality_flags=(),
    )
    async with database.transaction() as session:
        assert await EvidenceRepository(session).insert_signal(signal)


async def stored_text(database: db.Database, investigation_id: uuid.UUID) -> str:
    """Everything durable an investigation left that a UI or an operator could read."""
    async with database.transaction() as session:
        investigation = await session.get_one(Investigation, investigation_id)
        job = (
            await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
        ).one()
        steps = list(
            await session.scalars(
                select(AgentStep).where(AgentStep.investigation_id == investigation_id)
            )
        )
        progress = list(
            await session.scalars(
                select(ProgressEvent).where(ProgressEvent.investigation_id == investigation_id)
            )
        )
        step_ids = [step.id for step in steps]
        tools = list(
            await session.scalars(select(ToolCall).where(ToolCall.agent_step_id.in_(step_ids)))
        )
        llm = list(
            await session.scalars(select(LlmCall).where(LlmCall.agent_step_id.in_(step_ids)))
        )
    return json.dumps(
        [
            investigation.diagnosis,
            job.last_error,
            [(step.input, step.output, step.checkpoint) for step in steps],
            [event.payload for event in progress],
            [(call.arguments, call.result_ref) for call in tools],
            [(call.provider_request_id, call.error_category, call.request_hash) for call in llm],
        ],
        default=str,
    )


def anthropic_stub(handler: Any) -> AnthropicProvider:
    return AnthropicProvider(
        anthropic.AsyncAnthropic(
            api_key=API_KEY,
            max_retries=0,
            http_client=anthropic.DefaultAsyncHttpxClient(transport=httpx2.MockTransport(handler)),
        )
    )


@pytest.mark.parametrize(
    "steps",
    [
        [TIMEOUT, READ_LOGS, TIMEOUT, TIMEOUT],
        [FakeFailure(fail=ProviderErrorCategory.AUTHENTICATION)],
        [READ_LOGS, SUBMIT],
    ],
    ids=["timeouts", "rejected", "success"],
)
async def test_no_evidence_or_prompt_reaches_logs_or_stored_records(
    database: db.Database, captured: list[logging.LogRecord], steps: list[FakeStep]
) -> None:
    await store_sensitive_log(database)
    investigation_id = await new_job(database)

    await run_once(database, engine_with(database, FakeProvider.scripted(steps)))

    logs = rendered(captured)
    stored = await stored_text(database, investigation_id)
    assert "model call" in logs
    for text in (SECRET, EVIDENCE_TEXT):
        assert text not in logs
        assert text not in stored
    assert "You investigate" not in logs


async def test_the_real_adapter_leaks_no_key_or_payload_at_debug(
    database: db.Database, captured: list[logging.LogRecord]
) -> None:
    PRICES["anthropic", "test-model"] = ModelPrice(1, 1, 1, 1)
    await store_sensitive_log(database)
    investigation_id = await new_job(database)
    seen: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen.append(request)
        return httpx2.Response(
            500,
            json={"type": "error", "error": {"type": "api_error", "message": EVIDENCE_TEXT}},
            headers={"request-id": "req_failed"},
        )

    try:
        await run_once(database, engine_with(database, anthropic_stub(handler), model="test-model"))
    finally:
        del PRICES["anthropic", "test-model"]

    assert seen
    logs = rendered(captured)
    stored = await stored_text(database, investigation_id)
    for text in (API_KEY, SECRET, EVIDENCE_TEXT, "You investigate"):
        assert text not in logs
        assert text not in stored
    assert "req_failed" in logs


async def test_a_failing_node_logs_the_error_type_and_not_its_text(
    database: db.Database, captured: list[logging.LogRecord]
) -> None:
    investigation_id = await add_job(database)
    lease = await claim_job(database, "worker-a", JOBS)
    assert lease is not None

    async def handler(_lease: JobLease) -> Diagnosis:
        raise ValueError(f"input_value={EVIDENCE_TEXT}")

    await JobRunner(database, handler, JOBS, owner=lease.owner).execute(lease, asyncio.Event())

    logs = rendered(captured)
    assert "ValueError" in logs
    assert EVIDENCE_TEXT not in logs
    assert EVIDENCE_TEXT not in await stored_text(database, investigation_id)


async def test_a_timed_out_call_may_have_run_so_its_cost_stays_unknown(
    database: db.Database,
) -> None:
    # The provider receives this call and is still working when the client gives up.
    # It may finish and bill. The record must not claim otherwise.
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted(
        [FakeAnswer(content=(), delay_seconds=30), SUBMIT],
    )
    gateway = ModelGateway(
        database,
        provider,
        policy=GatewayPolicy(retry=RetryPolicy(2, 0.001, 0.001), request_timeout_seconds=0.1),
    )
    runtime = AgentRuntime(
        gateway,
        ToolExecutor(database, TextRedactor(("password",))),
        FAKE_MODEL,
        LIMITS,
    )

    await run_once(database, WorkflowEngine(database, runtime=runtime))

    assert len(provider.requests) == 2  # The first reached the provider.
    first, second = await calls_of(database, investigation_id)
    assert first.status == LlmCallStatus.UNKNOWN
    assert first.cost_basis == CostBasis.UNKNOWN
    assert first.cost_microusd is None
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    assert row.reserved_microusd == first.reserved_microusd
    assert row.spent_microusd == second.cost_microusd


async def test_usage_totals_separate_known_cost_from_unknown_calls(
    database: db.Database,
) -> None:
    investigation_id = await new_job(database)

    await run_once(database, engine_with(database, FakeProvider.scripted([TIMEOUT, SUBMIT])))

    unknown, succeeded = await calls_of(database, investigation_id)
    async with database.transaction() as session:
        totals = await LlmCallRepository(session).usage_of(investigation_id)
    assert totals == UsageTotals(
        model_calls=2,
        input_tokens=succeeded.input_tokens or 0,
        output_tokens=succeeded.output_tokens or 0,
        estimated_microusd=succeeded.cost_microusd or 0,
        unknown_calls=1,
        unknown_reserved_microusd=unknown.reserved_microusd,
    )


async def test_a_model_timeout_resumes_on_the_next_attempt_with_earlier_steps_reused(
    database: db.Database,
) -> None:
    # Every timed-out call below may have been billed; each keeps its reservation.
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([TIMEOUT, TIMEOUT, SUBMIT])
    engine = engine_with(database, provider)

    await run_once(database, engine)

    async with database.transaction() as session:
        job = (
            await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
        ).one()
        assert job.status == JobStatus.QUEUED
        assert job.last_error is not None
        assert "timeout" in job.last_error["detail"]
    await asyncio.sleep(0.01)

    await run_once(database, engine)

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    calls = await calls_of(database, investigation_id)
    assert [call.status for call in calls] == [
        LlmCallStatus.UNKNOWN,
        LlmCallStatus.UNKNOWN,
        LlmCallStatus.SUCCEEDED,
    ]
    assert row.reserved_microusd == calls[0].reserved_microusd + calls[1].reserved_microusd
    async with database.transaction() as session:
        steps = {
            step.step_key: step
            for step in await session.scalars(
                select(AgentStep).where(AgentStep.investigation_id == investigation_id)
            )
        }
    assert steps["load_context"].attempt == 1
    assert steps["investigate"].attempt == 2
    assert steps["investigate"].status == StepStatus.COMPLETED


async def test_retries_that_never_succeed_fail_the_job_with_reservations_held(
    database: db.Database,
) -> None:
    investigation_id = await new_job(database)
    engine = engine_with(database, FakeProvider.scripted([TIMEOUT]))

    for _ in range(3):
        await run_once(database, engine)
        await asyncio.sleep(0.01)

    row = await row_of(database, investigation_id)
    calls = await calls_of(database, investigation_id)
    assert row.status == InvestigationStatus.FAILED
    assert len(calls) == 6
    assert row.spent_microusd == 0
    assert row.reserved_microusd == sum(call.reserved_microusd for call in calls)


@pytest.mark.parametrize(
    ("category", "status"),
    [
        (ProviderErrorCategory.REJECTED, InvestigationStatus.FAILED),
        (ProviderErrorCategory.AUTHENTICATION, InvestigationStatus.FAILED),
    ],
)
async def test_a_permanent_error_ends_the_job_on_the_first_call(
    database: db.Database, category: ProviderErrorCategory, status: InvestigationStatus
) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([FakeFailure(fail=category)])

    await run_once(database, engine_with(database, provider))

    assert len(provider.requests) == 1
    row = await row_of(database, investigation_id)
    assert row.status == status
    assert row.reserved_microusd == 0
    async with database.transaction() as session:
        job = (
            await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
        ).one()
    assert job.attempts == 1


async def test_a_cancellation_during_a_hung_call_keeps_its_reservation(
    database: db.Database,
) -> None:
    # The hung call may still complete and bill after the worker walks away.
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([FakeAnswer(content=(), delay_seconds=30)])
    fast = JobPolicy(
        lease_seconds=0.3,
        poll_interval_seconds=0.02,
        retry=RetryPolicy(base_delay_seconds=0.001, max_delay_seconds=0.001),
    )
    runner = asyncio.create_task(run_once(database, engine_with(database, provider), fast))
    while not provider.requests:  # noqa: ASYNC110 - watches another task's effect
        await asyncio.sleep(0.01)

    async with database.transaction() as session:
        await cancel_investigation(session, LIVE, investigation_id)
    await asyncio.wait_for(runner, timeout=5)

    [call] = await calls_of(database, investigation_id)
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.CANCELLED
    assert call.status == LlmCallStatus.UNKNOWN
    assert call.error_category == "abandoned"
    assert row.reserved_microusd == call.reserved_microusd


async def test_a_stale_workers_late_answer_cannot_change_a_closed_call_or_continue(
    database: db.Database,
) -> None:
    investigation_id = await new_job(database)
    provider = FakeProvider.scripted([FakeAnswer(content=(), delay_seconds=0.3)])
    engine = engine_with(database, provider)
    first = await claim_job(database, "worker-a", JOBS)
    assert first is not None
    running = asyncio.create_task(engine(first))
    while not provider.requests:  # noqa: ASYNC110 - watches another task's effect
        await asyncio.sleep(0.01)

    # The lease expires under the first worker, and a second worker takes the job.
    async with database.transaction() as session:
        await session.execute(
            update(Job)
            .where(Job.id == first.job_id)
            .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
        )
    second = await claim_job(database, "worker-b", JOBS)
    assert second is not None
    async with database.transaction() as session:
        await LlmCallRepository(session).close_abandoned(
            investigation_id, before_generation=second.generation
        )

    await asyncio.gather(running, return_exceptions=True)

    [call] = await calls_of(database, investigation_id)
    assert call.status == LlmCallStatus.UNKNOWN
    assert call.lease_generation == first.generation
    row = await row_of(database, investigation_id)
    assert row.reserved_microusd == call.reserved_microusd
    assert row.spent_microusd == 0
