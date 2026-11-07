"""Budget reservation, settlement, retries, and limits on real PostgreSQL and Redis."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import delete, select, update

from signalops.agents.investigations import cancel_investigation
from signalops.agents.jobs import (
    JobPolicy,
    JobRunner,
    JobStoppedError,
    PermanentJobError,
    TransientJobError,
    claim_job,
)
from signalops.config import Settings
from signalops.contracts.diagnosis import Diagnosis
from signalops.contracts.workflow import (
    AgentRole,
    CostBasis,
    InvestigationStatus,
    JobStatus,
    LlmCallStatus,
)
from signalops.gateway.errors import BudgetExceededError, ProviderErrorCategory
from signalops.gateway.fake import FAKE_MODEL, FakeAnswer, FakeFailure, FakeProvider, FakeStep
from signalops.gateway.gateway import GatewayPolicy, ModelGateway
from signalops.gateway.limits import UNAVAILABLE_RETRY_SECONDS, RateLimits, RedisRateLimiter
from signalops.gateway.pricing import maximum_cost, price_of
from signalops.gateway.types import ModelMessage, ModelRequest, ModelRole, TextBlock
from signalops.persistence import database as db
from signalops.persistence.models import Investigation, Job, LlmCall
from signalops.persistence.repositories.jobs import JobLease, JobRepository, LeaseLostError
from signalops.persistence.repositories.llm_calls import LlmCallRepository, NewLlmCall
from signalops.streaming.retry import RetryPolicy
from tests.integration.job_support import ABSTENTION, LIVE, add_job

pytestmark = pytest.mark.integration

STEP = "investigate"
OK = FakeAnswer(content=(TextBlock(text="done"),))
JOBS = JobPolicy(
    lease_seconds=30,
    poll_interval_seconds=0.02,
    retry=RetryPolicy(base_delay_seconds=0.001, max_delay_seconds=0.001),
)
RETRIES = GatewayPolicy(retry=RetryPolicy(3, 0.001, 0.001), request_timeout_seconds=5)
REQUEST = ModelRequest(
    model=FAKE_MODEL,
    prompt_version="test-v1",
    system="You investigate.",
    messages=(ModelMessage(role=ModelRole.USER, content=(TextBlock(text="Why?"),)),),
    max_output_tokens=100,
)
MAX_COST = maximum_cost(REQUEST, price_of("fake", FAKE_MODEL))


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


async def running_step(
    database: db.Database, *, budget: int = 100 * MAX_COST
) -> tuple[uuid.UUID, JobLease]:
    investigation_id = await add_job(database)
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == investigation_id)
            .values(budget_microusd=budget)
        )
    lease = await claim_job(database, "worker-a", JOBS)
    assert lease is not None
    await open_step(database, lease)
    return investigation_id, lease


async def open_step(database: db.Database, lease: JobLease) -> None:
    async with database.transaction() as session:
        assert await JobRepository(session).save_checkpoint(
            lease, STEP, AgentRole.INVESTIGATOR, {}, {}
        )


def gateway(database: db.Database, *steps: FakeStep) -> tuple[ModelGateway, FakeProvider]:
    provider = FakeProvider.scripted(steps)
    return ModelGateway(database, provider, policy=RETRIES), provider


async def calls_of(database: db.Database, investigation_id: uuid.UUID) -> list[LlmCall]:
    async with database.transaction() as session:
        step = await LlmCallRepository(session).step_id(investigation_id, STEP)
        rows = await session.scalars(
            select(LlmCall).where(LlmCall.agent_step_id == step).order_by(LlmCall.attempt)
        )
        return list(rows)


async def budget_of(database: db.Database, investigation_id: uuid.UUID) -> tuple[int, int]:
    async with database.transaction() as session:
        row = await session.get_one(Investigation, investigation_id)
        return row.reserved_microusd, row.spent_microusd


async def test_a_success_replaces_its_reservation_with_its_cost(database: db.Database) -> None:
    investigation_id, lease = await running_step(database)
    model, _provider = gateway(database, OK)

    response = await model.complete(lease, STEP, REQUEST)

    [call] = await calls_of(database, investigation_id)
    assert call.status == LlmCallStatus.SUCCEEDED
    assert call.cost_basis == CostBasis.ESTIMATED
    assert call.reserved_microusd == MAX_COST
    assert call.input_tokens == response.usage.input_tokens
    assert call.output_tokens == response.usage.output_tokens
    assert call.provider_request_id == response.provider_request_id
    assert call.pricing_version
    assert call.prompt_version == "test-v1"
    assert call.request_hash == REQUEST.fingerprint()
    assert call.lease_generation == lease.generation
    assert call.latency_ms is not None
    assert call.completed_at is not None
    assert call.cost_microusd is not None
    assert 0 < call.cost_microusd <= MAX_COST
    assert await budget_of(database, investigation_id) == (0, call.cost_microusd)


async def test_parallel_calls_cannot_oversubscribe_the_budget(database: db.Database) -> None:
    investigation_id, lease = await running_step(database, budget=5 * MAX_COST)
    held = FakeAnswer(content=(TextBlock(text="done"),), delay_seconds=0.5)
    model, provider = gateway(database, held)

    results = await asyncio.gather(
        *(model.complete(lease, STEP, REQUEST) for _ in range(12)), return_exceptions=True
    )

    refused = [result for result in results if isinstance(result, BudgetExceededError)]
    assert len(refused) == 7
    assert len(provider.requests) == 5
    calls = await calls_of(database, investigation_id)
    assert sorted(call.attempt for call in calls) == [1, 2, 3, 4, 5]
    assert all(call.status == LlmCallStatus.SUCCEEDED for call in calls)


async def test_concurrent_holds_stop_at_the_budget_without_a_job_lock(
    database: db.Database,
) -> None:
    investigation_id, _lease = await running_step(database, budget=1_000)

    async def hold() -> bool:
        async with database.transaction() as session:
            return await LlmCallRepository(session).hold_budget(investigation_id, 300)

    held = await asyncio.gather(*(hold() for _ in range(8)))

    assert sum(held) == 3
    assert await budget_of(database, investigation_id) == (900, 0)


async def test_a_call_is_not_made_when_its_maximum_does_not_fit(database: db.Database) -> None:
    investigation_id, lease = await running_step(database, budget=MAX_COST - 1)
    model, provider = gateway(database, OK)

    with pytest.raises(BudgetExceededError):
        await model.complete(lease, STEP, REQUEST)

    assert provider.requests == []
    assert await calls_of(database, investigation_id) == []
    assert await budget_of(database, investigation_id) == (0, 0)


async def test_an_ambiguous_timeout_keeps_its_reservation_and_the_retry_reserves_again(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database)
    model, provider = gateway(database, FakeFailure(fail=ProviderErrorCategory.TIMEOUT), OK)

    await model.complete(lease, STEP, REQUEST)

    unknown, succeeded = await calls_of(database, investigation_id)
    assert len(provider.requests) == 2
    assert unknown.status == LlmCallStatus.UNKNOWN
    assert unknown.cost_basis == CostBasis.UNKNOWN
    assert unknown.cost_microusd is None
    assert unknown.error_category == ProviderErrorCategory.TIMEOUT
    assert succeeded.status == LlmCallStatus.SUCCEEDED
    assert succeeded.attempt == 2
    assert await budget_of(database, investigation_id) == (MAX_COST, succeeded.cost_microusd)


async def test_a_retry_that_no_longer_fits_ends_with_the_reservation_held(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database, budget=MAX_COST + 1)
    model, provider = gateway(database, FakeFailure(fail=ProviderErrorCategory.TIMEOUT), OK)

    with pytest.raises(BudgetExceededError):
        await model.complete(lease, STEP, REQUEST)

    assert len(provider.requests) == 1
    assert await budget_of(database, investigation_id) == (MAX_COST, 0)


async def test_a_call_that_outlasts_its_timeout_is_ambiguous(database: db.Database) -> None:
    investigation_id, lease = await running_step(database)
    provider = FakeProvider.scripted([FakeAnswer(content=(), delay_seconds=30)])
    model = ModelGateway(
        database,
        provider,
        policy=GatewayPolicy(retry=RetryPolicy(1, 0.001, 0.001), request_timeout_seconds=0.05),
    )

    with pytest.raises(TransientJobError, match="timeout"):
        await model.complete(lease, STEP, REQUEST)

    [call] = await calls_of(database, investigation_id)
    assert call.status == LlmCallStatus.UNKNOWN
    assert await budget_of(database, investigation_id) == (MAX_COST, 0)


async def test_a_throttled_call_releases_its_reservation_and_waits(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database)
    model, _provider = gateway(
        database,
        FakeFailure(fail=ProviderErrorCategory.THROTTLED, retry_after_seconds=0.05),
        OK,
    )
    loop = asyncio.get_running_loop()
    started = loop.time()

    await model.complete(lease, STEP, REQUEST)

    assert loop.time() - started >= 0.05
    failed, succeeded = await calls_of(database, investigation_id)
    assert failed.status == LlmCallStatus.FAILED
    assert (failed.cost_microusd, failed.cost_basis) == (0, CostBasis.ACTUAL)
    assert await budget_of(database, investigation_id) == (0, succeeded.cost_microusd)


async def test_a_rejected_call_is_permanent_and_is_not_retried(database: db.Database) -> None:
    investigation_id, lease = await running_step(database)
    model, provider = gateway(database, FakeFailure(fail=ProviderErrorCategory.AUTHENTICATION), OK)

    with pytest.raises(PermanentJobError, match="authentication"):
        await model.complete(lease, STEP, REQUEST)

    assert len(provider.requests) == 1
    [call] = await calls_of(database, investigation_id)
    assert call.status == LlmCallStatus.FAILED


async def test_exhausted_retries_raise_a_transient_job_error(database: db.Database) -> None:
    investigation_id, lease = await running_step(database)
    model, provider = gateway(database, FakeFailure(fail=ProviderErrorCategory.UNAVAILABLE))

    with pytest.raises(TransientJobError, match="unavailable"):
        await model.complete(lease, STEP, REQUEST)

    assert len(provider.requests) == 3
    assert await budget_of(database, investigation_id) == (3 * MAX_COST, 0)


async def test_no_call_starts_after_a_cancellation_commits(database: db.Database) -> None:
    investigation_id, lease = await running_step(database)
    async with database.transaction() as session:
        await cancel_investigation(session, LIVE, investigation_id)
    model, provider = gateway(database, OK)

    with pytest.raises(JobStoppedError):
        await model.complete(lease, STEP, REQUEST)

    assert provider.requests == []
    assert await calls_of(database, investigation_id) == []


async def test_a_stale_worker_cannot_start_a_call(database: db.Database) -> None:
    investigation_id, lease = await running_step(database)
    async with database.transaction() as session:
        await session.execute(
            update(Job).where(Job.id == lease.job_id).values(lease_generation=lease.generation + 1)
        )
    model, provider = gateway(database, OK)

    with pytest.raises(LeaseLostError):
        await model.complete(lease, STEP, REQUEST)

    assert provider.requests == []
    assert await calls_of(database, investigation_id) == []


async def add_pending(database: db.Database, investigation_id: uuid.UUID, generation: int) -> None:
    async with database.transaction() as session:
        calls = LlmCallRepository(session)
        assert await calls.hold_budget(investigation_id, MAX_COST)
        await calls.add(
            NewLlmCall(
                agent_step_id=await calls.step_id(investigation_id, STEP),
                provider="fake",
                model=FAKE_MODEL,
                prompt_version="test-v1",
                request_hash="abc",
                pricing_version="test",
                reserved_microusd=MAX_COST,
                lease_generation=generation,
            )
        )


async def test_a_later_lease_closes_a_dead_workers_call_as_unknown(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database)
    await add_pending(database, investigation_id, lease.generation - 1)
    model, _provider = gateway(database, OK)

    await model.complete(lease, STEP, REQUEST)

    dead, new = await calls_of(database, investigation_id)
    assert dead.status == LlmCallStatus.UNKNOWN
    assert dead.error_category == "abandoned"
    assert new.status == LlmCallStatus.SUCCEEDED
    assert await budget_of(database, investigation_id) == (MAX_COST, new.cost_microusd)


async def test_a_late_answer_does_not_reopen_a_closed_call(database: db.Database) -> None:
    investigation_id, lease = await running_step(database)
    await add_pending(database, investigation_id, lease.generation)
    [pending] = await calls_of(database, investigation_id)
    async with database.transaction() as session:
        calls = LlmCallRepository(session)
        await calls.close_abandoned(investigation_id)
        await calls.settle_failed(pending.id, "throttled", 1, None)

    [call] = await calls_of(database, investigation_id)
    assert call.status == LlmCallStatus.UNKNOWN
    assert await budget_of(database, investigation_id) == (MAX_COST, 0)


async def test_running_out_of_budget_ends_the_investigation_budget_exceeded(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database, budget=MAX_COST - 1)
    model, _provider = gateway(database, OK)

    async def handler(held: JobLease) -> Diagnosis:
        await model.complete(held, STEP, REQUEST)
        return ABSTENTION

    await JobRunner(database, handler, JOBS, owner=lease.owner).execute(lease, asyncio.Event())

    async with database.transaction() as session:
        investigation = await session.get_one(Investigation, investigation_id)
        job = await session.get_one(Job, lease.job_id)
    assert investigation.status == InvestigationStatus.BUDGET_EXCEEDED
    assert investigation.diagnosis is None
    assert job.status == JobStatus.FAILED
    assert job.last_error is not None
    assert job.last_error["category"] == "budget_exceeded"


async def test_a_call_interrupted_by_an_ending_stays_unknown_with_its_reservation(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database)
    await add_pending(database, investigation_id, lease.generation)
    async with database.transaction() as session:
        await cancel_investigation(session, LIVE, investigation_id)

    async def handler(_held: JobLease) -> Diagnosis:
        raise JobStoppedError(STEP)

    await JobRunner(database, handler, JOBS, owner=lease.owner).execute(lease, asyncio.Event())

    [call] = await calls_of(database, investigation_id)
    assert call.status == LlmCallStatus.UNKNOWN
    assert await budget_of(database, investigation_id) == (MAX_COST, 0)


@pytest.fixture
async def redis(infra_settings: Settings) -> AsyncIterator["Redis"]:
    client = Redis.from_url(str(infra_settings.redis_url))
    yield client
    await client.aclose()


async def test_redis_limits_requests_atomically_across_concurrent_callers(
    redis: "Redis",
) -> None:
    provider = f"test-{uuid.uuid4().hex[:8]}"
    limiter = RedisRateLimiter(redis, RateLimits(3, 1_000_000, max_wait_seconds=0.01))

    results = await asyncio.gather(
        *(limiter.acquire(provider, 10) for _ in range(10)), return_exceptions=True
    )

    assert sum(result is None for result in results) == 3
    assert all(isinstance(r, TransientJobError) for r in results if r is not None)


async def test_redis_limits_tokens_and_lets_an_oversized_call_have_an_empty_window(
    redis: "Redis",
) -> None:
    provider = f"test-{uuid.uuid4().hex[:8]}"
    limiter = RedisRateLimiter(redis, RateLimits(100, 1_000, max_wait_seconds=0.01))

    await limiter.acquire(provider, 5_000)
    with pytest.raises(TransientJobError):
        await limiter.acquire(provider, 1)


class Flaky:
    """Fails like an unreachable Redis until told to recover, then behaves like `client`."""

    def __init__(self, client: "Redis") -> None:
        self._client = client
        self.failures = 1

    async def eval(self, *args: object) -> object:
        if self.failures:
            self.failures -= 1
            raise RedisConnectionError("unreachable")
        return await self._client.eval(*args)  # type: ignore[arg-type]


async def test_calls_pause_while_redis_is_unavailable_and_resume(redis: "Redis") -> None:
    limiter = RedisRateLimiter(
        cast("Redis", Flaky(redis)), RateLimits(10, 1_000, max_wait_seconds=5)
    )
    loop = asyncio.get_running_loop()
    started = loop.time()

    await limiter.acquire(f"test-{uuid.uuid4().hex[:8]}", 1)

    assert loop.time() - started >= UNAVAILABLE_RETRY_SECONDS


async def test_a_pause_that_lasts_too_long_retries_the_job() -> None:
    unreachable = Redis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.1)
    limiter = RedisRateLimiter(unreachable, RateLimits(10, 1_000, max_wait_seconds=0.5))

    with pytest.raises(TransientJobError):
        await limiter.acquire("fake", 1)

    await unreachable.aclose()
