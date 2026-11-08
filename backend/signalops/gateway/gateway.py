"""Every model call: limit, reserve, call, settle, retry (ADR-026).

**Reservation.** One transaction checks the lease and any stop, closes calls of dead
leases, and reserves the call's maximum cost. So a stale worker cannot start a call,
no call starts after a cancellation or deadline commits, and in-flight calls count
against the budget. No transaction stays open during the call.

**Shares.** A `BudgetLimit` narrows the check for a call that must leave money for
later roles: it must also fit its step's cap and leave `held_back` free. Such a call
raises `BudgetShareExhaustedError` instead of `BudgetExceededError`.

**Settling.** A success replaces the reservation with its cost. A definite failure
releases it. An ambiguous failure keeps it and records no cost, since an external
call may have run and been billed before the timeout.

**Retries.** The gateway is the only place that retries a call. Each one is a new
`llm_calls` row with its own reservation. Attempts out of retries raise a job error,
and the next job attempt starts the node again.

Logs hold identifiers, categories, and counts. Prompts, evidence, and output are
never logged.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Self

from redis.asyncio import Redis

from signalops.agents.jobs import JobStoppedError, PermanentJobError, TransientJobError
from signalops.config import Settings
from signalops.contracts.workflow import CostBasis
from signalops.gateway.errors import (
    BudgetExceededError,
    BudgetShareExhaustedError,
    ProviderError,
    ProviderErrorCategory,
)
from signalops.gateway.limits import NoRateLimit, RateLimiter, RateLimits, RedisRateLimiter
from signalops.gateway.pricing import (
    PRICES,
    PRICING_VERSION,
    actual_cost,
    estimated_tokens,
    maximum_cost,
    price_of,
)
from signalops.gateway.provider import ModelProvider, build_provider
from signalops.gateway.types import ModelRequest, ModelResponse
from signalops.observability.metrics import MODEL_CALL_SECONDS, MODEL_COST, MODEL_TOKENS
from signalops.persistence.database import Database
from signalops.persistence.repositories.jobs import JobLease, JobRepository
from signalops.persistence.repositories.llm_calls import (
    LlmCallRepository,
    NewLlmCall,
    SettledUsage,
)
from signalops.streaming.retry import RetryPolicy

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GatewayPolicy:
    # Calls per logical request, including the first.
    retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=3))
    request_timeout_seconds: float = 60.0
    # The longest wait a provider's `retry-after` can impose.
    max_retry_after_seconds: float = 60.0

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        shared = RetryPolicy.from_settings(settings)
        return cls(
            retry=RetryPolicy(
                settings.model_max_attempts, shared.base_delay_seconds, shared.max_delay_seconds
            ),
            request_timeout_seconds=settings.model_request_timeout_seconds,
        )


@dataclass(frozen=True)
class BudgetLimit:
    """A tighter bound than the budget, for a call that must leave money for others.

    `held_back` stays free for later roles. `step_cap` bounds what the step's calls
    hold and spend together, across attempts. A call that does not fit raises
    `BudgetShareExhaustedError`, which ends the branch, not the investigation.
    """

    held_back: int = 0
    step_cap: int | None = None


@dataclass(frozen=True)
class Reservation:
    call_id: uuid.UUID
    attempt: int
    amount: int


class ModelGateway:
    def __init__(
        self,
        database: Database,
        provider: ModelProvider,
        *,
        limiter: RateLimiter | None = None,
        policy: GatewayPolicy | None = None,
    ) -> None:
        self._database = database
        self._provider = provider
        self._limiter = limiter or NoRateLimit()
        self._policy = policy or GatewayPolicy()

    @property
    def provider_name(self) -> str:
        return self._provider.name

    @property
    def request_timeout_seconds(self) -> float:
        return self._policy.request_timeout_seconds

    async def complete(
        self,
        lease: JobLease,
        step_key: str,
        request: ModelRequest,
        *,
        budget: BudgetLimit | None = None,
    ) -> ModelResponse:
        """The model's answer, reserving for each try. See the module docstring for errors."""
        retry = self._policy.retry
        tokens = estimated_tokens(len(request.canonical())) + request.max_output_tokens
        failures = 0
        while True:
            await self._limiter.acquire(self._provider.name, tokens)
            reservation = await self._reserve(lease, step_key, request, budget)
            started = asyncio.get_running_loop().time()
            try:
                async with asyncio.timeout(self._policy.request_timeout_seconds):
                    response = await self._provider.complete(request)
            except TimeoutError:
                error = ProviderError(ProviderErrorCategory.TIMEOUT)
            except ProviderError as provider_error:
                error = provider_error
            else:
                await self._settled(reservation, request, response, self._elapsed_ms(started))
                return response
            await self._settle_error(reservation, error, self._elapsed_ms(started))
            if not error.retryable:
                raise PermanentJobError(f"model call rejected: {error.category}")
            failures += 1
            if retry.exhausted(failures):
                raise TransientJobError(f"model call failed {failures} times: {error.category}")
            await asyncio.sleep(self._delay(error, failures))

    def _delay(self, error: ProviderError, failures: int) -> float:
        requested = error.retry_after_seconds or 0
        waited = min(requested, self._policy.max_retry_after_seconds)
        return max(waited, self._policy.retry.delay(failures))

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return round((asyncio.get_running_loop().time() - started) * 1000)

    async def _reserve(
        self, lease: JobLease, step_key: str, request: ModelRequest, budget: BudgetLimit | None
    ) -> Reservation:
        provider = self._provider.name
        amount = maximum_cost(request, price_of(provider, request.model))
        limit = budget or BudgetLimit()
        reservation = None
        async with self._database.transaction() as session:
            jobs = JobRepository(session)
            await jobs.hold(lease)
            if await jobs.stop_reason(lease.investigation_id) is not None:
                raise JobStoppedError(step_key)
            calls = LlmCallRepository(session)
            await calls.close_abandoned(lease.investigation_id, before_generation=lease.generation)
            step_id = await calls.step_id(lease.investigation_id, step_key)
            # The job row is locked, so the step's usage cannot change before the hold.
            fits_step = (
                limit.step_cap is None or await calls.step_usage(step_id) + amount <= limit.step_cap
            )
            if fits_step and await calls.hold_budget(
                lease.investigation_id, amount, held_back=limit.held_back
            ):
                call_id, attempt = await calls.add(
                    NewLlmCall(
                        agent_step_id=step_id,
                        provider=provider,
                        model=request.model,
                        prompt_version=request.prompt_version,
                        request_hash=request.fingerprint(),
                        pricing_version=PRICING_VERSION,
                        reserved_microusd=amount,
                        lease_generation=lease.generation,
                    )
                )
                reservation = Reservation(call_id, attempt, amount)
        if reservation is None and budget is not None:
            logger.info(
                "model call over its share",
                extra={"step_key": step_key, "reserved_microusd": amount},
            )
            raise BudgetShareExhaustedError(f"a call of up to {amount} microusd exceeds its share")
        if reservation is None:
            raise BudgetExceededError(f"a call of up to {amount} microusd does not fit the budget")
        logger.info(
            "model call reserved",
            extra={
                "call_id": str(reservation.call_id),
                "attempt": reservation.attempt,
                "provider": provider,
                "model": request.model,
                "prompt_version": request.prompt_version,
                "reserved_microusd": amount,
            },
        )
        return reservation

    async def _settled(
        self,
        reservation: Reservation,
        request: ModelRequest,
        response: ModelResponse,
        latency_ms: int,
    ) -> None:
        usage = response.usage
        cost, basis = self._cost(reservation, request, response)
        async with self._database.transaction() as session:
            await LlmCallRepository(session).settle(
                reservation.call_id,
                SettledUsage(
                    model=response.model,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    cache_read_tokens=usage.cache_read_tokens,
                    cache_write_tokens=usage.cache_write_tokens,
                    cost_microusd=cost,
                    cost_basis=basis,
                    latency_ms=latency_ms,
                    provider_request_id=response.provider_request_id,
                ),
            )
        provider = self._provider.name
        MODEL_CALL_SECONDS.labels(provider, "succeeded").observe(latency_ms / 1000)
        for kind, tokens in (
            ("input", usage.input_tokens),
            ("output", usage.output_tokens),
            ("cache_read", usage.cache_read_tokens),
            ("cache_write", usage.cache_write_tokens),
        ):
            if tokens:
                MODEL_TOKENS.labels(provider, kind).inc(tokens)
        MODEL_COST.labels(provider, basis.value).inc(cost)
        logger.info(
            "model call settled",
            extra={
                "call_id": str(reservation.call_id),
                "provider_request_id": response.provider_request_id,
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cost_microusd": cost,
                "cost_basis": str(basis),
                "latency_ms": latency_ms,
            },
        )

    def _cost(
        self, reservation: Reservation, request: ModelRequest, response: ModelResponse
    ) -> tuple[int, CostBasis]:
        price = PRICES.get((self._provider.name, response.model))
        if price is None:
            # The provider answered with a model we have no price for.
            return reservation.amount, CostBasis.ESTIMATED
        cost = actual_cost(response.usage, price)
        return cost, CostBasis.ESTIMATED if self._provider.name == "fake" else CostBasis.ACTUAL

    async def _settle_error(
        self, reservation: Reservation, error: ProviderError, latency_ms: int
    ) -> None:
        async with self._database.transaction() as session:
            calls = LlmCallRepository(session)
            settle = calls.settle_unknown if error.ambiguous else calls.settle_failed
            await settle(reservation.call_id, error.category, latency_ms, error.provider_request_id)
        MODEL_CALL_SECONDS.labels(
            self._provider.name, "unknown" if error.ambiguous else "failed"
        ).observe(latency_ms / 1000)
        logger.warning(
            "model call failed",
            extra={
                "call_id": str(reservation.call_id),
                "category": str(error.category),
                "status_code": error.status_code,
                "ambiguous": error.ambiguous,
                "provider_request_id": error.provider_request_id,
                "latency_ms": latency_ms,
            },
        )


def build_gateway(database: Database, redis: "Redis", settings: Settings) -> ModelGateway:
    """The gateway for this process. An unpriced model fails here, at startup."""
    provider = build_provider(settings)
    price_of(provider.name, settings.resolved_model())
    limiter = RedisRateLimiter(
        redis,
        RateLimits(
            settings.model_requests_per_minute,
            settings.model_tokens_per_minute,
            settings.model_limit_wait_seconds,
        ),
    )
    return ModelGateway(
        database, provider, limiter=limiter, policy=GatewayPolicy.from_settings(settings)
    )
