"""The durable ledger of model calls and the budget counters they move.

`hold_budget` is one conditional `UPDATE`, so parallel calls cannot oversubscribe:
whichever finds `reserved + spent + amount` over the budget changes nothing. A call
keeps its share of `reserved_microusd` until it settles, and an unknown outcome
keeps it for good, because the provider may have charged it.

Settling is keyed by the call, not the lease. A stale worker's answer is still a
fact about money, but a call already closed as unknown stays unknown.
"""

import uuid
from dataclasses import dataclass

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.workflow import CostBasis, LlmCallStatus
from signalops.persistence.models import AgentStep, Investigation, LlmCall


@dataclass(frozen=True)
class NewLlmCall:
    agent_step_id: uuid.UUID
    provider: str
    model: str
    prompt_version: str
    request_hash: str
    pricing_version: str
    reserved_microusd: int
    lease_generation: int


@dataclass(frozen=True)
class SettledUsage:
    model: str
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    cost_microusd: int
    cost_basis: CostBasis
    latency_ms: int
    provider_request_id: str | None


class LlmCallRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def step_id(self, investigation_id: uuid.UUID, step_key: str) -> uuid.UUID:
        return (
            await self._session.execute(
                select(AgentStep.id).where(
                    AgentStep.investigation_id == investigation_id,
                    AgentStep.step_key == step_key,
                )
            )
        ).scalar_one()

    async def hold_budget(self, investigation_id: uuid.UUID, amount: int) -> bool:
        """Reserve `amount`, or change nothing and return False if it does not fit."""
        held = await self._session.scalar(
            update(Investigation)
            .where(
                Investigation.id == investigation_id,
                Investigation.reserved_microusd + Investigation.spent_microusd + amount
                <= Investigation.budget_microusd,
            )
            .values(reserved_microusd=Investigation.reserved_microusd + amount)
            .returning(Investigation.id)
        )
        return held is not None

    async def add(self, call: NewLlmCall) -> tuple[uuid.UUID, int]:
        """Record a pending call; its attempt counts every call of the step.

        The caller holds the job row, which serializes attempt numbers.
        """
        attempt = await self._session.scalar(
            select(func.coalesce(func.max(LlmCall.attempt), 0) + 1).where(
                LlmCall.agent_step_id == call.agent_step_id
            )
        )
        assert attempt is not None  # noqa: S101 - an aggregate always returns a row
        row = LlmCall(
            agent_step_id=call.agent_step_id,
            attempt=attempt,
            provider=call.provider,
            model=call.model,
            prompt_version=call.prompt_version,
            request_hash=call.request_hash,
            pricing_version=call.pricing_version,
            reserved_microusd=call.reserved_microusd,
            lease_generation=call.lease_generation,
        )
        self._session.add(row)
        await self._session.flush()
        return row.id, attempt

    async def settle(self, call_id: uuid.UUID, usage: SettledUsage) -> None:
        """Replace a pending call's reservation with what it cost."""
        await self._settle(
            call_id,
            usage.cost_microusd,
            status=LlmCallStatus.SUCCEEDED,
            model=usage.model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
            cost_microusd=usage.cost_microusd,
            cost_basis=usage.cost_basis,
            latency_ms=usage.latency_ms,
            provider_request_id=usage.provider_request_id,
        )

    async def settle_failed(
        self,
        call_id: uuid.UUID,
        error_category: str,
        latency_ms: int,
        provider_request_id: str | None,
    ) -> None:
        """The provider did no work, so the reservation is released."""
        await self._settle(
            call_id,
            0,
            status=LlmCallStatus.FAILED,
            cost_microusd=0,
            cost_basis=CostBasis.ACTUAL,
            error_category=error_category,
            latency_ms=latency_ms,
            provider_request_id=provider_request_id,
        )

    async def settle_unknown(
        self,
        call_id: uuid.UUID,
        error_category: str,
        latency_ms: int,
        provider_request_id: str | None,
    ) -> None:
        """The provider may have charged, so the reservation stays."""
        await self._session.execute(
            update(LlmCall)
            .where(LlmCall.id == call_id, LlmCall.status == LlmCallStatus.PENDING)
            .values(
                status=LlmCallStatus.UNKNOWN,
                cost_basis=CostBasis.UNKNOWN,
                error_category=error_category,
                latency_ms=latency_ms,
                provider_request_id=provider_request_id,
                completed_at=func.clock_timestamp(),
            )
        )

    async def close_abandoned(
        self, investigation_id: uuid.UUID, *, before_generation: int | None = None
    ) -> None:
        """Mark calls nobody will settle as unknown: their workers died or their job ended.

        With `before_generation`, only calls of older leases.
        """
        steps = select(AgentStep.id).where(AgentStep.investigation_id == investigation_id)
        statement = update(LlmCall).where(
            LlmCall.agent_step_id.in_(steps), LlmCall.status == LlmCallStatus.PENDING
        )
        if before_generation is not None:
            statement = statement.where(LlmCall.lease_generation < before_generation)
        await self._session.execute(
            statement.values(
                status=LlmCallStatus.UNKNOWN,
                cost_basis=CostBasis.UNKNOWN,
                error_category="abandoned",
                completed_at=func.clock_timestamp(),
            )
        )

    async def _settle(self, call_id: uuid.UUID, cost: int, **values: object) -> None:
        settled = await self._session.execute(
            update(LlmCall)
            .where(LlmCall.id == call_id, LlmCall.status == LlmCallStatus.PENDING)
            .values(completed_at=func.clock_timestamp(), **values)
            .returning(LlmCall.reserved_microusd, LlmCall.agent_step_id)
        )
        row = settled.first()
        if row is None:
            return
        steps = select(AgentStep.investigation_id).where(AgentStep.id == row.agent_step_id)
        await self._session.execute(
            update(Investigation)
            .where(Investigation.id.in_(steps))
            .values(
                reserved_microusd=Investigation.reserved_microusd - row.reserved_microusd,
                spent_microusd=Investigation.spent_microusd + cost,
            )
        )
