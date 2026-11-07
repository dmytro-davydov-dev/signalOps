"""A deterministic provider for development and CI: no network, no key, no cost.

A script is a list of steps, one per call: answer, fail with a provider error, or
take a while first. Scripts live in code or in `fixtures/gateway/v1`. A call past
the end of a script repeats its last step. A responder decides each step from the
request instead, as `fake_investigator` does.

Tokens are counted from bytes, so usage is deterministic. They are not a provider's
tokenizer, and a fake result says nothing about how a real model would answer.
"""

import asyncio
import hashlib
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Self

from pydantic import Field, NonNegativeFloat, TypeAdapter

from signalops.contracts.common import ContractModel
from signalops.gateway.errors import ProviderError, ProviderErrorCategory
from signalops.gateway.pricing import estimated_tokens
from signalops.gateway.types import (
    ContentBlock,
    ModelRequest,
    ModelResponse,
    ModelUsage,
    StopReason,
)

FAKE_MODEL = "fake-1"


class FakeAnswer(ContractModel):
    content: tuple[ContentBlock, ...]
    stop_reason: StopReason = StopReason.END_TURN
    delay_seconds: NonNegativeFloat = 0


class FakeFailure(ContractModel):
    fail: ProviderErrorCategory
    retry_after_seconds: NonNegativeFloat | None = None
    delay_seconds: NonNegativeFloat = 0


FakeStep = FakeAnswer | FakeFailure
FakeResponder = Callable[[ModelRequest], FakeStep]


class FakeScript(ContractModel):
    name: str
    steps: tuple[FakeStep, ...] = Field(min_length=1)


SCRIPT_ADAPTER = TypeAdapter(FakeScript)


class FakeProvider:
    name = "fake"

    def __init__(
        self, steps: Sequence[FakeStep] = (), responder: FakeResponder | None = None
    ) -> None:
        self._steps = tuple(steps)
        self._responder = responder
        # Every request received, including those that then failed.
        self.requests: list[ModelRequest] = []

    @classmethod
    def scripted(cls, steps: Sequence[FakeStep]) -> Self:
        return cls(steps)

    @classmethod
    def from_fixture(cls, path: Path) -> Self:
        return cls(SCRIPT_ADAPTER.validate_json(path.read_bytes()).steps)

    async def complete(self, request: ModelRequest) -> ModelResponse:
        index = len(self.requests)
        self.requests.append(request)
        step = self._next_step(request, index)
        if step.delay_seconds:
            await asyncio.sleep(step.delay_seconds)
        request_id = self._request_id(request, index)
        if isinstance(step, FakeFailure):
            raise ProviderError(
                step.fail,
                retry_after_seconds=step.retry_after_seconds,
                provider_request_id=request_id,
            )
        return self._answer(request, step, request_id)

    def _next_step(self, request: ModelRequest, index: int) -> FakeStep:
        if self._responder is not None:
            return self._responder(request)
        if not self._steps:
            raise ProviderError(ProviderErrorCategory.REJECTED)
        return self._steps[min(index, len(self._steps) - 1)]

    def _answer(self, request: ModelRequest, step: FakeAnswer, request_id: str) -> ModelResponse:
        output = sum(len(block.model_dump_json()) for block in step.content)
        return ModelResponse(
            model=request.model,
            content=step.content,
            stop_reason=step.stop_reason,
            usage=ModelUsage(
                input_tokens=estimated_tokens(len(request.canonical())),
                output_tokens=min(estimated_tokens(output), request.max_output_tokens),
            ),
            provider_request_id=request_id,
        )

    @staticmethod
    def _request_id(request: ModelRequest, index: int) -> str:
        digest = hashlib.sha256(f"{request.fingerprint()}:{index}".encode()).hexdigest()
        return f"fake-{digest[:24]}"
