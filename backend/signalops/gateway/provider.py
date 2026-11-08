"""The interface every model provider implements, and the choice of one from settings."""

import itertools
from typing import Protocol

from signalops.config import Settings
from signalops.gateway.types import ModelRequest, ModelResponse


class ModelProvider(Protocol):
    """One model call. No retries: each one is a separate reservation in the gateway."""

    @property
    def name(self) -> str: ...

    async def complete(self, request: ModelRequest) -> ModelResponse:
        """The model's answer, or a `ProviderError`."""
        ...


def build_provider(settings: Settings) -> ModelProvider:
    if settings.model_provider == "anthropic":
        from signalops.gateway.anthropic_provider import AnthropicProvider

        return AnthropicProvider.from_settings(settings)
    from signalops.gateway.errors import ProviderErrorCategory
    from signalops.gateway.fake import FakeFailure, FakeProvider, FakeStep
    from signalops.gateway.fake_roles import role_step

    delay = settings.fake_model_delay_seconds
    every = settings.fake_model_throttle_every
    calls = itertools.count(1)

    def respond(request: ModelRequest) -> FakeStep:
        if every and next(calls) % every == 0:
            return FakeFailure(
                fail=ProviderErrorCategory.THROTTLED, retry_after_seconds=1, delay_seconds=delay
            )
        step = role_step(request)
        return step.model_copy(update={"delay_seconds": delay}) if delay else step

    return FakeProvider(responder=respond)
