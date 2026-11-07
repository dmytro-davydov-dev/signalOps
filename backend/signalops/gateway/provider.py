"""The interface every model provider implements, and the choice of one from settings."""

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
    from signalops.gateway.fake import FakeProvider

    return FakeProvider.scripted([])
