"""The Claude Messages API behind `ModelProvider` (ADR-025).

The client never retries, because each retry is a possibly billable call that the
gateway reserves for. Thinking stays at the model's default, so its blocks come back
as `ProviderBlock`s for the next turn. No refusal fallback is set: a fallback model
would be billed beyond the reservation for this model.
"""

from typing import Any, Self, cast

import anthropic
from anthropic import omit
from anthropic.types import Message, MessageParam, OutputConfigParam, ToolParam

from signalops.config import Settings
from signalops.gateway.errors import ProviderError, ProviderErrorCategory
from signalops.gateway.types import (
    ContentBlock,
    ModelRequest,
    ModelResponse,
    ModelUsage,
    ProviderBlock,
    StopReason,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
)

STOP_REASONS = {
    "end_turn": StopReason.END_TURN,
    "tool_use": StopReason.TOOL_USE,
    "max_tokens": StopReason.MAX_TOKENS,
    "refusal": StopReason.REFUSAL,
}


def provider_error(error: anthropic.AnthropicError) -> ProviderError:
    """The category of an SDK error, keeping none of its text."""
    match error:
        case anthropic.APITimeoutError():
            return ProviderError(ProviderErrorCategory.TIMEOUT)
        case anthropic.APIConnectionError():
            return ProviderError(ProviderErrorCategory.CONNECTION)
        case anthropic.APIResponseValidationError():
            return ProviderError(
                ProviderErrorCategory.INVALID_RESPONSE,
                status_code=error.status_code,
                provider_request_id=error.response.headers.get("request-id"),
            )
        case anthropic.APIStatusError():
            return ProviderError(
                _status_category(error.status_code),
                status_code=error.status_code,
                retry_after_seconds=_retry_after(error),
                provider_request_id=error.request_id,
            )
        case _:
            return ProviderError(ProviderErrorCategory.AUTHENTICATION)


def _status_category(status: int) -> ProviderErrorCategory:
    if status in (429, 529):
        return ProviderErrorCategory.THROTTLED
    if status == 408 or status >= 500:
        return ProviderErrorCategory.UNAVAILABLE
    if status in (401, 403):
        return ProviderErrorCategory.AUTHENTICATION
    return ProviderErrorCategory.REJECTED


def _retry_after(error: anthropic.APIStatusError) -> float | None:
    try:
        return float(error.response.headers["retry-after"])
    except (KeyError, ValueError):
        return None


def _wire_block(block: ContentBlock) -> dict[str, Any]:
    match block:
        case ProviderBlock():
            return block.data
        case TextBlock() | ToolUseBlock() | ToolResultBlock():
            return block.model_dump(mode="json")


def _block(block: Any) -> ContentBlock:
    match block.type:
        case "text":
            return TextBlock(text=block.text)
        case "tool_use":
            return ToolUseBlock(id=block.id, name=block.name, input=block.input)
        case _:
            return ProviderBlock(data=block.to_dict())


def _response(message: Message) -> ModelResponse:
    usage = message.usage
    return ModelResponse(
        model=message.model,
        content=tuple(_block(block) for block in message.content),
        stop_reason=STOP_REASONS.get(message.stop_reason or "", StopReason.OTHER),
        usage=ModelUsage(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_input_tokens or 0,
            cache_write_tokens=usage.cache_creation_input_tokens or 0,
        ),
        provider_request_id=message._request_id,
    )


class AnthropicProvider:
    name = "anthropic"

    def __init__(self, client: anthropic.AsyncAnthropic) -> None:
        self._client = client

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        if settings.model_api_key is None:
            raise ValueError("the anthropic provider needs SIGNALOPS_MODEL_API_KEY")
        return cls(
            anthropic.AsyncAnthropic(
                api_key=settings.model_api_key.get_secret_value(),
                timeout=settings.model_request_timeout_seconds,
                max_retries=0,
            )
        )

    async def complete(self, request: ModelRequest) -> ModelResponse:
        messages = [
            {"role": item.role.value, "content": [_wire_block(block) for block in item.content]}
            for item in request.messages
        ]
        tools = [tool.model_dump(mode="json") for tool in request.tools]
        effort = request.effort
        try:
            message = await self._client.messages.create(
                model=request.model,
                max_tokens=request.max_output_tokens,
                system=request.system,
                messages=cast("list[MessageParam]", messages),
                cache_control={"type": "ephemeral"},
                tools=cast("list[ToolParam]", tools) if tools else omit,
                output_config=(
                    cast("OutputConfigParam", {"effort": effort.value}) if effort else omit
                ),
            )
        except anthropic.AnthropicError as error:
            raise provider_error(error) from None
        return _response(message)
