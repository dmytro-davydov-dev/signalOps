"""Provider-neutral model requests and responses.

The agent loop and the gateway speak these types, so a provider adapter is the only
code that knows a provider's wire format. They hold what one conversation needs in
memory. A `ProviderBlock` may carry a provider's encrypted reasoning, which has to
go back unchanged on the next turn, so it is never stored or logged.
"""

import hashlib
import json
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import Field, JsonValue, NonNegativeInt, PositiveInt

from signalops.contracts.common import ContractModel, Version


class ModelRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"


class Effort(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class StopReason(StrEnum):
    END_TURN = "end_turn"
    TOOL_USE = "tool_use"
    MAX_TOKENS = "max_tokens"
    REFUSAL = "refusal"
    OTHER = "other"


class TextBlock(ContractModel):
    type: Literal["text"] = "text"
    text: str


class ToolUseBlock(ContractModel):
    type: Literal["tool_use"] = "tool_use"
    id: str
    name: str
    input: dict[str, JsonValue]


class ToolResultBlock(ContractModel):
    type: Literal["tool_result"] = "tool_result"
    tool_use_id: str
    content: str
    is_error: bool = False


class ProviderBlock(ContractModel):
    """A block only its provider reads, returned to it unchanged."""

    type: Literal["provider"] = "provider"
    data: dict[str, JsonValue]


ContentBlock = Annotated[
    TextBlock | ToolUseBlock | ToolResultBlock | ProviderBlock, Field(discriminator="type")
]


class ModelMessage(ContractModel):
    role: ModelRole
    content: tuple[ContentBlock, ...] = Field(min_length=1)


class ToolDefinition(ContractModel):
    name: str
    description: str
    input_schema: dict[str, JsonValue]


class ModelRequest(ContractModel):
    model: str
    prompt_version: Version
    system: str
    messages: tuple[ModelMessage, ...] = Field(min_length=1)
    tools: tuple[ToolDefinition, ...] = ()
    max_output_tokens: PositiveInt
    effort: Effort | None = None

    def canonical(self) -> bytes:
        """The request as stable bytes: its size bounds input tokens and its hash fingerprints it.

        A local fingerprint does not deduplicate at the provider.
        """
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()

    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical()).hexdigest()


class ModelUsage(ContractModel):
    # Uncached input; cached input is reported apart because it is priced apart.
    input_tokens: NonNegativeInt
    output_tokens: NonNegativeInt
    cache_read_tokens: NonNegativeInt = 0
    cache_write_tokens: NonNegativeInt = 0


class ModelResponse(ContractModel):
    # The model that answered, which prices the call.
    model: str
    content: tuple[ContentBlock, ...]
    stop_reason: StopReason
    usage: ModelUsage
    provider_request_id: str | None = None

    def tool_uses(self) -> tuple[ToolUseBlock, ...]:
        return tuple(block for block in self.content if isinstance(block, ToolUseBlock))

    def text(self) -> str:
        return "".join(block.text for block in self.content if isinstance(block, TextBlock))


def tool_definition(name: str, description: str, schema: dict[str, Any]) -> ToolDefinition:
    return ToolDefinition(name=name, description=description, input_schema=schema)
