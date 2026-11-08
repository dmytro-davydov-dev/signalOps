"""Provider behavior with no network: the fake, and the real adapter over a stub transport."""

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anthropic
import httpx2
import pytest
from pydantic import JsonValue, ValidationError

from signalops.config import Settings
from signalops.contracts.diagnosis import Diagnosis
from signalops.gateway.anthropic_provider import AnthropicProvider
from signalops.gateway.errors import ProviderError, ProviderErrorCategory
from signalops.gateway.fake import FAKE_MODEL, FakeAnswer, FakeProvider
from signalops.gateway.provider import build_provider
from signalops.gateway.types import (
    Effort,
    ModelMessage,
    ModelRequest,
    ModelRole,
    ProviderBlock,
    StopReason,
    TextBlock,
    ToolDefinition,
    ToolResultBlock,
    ToolUseBlock,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "gateway" / "v1"
REQUEST = ModelRequest(
    model=FAKE_MODEL,
    prompt_version="test-v1",
    system="You investigate.",
    messages=(ModelMessage(role=ModelRole.USER, content=(TextBlock(text="Why?"),)),),
    max_output_tokens=100,
)


def fixture(name: str) -> FakeProvider:
    return FakeProvider.from_fixture(FIXTURES / f"{name}.json")


async def test_fake_success_is_deterministic() -> None:
    first = await fixture("success").complete(REQUEST)
    second = await fixture("success").complete(REQUEST)
    assert first == second
    assert first.text() == "done"
    assert first.stop_reason is StopReason.END_TURN
    assert first.usage.input_tokens > 0
    assert first.provider_request_id is not None


async def test_fake_timeout_then_recovers() -> None:
    provider = fixture("timeout")
    with pytest.raises(ProviderError) as raised:
        await provider.complete(REQUEST)
    assert raised.value.category is ProviderErrorCategory.TIMEOUT
    assert raised.value.ambiguous
    assert raised.value.retryable
    assert (await provider.complete(REQUEST)).text() == "done"


async def test_fake_throttling_states_when_to_retry() -> None:
    provider = fixture("throttled")
    with pytest.raises(ProviderError) as raised:
        await provider.complete(REQUEST)
    assert raised.value.category is ProviderErrorCategory.THROTTLED
    assert raised.value.retry_after_seconds == pytest.approx(0.01)
    assert not raised.value.ambiguous
    assert raised.value.retryable


async def test_fake_invalid_output_reaches_the_caller_and_fails_validation() -> None:
    response = await fixture("invalid_output").complete(REQUEST)
    block = response.tool_uses()[0]
    assert response.stop_reason is StopReason.TOOL_USE
    with pytest.raises(ValidationError):
        Diagnosis.model_validate(block.input)


async def test_a_call_past_the_script_repeats_its_last_step() -> None:
    provider = FakeProvider.scripted([FakeAnswer(content=(TextBlock(text="once"),))])
    await provider.complete(REQUEST)
    assert (await provider.complete(REQUEST)).text() == "once"
    assert len(provider.requests) == 2


def test_the_default_provider_is_fake_and_needs_no_key() -> None:
    settings = Settings(database_url="postgresql+psycopg://u:p@h/d")  # type: ignore[arg-type]
    assert isinstance(build_provider(settings), FakeProvider)
    assert settings.resolved_model() == FAKE_MODEL


async def test_the_fake_delay_pauses_before_each_reply(monkeypatch: pytest.MonkeyPatch) -> None:
    from signalops.gateway import fake_investigator

    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(
        fake_investigator,
        "investigator_step",
        lambda _request: FakeAnswer(content=(TextBlock(text="ok"),)),
    )
    settings = Settings(
        database_url="postgresql+psycopg://u:p@h/d",  # type: ignore[arg-type]
        fake_model_delay_seconds=2.5,
    )
    provider = build_provider(settings)
    assert (await provider.complete(REQUEST)).text() == "ok"
    assert slept == [2.5]

    undelayed = build_provider(settings.model_copy(update={"fake_model_delay_seconds": 0.0}))
    await undelayed.complete(REQUEST)
    assert slept == [2.5]


def test_the_real_provider_refuses_to_start_without_a_key() -> None:
    with pytest.raises(ValidationError, match="model_api_key"):
        Settings(
            database_url="postgresql+psycopg://u:p@h/d",  # type: ignore[arg-type]
            model_provider="anthropic",
        )


Handler = Callable[[httpx2.Request], httpx2.Response]


def adapter(handler: Handler) -> AnthropicProvider:
    client = anthropic.AsyncAnthropic(
        api_key="test-key",
        max_retries=0,
        http_client=anthropic.DefaultAsyncHttpxClient(transport=httpx2.MockTransport(handler)),
    )
    return AnthropicProvider(client)


def message(content: list[dict[str, Any]], stop_reason: str = "end_turn") -> dict[str, Any]:
    return {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5-5",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": 10,
            "output_tokens": 20,
            "cache_read_input_tokens": 30,
            "cache_creation_input_tokens": 40,
        },
    }


async def test_the_adapter_maps_a_response_and_keeps_thinking_for_the_next_turn() -> None:
    thinking: dict[str, JsonValue] = {"type": "thinking", "thinking": "", "signature": "opaque"}
    tool_use = {"type": "tool_use", "id": "toolu_1", "name": "query_logs", "input": {"limit": 5}}
    sent: list[dict[str, Any]] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(json.loads(request.content))
        return httpx2.Response(
            200,
            json=message([thinking, tool_use], "tool_use"),
            headers={"request-id": "req_123"},
        )

    provider = adapter(handler)
    response = await provider.complete(
        REQUEST.model_copy(
            update={
                "model": "claude-opus-5-5",
                "effort": Effort.HIGH,
                "tools": (
                    ToolDefinition(
                        name="query_logs", description="d", input_schema={"type": "object"}
                    ),
                ),
            }
        )
    )

    assert response.stop_reason is StopReason.TOOL_USE
    assert response.provider_request_id == "req_123"
    assert response.usage.cache_read_tokens == 30
    assert response.usage.cache_write_tokens == 40
    assert response.tool_uses() == (
        ToolUseBlock(id="toolu_1", name="query_logs", input={"limit": 5}),
    )
    assert response.content[0] == ProviderBlock(data=thinking)
    body = sent[0]
    assert body["output_config"] == {"effort": "high"}
    assert body["max_tokens"] == 100
    assert body["tools"][0]["name"] == "query_logs"

    await provider.complete(
        REQUEST.model_copy(
            update={
                "messages": (
                    *REQUEST.messages,
                    ModelMessage(role=ModelRole.ASSISTANT, content=response.content),
                    ModelMessage(
                        role=ModelRole.USER,
                        content=(ToolResultBlock(tool_use_id="toolu_1", content="{}"),),
                    ),
                )
            }
        )
    )
    assert sent[1]["messages"][1]["content"][0] == thinking
    assert sent[1]["messages"][2]["content"][0] == {
        "type": "tool_result",
        "tool_use_id": "toolu_1",
        "content": "{}",
        "is_error": False,
    }


@pytest.mark.parametrize(
    ("status", "category", "ambiguous", "retryable"),
    [
        (400, ProviderErrorCategory.REJECTED, False, False),
        (401, ProviderErrorCategory.AUTHENTICATION, False, False),
        (403, ProviderErrorCategory.AUTHENTICATION, False, False),
        (404, ProviderErrorCategory.REJECTED, False, False),
        (408, ProviderErrorCategory.UNAVAILABLE, True, True),
        (429, ProviderErrorCategory.THROTTLED, False, True),
        (500, ProviderErrorCategory.UNAVAILABLE, True, True),
        (529, ProviderErrorCategory.THROTTLED, False, True),
    ],
)
async def test_the_adapter_categorizes_statuses_and_keeps_no_provider_text(
    status: int, category: ProviderErrorCategory, ambiguous: bool, retryable: bool
) -> None:
    def handler(_request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            status,
            json={"type": "error", "error": {"type": "x", "message": "quotes secret evidence"}},
            headers={"retry-after": "7", "request-id": "req_9"},
        )

    with pytest.raises(ProviderError) as raised:
        await adapter(handler).complete(REQUEST)
    assert raised.value.category is category
    assert raised.value.ambiguous is ambiguous
    assert raised.value.retryable is retryable
    assert raised.value.provider_request_id == "req_9"
    assert raised.value.retry_after_seconds == 7
    assert "evidence" not in str(raised.value)
    assert raised.value.__cause__ is None


async def test_the_adapter_reports_timeouts_and_lost_connections_as_ambiguous() -> None:
    def timing_out(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ReadTimeout("slow", request=request)

    def disconnecting(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("gone", request=request)

    with pytest.raises(ProviderError) as timed_out:
        await adapter(timing_out).complete(REQUEST)
    with pytest.raises(ProviderError) as lost:
        await adapter(disconnecting).complete(REQUEST)
    assert timed_out.value.category is ProviderErrorCategory.TIMEOUT
    assert lost.value.category is ProviderErrorCategory.CONNECTION
    assert timed_out.value.ambiguous
    assert lost.value.ambiguous


async def test_a_refusal_is_a_response_not_an_error() -> None:
    def handler(_request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json=message([], "refusal"))

    response = await adapter(handler).complete(REQUEST)
    assert response.stop_reason is StopReason.REFUSAL
    assert response.content == ()
