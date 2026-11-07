"""Cost arithmetic: integer microUSD, rounded up, with a bound no usage can exceed."""

import pytest

from signalops.gateway.pricing import (
    UnpricedModelError,
    actual_cost,
    maximum_cost,
    maximum_input_tokens,
    price_of,
)
from signalops.gateway.types import (
    ModelMessage,
    ModelRequest,
    ModelRole,
    ModelUsage,
    TextBlock,
)

OPUS = price_of("anthropic", "claude-opus-5-5")
REQUEST = ModelRequest(
    model="claude-opus-5-5",
    prompt_version="test-v1",
    system="You investigate.",
    messages=(ModelMessage(role=ModelRole.USER, content=(TextBlock(text="Why? " * 50),)),),
    max_output_tokens=1_000,
)


def test_cost_counts_each_token_class_at_its_rate() -> None:
    usage = ModelUsage(
        input_tokens=1_000,
        output_tokens=100,
        cache_read_tokens=10_000,
        cache_write_tokens=2_000,
    )
    assert actual_cost(usage, OPUS) == 4_000 + 2_000 + 2_000 + 10_000


def test_cost_rounds_up_so_small_calls_are_not_free() -> None:
    usage = ModelUsage(input_tokens=0, output_tokens=0, cache_read_tokens=1)
    assert actual_cost(usage, OPUS) == 1


def test_the_maximum_covers_full_output_and_all_input_as_cache_writes() -> None:
    worst = ModelUsage(
        input_tokens=0,
        output_tokens=REQUEST.max_output_tokens,
        cache_write_tokens=maximum_input_tokens(REQUEST),
    )
    assert maximum_cost(REQUEST, OPUS) == actual_cost(worst, OPUS)


def test_a_model_without_a_price_cannot_be_called() -> None:
    with pytest.raises(UnpricedModelError):
        price_of("anthropic", "claude-unknown")
