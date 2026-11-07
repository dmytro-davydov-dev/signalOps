"""Versioned model prices in integer microUSD per million tokens.

`PRICING_VERSION` is stored with every call, so a past cost can be traced to the
prices it used. A model with no price cannot be reserved for, so it cannot be called.

The fake provider has prices so budgets behave the same in development. Its token
counts are not a tokenizer's, so its costs are `estimated`.
"""

import math
from dataclasses import dataclass
from typing import Final

from signalops.gateway.types import ModelRequest, ModelUsage

# Anthropic's published rates as of this date, with the 5-minute cache write rate.
PRICING_VERSION: Final = "2026-10-06"
TOKENS_PER_PRICE_UNIT: Final = 1_000_000
BYTES_PER_TOKEN: Final = 4


@dataclass(frozen=True)
class ModelPrice:
    input: int
    output: int
    cache_read: int
    cache_write: int


PRICES: Final[dict[tuple[str, str], ModelPrice]] = {
    ("anthropic", "claude-opus-5-5"): ModelPrice(4_000_000, 20_000_000, 200_000, 5_000_000),
    ("anthropic", "claude-sonnet-5-5"): ModelPrice(2_000_000, 10_000_000, 200_000, 2_500_000),
    ("fake", "fake-1"): ModelPrice(4_000_000, 20_000_000, 200_000, 5_000_000),
}


class UnpricedModelError(ValueError):
    """No price is known for this provider and model."""


def price_of(provider: str, model: str) -> ModelPrice:
    try:
        return PRICES[provider, model]
    except KeyError:
        raise UnpricedModelError(f"no price for {provider} {model}") from None


def _cost(tokens: int, price: int) -> int:
    return math.ceil(tokens * price / TOKENS_PER_PRICE_UNIT)


def estimated_tokens(size: int) -> int:
    """A rough count from bytes, for rate limits and the fake provider, never for a bound."""
    return math.ceil(size / BYTES_PER_TOKEN)


def maximum_input_tokens(request: ModelRequest) -> int:
    """No tokenizer runs here. A token is at least about a byte, so bytes bound the count."""
    return len(request.canonical())


def maximum_cost(request: ModelRequest, price: ModelPrice) -> int:
    """The most this call can cost: all input written to the cache, all output used."""
    highest_input = max(price.input, price.cache_write)
    return _cost(maximum_input_tokens(request), highest_input) + _cost(
        request.max_output_tokens, price.output
    )


def actual_cost(usage: ModelUsage, price: ModelPrice) -> int:
    return (
        _cost(usage.input_tokens, price.input)
        + _cost(usage.output_tokens, price.output)
        + _cost(usage.cache_read_tokens, price.cache_read)
        + _cost(usage.cache_write_tokens, price.cache_write)
    )
