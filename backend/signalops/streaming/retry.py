"""Capped exponential backoff with full jitter for transient failures.

The delay before retry n (n = 1 after the first failure) is drawn uniformly
from [0, min(max_delay, base_delay * 2**(n - 1))], so retries from many
records spread out instead of arriving together.
"""

import random
from dataclasses import dataclass
from typing import Self

from signalops.config import Settings

_JITTER = random.Random()  # noqa: S311 - timing jitter, not a secret


@dataclass(frozen=True)
class RetryPolicy:
    # Attempts per record, including the first.
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if not 0 < self.base_delay_seconds <= self.max_delay_seconds:
            raise ValueError("delays must satisfy 0 < base_delay <= max_delay")

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(
            max_attempts=settings.retry_max_attempts,
            base_delay_seconds=settings.retry_base_delay_ms / 1000,
            max_delay_seconds=max(settings.retry_max_delay_ms, settings.retry_base_delay_ms) / 1000,
        )

    def ceiling(self, failures: int) -> float:
        """The largest delay after `failures` consecutive failures (at least 1)."""
        exponent = min(max(failures, 1) - 1, 32)
        return min(self.max_delay_seconds, self.base_delay_seconds * 2.0**exponent)

    def delay(self, failures: int, rng: random.Random | None = None) -> float:
        ceiling = self.ceiling(failures)
        # Jitter only spreads retries out; it needs no cryptographic randomness.
        return (rng or _JITTER).uniform(0, ceiling)

    def exhausted(self, failures: int) -> bool:
        return failures >= self.max_attempts
