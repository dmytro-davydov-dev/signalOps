import random

import pytest

from signalops.config import Settings
from signalops.streaming.retry import RetryPolicy


def test_ceiling_doubles_up_to_the_cap() -> None:
    policy = RetryPolicy(max_attempts=10, base_delay_seconds=1, max_delay_seconds=30)
    assert [policy.ceiling(n) for n in range(1, 8)] == [1, 2, 4, 8, 16, 30, 30]


def test_delay_is_jittered_within_the_ceiling() -> None:
    policy = RetryPolicy(base_delay_seconds=1, max_delay_seconds=30)
    rng = random.Random(7)  # noqa: S311 - deterministic jitter for the test
    delays = [policy.delay(4, rng) for _ in range(200)]
    assert all(0 <= delay <= 8 for delay in delays)
    assert len(set(delays)) > 100


def test_exhausted_counts_the_first_attempt() -> None:
    policy = RetryPolicy(max_attempts=5)
    assert not policy.exhausted(4)
    assert policy.exhausted(5)


def test_huge_failure_counts_stay_capped() -> None:
    assert RetryPolicy(max_delay_seconds=30).ceiling(10_000) == 30


@pytest.mark.parametrize(
    ("max_attempts", "base", "cap"), [(0, 1.0, 30.0), (5, 0.0, 30.0), (5, 31.0, 30.0)]
)
def test_invalid_policies_are_rejected(max_attempts: int, base: float, cap: float) -> None:
    with pytest.raises(ValueError, match="must"):
        RetryPolicy(max_attempts, base, cap)


def test_from_settings_uses_milliseconds() -> None:
    settings = Settings(
        database_url="postgresql://u:p@localhost/db",  # type: ignore[arg-type]
        retry_max_attempts=3,
        retry_base_delay_ms=250,
        retry_max_delay_ms=4_000,
    )
    assert RetryPolicy.from_settings(settings) == RetryPolicy(3, 0.25, 4.0)
