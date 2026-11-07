"""Retry delays: capped, and never shorter than a provider asked for up to the cap."""

from typing import cast

import pytest

from signalops.gateway.errors import (
    AMBIGUOUS_CATEGORIES,
    PERMANENT_CATEGORIES,
    ProviderError,
    ProviderErrorCategory,
)
from signalops.gateway.fake import FakeProvider
from signalops.gateway.gateway import GatewayPolicy, ModelGateway
from signalops.persistence.database import Database
from signalops.streaming.retry import RetryPolicy

POLICY = GatewayPolicy(
    retry=RetryPolicy(max_attempts=5, base_delay_seconds=1, max_delay_seconds=4),
    max_retry_after_seconds=10,
)
MODEL = ModelGateway(cast("Database", None), FakeProvider.scripted([]), policy=POLICY)


def test_every_category_is_classified_exactly_once() -> None:
    assert not AMBIGUOUS_CATEGORIES & PERMANENT_CATEGORIES
    expected_retryable = {
        ProviderErrorCategory.TIMEOUT,
        ProviderErrorCategory.CONNECTION,
        ProviderErrorCategory.UNAVAILABLE,
        ProviderErrorCategory.INVALID_RESPONSE,
        ProviderErrorCategory.THROTTLED,
    }
    for category in ProviderErrorCategory:
        assert ProviderError(category).retryable is (category in expected_retryable)


def test_a_definite_failure_is_never_ambiguous() -> None:
    for category in (
        ProviderErrorCategory.THROTTLED,
        ProviderErrorCategory.REJECTED,
        ProviderErrorCategory.AUTHENTICATION,
    ):
        assert not ProviderError(category).ambiguous


@pytest.mark.parametrize("failures", [1, 2, 3, 10, 100])
def test_backoff_stays_under_its_cap(failures: int) -> None:
    error = ProviderError(ProviderErrorCategory.UNAVAILABLE)
    for _ in range(50):
        assert 0 <= MODEL._delay(error, failures) <= POLICY.retry.max_delay_seconds


def test_a_provider_wait_is_honored_up_to_its_cap() -> None:
    asked = ProviderError(ProviderErrorCategory.THROTTLED, retry_after_seconds=7)
    excessive = ProviderError(ProviderErrorCategory.THROTTLED, retry_after_seconds=3_600)
    assert MODEL._delay(asked, 1) >= 7
    assert MODEL._delay(excessive, 1) == POLICY.max_retry_after_seconds
