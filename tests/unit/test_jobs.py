"""Pure parts of job handling: policy, owner identity, and stored errors (P06-T02)."""

import pytest

from signalops.agents.jobs import (
    JobErrorCategory,
    JobPolicy,
    PermanentJobError,
    TransientJobError,
    error_record,
    worker_owner,
)
from signalops.config import Settings
from signalops.contracts.common import MAX_ERROR_DETAIL_LENGTH


def test_policy_reads_settings() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://u:p@localhost/d",  # type: ignore[arg-type]
        job_lease_seconds=60,
        job_poll_interval_ms=250,
        retry_base_delay_ms=500,
        retry_max_delay_ms=2_000,
    )

    policy = JobPolicy.from_settings(settings)

    assert (policy.lease_seconds, policy.poll_interval_seconds) == (60, 0.25)
    assert (policy.retry.base_delay_seconds, policy.retry.max_delay_seconds) == (0.5, 2.0)


def test_three_heartbeats_fit_in_a_lease() -> None:
    assert JobPolicy(lease_seconds=30).heartbeat_seconds == 10


def test_a_new_process_start_is_a_new_owner() -> None:
    assert worker_owner() != worker_owner()


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (TransientJobError("throttled"), JobErrorCategory.TRANSIENT),
        (PermanentJobError("unknown workflow"), JobErrorCategory.PERMANENT),
    ],
)
def test_a_declared_error_keeps_its_message(error: Exception, category: JobErrorCategory) -> None:
    assert error_record(error) == {
        "category": category,
        "type": type(error).__name__,
        "detail": str(error),
    }


def test_an_unexpected_error_keeps_only_its_type() -> None:
    assert error_record(KeyError("token=hunter2")) == {
        "category": JobErrorCategory.UNEXPECTED,
        "type": "KeyError",
    }


def test_a_stored_message_is_bounded() -> None:
    record = error_record(TransientJobError("x" * (MAX_ERROR_DETAIL_LENGTH + 1)))

    assert len(record["detail"]) == MAX_ERROR_DETAIL_LENGTH
