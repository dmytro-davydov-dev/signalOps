from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from signalops.adapters.quality import (
    FLAG_EFFECTS,
    MissingIdentityError,
    TimingPolicy,
    assess,
    derive_source_event_id,
)
from signalops.contracts.common import MAX_SOURCE_EVENT_ID_LENGTH
from signalops.contracts.evidence import QualityFlag, QualityStatus, SourceKind

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
TIMING = TimingPolicy(max_clock_skew=timedelta(minutes=5), late_arrival=timedelta(minutes=15))


def test_every_flag_has_an_effect() -> None:
    assert set(FLAG_EFFECTS) == set(QualityFlag)


@pytest.mark.parametrize(
    ("flags", "status"),
    [
        ((), QualityStatus.ACCEPTED),
        ((QualityFlag.LATE_ARRIVAL, QualityFlag.DERIVED_EVENT_ID), QualityStatus.ACCEPTED),
        ((QualityFlag.LATE_ARRIVAL, QualityFlag.UNKNOWN_SERVICE), QualityStatus.DEGRADED),
        ((QualityFlag.CLOCK_SKEW,), QualityStatus.DEGRADED),
        ((QualityFlag.CLOCK_SKEW, QualityFlag.MALFORMED_PAYLOAD), QualityStatus.QUARANTINED),
        ((QualityFlag.CONTRADICTORY_IDENTITY,), QualityStatus.QUARANTINED),
    ],
)
def test_most_severe_flag_sets_the_status(
    flags: tuple[QualityFlag, ...], status: QualityStatus
) -> None:
    assert assess(flags)[0] is status


def test_flags_are_deduplicated_in_declaration_order() -> None:
    _, flags = assess(
        [QualityFlag.DERIVED_EVENT_ID, QualityFlag.MALFORMED_PAYLOAD, QualityFlag.DERIVED_EVENT_ID]
    )

    assert flags == (QualityFlag.MALFORMED_PAYLOAD, QualityFlag.DERIVED_EVENT_ID)


@pytest.mark.parametrize(
    ("occurred_at", "flags"),
    [
        (NOW, ()),
        (NOW + timedelta(minutes=5), ()),
        (NOW + timedelta(minutes=5, seconds=1), (QualityFlag.CLOCK_SKEW,)),
        (NOW - timedelta(minutes=15), ()),
        (NOW - timedelta(minutes=15, seconds=1), (QualityFlag.LATE_ARRIVAL,)),
    ],
)
def test_timing_flags(occurred_at: datetime, flags: tuple[QualityFlag, ...]) -> None:
    assert TIMING.flags(occurred_at=occurred_at, ingested_at=NOW, replay_scope="live") == flags


def test_timing_is_not_judged_for_replays() -> None:
    for occurred_at in (NOW + timedelta(hours=1), NOW - timedelta(days=3)):
        assert (
            TIMING.flags(occurred_at=occurred_at, ingested_at=NOW, replay_scope="replay:r1") == ()
        )


TRACE: dict[str, Any] = {
    "trace_id": "4bf92f35",
    "service": "payment",
    "operation": "POST /charges",
    "duration_ms": 40,
    "status": "ok",
    "span_count": 2,
}
DEPLOYMENT: dict[str, Any] = {"service": "payment", "environment": "demo", "revision": "p-9"}
LOG: dict[str, Any] = {"service": "payment", "level": "error", "message": "timeout"}


def test_derived_ids_are_stable_bounded_and_marked() -> None:
    for kind, payload in (
        (SourceKind.TRACE_SUMMARY, TRACE),
        (SourceKind.DEPLOYMENT, DEPLOYMENT),
        (SourceKind.STRUCTURED_LOG, LOG),
    ):
        derived = derive_source_event_id(kind, NOW, payload)
        assert derived == derive_source_event_id(kind, NOW, dict(payload))
        assert derived.startswith(f"derived:{kind.value}:")
        assert len(derived) <= MAX_SOURCE_EVENT_ID_LENGTH


def test_timestamp_offset_spelling_does_not_change_the_id() -> None:
    kyiv = NOW.astimezone(timezone(timedelta(hours=2)))

    assert derive_source_event_id(SourceKind.STRUCTURED_LOG, kyiv, LOG) == (
        derive_source_event_id(SourceKind.STRUCTURED_LOG, NOW, LOG)
    )


def test_trace_identity_ignores_measurements() -> None:
    # A re-sent summary with an updated duration is the same trace summary.
    first = derive_source_event_id(SourceKind.TRACE_SUMMARY, NOW, TRACE)
    resent = derive_source_event_id(
        SourceKind.TRACE_SUMMARY, NOW + timedelta(seconds=1), TRACE | {"duration_ms": 41}
    )
    other_service = derive_source_event_id(
        SourceKind.TRACE_SUMMARY, NOW, TRACE | {"service": "checkout"}
    )

    assert first == resent
    assert first != other_service


def test_redeploying_a_revision_later_is_a_new_event() -> None:
    first = derive_source_event_id(SourceKind.DEPLOYMENT, NOW, DEPLOYMENT)
    later = derive_source_event_id(SourceKind.DEPLOYMENT, NOW + timedelta(hours=1), DEPLOYMENT)

    assert first != later


def test_identical_log_text_at_different_times_or_fields_stays_distinct() -> None:
    base = derive_source_event_id(SourceKind.STRUCTURED_LOG, NOW, LOG)

    assert base != derive_source_event_id(
        SourceKind.STRUCTURED_LOG, NOW + timedelta(microseconds=1), LOG
    )
    assert base != derive_source_event_id(SourceKind.STRUCTURED_LOG, NOW, LOG | {"span_id": "a1"})


@pytest.mark.parametrize(
    ("kind", "payload", "missing"),
    [
        (SourceKind.TRACE_SUMMARY, {k: v for k, v in TRACE.items() if k != "trace_id"}, "trace_id"),
        (SourceKind.TRACE_SUMMARY, TRACE | {"service": None}, "service"),
        (SourceKind.DEPLOYMENT, DEPLOYMENT | {"revision": " "}, "revision"),
        (SourceKind.DEPLOYMENT, DEPLOYMENT | {"environment": 3}, "environment"),
    ],
)
def test_fallback_needs_its_identity_fields(
    kind: SourceKind, payload: dict[str, Any], missing: str
) -> None:
    with pytest.raises(MissingIdentityError, match=missing):
        derive_source_event_id(kind, NOW, payload)
