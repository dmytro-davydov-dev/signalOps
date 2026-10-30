"""Normalization quality policy: which flags a signal gets and what status follows.

Each flag has a fixed effect on the signal's status; the most severe effect of
all its flags wins:

- quarantine: the record cannot be trusted as evidence (`malformed_payload`,
  `contradictory_identity`). Kept for audit only.
- degrade: usable with care (`missing_service`, `unknown_service`,
  `clock_skew`).
- note: the evidence itself is sound (`late_arrival`, `derived_event_id`,
  `truncated_message`). The status stays `accepted`.

Timing is judged against the envelope's `ingested_at`, so reprocessing a record
later gives the same answer. It applies to the live scope only: a replay copies
historical records, whose distance from ingestion says nothing about the source.

This module also defines the stable identity used when a source sends no
`source_event_id`; ingestion applies it before the raw event ID is derived.
"""

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import IntEnum
from typing import Final

from pydantic import JsonValue

from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE
from signalops.contracts.evidence import QualityFlag, QualityStatus, SourceKind


class FlagEffect(IntEnum):
    NOTE = 0
    DEGRADE = 1
    QUARANTINE = 2


FLAG_EFFECTS: Final[Mapping[QualityFlag, FlagEffect]] = {
    QualityFlag.MALFORMED_PAYLOAD: FlagEffect.QUARANTINE,
    QualityFlag.CONTRADICTORY_IDENTITY: FlagEffect.QUARANTINE,
    QualityFlag.MISSING_SERVICE: FlagEffect.DEGRADE,
    QualityFlag.UNKNOWN_SERVICE: FlagEffect.DEGRADE,
    QualityFlag.CLOCK_SKEW: FlagEffect.DEGRADE,
    QualityFlag.LATE_ARRIVAL: FlagEffect.NOTE,
    QualityFlag.DERIVED_EVENT_ID: FlagEffect.NOTE,
    QualityFlag.TRUNCATED_MESSAGE: FlagEffect.NOTE,
}

STATUS_BY_EFFECT: Final[Mapping[FlagEffect, QualityStatus]] = {
    FlagEffect.NOTE: QualityStatus.ACCEPTED,
    FlagEffect.DEGRADE: QualityStatus.DEGRADED,
    FlagEffect.QUARANTINE: QualityStatus.QUARANTINED,
}


def assess(flags: Iterable[QualityFlag]) -> tuple[QualityStatus, tuple[QualityFlag, ...]]:
    """The status for a set of flags, and the flags deduplicated in declaration order."""
    present = set(flags)
    ordered = tuple(flag for flag in QualityFlag if flag in present)
    effect = max((FLAG_EFFECTS[flag] for flag in ordered), default=FlagEffect.NOTE)
    return STATUS_BY_EFFECT[effect], ordered


@dataclass(frozen=True)
class TimingPolicy:
    # occurred_at further ahead of ingested_at than this: the source clock is wrong.
    max_clock_skew: timedelta
    # ingested_at further behind occurred_at than this: the record arrived late.
    late_arrival: timedelta

    @classmethod
    def from_settings(cls, settings: Settings) -> "TimingPolicy":
        return cls(
            max_clock_skew=timedelta(seconds=settings.normalizer_max_clock_skew_seconds),
            late_arrival=timedelta(seconds=settings.normalizer_late_arrival_seconds),
        )

    def flags(
        self, *, occurred_at: datetime, ingested_at: datetime, replay_scope: str
    ) -> tuple[QualityFlag, ...]:
        if replay_scope != LIVE_REPLAY_SCOPE:
            return ()
        if occurred_at - ingested_at > self.max_clock_skew:
            return (QualityFlag.CLOCK_SKEW,)
        if ingested_at - occurred_at > self.late_arrival:
            return (QualityFlag.LATE_ARRIVAL,)
        return ()


DERIVED_ID_PREFIX: Final = "derived"


class MissingIdentityError(ValueError):
    """The record lacks the fields its kind's fallback identity needs."""


def derive_source_event_id(
    kind: SourceKind, occurred_at: datetime, payload: Mapping[str, JsonValue]
) -> str:
    """A stable `source_event_id` for a record sent without one.

    The same record always yields the same ID, so a client retry collapses.
    What counts as "the same record" depends on the kind:

    - trace summary: `trace_id`, `service`, `operation`. One summary per service
      and operation per trace.
    - deployment: `service`, `environment`, `revision`, `occurred_at`. Redeploying
      a revision later is a separate event.
    - structured log: `occurred_at` and the whole (redacted) payload. Never the
      message alone; two lines identical in every field and timestamp collapse.
    """
    match kind:
        case SourceKind.TRACE_SUMMARY:
            parts: list[JsonValue] = _required(payload, "trace_id", "service", "operation")
        case SourceKind.DEPLOYMENT:
            parts = [
                *_required(payload, "service", "environment", "revision"),
                _timestamp(occurred_at),
            ]
        case SourceKind.STRUCTURED_LOG:
            parts = [_timestamp(occurred_at), dict(payload)]
    canonical = json.dumps(parts, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(canonical.encode()).hexdigest()[:40]
    return f"{DERIVED_ID_PREFIX}:{kind.value}:{digest}"


def _required(payload: Mapping[str, JsonValue], *keys: str) -> list[JsonValue]:
    values: list[JsonValue] = []
    for key in keys:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise MissingIdentityError(f"fallback identity needs a non-empty string {key!r}")
        values.append(value)
    return values


def _timestamp(value: datetime) -> str:
    # Microsecond precision in UTC, independent of how the client wrote the offset.
    return value.astimezone(UTC).isoformat(timespec="microseconds")
