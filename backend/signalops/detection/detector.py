"""The detector's consumer effect: normalized signal in, window aggregate and triggers out.

`Detector` runs inside `TransactionalProcessor`'s transaction, after the
consumer receipt. For a signal it can count, it loads the stored row (the
event carries a reference, not the evidence), records the sample, adds it to
its window, and evaluates the metric's rules against the updated window, all
in that one transaction.

Counted signals: kind `log` or `trace`, status `accepted`, and a resolved
service. Degraded signals are skipped: with a resolved service the only
degrading flag is `clock_skew`, whose `occurred_at` cannot place it in a
window. Quarantined signals and deployments never feed a window.

Deduplication happens twice before an aggregate changes: the consumer receipt
per event, then the sample key per (signal, series), which also holds when the
same signal arrives under another event ID.

A rule fires when the window has at least `min_samples` samples and crosses
its threshold, and the same rule has not fired for the service within
`cooldown_seconds` of the window (event time, so replay gives the same result).
Rules are evaluated as windows fill; closure and late data are P05-T02.
"""

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from sqlalchemy.ext.asyncio import AsyncSession

from signalops.config import Settings
from signalops.contracts.common import ContractError, ContractErrorCategory
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    NormalizedSignalPayload,
)
from signalops.contracts.evidence import QualityStatus, Severity, SignalKind
from signalops.detection.rules import (
    DetectionMetric,
    DetectionRule,
    duration_bucket,
    evaluate,
    series_name,
    window_for,
)
from signalops.persistence.models import Signal
from signalops.persistence.repositories.detection import (
    DetectionRepository,
    Sample,
    SeriesKey,
    TriggerRecord,
)
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

DETECTOR_CONSUMER_GROUP: Final = "detector-v1"

METRIC_BY_KIND: Final = {
    SignalKind.TRACE: DetectionMetric.TRACE,
    SignalKind.LOG: DetectionMetric.LOG,
}
ERROR_SEVERITIES: Final = frozenset({Severity.ERROR, Severity.CRITICAL})


@dataclass(frozen=True)
class DetectionPolicy:
    window_seconds: int
    rules: tuple[DetectionRule, ...]

    @classmethod
    def from_settings(cls, settings: Settings) -> "DetectionPolicy":
        return cls(settings.detection_window_seconds, settings.detection_rules)


def sample_for(signal: Signal, metric: DetectionMetric) -> Sample:
    """The sample a stored signal contributes; `ContractError` if its fields are unusable."""
    if metric is DetectionMetric.LOG:
        return Sample(signal.id, is_error=Severity(signal.severity) in ERROR_SEVERITIES)
    record = signal.attributes.get("record")
    duration = record.get("duration_ms") if isinstance(record, dict) else None
    status = record.get("status") if isinstance(record, dict) else None
    if (
        not isinstance(duration, int)
        or isinstance(duration, bool)
        or duration < 0
        or status not in ("ok", "error")
    ):
        raise ContractError(
            ContractErrorCategory.INVALID_PAYLOAD,
            "trace signal lacks record.duration_ms or record.status",
        )
    return Sample(
        signal.id,
        is_error=status == "error",
        value=float(duration),
        bucket=duration_bucket(duration),
    )


class Detector:
    """`EventHandler` for signals.normalized.v1."""

    def __init__(self, policy: DetectionPolicy) -> None:
        self._policy = policy

    async def __call__(self, session: AsyncSession, envelope: EventEnvelope[EventPayload]) -> None:
        payload = envelope.payload
        if envelope.event_type is not EventType.SIGNAL_NORMALIZED or not isinstance(
            payload, NormalizedSignalPayload
        ):
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                f"detector accepts {EventType.SIGNAL_NORMALIZED} only, not {envelope.event_type}",
                event_id=envelope.event_id,
            )
        metric = METRIC_BY_KIND.get(payload.kind)
        if (
            metric is None
            or payload.service_id is None
            or payload.quality_status is not QualityStatus.ACCEPTED
        ):
            return

        scope = EvidenceScope(envelope.workspace_id, envelope.replay_scope)
        signal = await EvidenceRepository(session).get_signal(scope, payload.signal_id)
        if signal is None:
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                "signal_id does not name a stored signal in this workspace and replay scope",
                event_id=envelope.event_id,
            )
        if signal.service_id is None or signal.quality_status != QualityStatus.ACCEPTED:
            # The stored row is authoritative over the event's summary of it.
            return
        try:
            sample = sample_for(signal, metric)
        except ContractError as error:
            raise ContractError(error.category, error.detail, event_id=envelope.event_id) from None

        window_seconds = self._policy.window_seconds
        series = SeriesKey(
            workspace_id=signal.workspace_id,
            service_id=signal.service_id,
            replay_scope=signal.replay_scope,
            metric_name=series_name(metric, window_seconds),
        )
        window = window_for(signal.occurred_at, window_seconds)
        detection = DetectionRepository(session)
        if not await detection.record_sample(series, window, sample):
            return
        counts = await detection.add_to_window(series, window, sample)

        for rule in self._policy.rules:
            if rule.metric is not metric:
                continue
            result = evaluate(rule, counts)
            if not result.breached:
                continue
            assert result.observed is not None  # noqa: S101 - a breach has a value
            await detection.lock_rule(series, rule.id)
            if await detection.triggered_near(
                series, rule.id, window.start, timedelta(seconds=rule.cooldown_seconds)
            ):
                continue
            if await detection.add_trigger(
                series,
                window,
                TriggerRecord(rule.id, rule.version, result.observed, rule.threshold, counts),
            ):
                logger.info(
                    "detection rule fired",
                    extra={
                        "rule_id": rule.id,
                        "rule_version": rule.version,
                        "service_id": str(series.service_id),
                        "replay_scope": series.replay_scope,
                        "window_start": window.start.isoformat(),
                        "observed": result.observed,
                        "threshold": rule.threshold,
                        "sample_count": counts.sample_count,
                    },
                )
