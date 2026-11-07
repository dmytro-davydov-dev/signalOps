"""The detector's consumer effect: normalized signal in, windows, triggers, and incidents out.

`Detector` runs inside `TransactionalProcessor`'s transaction, after the
consumer receipt. For a signal it can count, it loads the stored row (the
event carries a reference, not the evidence), records the sample, and adds it
to its window, all in that one transaction.

Counted signals: kind `log` or `trace`, status `accepted`, and a resolved
service. Degraded signals are skipped: with a resolved service the only
degrading flag is `clock_skew`, whose `occurred_at` cannot place it in a
window. Quarantined signals and deployments never feed a window.

Deduplication happens twice before an aggregate changes: the consumer receipt
per event, then the sample key per (signal, series), which also holds when the
same signal arrives under another event ID.

Rules are evaluated when a window closes, so arrival order inside the lateness
bound cannot change the result. A window closes once its clock is past
`window_end` plus the allowed lateness. Live, that clock is the wall clock
(`closure.WindowCloser`) and a sample is judged by its `ingested_at`, so
consumer lag does not make it late. In a replay scope it is the service's
durable simulated clock, advanced here, plus a final flush.

A sample past the deadline is recorded with `late` set and counted only in
`late_count`; it never reaches a rule, so old data cannot raise a historical
trigger. An on-time sample that finds its window already closed (consumer lag,
or the race at the deadline) joins the counts and re-evaluates the window;
triggers are unique per window, so this is idempotent, and one already raised
is not withdrawn.

A rule fires when the window has at least `min_samples` samples and crosses
its threshold, and the same rule has not fired for the service within
`cooldown_seconds` of the window (event time, so replay gives the same result).
Every breach, cooled down or not, also reaches `incidents`, in the same
transaction as the trigger. A closed window that could have fired and did not
counts toward recovery instead.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Final

from sqlalchemy.ext.asyncio import AsyncSession

from signalops.agents.investigations import InvestigationPolicy
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE, ContractError, ContractErrorCategory
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    NormalizedSignalPayload,
)
from signalops.contracts.evidence import QualityStatus, Severity, SignalKind
from signalops.detection.incidents import Breach, IncidentPolicy, record_breach, record_healthy
from signalops.detection.rules import (
    DetectionMetric,
    DetectionRule,
    Window,
    WindowClosure,
    WindowCounts,
    duration_bucket,
    evaluate,
    metric_of_series,
    series_name,
    window_for,
)
from signalops.persistence.models import Signal
from signalops.persistence.repositories.detection import (
    DetectionRepository,
    OpenWindow,
    Sample,
    SeriesKey,
    TriggerRecord,
)
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.incidents import IncidentRepository
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
    allowed_lateness_seconds: int
    incidents: IncidentPolicy = field(default_factory=IncidentPolicy)

    @classmethod
    def from_settings(cls, settings: Settings) -> "DetectionPolicy":
        return cls(
            settings.detection_window_seconds,
            settings.detection_rules,
            settings.detection_allowed_lateness_seconds,
            IncidentPolicy(
                settings.incident_correlation_gap_seconds,
                settings.incident_recovery_seconds,
                (
                    InvestigationPolicy.from_settings(settings)
                    if settings.investigation_auto_request
                    else None
                ),
            ),
        )

    @property
    def lateness(self) -> timedelta:
        return timedelta(seconds=self.allowed_lateness_seconds)


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


async def evaluate_window(
    session: AsyncSession,
    policy: DetectionPolicy,
    series: SeriesKey,
    window: Window,
    counts: WindowCounts,
) -> bool:
    """Run the series' rules against a closed window, recording what fires.

    Returns whether the window shows the service healthy: a rule had enough
    samples to fire and none breached.
    """
    detection = DetectionRepository(session)
    metric = metric_of_series(series.metric_name)
    sufficient = breached = False
    for rule in policy.rules:
        if rule.metric is not metric:
            continue
        result = evaluate(rule, counts)
        sufficient = sufficient or result.sufficient
        if not result.breached:
            continue
        breached = True
        assert result.observed is not None  # noqa: S101 - a breach has a value
        # Taken before the rule lock, so writers that hold both cannot deadlock.
        await IncidentRepository(session).lock_scope(series.replay_scope)
        await detection.lock_rule(series, rule.id)
        trigger_id = None
        if not await detection.triggered_near(
            series, rule.id, window.start, timedelta(seconds=rule.cooldown_seconds)
        ):
            trigger_id = await detection.add_trigger(
                series,
                window,
                TriggerRecord(rule.id, rule.version, result.observed, rule.threshold, counts),
            )
        if trigger_id is not None:
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
        await record_breach(
            session,
            policy.incidents,
            Breach(series, window, rule, trigger_id, deadline=window.end + policy.lateness),
        )
    return sufficient and not breached


async def close_windows(
    session: AsyncSession,
    policy: DetectionPolicy,
    windows: Sequence[OpenWindow],
    closed_by: WindowClosure,
) -> None:
    """Close locked windows and evaluate them, in the given order (oldest first for cooldowns).

    Recovery runs after every breach in the batch, so a healthy trace window
    cannot resolve an incident that a log window of the same minute extends.
    """
    if not windows:
        return
    await DetectionRepository(session).mark_closed(
        [open_window.id for open_window in windows], closed_by
    )
    healthy = [
        open_window
        for open_window in windows
        if await evaluate_window(
            session, policy, open_window.series, open_window.window, open_window.counts
        )
    ]
    for open_window in healthy:
        await record_healthy(session, policy.incidents, open_window.series, open_window.window)


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
        live = series.replay_scope == LIVE_REPLAY_SCOPE
        clock = (
            signal.ingested_at
            if live
            else await detection.advance_clock(series, signal.occurred_at)
        )
        late = window.is_past(clock, self._policy.lateness)
        if not await detection.record_sample(series, window, sample, late=late):
            return
        if late:
            await detection.add_late_to_window(series, window)
        else:
            update = await detection.add_to_window(series, window, sample)
            if update.closed and await evaluate_window(
                session, self._policy, series, window, update.counts
            ):
                await record_healthy(session, self._policy.incidents, series, window)
        if not live:
            due = await detection.due_service_windows(series, clock - self._policy.lateness)
            await close_windows(session, self._policy, due, WindowClosure.SIMULATED_CLOCK)
