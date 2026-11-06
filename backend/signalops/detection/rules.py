"""Detection rules, window arithmetic, and rule evaluation. Pure: no I/O.

A detection metric turns one accepted signal into one sample: a count, an
error bit, and for traces a duration. Samples fall into epoch-aligned tumbling
windows of `window_seconds`, and the aggregate series name carries the window
size (`trace.60s`), so changing the size starts new series instead of mixing
windows of different lengths under one start time.

Durations go into a fixed histogram whose last bound is the largest duration a
trace summary may report, so every sample lands in a finite bucket. A latency
threshold must be a bucket bound, which makes the breach test exact: the
quantile exceeds the threshold exactly when fewer than ceil(q * n) samples are
at or below it. The reported quantile is the upper bound of the bucket that
reaches it, an overestimate by at most one bucket.

A rule fires only when its window holds at least `min_samples` samples, so a
single error in an almost empty window is not a 100 % error rate.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Final, Self

from pydantic import Field, model_validator

from signalops.contracts.adapters import MAX_DURATION_MS
from signalops.contracts.common import ContractModel, Identifier, bounded_text


class DetectionMetric(StrEnum):
    # One sample per trace summary: error when its status is "error"; value is duration_ms.
    TRACE = "trace"
    # One sample per log: error at severity error or critical; no value.
    LOG = "log"


class RuleKind(StrEnum):
    # Errors divided by samples in the window; breaches at or above the threshold.
    ERROR_RATE = "error_rate"
    # A duration quantile in milliseconds; breaches above the threshold.
    LATENCY_QUANTILE = "latency_quantile"


# Upper bounds in milliseconds, ascending. Stored as string keys of the
# aggregate's histogram; reading parses the keys, so a later change of bounds
# does not misread older windows.
DURATION_BUCKETS_MS: Final[tuple[int, ...]] = (
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    30_000,
    60_000,
    300_000,
    MAX_DURATION_MS,
)

METRICS_WITH_VALUES: Final = frozenset({DetectionMetric.TRACE})

RuleId = Annotated[Identifier, bounded_text(64)]


class DetectionRule(ContractModel):
    """One threshold rule over one metric. Bump `version` whenever its settings change."""

    id: RuleId
    version: int = Field(ge=1)
    metric: DetectionMetric
    kind: RuleKind
    # Error rate in (0, 1], or milliseconds for a latency quantile (a bucket bound).
    threshold: float = Field(gt=0)
    # Required for latency rules, for example 0.95.
    quantile: float | None = Field(default=None, gt=0, lt=1)
    min_samples: int = Field(ge=1, le=1_000_000)
    # A rule that fired for a service does not fire again for windows starting
    # within this many seconds of that window, in event time.
    cooldown_seconds: int = Field(ge=0, le=7 * 24 * 60 * 60)

    @model_validator(mode="after")
    def _check_kind(self) -> Self:
        if self.kind is RuleKind.ERROR_RATE:
            if self.threshold > 1:
                raise ValueError("an error_rate threshold must not exceed 1")
            if self.quantile is not None:
                raise ValueError("an error_rate rule takes no quantile")
        else:
            if self.quantile is None:
                raise ValueError("a latency_quantile rule needs a quantile")
            if self.metric not in METRICS_WITH_VALUES:
                raise ValueError(f"metric {self.metric} has no durations")
            if self.threshold not in DURATION_BUCKETS_MS:
                raise ValueError(
                    f"a latency threshold must be a bucket bound: {list(DURATION_BUCKETS_MS)}"
                )
        return self


DEFAULT_RULES: Final[tuple[DetectionRule, ...]] = (
    DetectionRule(
        id="trace_error_rate",
        version=1,
        metric=DetectionMetric.TRACE,
        kind=RuleKind.ERROR_RATE,
        threshold=0.2,
        min_samples=15,
        cooldown_seconds=300,
    ),
    DetectionRule(
        id="trace_latency_p95",
        version=1,
        metric=DetectionMetric.TRACE,
        kind=RuleKind.LATENCY_QUANTILE,
        threshold=1_000,
        quantile=0.95,
        # Below 20 samples a p95 is the window's maximum: one slow request.
        min_samples=20,
        cooldown_seconds=300,
    ),
    DetectionRule(
        id="log_error_rate",
        version=1,
        metric=DetectionMetric.LOG,
        kind=RuleKind.ERROR_RATE,
        threshold=0.5,
        min_samples=10,
        cooldown_seconds=300,
    ),
)


def check_unique_rule_ids(rules: Sequence[DetectionRule]) -> None:
    seen: set[str] = set()
    for rule in rules:
        if rule.id in seen:
            raise ValueError(f"duplicate detection rule id {rule.id!r}")
        seen.add(rule.id)


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime


def window_for(at: datetime, window_seconds: int) -> Window:
    """The epoch-aligned tumbling window containing `at`."""
    if at.tzinfo is None:
        raise ValueError("window_for needs a timezone-aware datetime")
    epoch = int(at.timestamp())
    start = datetime.fromtimestamp(epoch - epoch % window_seconds, tz=UTC)
    return Window(start, start + timedelta(seconds=window_seconds))


def series_name(metric: DetectionMetric, window_seconds: int) -> str:
    return f"{metric}.{window_seconds}s"


def duration_bucket(duration_ms: float) -> str:
    """Histogram key for a duration: the smallest bound not below it."""
    for bound in DURATION_BUCKETS_MS:
        if duration_ms <= bound:
            return str(bound)
    raise ValueError(f"duration {duration_ms} ms exceeds the largest bucket")


def histogram_quantile(histogram: Mapping[str, int], quantile: float) -> float | None:
    """Upper bound of the bucket where the cumulative count reaches `quantile`."""
    buckets = sorted((float(bound), count) for bound, count in histogram.items() if count > 0)
    total = sum(count for _, count in buckets)
    if total == 0:
        return None
    rank = math.ceil(quantile * total)
    cumulative = 0
    for bound, count in buckets:
        cumulative += count
        if cumulative >= rank:
            return bound
    return buckets[-1][0]


@dataclass(frozen=True)
class WindowCounts:
    sample_count: int
    error_count: int
    histogram: Mapping[str, int]


@dataclass(frozen=True)
class Evaluation:
    # None when the window has nothing to measure (no durations).
    observed: float | None
    # min_samples reached; below it the rule never fires.
    sufficient: bool
    breached: bool


def evaluate(rule: DetectionRule, counts: WindowCounts) -> Evaluation:
    sufficient = counts.sample_count >= rule.min_samples
    if rule.kind is RuleKind.ERROR_RATE:
        observed = counts.error_count / counts.sample_count if counts.sample_count else None
        crossed = observed is not None and observed >= rule.threshold
    else:
        assert rule.quantile is not None  # noqa: S101 - enforced by DetectionRule
        observed = histogram_quantile(counts.histogram, rule.quantile)
        total = sum(counts.histogram.values())
        at_or_below = sum(
            count for bound, count in counts.histogram.items() if float(bound) <= rule.threshold
        )
        crossed = total > 0 and at_or_below < math.ceil(rule.quantile * total)
    return Evaluation(observed=observed, sufficient=sufficient, breached=sufficient and crossed)
