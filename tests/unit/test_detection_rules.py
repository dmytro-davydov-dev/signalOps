"""Detection rules, windows, and histogram arithmetic (P05-T01, P05-T02)."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr, ValidationError

from signalops.config import Settings
from signalops.contracts.adapters import MAX_DURATION_MS
from signalops.detection.rules import (
    DEFAULT_RULES,
    DURATION_BUCKETS_MS,
    DetectionMetric,
    DetectionRule,
    RuleKind,
    WindowCounts,
    duration_bucket,
    evaluate,
    histogram_quantile,
    metric_of_series,
    series_name,
    window_for,
)

ERROR_RATE = DetectionRule(
    id="errors",
    version=1,
    metric=DetectionMetric.TRACE,
    kind=RuleKind.ERROR_RATE,
    threshold=0.2,
    min_samples=10,
    cooldown_seconds=300,
)
P95 = DetectionRule(
    id="p95",
    version=1,
    metric=DetectionMetric.TRACE,
    kind=RuleKind.LATENCY_QUANTILE,
    threshold=1_000,
    quantile=0.95,
    min_samples=10,
    cooldown_seconds=0,
)


def test_windows_are_epoch_aligned_and_half_open() -> None:
    at = datetime(2026, 10, 30, 12, 0, 59, 999_999, tzinfo=UTC)
    window = window_for(at, 60)
    assert window.start == datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
    assert window.end == window.start + timedelta(seconds=60)
    assert window_for(window.end, 60).start == window.end
    assert window_for(datetime(2026, 10, 30, 12, 7, 30, tzinfo=UTC), 300).start == datetime(
        2026, 10, 30, 12, 5, tzinfo=UTC
    )


def test_window_is_past_only_beyond_its_lateness_deadline() -> None:
    window = window_for(datetime(2026, 10, 30, 12, 0, tzinfo=UTC), 60)
    lateness = timedelta(seconds=30)
    deadline = window.end + lateness
    assert not window.is_past(deadline - timedelta(microseconds=1), lateness)
    assert not window.is_past(deadline, lateness)
    assert window.is_past(deadline + timedelta(microseconds=1), lateness)
    assert window.is_past(window.end + timedelta(microseconds=1), timedelta(0))


def test_series_name_round_trips_to_its_metric() -> None:
    for metric in DetectionMetric:
        assert metric_of_series(series_name(metric, 300)) is metric


def test_window_requires_aware_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        window_for(datetime(2026, 10, 30, 12, 0), 60)  # noqa: DTZ001


def test_series_name_carries_window_size() -> None:
    assert series_name(DetectionMetric.TRACE, 60) == "trace.60s"
    assert series_name(DetectionMetric.LOG, 300) == "log.300s"


@pytest.mark.parametrize(
    ("duration", "bucket"),
    [(0, "5"), (5, "5"), (6, "10"), (999, "1000"), (1_000, "1000"), (1_001, "2500")],
)
def test_duration_bucket_is_smallest_bound_not_below(duration: int, bucket: str) -> None:
    assert duration_bucket(duration) == bucket


def test_every_reportable_duration_has_a_finite_bucket() -> None:
    assert DURATION_BUCKETS_MS[-1] == MAX_DURATION_MS
    assert duration_bucket(MAX_DURATION_MS) == str(MAX_DURATION_MS)
    with pytest.raises(ValueError, match="largest bucket"):
        duration_bucket(MAX_DURATION_MS + 1)


def test_histogram_quantile_reads_bucket_upper_bound() -> None:
    histogram = {"100": 90, "2500": 6, "5000": 4}
    assert histogram_quantile(histogram, 0.5) == 100
    assert histogram_quantile(histogram, 0.9) == 100
    assert histogram_quantile(histogram, 0.95) == 2500
    assert histogram_quantile(histogram, 0.99) == 5000
    assert histogram_quantile({}, 0.95) is None
    assert histogram_quantile({"100": 0}, 0.95) is None


def test_low_sample_window_never_breaches() -> None:
    # One timeout in an almost empty window is a 100 % error rate.
    sparse = WindowCounts(sample_count=1, error_count=1, histogram={"5000": 1})
    for rule in (ERROR_RATE, P95):
        result = evaluate(rule, sparse)
        assert not result.sufficient
        assert not result.breached
    assert evaluate(ERROR_RATE, sparse).observed == 1.0


def test_error_rate_breaches_at_threshold_with_enough_samples() -> None:
    assert evaluate(ERROR_RATE, WindowCounts(10, 2, {})).breached
    below = evaluate(ERROR_RATE, WindowCounts(10, 1, {}))
    assert below.sufficient
    assert not below.breached
    assert below.observed == pytest.approx(0.1)


def test_latency_quantile_breach() -> None:
    slow = WindowCounts(20, 0, {"100": 18, "2500": 2})
    assert evaluate(P95, slow).observed == 2500
    assert evaluate(P95, slow).breached
    assert not evaluate(P95, WindowCounts(20, 0, {"100": 19, "2500": 1})).breached


def test_latency_breach_is_exact_at_the_threshold_bucket() -> None:
    # Every sample is at most 1000 ms; the bucket bound equals the threshold.
    at_threshold = evaluate(P95, WindowCounts(20, 0, {"1000": 20}))
    assert at_threshold.observed == 1000
    assert not at_threshold.breached
    assert evaluate(P95, WindowCounts(20, 0, {"1000": 18, "2500": 2})).breached


def test_rule_validation() -> None:
    base = ERROR_RATE.model_dump()
    with pytest.raises(ValidationError, match="must not exceed 1"):
        DetectionRule.model_validate({**base, "threshold": 1.5})
    with pytest.raises(ValidationError, match="takes no quantile"):
        DetectionRule.model_validate({**base, "quantile": 0.9})
    with pytest.raises(ValidationError, match="needs a quantile"):
        DetectionRule.model_validate({**base, "kind": "latency_quantile", "threshold": 500})
    with pytest.raises(ValidationError, match="must be a bucket bound"):
        DetectionRule.model_validate({**P95.model_dump(), "threshold": 750})
    with pytest.raises(ValidationError, match="has no durations"):
        DetectionRule.model_validate(
            {**P95.model_dump(), "metric": DetectionMetric.LOG, "id": "log_p95"}
        )
    with pytest.raises(ValidationError):
        DetectionRule.model_validate({**base, "min_samples": 0})
    with pytest.raises(ValidationError):
        DetectionRule.model_validate({**base, "version": 0})


def _settings(**values: object) -> Settings:
    return Settings(database_url=SecretStr("postgresql://localhost/x"), **values)  # type: ignore[arg-type]


def test_settings_default_rules_and_json_override(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _settings().detection_rules == DEFAULT_RULES
    assert {rule.metric for rule in DEFAULT_RULES} == set(DetectionMetric)
    monkeypatch.setenv("SIGNALOPS_DATABASE_URL", "postgresql://localhost/x")
    monkeypatch.setenv("SIGNALOPS_DETECTION_RULES", f"[{P95.model_dump_json()}]")
    assert Settings().detection_rules == (P95,)  # type: ignore[call-arg]


def test_settings_refuse_duplicate_rule_ids() -> None:
    with pytest.raises(ValidationError, match="duplicate detection rule id"):
        _settings(detection_rules=(ERROR_RATE, ERROR_RATE.model_copy(update={"version": 2})))


def test_settings_default_and_bound_allowed_lateness() -> None:
    assert _settings().detection_allowed_lateness_seconds == 30
    assert _settings(detection_allowed_lateness_seconds=0).detection_allowed_lateness_seconds == 0
    with pytest.raises(ValidationError):
        _settings(detection_allowed_lateness_seconds=-1)
