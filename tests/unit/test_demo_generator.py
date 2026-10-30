import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from signalops.cli import main
from signalops.contracts.adapters import ADAPTER_INPUTS
from signalops.contracts.evidence import Severity, SourceKind
from signalops.demo.generator import (
    DemoDataset,
    DemoRecord,
    Scenario,
    ScenarioLabels,
    generate,
    serialize_records,
)
from signalops.persistence.seed import DEMO_ENVIRONMENT, DEMO_SERVICES, DEMO_SOURCES

SCENARIO_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "scenarios" / "v1"
SOURCE_KINDS = {source.id: source.kind for source in DEMO_SOURCES}
SERVICE_NAMES = {service.name for service in DEMO_SERVICES}
# Words that would let the runtime read the scenario or its labels from a record.
LABEL_WORDS = ("scenario", "fault", "label", "inject", "expected", "root_cause", "synthetic")


@pytest.fixture(scope="module")
def datasets() -> dict[Scenario, DemoDataset]:
    return {scenario: generate(scenario, seed=42) for scenario in Scenario}


@pytest.mark.parametrize("scenario", list(Scenario))
def test_committed_labels_match_regeneration(scenario: Scenario) -> None:
    # A mismatch means generator output changed. If intended, bump GENERATOR_VERSION
    # when existing datasets must stay distinguishable, then regenerate with
    # `signalops demo-generate <scenario> --out <file> --labels <fixture>`.
    path = SCENARIO_FIXTURES / f"{scenario}.labels.json"
    committed = ScenarioLabels.model_validate_json(path.read_text())
    regenerated = generate(
        scenario,
        seed=committed.seed,
        start=committed.start,
        duration=committed.end - committed.start,
    )
    assert regenerated.labels == committed


@pytest.mark.parametrize("scenario", list(Scenario))
def test_same_seed_reproduces_bytes_and_other_seeds_differ(scenario: Scenario) -> None:
    first = generate(scenario, seed=7)
    assert serialize_records(first.records) == serialize_records(generate(scenario, seed=7).records)
    assert first.labels.dataset_hash != generate(scenario, seed=8).labels.dataset_hash


def test_event_ids_are_unique_within_and_across_datasets(
    datasets: dict[Scenario, DemoDataset],
) -> None:
    ids = [record.source_event_id for d in datasets.values() for record in d.records]
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("scenario", list(Scenario))
def test_records_are_valid_adapter_input_for_seeded_catalog(
    scenario: Scenario, datasets: dict[Scenario, DemoDataset]
) -> None:
    dataset = datasets[scenario]
    assert {SOURCE_KINDS[record.source_id] for record in dataset.records} == set(SourceKind)
    for record in dataset.records:
        ADAPTER_INPUTS[SOURCE_KINDS[record.source_id]].model_validate(record.payload)
        assert record.payload["service"] in SERVICE_NAMES
        assert record.payload["environment"] == DEMO_ENVIRONMENT
        assert dataset.labels.start <= record.occurred_at < dataset.labels.end
    times = [record.occurred_at for record in dataset.records]
    assert times == sorted(times)


@pytest.mark.parametrize("scenario", list(Scenario))
def test_records_carry_no_labels(scenario: Scenario, datasets: dict[Scenario, DemoDataset]) -> None:
    dataset = datasets[scenario]
    text = serialize_records(dataset.records).decode().lower()
    for word in (*LABEL_WORDS, *(s.value for s in Scenario)):
        assert word not in text, word
    for line in text.splitlines():
        assert set(json.loads(line)) == {"source_id", "source_event_id", "occurred_at", "payload"}


def test_fault_and_healthy_records_share_attribute_keys(
    datasets: dict[Scenario, DemoDataset],
) -> None:
    # The fault changes values and frequencies, never the shape of a record.
    def shapes(dataset: DemoDataset) -> set[tuple[object, ...]]:
        return {
            (record.source_id, record.payload.get("service"), *sorted(attributes))
            for record in dataset.records
            if isinstance(attributes := record.payload["attributes"], dict)
        }

    assert shapes(datasets[Scenario.DOWNSTREAM_TIMEOUT]) <= shapes(datasets[Scenario.HEALTHY])


def test_logs_link_to_trace_summaries(datasets: dict[Scenario, DemoDataset]) -> None:
    for dataset in datasets.values():
        traced = {
            record.payload["trace_id"]
            for record in dataset.records
            if SOURCE_KINDS[record.source_id] is SourceKind.TRACE_SUMMARY
        }
        log_traces = {
            record.payload["trace_id"]
            for record in dataset.records
            if SOURCE_KINDS[record.source_id] is SourceKind.STRUCTURED_LOG
            and "trace_id" in record.payload
        }
        assert log_traces
        assert log_traces <= traced


def test_deployments_are_versioned(datasets: dict[Scenario, DemoDataset]) -> None:
    for dataset in datasets.values():
        deployments = [
            record.payload
            for record in dataset.records
            if SOURCE_KINDS[record.source_id] is SourceKind.DEPLOYMENT
        ]
        assert deployments
        for payload in deployments:
            assert payload["revision"] != payload["previous_revision"]
            assert str(payload["revision"]).startswith(f"{payload['service']}-")


def _payment_timeouts(records: tuple[DemoRecord, ...], start: datetime, end: datetime) -> int:
    return sum(
        1
        for record in records
        if record.payload.get("service") == "payment"
        and record.payload.get("level") == Severity.ERROR
        and start <= record.occurred_at < end
    )


def test_downstream_timeout_concentrates_errors_in_the_fault_window(
    datasets: dict[Scenario, DemoDataset],
) -> None:
    dataset = datasets[Scenario.DOWNSTREAM_TIMEOUT]
    labels = dataset.labels
    assert labels.expected_incident
    assert labels.fault_start is not None
    assert labels.fault_end is not None
    assert labels.root_cause_service == "bank-gateway"
    inside = _payment_timeouts(dataset.records, labels.fault_start, labels.fault_end)
    outside = _payment_timeouts(dataset.records, labels.start, labels.end) - inside
    assert inside >= 30
    assert outside <= 5


def test_healthy_has_noise_but_no_fault(datasets: dict[Scenario, DemoDataset]) -> None:
    dataset = datasets[Scenario.HEALTHY]
    labels = dataset.labels
    assert not labels.expected_incident
    assert labels.fault_start is None
    assert labels.root_cause_service is None
    assert labels.affected_services == ()
    assert _payment_timeouts(dataset.records, labels.start, labels.end) <= 5
    warnings = [r for r in dataset.records if r.payload.get("level") == Severity.WARNING]
    assert len(warnings) >= 5


def test_generate_rejects_naive_start_and_out_of_range_duration() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        generate(Scenario.HEALTHY, seed=1, start=datetime(2026, 10, 16))  # noqa: DTZ001
    with pytest.raises(ValueError, match="duration"):
        generate(Scenario.HEALTHY, seed=1, duration=timedelta(seconds=10))


def test_start_is_normalized_to_utc() -> None:
    local = datetime(2026, 10, 16, 12, 30, tzinfo=UTC).astimezone()
    assert generate(Scenario.HEALTHY, seed=1, start=local).labels.start.tzinfo is UTC


def test_cli_writes_signals_and_labels_separately(tmp_path: Path) -> None:
    signals = tmp_path / "signals.jsonl"
    labels = tmp_path / "labels.json"
    code = main(
        [
            "demo-generate",
            "downstream-timeout",
            "--seed",
            "3",
            "--duration-s",
            "120",
            "--out",
            str(signals),
            "--labels",
            str(labels),
        ]
    )
    assert code == 0
    expected = generate(Scenario.DOWNSTREAM_TIMEOUT, seed=3, duration=timedelta(seconds=120))
    assert signals.read_bytes() == serialize_records(expected.records)
    assert ScenarioLabels.model_validate_json(labels.read_text()) == expected.labels
    assert "fault" not in signals.read_text()


def test_cli_rejects_start_without_offset(tmp_path: Path) -> None:
    code = main(
        [
            "demo-generate",
            "healthy",
            "--start",
            "2026-10-16T10:30:00",
            "--out",
            str(tmp_path / "s.jsonl"),
        ]
    )
    assert code == 2
    assert not (tmp_path / "s.jsonl").exists()
