"""Unsupported versions, malformed IDs, oversized fields, and bad timestamps fail predictably."""

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from signalops.contracts.adapters import StructuredLogInput, TraceSummaryInput
from signalops.contracts.checkpoints import decode_checkpoint
from signalops.contracts.common import (
    MAX_ATTRIBUTES_BYTES,
    MAX_MESSAGE_LENGTH,
    ContractError,
    ContractErrorCategory,
    replay_scope_for,
)
from signalops.contracts.diagnosis import Diagnosis
from signalops.contracts.events import (
    MAX_ENVELOPE_BYTES,
    MAX_RAW_RECORD_BYTES,
    EventEnvelope,
    IncidentChangedPayload,
    RawSignalPayload,
    decode_event,
)
from signalops.contracts.evidence import NormalizedSignal, Severity
from signalops.contracts.incidents import IncidentSummary
from signalops.contracts.tools import MAX_TOOL_ROWS, TOOL_RESULT_ADAPTER

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1"
CANARY = "s3cr3t-canary-value"


def fixture(relative: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((FIXTURES / relative).read_text())
    return document


def encode(document: Any) -> bytes:
    return json.dumps(document).encode()


def decode_error(document: Any) -> ContractError:
    with pytest.raises(ContractError) as caught:
        decode_event(document if isinstance(document, bytes) else encode(document))
    return caught.value


# Envelopes


def test_unsupported_schema_version_is_reported_with_event_id() -> None:
    document = fixture("events/signal_raw.json") | {"schema_version": 2}
    error = decode_error(document)
    assert error.category is ContractErrorCategory.UNSUPPORTED_VERSION
    assert error.event_id == uuid.UUID(document["event_id"])


@pytest.mark.parametrize("version", ["1", 1.0, True, None])
def test_non_integer_schema_version_is_invalid(version: object) -> None:
    error = decode_error(fixture("events/signal_raw.json") | {"schema_version": version})
    assert error.category is ContractErrorCategory.INVALID_ENVELOPE


@pytest.mark.parametrize("event_type", ["signal.deleted", None, 7])
def test_unknown_event_type(event_type: object) -> None:
    error = decode_error(fixture("events/signal_raw.json") | {"event_type": event_type})
    assert error.category is ContractErrorCategory.UNKNOWN_EVENT_TYPE


@pytest.mark.parametrize("data", [b"{not json", b"\xff\xfe", b""])
def test_malformed_json(data: bytes) -> None:
    assert decode_error(data).category is ContractErrorCategory.MALFORMED_JSON


def test_non_object_envelope() -> None:
    assert decode_error([1, 2]).category is ContractErrorCategory.INVALID_ENVELOPE


def test_oversized_envelope_is_rejected_before_parsing() -> None:
    data = b" " * (MAX_ENVELOPE_BYTES + 1)
    assert decode_error(data).category is ContractErrorCategory.OVERSIZED


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "not-a-uuid"),
        ("workspace_id", "82631aa1-86f5-4d37-b39f"),
        ("source_id", 12345),
        ("causation_id", "original-envelope-event-id"),
        ("replay_run_id", "run-1"),
    ],
)
def test_malformed_ids(field: str, value: object) -> None:
    error = decode_error(fixture("events/signal_normalized.json") | {field: value})
    assert error.category is ContractErrorCategory.INVALID_ENVELOPE
    assert field in error.detail


@pytest.mark.parametrize(
    "value",
    [
        "2026-10-16T10:40:00",  # naive
        "2026-13-01T00:00:00Z",  # no such month
        "2026-10-16T25:00:00Z",
        "yesterday",
        1760611200,  # Unix timestamps are not accepted
        "",
    ],
)
def test_invalid_timestamps(value: object) -> None:
    error = decode_error(fixture("events/signal_raw.json") | {"occurred_at": value})
    assert error.category is ContractErrorCategory.INVALID_ENVELOPE
    assert "occurred_at" in error.detail


def test_offset_timestamps_are_normalized_to_utc() -> None:
    document = fixture("events/signal_raw.json") | {"occurred_at": "2026-10-16T12:40:00+02:00"}
    envelope = decode_event(encode(document))
    assert envelope.occurred_at == datetime(2026, 10, 16, 10, 40, tzinfo=UTC)
    assert envelope.occurred_at.tzinfo is UTC
    assert json.loads(envelope.encode())["occurred_at"] == "2026-10-16T10:40:00Z"


def test_source_events_require_source_id() -> None:
    error = decode_error(fixture("events/signal_raw.json") | {"source_id": None})
    assert error.category is ContractErrorCategory.INVALID_ENVELOPE
    assert "source_id" in error.detail


def test_domain_events_may_omit_source_and_causation() -> None:
    document = fixture("events/incident_changed.json")
    del document["source_id"], document["causation_id"]
    envelope = decode_event(encode(document))
    assert envelope.source_id is None
    assert envelope.causation_id is None


def test_invalid_payload_is_distinguished_from_invalid_envelope() -> None:
    document = fixture("events/signal_normalized.json")
    document["payload"]["kind"] = "metric"
    error = decode_error(document)
    assert error.category is ContractErrorCategory.INVALID_PAYLOAD
    assert error.event_id == uuid.UUID(document["event_id"])


def test_oversized_raw_record_is_an_invalid_payload() -> None:
    document = fixture("events/signal_raw.json")
    document["payload"]["record"] = {"message": "x" * MAX_RAW_RECORD_BYTES}
    error = decode_error(document)
    assert error.category is ContractErrorCategory.INVALID_PAYLOAD
    assert "record" in error.detail


def test_oversized_source_event_id() -> None:
    document = fixture("events/signal_raw.json")
    document["payload"]["source_event_id"] = "x" * 513
    assert decode_error(document).category is ContractErrorCategory.INVALID_PAYLOAD


def test_error_detail_never_contains_input_values() -> None:
    document = fixture("events/signal_raw.json")
    document["payload"]["source_kind"] = CANARY
    document["correlation_id"] = f"bad value {CANARY}"
    error = decode_error(document)
    assert CANARY not in error.detail
    assert CANARY not in str(error)


def test_additive_fields_from_newer_producers_are_ignored() -> None:
    document = fixture("events/signal_normalized.json") | {"tenant_hint": "x"}
    document["payload"]["new_optional_field"] = {"nested": True}
    envelope = decode_event(encode(document))
    assert envelope.model_dump(mode="json") == fixture("events/signal_normalized.json")


def test_envelope_rejects_a_payload_of_another_event_type() -> None:
    header = fixture("events/signal_raw.json")
    other = fixture("events/incident_changed.json")["payload"]
    payload = IncidentChangedPayload.model_validate(other)
    with pytest.raises(ValidationError, match="payload must be RawSignalPayload"):
        EventEnvelope[IncidentChangedPayload].model_validate(header | {"payload": payload})


def test_encode_rejects_oversized_envelopes() -> None:
    envelope = decode_event(encode(fixture("events/signal_raw.json")))
    record = {f"k{i}": "x" * 60_000 for i in range(5)}
    # model_construct skips the record bound so the encoder's own check is exercised.
    payload = RawSignalPayload.model_construct(**{**dict(envelope.payload), "record": record})
    big = envelope.model_copy(update={"payload": payload})
    with pytest.raises(ContractError) as caught:
        big.encode()
    assert caught.value.category is ContractErrorCategory.OVERSIZED


def test_replay_scope_derives_from_replay_run() -> None:
    run_id = uuid.UUID("11111111-2222-4333-8444-555555555555")
    assert replay_scope_for(None) == "live"
    assert replay_scope_for(run_id) == f"replay:{run_id}"
    document = fixture("events/signal_raw.json") | {"replay_run_id": str(run_id)}
    assert decode_event(encode(document)).replay_scope == f"replay:{run_id}"


# Adapter inputs and normalized evidence


def test_unknown_adapter_fields_are_preserved_in_attributes() -> None:
    record = StructuredLogInput.model_validate(
        {"level": "WARN", "message": "slow", "region": "eu-1", "attributes": {"pod": "p-1"}}
    )
    assert record.level is Severity.WARNING
    assert record.attributes == {"pod": "p-1", "region": "eu-1"}
    assert StructuredLogInput.model_validate(record.model_dump()) == record


def test_unknown_field_conflicting_with_attributes_is_rejected() -> None:
    with pytest.raises(ValidationError, match="region"):
        StructuredLogInput.model_validate(
            {"level": "info", "message": "m", "region": "a", "attributes": {"region": "b"}}
        )


@pytest.mark.parametrize(
    "record",
    [
        {"level": "info", "message": "x" * (MAX_MESSAGE_LENGTH + 1)},
        {"level": "info", "message": ""},
        {"level": "info", "message": "m", "attributes": {"blob": "x" * MAX_ATTRIBUTES_BYTES}},
        {"level": "info", "message": "m", "blob": "x" * MAX_ATTRIBUTES_BYTES},
        {"level": "loud", "message": "m"},
        {"level": "info", "message": "m", "trace_id": "has spaces"},
    ],
    ids=["long-message", "empty-message", "big-attributes", "big-unknown", "level", "trace-id"],
)
def test_invalid_log_records(record: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        StructuredLogInput.model_validate(record)


def test_trace_error_spans_cannot_exceed_spans() -> None:
    document = fixture("adapter_inputs/trace_summary.json") | {"error_span_count": 15}
    with pytest.raises(ValidationError, match="error_span_count"):
        TraceSummaryInput.model_validate(document)


def test_degraded_signal_needs_a_quality_flag() -> None:
    document = fixture("evidence/normalized_signal_degraded.json") | {"quality_flags": []}
    with pytest.raises(ValidationError, match="quality flag"):
        NormalizedSignal.model_validate(document)


def test_duplicate_quality_flags_are_rejected() -> None:
    document = fixture("evidence/normalized_signal_degraded.json")
    document["quality_flags"] = ["missing_service", "missing_service"]
    with pytest.raises(ValidationError, match="duplicates"):
        NormalizedSignal.model_validate(document)


@pytest.mark.parametrize("scope", ["", "Live", "x" * 129, "has space"])
def test_invalid_replay_scope(scope: str) -> None:
    document = fixture("evidence/normalized_signal.json") | {"replay_scope": scope}
    with pytest.raises(ValidationError, match="replay_scope"):
        NormalizedSignal.model_validate(document)


# Diagnoses


def diagnosis(**changes: Any) -> dict[str, Any]:
    return fixture("diagnoses/diagnosed.json") | changes


def root_cause(**changes: Any) -> dict[str, Any]:
    cause: dict[str, Any] = fixture("diagnoses/diagnosed.json")["root_cause"]
    return cause | changes


@pytest.mark.parametrize(
    ("document", "message"),
    [
        (diagnosis(root_cause=None), "needs a root_cause"),
        (diagnosis(status="insufficient_evidence"), "null root_cause"),
        (
            fixture("diagnoses/insufficient_evidence.json") | {"missing_evidence": []},
            "missing_evidence",
        ),
        (diagnosis(root_cause=root_cause(confidence=1.5)), "confidence"),
        (diagnosis(root_cause=root_cause(confidence=float("nan"))), "confidence"),
        (diagnosis(root_cause=root_cause(evidence_signal_ids=["signal-uuid-1"])), "evidence"),
        (diagnosis(root_cause=root_cause(evidence_signal_ids=[])), "evidence"),
        (
            diagnosis(root_cause=root_cause(evidence_signal_ids=[str(uuid.UUID(int=1))] * 2)),
            "duplicates",
        ),
        (diagnosis(root_cause=root_cause(failure_type="Pool Exhaustion")), "failure_type"),
        (
            diagnosis(suggested_actions=[{"action": "Revert", "requires_approval": False}]),
            "requires_approval",
        ),
        (diagnosis(summary="x" * 2001), "summary"),
        (diagnosis(reasoning="hidden chain of thought"), "reasoning"),
        (diagnosis(status="probably"), "status"),
    ],
    ids=[
        "diagnosed-without-cause",
        "abstain-with-cause",
        "abstain-without-missing",
        "confidence-high",
        "confidence-nan",
        "placeholder-id",
        "no-evidence",
        "duplicate-evidence",
        "failure-type",
        "unapproved-action",
        "long-summary",
        "extra-field",
        "status",
    ],
)
def test_invalid_diagnoses(document: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        Diagnosis.model_validate(document)


# Checkpoints


@pytest.mark.parametrize("version", [2, 0, "1", None, True])
def test_unsupported_checkpoint_versions(version: object) -> None:
    document = fixture("checkpoints/step_checkpoint.json") | {"schema_version": version}
    with pytest.raises(ContractError) as caught:
        decode_checkpoint(document)
    assert caught.value.category is ContractErrorCategory.UNSUPPORTED_VERSION


def test_invalid_checkpoint_body() -> None:
    document = fixture("checkpoints/step_checkpoint.json") | {"lease_generation": -1}
    with pytest.raises(ContractError) as caught:
        decode_checkpoint(document)
    assert caught.value.category is ContractErrorCategory.INVALID_PAYLOAD
    assert "lease_generation" in caught.value.detail


def test_checkpoint_is_reused_only_by_the_same_workflow_version() -> None:
    checkpoint = decode_checkpoint(fixture("checkpoints/step_checkpoint.json"))
    assert checkpoint.reusable_for("v1")
    assert not checkpoint.reusable_for("v2")


# Tool results and incidents


def test_tool_entries_after_the_evidence_cutoff_are_rejected() -> None:
    document = fixture("tool_results/query_logs.json")
    document["items"][0]["occurred_at"] = "2026-10-16T10:41:01Z"
    with pytest.raises(ValidationError, match="evidence_cutoff_at"):
        TOOL_RESULT_ADAPTER.validate_python(document)


def test_tool_results_are_row_bounded() -> None:
    document = fixture("tool_results/query_logs.json")
    document["items"] = document["items"] * (MAX_TOOL_ROWS + 1)
    with pytest.raises(ValidationError, match="at most"):
        TOOL_RESULT_ADAPTER.validate_python(document)


def test_unknown_tool_is_rejected() -> None:
    document = fixture("tool_results/query_logs.json") | {"tool": "run_sql"}
    with pytest.raises(ValidationError, match="tool"):
        TOOL_RESULT_ADAPTER.validate_python(document)


def test_resolved_incident_needs_resolved_at() -> None:
    document = fixture("incidents/incident_detail.json") | {"status": "resolved"}
    for key in (
        "fingerprint",
        "correlation_window_start",
        "services",
        "evidence",
        "triggers",
        "triggers_truncated",
    ):
        del document[key]
    with pytest.raises(ValidationError, match="resolved_at"):
        IncidentSummary.model_validate(document)
