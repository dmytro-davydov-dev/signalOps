import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from signalops.adapters.normalizer import identity_flags, signal_id_for
from signalops.adapters.sources import ReportedIdentity, adapt, reported_identity
from signalops.contracts.adapters import StructuredLogInput
from signalops.contracts.common import ContractError, ContractErrorCategory
from signalops.contracts.evidence import (
    QualityFlag,
    Severity,
    SignalKind,
    SourceKind,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "adapter_inputs"


def _fixture(name: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((FIXTURES / f"{name}.json").read_text())
    return document


def test_structured_log_maps_level_message_trace_and_span() -> None:
    adapted = adapt(SourceKind.STRUCTURED_LOG, _fixture("structured_log"))

    assert adapted.kind is SignalKind.LOG
    assert adapted.severity is Severity.ERROR
    assert adapted.message == "upstream bank-gateway call timed out after 2000 ms"
    assert adapted.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert adapted.record_fields == {"span_id": "00f067aa0ba902b7"}
    assert adapted.source_attributes == {
        "http.route": "/v1/charges",
        "pool.active": 50,
        "pool.max": 50,
    }
    assert adapted.identity == ReportedIdentity("payment", "demo")
    assert adapted.deployment is None


def test_trace_summary_derives_severity_and_message() -> None:
    adapted = adapt(SourceKind.TRACE_SUMMARY, _fixture("trace_summary"))

    assert adapted.kind is SignalKind.TRACE
    assert adapted.severity is Severity.ERROR
    assert adapted.message == "POST /checkout error in 2140 ms (2/14 spans failed)"
    assert adapted.record_fields == {
        "operation": "POST /checkout",
        "duration_ms": 2140,
        "status": "error",
        "span_count": 14,
        "error_span_count": 2,
        "downstream_services": ["payment", "inventory"],
    }


def test_successful_trace_is_info() -> None:
    record = {**_fixture("trace_summary"), "status": "ok", "error_span_count": 0}

    adapted = adapt(SourceKind.TRACE_SUMMARY, record)

    assert adapted.severity is Severity.INFO
    assert adapted.message == "POST /checkout ok in 2140 ms"


def test_deployment_keeps_revisions() -> None:
    adapted = adapt(SourceKind.DEPLOYMENT, _fixture("deployment"))

    assert adapted.kind is SignalKind.DEPLOYMENT
    assert adapted.severity is Severity.INFO
    assert adapted.trace_id is None
    assert adapted.deployment is not None
    assert adapted.deployment.revision == "payment-2026.10.16-3"
    assert adapted.deployment.previous_revision == "payment-2026.10.15-1"
    assert adapted.message.startswith(
        "deployed payment-2026.10.16-3 (previous payment-2026.10.15-1)"
    )
    assert adapted.source_attributes == {"pipeline": "demo-cd", "commit": "9f2c1e7"}


def test_unknown_fields_are_preserved_as_source_attributes() -> None:
    record = {**_fixture("structured_log"), "host": "10.0.4.17", "pid": 812}

    adapted = adapt(SourceKind.STRUCTURED_LOG, record)

    assert adapted.source_attributes["host"] == "10.0.4.17"
    assert adapted.source_attributes["pid"] == 812


@pytest.mark.parametrize(
    ("kind", "drop"),
    [
        (SourceKind.STRUCTURED_LOG, "message"),
        (SourceKind.TRACE_SUMMARY, "trace_id"),
        (SourceKind.DEPLOYMENT, "revision"),
    ],
)
def test_contract_violation_is_an_invalid_payload(kind: SourceKind, drop: str) -> None:
    record = {key: value for key, value in _fixture(kind.value).items() if key != drop}

    with pytest.raises(ContractError) as caught:
        adapt(kind, record)

    assert caught.value.category is ContractErrorCategory.INVALID_PAYLOAD
    assert drop in caught.value.detail


def test_a_log_record_is_not_accepted_as_a_deployment() -> None:
    with pytest.raises(ContractError):
        adapt(SourceKind.DEPLOYMENT, _fixture("structured_log"))


def _identity(**fields: Any) -> ReportedIdentity:
    record = {"level": "info", "message": "m", **fields}
    return reported_identity(StructuredLogInput.model_validate(record))


def test_identity_fields_are_reported_as_given() -> None:
    assert _identity(service="payment", environment="demo") == ReportedIdentity("payment", "demo")


def test_missing_fields_are_not_filled_in() -> None:
    assert _identity(service="payment") == ReportedIdentity("payment", None)
    assert _identity() == ReportedIdentity(None, None)


def test_well_known_attributes_stand_in_for_missing_fields() -> None:
    identity = _identity(attributes={"service.name": "payment", "deployment.environment": "demo"})

    assert identity == ReportedIdentity("payment", "demo")


def test_agreeing_attributes_are_not_a_conflict() -> None:
    identity = _identity(
        service="payment", environment="demo", attributes={"service.name": "payment", "env": "demo"}
    )

    assert not identity.contradictory


@pytest.mark.parametrize(
    ("fields", "conflicts"),
    [
        ({"service": "payment", "attributes": {"service.name": "checkout"}}, ("service.name",)),
        ({"environment": "demo", "attributes": {"env": "prod"}}, ("env",)),
        # Two attributes that disagree with each other when the field is absent.
        (
            {"attributes": {"service.name": "payment", "service_name": "checkout"}},
            ("service_name",),
        ),
        # An unknown top-level field is moved into attributes and still counts.
        ({"service": "payment", "service_name": "checkout"}, ("service_name",)),
    ],
)
def test_disagreeing_claims_are_listed(fields: dict[str, Any], conflicts: tuple[str, ...]) -> None:
    identity = _identity(**fields)

    assert identity.contradictory
    assert identity.conflicts == conflicts


def test_non_string_attribute_values_are_not_identity_claims() -> None:
    identity = _identity(service="payment", attributes={"service": {"name": "x"}, "env": " "})

    assert identity == ReportedIdentity("payment", None)


SERVICE_ID = uuid.UUID("5f3e2d1c-0b9a-4876-9543-210fedcba987")


@pytest.mark.parametrize(
    ("identity", "service_id", "flags"),
    [
        (ReportedIdentity("payment", "demo"), SERVICE_ID, ()),
        (ReportedIdentity("payment", None), None, (QualityFlag.MISSING_SERVICE,)),
        (ReportedIdentity(None, "demo"), None, (QualityFlag.MISSING_SERVICE,)),
        (ReportedIdentity("payment", "prod"), None, (QualityFlag.UNKNOWN_SERVICE,)),
        (
            ReportedIdentity("payment", "demo", ("service.name",)),
            None,
            (QualityFlag.CONTRADICTORY_IDENTITY,),
        ),
    ],
)
def test_identity_flags(
    identity: ReportedIdentity,
    service_id: uuid.UUID | None,
    flags: tuple[QualityFlag, ...],
) -> None:
    assert identity_flags(identity, service_id) == flags


def test_signal_id_is_stable_and_scoped() -> None:
    source = uuid.uuid4()

    assert signal_id_for(source, "live", "e-1") == signal_id_for(source, "live", "e-1")
    assert signal_id_for(source, "live", "e-1") != signal_id_for(source, "replay:x", "e-1")
    assert signal_id_for(source, "live", "e-1") != signal_id_for(uuid.uuid4(), "live", "e-1")
