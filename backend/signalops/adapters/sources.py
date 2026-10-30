"""Per-kind source adapters: parse a raw record and map it onto the evidence model.

These functions are pure. Identity is only *reported* here; resolving it
against the service catalog happens in `adapters.normalizer`, inside the
consumer transaction.

A normalized signal's `attributes` keep three sections apart so source keys can
never collide with fields the normalizer adds:

- `source`: the record's attributes, including fields the adapter model does
  not define, exactly as the source sent them (after ingestion redaction).
- `record`: kind-specific fields that have no column of their own, such as a
  log's span ID or a trace's duration.
- `provenance`: where the signal came from and the identity the source
  reported, so an unresolved signal still shows what it claimed.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final

from pydantic import JsonValue, ValidationError

from signalops.contracts.adapters import (
    ADAPTER_INPUTS,
    AdapterInput,
    DeploymentInput,
    StructuredLogInput,
    TraceSummaryInput,
)
from signalops.contracts.common import (
    ContractError,
    ContractErrorCategory,
    describe_validation_error,
)
from signalops.contracts.evidence import Severity, SignalKind, SourceKind

# Bumped when an adapter's mapping changes, so stored evidence records which
# mapping produced it.
ADAPTER_VERSION: Final = 1

SIGNAL_KIND_BY_SOURCE: dict[SourceKind, SignalKind] = {
    SourceKind.STRUCTURED_LOG: SignalKind.LOG,
    SourceKind.TRACE_SUMMARY: SignalKind.TRACE,
    SourceKind.DEPLOYMENT: SignalKind.DEPLOYMENT,
}

# Attribute keys that also name the emitting service or its environment
# (OpenTelemetry resource conventions and common logger fields). A value that
# disagrees with the record's own field is a contradiction, not a fallback.
SERVICE_ATTRIBUTE_KEYS: Final = ("service", "service.name", "service_name")
ENVIRONMENT_ATTRIBUTE_KEYS: Final = (
    "environment",
    "env",
    "deployment.environment",
    "deployment.environment.name",
)


@dataclass(frozen=True)
class ReportedIdentity:
    """What the record says about its service and environment, unresolved."""

    service: str | None
    environment: str | None
    # Attribute keys whose value disagrees with another identity claim.
    conflicts: tuple[str, ...] = ()

    @property
    def contradictory(self) -> bool:
        return bool(self.conflicts)


@dataclass(frozen=True)
class DeploymentFields:
    revision: str
    previous_revision: str | None
    change_summary: str | None


@dataclass(frozen=True)
class AdaptedRecord:
    kind: SignalKind
    severity: Severity
    message: str
    trace_id: str | None
    identity: ReportedIdentity
    source_attributes: dict[str, JsonValue]
    record_fields: dict[str, JsonValue] = field(default_factory=dict)
    deployment: DeploymentFields | None = None


def parse_record(kind: SourceKind, record: Mapping[str, JsonValue]) -> AdapterInput:
    """Validate a raw record against its adapter contract, raising `ContractError`."""
    try:
        return ADAPTER_INPUTS[kind].model_validate(record)
    except ValidationError as error:
        raise ContractError(
            ContractErrorCategory.INVALID_PAYLOAD,
            f"{kind} record: {describe_validation_error(error)}",
        ) from None


def reported_identity(parsed: AdapterInput) -> ReportedIdentity:
    """The record's identity claims: its own fields, else a well-known attribute.

    An attribute is the source's own claim, so it stands in for a missing
    field. Every claim must agree with the first one seen, starting with the
    record's field; disagreeing keys are listed and nothing is chosen.
    """
    conflicts: list[str] = []
    resolved: list[str | None] = []
    for keys, claimed in (
        (SERVICE_ATTRIBUTE_KEYS, getattr(parsed, "service", None)),
        (ENVIRONMENT_ATTRIBUTE_KEYS, getattr(parsed, "environment", None)),
    ):
        agreed = claimed
        for key in keys:
            value = parsed.attributes.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            if agreed is None:
                agreed = value
            elif value != agreed:
                conflicts.append(key)
        resolved.append(agreed)
    service, environment = resolved
    return ReportedIdentity(service, environment, tuple(conflicts))


def adapt(kind: SourceKind, record: Mapping[str, JsonValue]) -> AdaptedRecord:
    parsed = parse_record(kind, record)
    identity = reported_identity(parsed)
    attributes = dict(parsed.attributes)
    match parsed:
        case StructuredLogInput():
            return AdaptedRecord(
                kind=SignalKind.LOG,
                severity=parsed.level,
                message=parsed.message,
                trace_id=parsed.trace_id,
                identity=identity,
                source_attributes=attributes,
                record_fields=_present({"span_id": parsed.span_id}),
            )
        case TraceSummaryInput():
            return AdaptedRecord(
                kind=SignalKind.TRACE,
                severity=Severity.ERROR if parsed.status == "error" else Severity.INFO,
                message=_trace_message(parsed),
                trace_id=parsed.trace_id,
                identity=identity,
                source_attributes=attributes,
                record_fields={
                    "operation": parsed.operation,
                    "duration_ms": parsed.duration_ms,
                    "status": parsed.status,
                    "span_count": parsed.span_count,
                    "error_span_count": parsed.error_span_count,
                    "downstream_services": list(parsed.downstream_services),
                },
            )
        case DeploymentInput():
            deployment = DeploymentFields(
                revision=parsed.revision,
                previous_revision=parsed.previous_revision,
                change_summary=parsed.change_summary,
            )
            return AdaptedRecord(
                kind=SignalKind.DEPLOYMENT,
                severity=Severity.INFO,
                message=_deployment_message(parsed),
                trace_id=None,
                identity=identity,
                source_attributes=attributes,
                record_fields=_present(
                    {
                        "revision": deployment.revision,
                        "previous_revision": deployment.previous_revision,
                        "change_summary": deployment.change_summary,
                    }
                ),
                deployment=deployment,
            )
    raise TypeError(f"no adapter for {type(parsed).__name__}")


def _trace_message(parsed: TraceSummaryInput) -> str:
    message = f"{parsed.operation} {parsed.status} in {parsed.duration_ms} ms"
    if parsed.error_span_count:
        message += f" ({parsed.error_span_count}/{parsed.span_count} spans failed)"
    return message


def _deployment_message(parsed: DeploymentInput) -> str:
    message = f"deployed {parsed.revision}"
    if parsed.previous_revision is not None:
        message += f" (previous {parsed.previous_revision})"
    if parsed.change_summary is not None:
        message += f": {parsed.change_summary}"
    return message


def _present(values: dict[str, JsonValue]) -> dict[str, JsonValue]:
    return {key: value for key, value in values.items() if value is not None}
