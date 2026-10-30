"""Diagnosis status versus investigation status, across API and event contracts."""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.events import InvestigationCompletedPayload
from signalops.contracts.investigations import (
    DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS,
    InvestigationView,
    terminal_status_for,
)
from signalops.contracts.workflow import TERMINAL_INVESTIGATION_STATUSES, InvestigationStatus

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1"
DIAGNOSES = {
    DiagnosisStatus.DIAGNOSED: "diagnoses/diagnosed.json",
    DiagnosisStatus.INSUFFICIENT_EVIDENCE: "diagnoses/insufficient_evidence.json",
}


def fixture(relative: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((FIXTURES / relative).read_text())
    return document


def view(status: InvestigationStatus, diagnosis: DiagnosisStatus | None) -> dict[str, Any]:
    document = fixture("investigations/running.json")
    terminal = status in TERMINAL_INVESTIGATION_STATUSES
    return document | {
        "status": status,
        "completed_at": "2026-10-16T10:44:00Z" if terminal else None,
        "diagnosis": fixture(DIAGNOSES[diagnosis]) if diagnosis else None,
    }


def test_mapping_covers_every_status_and_is_one_to_one() -> None:
    assert set(DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS) == set(InvestigationStatus)
    mapped = [d for d in DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS.values() if d is not None]
    assert sorted(mapped) == sorted(DiagnosisStatus)
    for status, diagnosis in DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS.items():
        if diagnosis is not None:
            assert status in TERMINAL_INVESTIGATION_STATUSES


def test_completed_investigation_contains_diagnosed_output() -> None:
    investigation = InvestigationView.model_validate(fixture("investigations/completed.json"))
    assert investigation.status is InvestigationStatus.COMPLETED
    assert investigation.diagnosis is not None
    assert investigation.diagnosis.status is DiagnosisStatus.DIAGNOSED


def test_abstention_maps_to_insufficient_evidence_investigation() -> None:
    abstention = Diagnosis.model_validate(fixture("diagnoses/insufficient_evidence.json"))
    assert terminal_status_for(abstention) is InvestigationStatus.INSUFFICIENT_EVIDENCE
    diagnosed = Diagnosis.model_validate(fixture("diagnoses/diagnosed.json"))
    assert terminal_status_for(diagnosed) is InvestigationStatus.COMPLETED


@pytest.mark.parametrize("status", list(InvestigationStatus))
@pytest.mark.parametrize("diagnosis", [None, *DiagnosisStatus])
def test_investigation_view_enforces_mapping(
    status: InvestigationStatus, diagnosis: DiagnosisStatus | None
) -> None:
    document = view(status, diagnosis)
    if DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS[status] == diagnosis:
        InvestigationView.model_validate(document)
    else:
        with pytest.raises(ValidationError, match="diagnosis"):
            InvestigationView.model_validate(document)


@pytest.mark.parametrize("status", sorted(TERMINAL_INVESTIGATION_STATUSES))
@pytest.mark.parametrize("diagnosis", [None, *DiagnosisStatus])
def test_completed_event_enforces_mapping(
    status: InvestigationStatus, diagnosis: DiagnosisStatus | None
) -> None:
    payload = fixture("events/investigation_completed.json")["payload"] | {
        "status": status,
        "diagnosis_status": diagnosis,
    }
    if DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS[status] == diagnosis:
        InvestigationCompletedPayload.model_validate(payload)
    else:
        with pytest.raises(ValidationError, match="diagnosis"):
            InvestigationCompletedPayload.model_validate(payload)


def test_terminal_status_requires_completed_at() -> None:
    document = view(InvestigationStatus.FAILED, None) | {"completed_at": None}
    with pytest.raises(ValidationError, match="completed_at"):
        InvestigationView.model_validate(document)
