"""Progress events are typed by their event type (P06-T05)."""

from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from signalops.contracts.progress import (
    PROGRESS_PAYLOADS,
    InvestigationEnded,
    ProgressEventType,
    ProgressEventView,
    StepFailed,
    StepProgress,
)
from signalops.contracts.workflow import AgentRole, InvestigationStatus

STEP = {"step_key": "investigate", "agent_role": "investigator", "attempt": 1}


def event(event_type: str, payload: dict[str, Any]) -> ProgressEventView:
    return ProgressEventView.model_validate(
        {
            "id": 1,
            "event_type": event_type,
            "payload": payload,
            "created_at": datetime(2026, 11, 7, tzinfo=UTC),
        }
    )


def test_every_event_type_has_a_payload() -> None:
    assert set(PROGRESS_PAYLOADS) == set(ProgressEventType)


def test_each_payload_decodes_to_its_type() -> None:
    assert type(event("step.started", STEP).payload) is StepProgress
    failed = event("step.failed", {**STEP, "error_category": "transient"})
    assert type(failed.payload) is StepFailed
    ended = event("investigation.ended", {"status": "cancelled"})
    assert type(ended.payload) is InvestigationEnded


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        ("step.failed", STEP),
        ("step.completed", {**STEP, "error_category": "transient"}),
        ("step.started", {"status": "cancelled"}),
        ("investigation.ended", STEP),
        ("step.completed", {**STEP, "output": {"reasoning": "free text"}}),
        ("step.started", {**STEP, "attempt": 0}),
        ("investigation.paused", STEP),
    ],
)
def test_a_payload_must_match_its_event_type(event_type: str, payload: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        event(event_type, payload)


@pytest.mark.parametrize(
    ("status", "diagnosis_status"),
    [
        (InvestigationStatus.RUNNING, None),
        (InvestigationStatus.COMPLETED, None),
        (InvestigationStatus.FAILED, "diagnosed"),
        (InvestigationStatus.INSUFFICIENT_EVIDENCE, "diagnosed"),
    ],
)
def test_an_ending_follows_the_status_mapping(
    status: InvestigationStatus, diagnosis_status: str | None
) -> None:
    with pytest.raises(ValidationError):
        InvestigationEnded.model_validate({"status": status, "diagnosis_status": diagnosis_status})


def test_step_failed_keeps_the_step_fields() -> None:
    failed = StepFailed(
        step_key="conclude", agent_role=AgentRole.SYNTHESIZER, attempt=2, error_category="abandoned"
    )
    assert failed.model_dump(mode="json") == {
        "step_key": "conclude",
        "agent_role": "synthesizer",
        "attempt": 2,
        "error_category": "abandoned",
    }
