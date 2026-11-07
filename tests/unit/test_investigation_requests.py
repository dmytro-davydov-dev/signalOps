"""Request hashing, keys, and limits without a database (P06-T01)."""

import uuid

import pytest
from pydantic import ValidationError

from signalops.agents.investigations import (
    InvestigationPolicy,
    operator_request_key,
    request_hash_for,
)
from signalops.config import Settings
from signalops.contracts.common import MAX_KEY_LENGTH
from signalops.contracts.investigations import InvestigationRequest
from signalops.detection.detector import DetectionPolicy

INCIDENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
WORKSPACE = uuid.UUID("22222222-2222-4222-8222-222222222222")
DATABASE = {"database_url": "postgresql+asyncpg://user:placeholder@localhost/test"}


def test_the_hash_is_stable_and_covers_the_incident_and_every_field() -> None:
    body = InvestigationRequest(max_steps=5)
    digest = request_hash_for(INCIDENT, body)

    assert len(digest) == 64
    assert digest == request_hash_for(
        INCIDENT, InvestigationRequest.model_validate({"max_steps": 5})
    )
    assert digest != request_hash_for(uuid.uuid4(), body)
    assert digest != request_hash_for(INCIDENT, InvestigationRequest(max_steps=6))
    assert digest != request_hash_for(INCIDENT, InvestigationRequest(budget_microusd=5))
    assert digest != request_hash_for(INCIDENT, InvestigationRequest(deadline_seconds=60))
    assert request_hash_for(INCIDENT, InvestigationRequest()) != digest


def test_the_request_key_separates_workspace_route_and_key() -> None:
    key = operator_request_key(WORKSPACE, "route", "key")

    assert key.startswith("operator:")
    assert len(key) <= MAX_KEY_LENGTH
    assert key == operator_request_key(WORKSPACE, "route", "key")
    assert key != operator_request_key(uuid.uuid4(), "route", "key")
    assert key != operator_request_key(WORKSPACE, "other", "key")
    assert key != operator_request_key(WORKSPACE, "route", "other")
    # The separator keeps a field's end from reading as the next one's start.
    assert operator_request_key(WORKSPACE, "ab", "c") != operator_request_key(WORKSPACE, "a", "bc")


def test_a_key_at_the_length_limit_still_fits_the_request_key() -> None:
    assert len(operator_request_key(WORKSPACE, "route", "k" * MAX_KEY_LENGTH)) <= MAX_KEY_LENGTH


@pytest.mark.parametrize(
    "body",
    [
        {"budget_microusd": 0},
        {"max_steps": 0},
        {"deadline_seconds": 59},
        {"unknown": 1},
    ],
)
def test_the_request_contract_rejects_bad_limits(body: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        InvestigationRequest.model_validate(body)


def test_settings_set_the_policy_and_can_turn_detector_requests_off() -> None:
    settings = Settings.model_validate(
        DATABASE
        | {
            "investigation_budget_microusd": 5,
            "investigation_max_steps": 6,
            "investigation_deadline_seconds": 70,
        }
    )
    policy = InvestigationPolicy.from_settings(settings)
    assert policy == InvestigationPolicy(5, 6, 70)
    assert DetectionPolicy.from_settings(settings).incidents.investigations == policy

    off = settings.model_copy(update={"investigation_auto_request": False})
    assert DetectionPolicy.from_settings(off).incidents.investigations is None
