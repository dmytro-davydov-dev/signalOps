"""The deterministic baseline's rule and the run request (P10-T04)."""

import pytest
from pydantic import ValidationError

from signalops.agents.deterministic import classify
from signalops.agents.workflow import WORKFLOWS
from signalops.contracts.evaluation import ScenarioSplit, SystemVariant
from signalops.evaluation.runner import (
    VARIANT_WORKFLOWS,
    RunRequest,
    UnknownScenarioError,
    selected_entries,
)


@pytest.mark.parametrize(
    ("messages", "deployed", "expected"),
    [
        (
            ["connection pool exhausted: no connection available"],
            False,
            "connection_pool_exhaustion",
        ),
        (["java.lang.OutOfMemoryError: Java heap space"], False, "memory_pressure"),
        (["upstream bank-gateway call timed out after 2000 ms"], False, "downstream_timeout"),
        (["upstream bank-gateway call timed out after 2000 ms"], True, "deployment_regression"),
        (["request failed: internal error"], False, "elevated_errors"),
        ([], False, "elevated_errors"),
    ],
)
def test_the_keyword_rule(messages: list[str], deployed: bool, expected: str) -> None:
    assert classify(messages, deployed) == expected


def test_every_variant_runs_a_registered_workflow() -> None:
    assert set(VARIANT_WORKFLOWS) == set(SystemVariant)
    assert set(VARIANT_WORKFLOWS.values()) <= set(WORKFLOWS)


def test_run_requests_are_bounded() -> None:
    with pytest.raises(ValidationError):
        RunRequest(label="x", variants=(SystemVariant.MULTI_AGENT, SystemVariant.MULTI_AGENT))
    with pytest.raises(ValidationError):
        RunRequest(label="x", trials=0)
    with pytest.raises(ValidationError):
        RunRequest(label="x", max_steps=5)
    with pytest.raises(UnknownScenarioError):
        selected_entries(
            RunRequest(label="x", split=ScenarioSplit.HELD_OUT, scenarios=("healthy/dev",))
        )
    with pytest.raises(ValidationError):
        RunRequest(label="x", catalog_version="9")
    # Catalog v2 by default; version 1 stays selectable with its seven held-out variants.
    held_out = selected_entries(RunRequest(label="x"))
    assert len(held_out) == 37
    assert all(entry.split is ScenarioSplit.HELD_OUT for entry in held_out)
    assert {entry.version for entry in held_out} == {"2"}
    v1 = selected_entries(RunRequest(label="x", catalog_version="1"))
    assert len(v1) == 7
    assert {entry.version for entry in v1} == {"1"}
