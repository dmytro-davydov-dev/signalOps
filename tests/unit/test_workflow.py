"""Pure parts of the workflow: its shape, typed output, and reuse rules (P06-T03)."""

import uuid
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from signalops.agents.jobs import PermanentJobError
from signalops.agents.workflow import (
    CONCLUDE,
    INVESTIGATE,
    LOAD_CONTEXT,
    WORKFLOWS,
    NodeContext,
    WorkflowRun,
    conclude,
    reused_output,
)
from signalops.contracts.checkpoints import StepCheckpoint
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.steps import IncidentBrief, InvestigatorFindings
from signalops.persistence.database import Database
from signalops.persistence.models import AgentStep
from signalops.persistence.repositories.scope import EvidenceScope

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)
BRIEF = IncidentBrief(
    incident_id=uuid.uuid4(), title="High error rate", detected_at=NOW, evidence_cutoff_at=NOW
)


def run(workflow_version: str = "v1") -> WorkflowRun:
    return WorkflowRun(
        uuid.uuid4(), BRIEF.incident_id, EvidenceScope(uuid.uuid4()), NOW, workflow_version, 20
    )


def checkpoint(workflow_version: str = "v1") -> dict[str, Any]:
    return StepCheckpoint(
        workflow_version=workflow_version,
        step_key=LOAD_CONTEXT.key,
        agent_role=LOAD_CONTEXT.role,
        attempt=1,
        lease_generation=1,
    ).model_dump(mode="json")


def test_step_keys_are_stable_and_unique() -> None:
    """Stored steps name these keys, so renaming one strands running investigations."""
    assert [node.key for node in WORKFLOWS["v1"].steps] == [
        "load_context",
        "investigate",
        "conclude",
    ]


async def test_conclude_abstains_with_the_findings_missing_evidence() -> None:
    context = NodeContext(cast("Database", None), run())
    context.keep(LOAD_CONTEXT, BRIEF)
    context.keep(INVESTIGATE, InvestigatorFindings(missing_evidence=("No metrics",)))

    diagnosis = await conclude(context)

    assert diagnosis == Diagnosis(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="No diagnosis for: High error rate",
        root_cause=None,
        missing_evidence=("No metrics",),
        workflow_version="v1",
    )


def test_the_context_lists_finished_nodes_in_order() -> None:
    context = NodeContext(cast("Database", None), run())
    context.keep(LOAD_CONTEXT, BRIEF)
    context.keep(INVESTIGATE, InvestigatorFindings(missing_evidence=("No metrics",)))

    assert context.finished == ["load_context", "investigate"]
    assert context.output(LOAD_CONTEXT) is BRIEF


def test_a_stored_step_reads_back_equal_to_its_output() -> None:
    step = AgentStep(output=BRIEF.model_dump(mode="json"), checkpoint=checkpoint())

    assert reused_output(LOAD_CONTEXT, step, "v1") == BRIEF


def test_a_stored_diagnosis_reads_back_equal() -> None:
    diagnosis = Diagnosis(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="None",
        root_cause=None,
        missing_evidence=("No metrics",),
        workflow_version="v1",
    )
    step = AgentStep(output=diagnosis.model_dump(mode="json"), checkpoint=checkpoint())

    assert reused_output(CONCLUDE, step, "v1") == diagnosis


def test_output_of_another_workflow_version_is_refused() -> None:
    step = AgentStep(output=BRIEF.model_dump(mode="json"), checkpoint=checkpoint("v2"))

    with pytest.raises(PermanentJobError, match="written by workflow v2"):
        reused_output(LOAD_CONTEXT, step, "v1")


def test_the_refusal_quotes_no_stored_value() -> None:
    step = AgentStep(output={"title": "secret log line"}, checkpoint=checkpoint())

    with pytest.raises(PermanentJobError) as caught:
        reused_output(LOAD_CONTEXT, step, "v1")

    assert "secret" not in str(caught.value)
