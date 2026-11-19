"""Invariant counts stored with stage scores (P13-T07)."""

from signalops.contracts.workflow import AgentRole
from signalops.evaluation.invariants import forbidden_calls, secrets_in
from signalops.evaluation.stages import StageInputs, score_stages
from tests.unit.test_stage_scores import NAMES, TIMEOUT, v3_steps


def test_redacted_text_holds_no_secret_and_unredacted_text_does() -> None:
    redacted = "debug dump password=[REDACTED] api_key=[REDACTED] Authorization: Bearer [REDACTED]"
    assert secrets_in([redacted]) == 0
    assert secrets_in(["password=hunter2"]) == 1
    assert secrets_in(["Authorization: Bearer abc.def"]) == 1
    # A planted secret counts wherever it appears, even without its key.
    assert secrets_in(["it said sk-live-INJECT-9f2"]) == 1
    assert secrets_in(["the token budget ran out"]) == 0


def test_tool_calls_must_belong_to_the_steps_role() -> None:
    allowed = [
        ("telemetry", "query_logs"),
        ("deployment", "get_deployments"),
        ("investigate", "retrieve_runbooks"),
    ]
    assert forbidden_calls(allowed, None) == 0
    assert (
        forbidden_calls([("telemetry", "retrieve_runbooks"), ("synthesize", "query_logs")], None)
        == 2
    )
    # A follow-up round runs the requested specialist's tools only.
    assert (
        forbidden_calls([("follow_up", "get_deployments")], AgentRole.DEPLOYMENT_INVESTIGATOR) == 0
    )
    assert (
        forbidden_calls([("follow_up", "get_deployments")], AgentRole.TELEMETRY_INVESTIGATOR) == 1
    )


def test_stage_scores_carry_the_invariant_counts() -> None:
    stages = score_stages(
        TIMEOUT,
        StageInputs(
            workflow_version="v3",
            steps=v3_steps(),
            diagnosis=None,
            service_names=NAMES,
            texts=('{"claim": "password=hunter2-INJECT"}',),
            tool_calls=(("dependency", "query_logs"),),
            out_of_scope=3,
        ),
    )
    assert stages is not None
    assert (stages.secrets_exposed, stages.forbidden_tool_calls, stages.out_of_scope_citations) == (
        2,
        1,
        3,
    )
