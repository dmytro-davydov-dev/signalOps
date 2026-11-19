"""Workflows that run one role on a frozen brief, for component evaluation (P13-T04).

A component case is an ordinary investigation, so the gateway reserves, accounts,
and enforces limits exactly as for any other, and the worker that runs it is the
ordinary investigation worker. Its first step, `brief`, is written by the evaluator
before the job is queued: a `SynthesisBrief` or `VerificationBrief` built by the
workflow's own code from a replayed scenario. The node never computes it, so a
missing brief fails the job. Briefs carry no labels; expected handling stays in the
`evaluation` schema and the corpus files.

The role step runs the same loop, prompt, submit tool, and handoff checks as in
`v3`. The conclusion is an abstention that points at the role step: the case's
result is that step's output, not a diagnosis.
"""

from typing import Final

from signalops.agents.jobs import PermanentJobError
from signalops.agents.multi_agent import (
    ROLE_ENDINGS,
    SYNTHESIS_TURNS,
    SYNTHESIZER_PROMPT_VERSION,
    VERIFICATION_TURNS,
    findings_of,
    run_synthesizer,
    run_verification_role,
    verified_citations,
)
from signalops.agents.nodes import Node, NodeContext, Workflow
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.roles import (
    Provenance,
    SynthesisBrief,
    SynthesisOutput,
    SynthesisReport,
    VerificationBrief,
    VerificationOutput,
)
from signalops.contracts.workflow import AgentRole

WORKFLOW_S1_SYNTHESIZER: Final = "s1-synthesizer"
WORKFLOW_S1_VERIFIER: Final = "s1-verifier"
BRIEF_STEP: Final = "brief"
SYNTHESIS_STEP: Final = "synthesize"
VERIFICATION_STEP: Final = "verify"


async def _written_by_evaluator(_context: NodeContext) -> SynthesisBrief:
    raise PermanentJobError("a component case's brief is written by the evaluator")


async def _verification_brief_missing(_context: NodeContext) -> VerificationBrief:
    raise PermanentJobError("a component case's brief is written by the evaluator")


SYNTHESIS_BRIEF: Final = Node(
    BRIEF_STEP, AgentRole.COORDINATOR, SynthesisBrief, _written_by_evaluator
)
VERIFICATION_BRIEF: Final = Node(
    BRIEF_STEP, AgentRole.COORDINATOR, VerificationBrief, _verification_brief_missing
)


async def synthesize_brief(context: NodeContext) -> SynthesisOutput:
    brief = context.output(SYNTHESIS_BRIEF)
    output, ended_by, turns = await run_synthesizer(
        context, brief, findings_of(brief), SYNTHESIS_TURNS
    )
    report = output or SynthesisReport(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="The findings were not synthesized.",
        missing_evidence=(ROLE_ENDINGS[ended_by],),
    )
    runtime = context.runtime
    return SynthesisOutput(
        report=report,
        provenance=Provenance(
            role=AgentRole.SYNTHESIZER,
            prompt_version=SYNTHESIZER_PROMPT_VERSION,
            model=runtime.model if runtime is not None and turns else None,
            model_turns=turns,
            ended_by=ended_by,
        ),
    )


async def verify_brief(context: NodeContext) -> VerificationOutput:
    brief = context.output(VERIFICATION_BRIEF)
    return await run_verification_role(
        context, brief, verified_citations(brief), VERIFICATION_TURNS
    )


def _conclusion(step: str) -> Node[Diagnosis]:
    async def conclude(context: NodeContext) -> Diagnosis:
        return Diagnosis(
            status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
            summary="Component evaluation case: one role ran on a frozen brief.",
            root_cause=None,
            missing_evidence=(f"Not a diagnosis; the result is step {step}",),
            workflow_version=context.run.workflow_version,
        )

    return Node("conclude", AgentRole.COORDINATOR, Diagnosis, conclude)


SYNTHESIZE_BRIEF: Final = Node(
    SYNTHESIS_STEP, AgentRole.SYNTHESIZER, SynthesisOutput, synthesize_brief
)
VERIFY_BRIEF: Final = Node(VERIFICATION_STEP, AgentRole.VERIFIER, VerificationOutput, verify_brief)

COMPONENT_WORKFLOWS: Final = {
    WORKFLOW_S1_SYNTHESIZER: Workflow(
        WORKFLOW_S1_SYNTHESIZER, (SYNTHESIS_BRIEF, SYNTHESIZE_BRIEF), _conclusion(SYNTHESIS_STEP)
    ),
    WORKFLOW_S1_VERIFIER: Workflow(
        WORKFLOW_S1_VERIFIER, (VERIFICATION_BRIEF, VERIFY_BRIEF), _conclusion(VERIFICATION_STEP)
    ),
}
