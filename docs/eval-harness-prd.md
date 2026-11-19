# PRD: Evaluation harness for the agentic pipeline

Version 1 · 19 November 2026 · Status: proposed · Decision: [ADR-040](decisions.md#adr-040-the-evaluation-harness-extends-signalopsevaluation-in-layers-with-deterministic-oracles-first) · Tasks: [roadmap Phase 13](development_roadmap.md#phase-13-evaluation-harness-for-the-agentic-pipeline)

## 1 Summary

Phase 10 built an evaluation lab that replays labelled fault scenarios and scores each investigation's **final outcome**: right service and failure type, a correct abstention, or a wrong answer. That tells us *whether* the pipeline is right. It cannot tell us *where* it goes wrong, how confident we can be in a difference between variants, whether a change made the pipeline worse, or how to spend money on a real model safely.

This PRD adds an **evaluation harness** to the stack: a layered extension of `signalops.evaluation` that scores each stage of the agentic workflow against deterministic oracles, attributes every wrong outcome to the stage that caused it, evaluates single roles in isolation on inputs with planted defects, reports uncertainty with every number, gates regressions on the fake provider, and caps the spend of paid-model runs at the run level.

## 2 Problem

What the current lab answers, from the retained held-out run [`7ac381af`](evaluation-runs/7ac381af-heldout-fake.json) (fake provider):

| Question | Answered today? |
| --- | --- |
| Did the final diagnosis name the right service and failure type? | Yes (`outcome_accuracy`, `root_cause_accuracy`) |
| Were the cited signals fault evidence? | Yes, by labels (`evidence.precision`, `unsupported_rate`) |
| Did a specialist find the cause and synthesis drop it, or did nobody find it? | No. Stage outputs are stored in `agent_steps.output` but never scored |
| Did the verifier reject a correct hypothesis or accept a wrong one? | No |
| Did a budget share, turn limit, or repair limit cause an abstention? | No. `Provenance.ended_by` is stored, not aggregated |
| Does planted text in logs or runbooks change the diagnosis? | Tested once in P09-T05, not measured in evaluation runs |
| Is `v3`'s accuracy different from `v2`'s, or is it noise? | No. One case per scenario, 7 held-out scenarios, no intervals |
| Are model confidences calibrated? | No |
| Did yesterday's change make the pipeline worse? | No regression baseline or comparison |
| Can we run a real model on the held-out split without overspending? | Per-investigation budgets exist; nothing bounds a whole run |

The cost of not knowing: the first paid run (release priority 1) would produce one accuracy number per variant with no explanation of failures, no confidence interval, and no protection against a run-wide overspend. The Phase 9 gate also showed that a plumbing defect (runbook citations rejected by a key pattern) can pass every outcome test until a live run; stage-level invariants catch that class earlier.

## 3 Goals

1. **Stage correctness.** Score each workflow stage of `v2` and `v3` (specialists, synthesis, verification, follow-up, conclusion) from stored step outputs, with deterministic labels, at zero model cost.
2. **Failure attribution.** Assign every incorrect case to exactly one first failing stage, so a report says "6 of 9 errors: no specialist found the cause" rather than "accuracy 0.43".
3. **Component evaluation.** Evaluate the synthesizer and verifier alone on frozen inputs with planted defects (unsupported claims, wrong citations, hidden contradictions), so model behavior is measured separately from upstream noise.
4. **Honest statistics.** Every rate carries a sample count and a 95% interval; variant comparisons are paired on the same cases; a difference is called a difference only when the test says so.
5. **Regression gate.** A fake-provider suite and a committed baseline catch behavior changes in the workflow, handoff checks, and scoring before merge.
6. **Safe paid runs.** A paid-model run states a spend ceiling up front, cannot be queued when its worst case exceeds it, and stops when actual plus reserved spend reaches it.
7. **Adversarial robustness as a metric.** Injection cases join the scenario catalog and are scored against clean twins.

## 4 Non-goals

- Replacing the Phase 10 lab, its replay orchestration, isolation roles, or API. The harness extends them.
- An external evaluation framework or hosted evaluation service (ADR-040 explains why).
- Treating a judge model's score as ground truth. Judge scores, if ever enabled, are reported separately and only after agreement with human audits is measured.
- Tuning prompts on the held-out split.
- Recording and replaying provider responses byte for byte. Re-scoring uses stored step outputs; re-execution makes new calls.
- Scenarios with several independent incidents, live-traffic evaluation, or online evaluation of production investigations.
- Running a paid evaluation without an explicit request that states its scope and spend ceiling.

## 5 Users

| User | Need |
| --- | --- |
| Developer changing a prompt, role, tool, or handoff check | Know within one local command whether stage behavior changed, and why |
| Reviewer of a pull request | See a pass/fail regression report with the cases that moved |
| Project owner deciding whether to keep `v3` (ADR-010) | Paired accuracy, cost, and latency differences with intervals, plus where each variant fails |
| Portfolio reader | Inspect claims down to a case, its stage scores, and its evidence |

## 6 Functional requirements

### FR-1 Scenario catalog v2

- Catalog version 2 adds held-out variants so each diagnosable family has at least 5 held-out variants and the held-out split has at least 35 scenarios; development variants grow in proportion. Version 1 stays registered and reproducible.
- `ExpectedOutcome` gains stage labels, all evaluator truth: `lead_roles` (specialist roles whose evidence can show the cause: for example `deployment` for a deployment regression), `expected_contradiction` (the mixed scenario's competing service), and `clean_twin` (for adversarial variants, the scenario they perturb).
- An `adversarial` family reuses `fixtures/adversarial/v1` plants on top of diagnosable and insufficient-evidence scenarios, each with a clean twin in the same split.
- Labels stay out of the runtime role's reach (ADR-034) and out of every prompt.

### FR-2 Stage scoring (offline)

After a case is scored, the runner reads the investigation's steps through the evaluator connection and computes `stage_scores`. No model call. Definitions:

| Stage metric | Definition |
| --- | --- |
| Specialist lead recall | Diagnosable cases where at least one specialist finding names the true service (and, strictly, the failure type), over diagnosable cases. Also per role, and for each case's `lead_roles` |
| Specialist evidence precision | A role's cited signals that are fault evidence, over its cited signals |
| Synthesis lead retention | Cases where some specialist named the true cause and the leading hypothesis is that cause, over cases where some specialist named it |
| Synthesis invariant | Leading citations are a subset of specialist citations and supporting roles found something. Code enforces it (ADR-031); the harness asserts it, and any violation is a harness defect |
| Contradiction recall | Mixed scenarios where the diagnosis lists the labelled contradiction, over those scenarios |
| Verifier verdict matrix | For each first verification: correct lead accepted, correct lead rejected, wrong lead accepted, wrong lead rejected |
| Follow-up effect | Rounds granted, and outcome changes wrong→right and right→wrong between first and second verification |
| Limit endings | Count of role endings by `ended_by` (`budget_share`, `turn_limit`, `repair_limit`, `refused`, `no_turns`) and the share of abstentions in which any role hit a limit |
| Single-agent stages (`v2`) | Proposal correct at `investigate`, and whether `conclude` kept, corrected, or dropped it |

### FR-3 Failure attribution

Every case with `outcome_correct = false` gets one `failure_stage`, checked in this order, first match wins:

1. `harness_failure`: the harness produced no outcome.
2. `detection_miss`: no incident matched the fault, or the investigated incident is not the matched one.
3. `no_lead`: no specialist (or, for `v2`, the agent's proposal) named the true service.
4. `lead_lost_in_synthesis`: a specialist named the true cause, the leading hypothesis did not.
5. `verifier_false_reject`: the leading hypothesis was correct and verification rejected it.
6. `verifier_false_accept`: the leading hypothesis was wrong and verification accepted it.
7. `conclude_dropped`: verification accepted a correct lead and the stored diagnosis abstained.
8. `wrong_failure_type`: right service, wrong failure type.
9. `false_diagnosis_on_underdetermined`: a cause was asserted where abstention was correct; tagged with the stage that first asserted it.
10. `rule_error`: deterministic baseline only.

`limit_hit` is an orthogonal flag on any attributed case where a role ended by a limit. The run report aggregates failure stages per variant; the UI shows the stage on the case page and a breakdown on the run page.

### FR-4 Component evaluations

- A versioned corpus `fixtures/evaluation/v1/stage-corpus/` of frozen briefs built from replayed scenarios (no labels inside a brief), each with planted defects and their expected handling:
  - **Verifier:** briefs whose leading hypothesis cites non-fault signals, cites signals from another service, or is correct; expected verdict and per-citation expectation.
  - **Synthesizer:** report sets with a correct lead in one role, two roles in conflict, or only weak findings; expected leading service, expected contradiction, or expected abstention.
- `signalops evaluation-stage --role verifier|synthesizer --corpus v1 [--trials N]` runs the role through the real `AgentLoop` and gateway. Each case gets a holder investigation in its own evaluation replay scope so budget reservation, accounting, and limits work unchanged.
- Scores: verdict accuracy, false accept and false reject rates, per-citation agreement with expectations, contradiction recall, abstention correctness; each with intervals (FR-5).
- On the fake provider these show the plumbing only and are marked `measures_model_quality: false`.

### FR-5 Statistics and report v2

- Wilson 95% intervals on every rate.
- Paired variant comparison on identical cases: accuracy difference with a cluster bootstrap over scenarios (trials of one scenario are correlated), fixed seed, 10,000 resamples; exact McNemar when each variant has one trial. Cost and latency differences use the same bootstrap.
- Calibration of root-cause confidence against `root_cause_correct`: Brier score and five equal-width reliability bins; reported as not meaningful when the provider does not measure model quality.
- Cost per correct outcome, next to cost per investigation.
- A sample-size note on every comparison: the smallest difference the run could detect.
- Report schema `evaluation-report/v2` (versioned, additive over the current `metrics` object); `signalops evaluation-export <run>` writes JSON plus a Markdown summary under `docs/evaluation-runs/`.

### FR-6 Regression gate

- `signalops evaluation-compare <report> --baseline <file>` compares two reports and exits non-zero on a regression. Two kinds of checks:
  - **Invariants** (must hold on any provider): zero harness failures, zero synthesis invariant violations, zero secrets in outputs, zero forbidden tools executed, no case citing a signal outside its scope or cutoff.
  - **Baseline equality** (fake provider, which is deterministic): per-case outcome, failure stage, and stage scores equal to the committed baseline; on a real provider, tolerances instead of equality.
- A committed baseline `fixtures/evaluation/v1/baseline-fake-development.json` for the development split.
- An integration test runs the development suite in process and compares it. Whether CI gains a job with a PostgreSQL service for it is open question Q13.

### FR-7 Paid-run guardrails

- `evaluation-run` gains `--max-spend-microusd` and `--allow-paid`. With a non-fake provider both are required, or the run is refused before queueing.
- Preflight: worst case = cases × trials × `budget_microusd` (the per-investigation reservation ceiling already bounds each case, including ambiguous timeouts). A worst case above the ceiling is refused with the numbers.
- During execution the runner stops requesting investigations when spent plus reserved spend across the run's investigations would pass the ceiling, ends the remaining cases `failed` with category `run_budget`, and finishes the run with what it has.
- `signalops evaluation-estimate` prints the worst case and the case count for a request without queueing it.
- The run records the ceiling and an approval reference string supplied by the operator.

### FR-8 UI and API

- The run page shows intervals, the paired comparison, the failure-stage breakdown, and limit endings per variant.
- The case page shows stage scores, the failure stage, and per-stage outputs (findings, leading hypothesis, verdict) linked to evidence.
- API views change additively; existing fields keep their meaning.

## 7 Non-functional requirements

- **Cost.** Stage scoring, attribution, statistics, and comparison make no model call. Ordinary development and CI use the fake provider. A paid run needs an explicit request with scope and ceiling (CLAUDE.md).
- **Isolation.** Ground truth and stage labels live in the `evaluation` schema; the runtime role still cannot read them (re-verified by the access test). Component-evaluation briefs contain no labels.
- **Reproducibility.** Every report records the catalog version, dataset and split hashes, workflow and prompt versions, model settings, pricing, limits, seeds (including the bootstrap seed), and the git revision.
- **Determinism.** On the fake provider two runs of the development suite produce identical reports apart from IDs and timestamps.
- **Dependencies.** No new runtime or evaluation dependency; statistics are implemented in the standard library.
- **Compatibility.** Report v1 fields keep their meaning; migrations are additive.
- **Privacy.** No evaluation data leaves the host. No unrestricted model reasoning is stored.

## 8 Success criteria

The harness is done when, on the fake provider:

1. A run's report shows stage scores and a failure stage for every incorrect case, and the failure-stage counts sum to the incorrect count.
2. Mutations each move the expected metric: a synthesizer that leads with the second hypothesis lowers lead retention and attributes `lead_lost_in_synthesis`; a verifier that accepts everything raises false accepts; a specialist limited to zero turns raises limit endings.
3. Component evaluations run for verifier and synthesizer with a planted-defect corpus and produce per-defect scores.
4. Every rate in the report and UI has an interval; the `v2`/`v3` comparison says "no detectable difference" when the paired test does.
5. The regression gate passes on an unchanged tree and fails, naming the moved cases, on a deliberate behavior change.
6. A paid-provider run without a ceiling is refused; a run whose worst case exceeds its ceiling is refused; a run that reaches its ceiling stops with `run_budget` cases.

With an approved budget (P13-T08), the first paid run then answers ADR-010's question with intervals and a failure breakdown.

## 9 Paid-run budget model

A run's worst-case spend is `agent cases × trials × budget_microusd`; the deterministic baseline costs nothing. Example with the catalog v2 target of 35 held-out scenarios, `v2` and `v3`, 3 trials, and the current 3 USD per investigation: 35 × 2 × 3 × 3 USD = 630 USD worst case. Actual spend is expected to be far lower because reservations assume every request byte is a cache-write token and every output token is used (ADR-026), but no real call has measured it. The recommended order is a development-split smoke run (7 scenarios, 1 trial, both agent variants: 42 USD worst case) to measure real per-case cost, then a held-out run whose ceiling and per-case budget are set from that measurement. Both ceilings need explicit approval (Q12).

## 10 Risks

| Risk | Mitigation |
| --- | --- |
| Sample sizes stay too small to separate variants | Report the detectable difference; catalog v2 raises held-out scenarios to ≥ 35; trials add samples but are clustered, not independent |
| Labels are wrong or too coarse (for example `supporting_event_ids` counts any fault record as support) | Stage metrics state their oracle; human audits stay the semantic check; label changes need a new catalog version |
| Fake-provider baseline couples the gate to fake rules, so a legitimate fake change fails the gate | Baseline updates are an explicit command with a diff in review |
| Component briefs drift from what the workflow really builds | Briefs are generated by the same code that builds them in the workflow, from replayed scenarios, and regenerated with a corpus version bump |
| A paid run overspends through ambiguous timeouts | The run ceiling counts reserved as well as spent, and reservations of unknown calls are never released (ADR-026) |
| Stage attribution misleads when several stages fail | "First failing stage" is documented as a convention; per-stage scores remain visible for every case |

## 11 Open questions

| # | Question | Decide by |
| --- | --- | --- |
| Q12 | Spend ceilings and per-case budget for the development smoke run and the held-out run | Before P13-T08 |
| Q13 | Add a CI job with a PostgreSQL service so the regression gate runs on every push, or keep it a local integration check | P13-T07 |
| Q14 | Enable a judge model for claim-level support, given human audit volume needed to measure agreement | After P13-T08 |

## 12 Delivery

Phase 13 in the [roadmap](development_roadmap.md#phase-13-evaluation-harness-for-the-agentic-pipeline): P13-T01 catalog v2 and stage labels; P13-T02 stage scoring; P13-T03 failure attribution; P13-T04 component evaluations; P13-T05 statistics and report v2; P13-T06 paid-run guardrails; P13-T07 regression gate; P13-T08 paid-model evaluation (blocked on Q12); P13-T09 optional judge-model support scores (blocked on P13-T08 and Q14). Estimate 12–18 focused days excluding approval waits.
