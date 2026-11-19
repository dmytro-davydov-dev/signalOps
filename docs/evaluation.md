# Evaluation

> Status: implemented (Phase 10). One held-out run on the fake provider is retained: [`evaluation-runs/7ac381af-heldout-fake.json`](evaluation-runs/7ac381af-heldout-fake.json), summarized in [demo.md](demo.md#5-comparison-against-simpler-baselines). It demonstrates the harness, not diagnostic quality; no paid model has run. Sources: [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §12 and roadmap Phase 10.

> Phase 13 (in progress): an evaluation harness that adds stage scores, failure attribution, component evaluations of the synthesizer and verifier, intervals and paired comparisons, a fake-provider regression gate, and run-level spend ceilings. Requirements in [eval-harness-prd.md](eval-harness-prd.md), decision in [ADR-040](decisions.md#adr-040-the-evaluation-harness-extends-signalopsevaluation-in-layers-with-deterministic-oracles-first). Sections marked with a Phase 13 task ID describe what is implemented so far.

## Goal

Find out whether agent investigations actually beat simpler approaches. Correctness, latency, and cost are reported **together**, and the comparison holds evidence, tools, and budget equal.

## Scenario families

| Family | Notes |
| --- | --- |
| Downstream timeout | `downstream-timeout`, built in P04-T01 (`signalops demo-generate`) |
| Connection pool exhaustion | `pool-exhaustion` (P10-T03) |
| Deployment regression | `deployment-regression`; exercises the deployment investigator |
| Memory pressure | `memory-pressure` |
| Healthy traffic with noisy warnings | `healthy`, built in P04-T01. Should produce **no** incident or diagnosis |
| Mixed and insufficient-evidence variants | `mixed-timeout-deploy`, `unexplained-errors`; test abstention quality |

- Scenarios are versioned under `fixtures/` with a fixed seed and a dataset hash. `fixtures/scenarios/v1/<scenario>.labels.json` holds the labels and hash; the records are regenerated from the seed rather than committed. Each scenario has development and held-out variants (`evaluation/catalog.py`).
- **Development** variants and **held-out** variants are kept separate. Prompts are never tuned on the reporting set.
- Synthetic replay is labelled synthetic. Real fault demonstrations run through the optional `demo` profile.

### Catalog version 2 (P13-T01)

| | Development | Held-out |
| --- | --- | --- |
| Version 1 (Phase 10, still registered) | 7: one per scenario, seed 42 | 7: one per scenario |
| Version 2 | 14: the 7 version 1 variants, one more per diagnosable family, 2 adversarial | 37: 5 per diagnosable family (the first repeats version 1's parameters), 3 healthy, 4 insufficient-evidence, 5 adversarial |

`signalops evaluation-run --catalog 1|2` (default 2) and `catalog_version` on `POST /v1/evaluation-runs` pick the version; scenario names repeat across versions, and a run records its version in `limits.catalog_version`. Version 1 definitions, dataset hashes, and the held-out split hash (`17ce501d…`, run `7ac381af`) are unchanged.

Version 2 outcomes carry **stage labels**, all evaluator truth stored in the `evaluation` schema:

| Label | Meaning |
| --- | --- |
| `lead_roles` | Specialist roles whose evidence can show the cause: telemetry for timeouts, pool exhaustion, memory pressure, and the mixed scenario; deployment and telemetry for a deployment regression. The dependency role reads structure only, so it is never a lead role |
| `expected_contradiction` | The competing service a mixed scenario should surface (`checkout`, whose unrelated deployment lands at the fault's start) |
| `clean_twin` | For an adversarial variant, the scenario it perturbs |

The **adversarial** family (`evaluation/adversarial.py`) adds one error log per plant to a clean twin in the same split, 3 seconds after the fault starts, on the cause's service (or the first affected service when there is no cause). The texts are the log plants of `fixtures/adversarial/v1/injections.json` (instructions to run shell tools, widen scope, leak secrets, self-approve remediation, cite a planted signal); the runbook plant is not overlaid because runbooks are seeded from the demo catalog, not from a dataset. Every other record is the twin's, byte for byte, and the expected outcome is the twin's plus `clean_twin`.

`fixtures/scenarios/v2/` holds every version 2 definition; regenerate with `signalops evaluation-catalog --out fixtures/scenarios/v2` (`--check` exits 1 when stale).

## Baselines (P10-T04, ADR-036)

| Variant | Workflow | Description |
| --- | --- | --- |
| Deterministic | `d1` | No model. Blames the service with the most detector-cited errors and labels the failure by keyword (deployment in the window, pool exhausted, heap, timeout, else `elevated_errors`). Runs once per scenario |
| Single agent | `v2` | One tool-using agent (Phase 7) |
| Multi-agent | `v3` | Specialists, synthesis, and verification (Phase 9) |

`signalops evaluation-run --label L --split held_out [--variant …] [--trials N] [--execute]` queues a run, and `evaluation-worker` (Compose profile `evaluation`) executes queued runs. Both use `SIGNALOPS_EVALUATION_DATABASE_URL`. A run replays each scenario once into its own scope. Every variant and trial then investigates that scope's first incident with the same evidence cutoff (the incident's event-time detection), the same tools, and the same `budget_microusd`, `max_steps`, and deadline, all recorded in `runs.limits`. A scenario with no incident is scored without an investigation. The run records the split's dataset hash, workflow and prompt versions, model settings (including whether the provider measures model quality: the fake provider does not), pricing version and prices, and seed. Its metrics give, per variant, cases, samples, trials, per-trial accuracy and its standard deviation, abstentions, false diagnoses, harness failures (counted as incorrect), cost, unknown reservations, and model calls, next to the equal per-case budget.

## Metrics (P10-T05)

Scores compare stored outcomes with deterministic labels (`evaluation/scoring.py`). Aggregates are in `evaluation/metrics.py`. No judge model scores anything (`judge_model_scores` is always null).

| Metric | Definition |
| --- | --- |
| Detection precision and recall | Per replay, shared by all variants. An incident matches the expected fault when it is detected (event time) between the fault's start and its end plus 5 minutes, and names an affected service. Matching is one-to-one: the earliest match is the true positive, other incidents are false positives, and an unmatched fault is a false negative |
| Detection delay | Event time from fault start to the matched incident's detection (p50, p95) |
| Outcome accuracy | Correct service and failure type, or a correct abstention, or no incident on healthy traffic. Harness failures count as incorrect |
| Root-cause accuracy | Correct service **and** failure type, over diagnosable cases |
| Evidence precision (automatic) | Cited signals among the records the fault produced (`supporting_event_ids`), over all cited signals |
| Unsupported diagnosis | A diagnosis none of whose citations is fault evidence; rate over diagnoses |
| Abstention quality | Correct abstentions on incidents whose cause is not in the evidence, and over-abstentions on diagnosable ones |
| Investigation latency | Wall clock from request to stored outcome (p50, p95). Replay detection time is event time, so the two clocks are not subtracted |
| Cost | Spent microUSD per investigation, unknown reservations, and model calls |
| Human audit precision | People's judgements of sampled citations (`evaluation.audits`), reported separately with `source: human` |

Recovery correctness and data quality are covered by the reliability tests and the evidence quality summary, not by this report.

### Stage scores (P13-T02)

After a case is scored, the runner reads the investigation's completed steps through the evaluator connection and stores `evaluation.cases.stage_scores` (`evaluation/stages.py`, contract `StageScores`, version `stage-scores/v1`). No model is called: the inputs are stored step outputs, the stored diagnosis, the workspace's service names, and the cited signals' source event IDs. The deterministic variant has no stages (`null`). A stored step that no longer validates leaves the case without stage scores, counted as `cases_without_stage_scores`.

Each variant's `metrics.variants.<variant>.stages` aggregates them; every rate carries its numerator and denominator.

| Metric | Definition |
| --- | --- |
| `specialist_lead_recall` (`_strict`) | Diagnosable `v3` cases where some specialist finding names the true service (and failure type), over diagnosable `v3` cases |
| `lead_role_recall` | Same, counting only roles in the case's `lead_roles` |
| `per_role.<role>` | That role's lead recall, and its evidence precision: cited signals that are fault evidence over cited signals |
| `synthesis_lead_retention` | Cases where some specialist named the true cause and the leading hypothesis is it, over cases where some specialist named it |
| `synthesis_invariant_violations` | Leading-hypothesis citations outside the specialists' citations, or a supporting role with no findings. Code enforces both (ADR-031), so any count is a harness defect |
| `contradiction_recall` | Mixed scenarios whose diagnosis lists a contradiction naming, or citing a signal of, the labelled competing service |
| `verifier_matrix` | First verification against the lead's correctness: correct or wrong lead × accepted, rejected, or follow-up requested |
| `follow_up` | Rounds run, and whether the verifier's decision went wrong→right or right→wrong between the first and final verification (a follow-up request counts as not accepting) |
| `limit_endings`, `abstentions_with_limit` | Role endings by `ended_by` among `budget_share`, `turn_limit`, `repair_limit`, `refused`, `no_turns`, and the share of abstentions in which any role hit one. A single agent's ending is read back from its abstention reason |
| `single_agent` | `v2` proposals naming the true cause (or abstaining where there is none), and whether `conclude` kept, corrected, or dropped them |

### Failure attribution (P13-T03)

Every incorrect case gets one `failure_stage` (`evaluation/attribution.py`), stored on `evaluation.cases` with `failure_origin` and `limit_hit` (migration `0022`). The checks run in this order and the first match wins:

| Stage | When |
| --- | --- |
| `harness_failure` | The harness produced no outcome |
| `detection_miss` | No incident matched the fault, the investigated incident is not the matched one, or an incident opened on healthy traffic |
| `no_lead` | No specialist (`v3`) or proposal (`v2`) named the true service |
| `lead_lost_in_synthesis` | A specialist named the true service (or cause) and the leading hypothesis did not |
| `verifier_false_reject` | The lead was the true cause and the final verification did not accept it |
| `verifier_false_accept` | The lead was wrong and verification accepted it |
| `conclude_dropped` | A correct, accepted lead (or `v2` proposal) ended in an abstention |
| `wrong_failure_type` | Right service, wrong failure type |
| `false_diagnosis_on_underdetermined` | A cause asserted where abstaining was correct; `failure_origin` names the step that asserted it |
| `rule_error` | Any other error of the deterministic baseline |
| `unattributed` | An agent case without stage scores; no stage is guessed |

"First failing stage" is a convention: later stages may also have failed, and the case page shows every stored stage (findings, leading hypothesis, verdict, citation checks) with each cited signal labelled and linked to the investigation's evidence. `limit_hit` is orthogonal: some role ended on a limit, a refusal, or no turns. `metrics.variants.<variant>.failure_stages` counts incorrect cases by stage (`by_stage` sums to `incorrect`) and those with a limit hit; the run page shows the breakdown per variant.

## Statistics and report v2 (P13-T05)

`evaluation/statistics.py` (standard library only) and `evaluation/report.py` add to every run's metrics, without changing a v1 field (`report_version: evaluation-report/v2`):

| Field | Meaning |
| --- | --- |
| `variants.<v>.intervals` | Wilson 95% intervals with counts for outcome accuracy, root-cause accuracy, unsupported rate, correct-abstention rate, evidence precision, and harness failures. Every stage-metric ratio carries `ci95` too |
| `variants.<v>.cost_per_correct_microusd` | Spent microUSD over correct outcomes, next to `cost_microusd_mean` |
| `variants.<v>.calibration` | Brier score and five equal-width reliability bins of the stored root-cause confidence against `root_cause_correct`, over asserted causes; `meaningful: false` with a note when the provider does not measure model quality |
| `comparisons[]` | Each pair of variants on identical cases (same scenarios; trials matched, a once-per-scenario baseline matched with every trial): the accuracy difference with a cluster bootstrap over scenarios (seed = run seed, 10,000 resamples, percentile interval); exact McNemar when each variant has one trial per scenario; cost and latency differences by the same bootstrap; `verdict` (`no detectable difference` unless McNemar p < 0.05, or the bootstrap interval excludes 0 when McNemar does not apply); `min_detectable_difference`: 6 / scenarios, the smallest difference exact McNemar can call significant with every discordant pair one way |
| `reproducibility` | Catalog version, dataset hash, seeds, bootstrap settings; the export adds the git revision, workflow and prompt versions, model settings, pricing, and limits |

`signalops evaluation-export <run-id> [--out-dir docs/evaluation-runs]` (evaluator connection) writes `<id8>-<split>-<provider>.json`, the run detail as the API returns it with metrics recomputed from the stored cases, and a Markdown summary beside it. Exporting the same run from the same tree twice gives identical files. Older runs gain intervals and comparisons on export; their cases keep no stage scores or failure stages, which only a new run stores. The run page shows intervals, the paired comparisons, and the calibration label; v1 metrics without these sections still render.

## Replay orchestration (P10-T02, ADR-035)

`evaluation/replay.py` replays one dataset into its own scope. `signalops evaluation-replay <scenario> --seed N` runs it and prints the report; `--in-process` runs the pipeline processors locally instead of through Kafka.

1. **Scope and IDs.** Each replay gets its own run ID and scope, `replay:<id>`. `ingest_batch(..., replay_run_id=...)` derives raw event IDs within that scope. Republishing a replay is therefore a no-op, and two replays of one dataset share no ID.
2. **Simulated time.** Records are ingested in event-time order, in 2-second ticks. Each tick's batch is received at the tick's end plus 1 second, so `ingested_at` is at most 3 seconds after `occurred_at`. Replay incidents carry their detection time in event time (ADR-016). An investigation whose cutoff is that time sees what had arrived by then and nothing later.
3. **Completion markers.** The orchestrator polls PostgreSQL. It waits until every accepted raw event has a `normalizer-v1` receipt, every normalized event of the scope is published from the outbox, and `detector-v1` and `graph-projector-v1` each hold a receipt for every one of them. Then `flush_replay` closes the remaining windows by the simulated clock.
4. **Timeout and reconciliation.** If the markers are not met before the timeout, the replay ends `timed_out`. Windows are not flushed, and the report lists the accepted raw events that were never normalized. Every report records accepted, rejected, and failed records, each marker count, windows flushed, incidents, and drain time.

## Isolation and leakage prevention

- Each run gets its own `replay_scope`. Identical runs cannot contaminate each other or live data.
- Ground truth lives in evaluation tables that the runtime database role **cannot read**.
- Tools enforce the evidence cutoff, so agents cannot see future signals.
- A run finishes on explicit pipeline completion markers (drained lag, flushed windows), never on arbitrary sleeps.
- Each run records its dataset hash, workflow and prompt versions, model config, seed, pricing, and hardware.

## Engineering targets

Measured in P11-T04; results, including the missed sustained target, are in [benchmarks.md](benchmarks.md):

- Replay 1,000,000 synthetic signals in a bounded dataset.
- Sustain 100 events/s for 30 minutes on documented hardware.
- Lose no accepted logical events within retention, and create no duplicate logical records after redelivery.

## Cost control

Ordinary development and CI use the fake provider. Paid model evaluations, large replays, and long load tests run only on an explicit request that states their scope and budget.
