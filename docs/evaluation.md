# Evaluation

> Status: planned. Sources: [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §12 and roadmap Phase 10. No results exist yet. Targets below are hypotheses to test, not achievements.

## Goal

Find out whether agent investigations actually beat simpler approaches. Correctness, latency, and cost are reported **together**, and the comparison holds evidence, tools, and budget equal.

## Scenario families

| Family | Notes |
| --- | --- |
| Downstream timeout | `downstream-timeout`, built in P04-T01 (`signalops demo-generate`) |
| Connection pool exhaustion | |
| Deployment regression | Exercises the deployment investigator |
| Memory pressure | |
| Healthy traffic with noisy warnings | `healthy`, built in P04-T01. Should produce **no** incident or diagnosis |
| Mixed and insufficient-evidence variants | Tests abstention quality |

- Scenarios are versioned under `fixtures/` with a fixed seed and a dataset hash. `fixtures/scenarios/v1/<scenario>.labels.json` holds the labels and hash; the records are regenerated from the seed rather than committed. Both P04-T01 scenarios are development variants; held-out variants come with P10-T03.
- **Development** variants and **held-out** variants are kept separate. Prompts are never tuned on the reporting set.
- Synthetic replay is labelled synthetic. Real fault demonstrations run through the optional `demo` profile.

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

## Engineering targets (unvalidated)

These are measured in P11-T04 and published with failures included:

- Replay 1,000,000 synthetic signals in a bounded dataset.
- Sustain 100 events/s for 30 minutes on documented hardware.
- Lose no accepted logical events within retention, and create no duplicate logical records after redelivery.

## Cost control

Ordinary development and CI use the fake provider. Paid model evaluations, large replays, and long load tests run only on an explicit request that states their scope and budget.
