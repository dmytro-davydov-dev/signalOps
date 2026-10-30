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

## Baselines

| Variant | Description |
| --- | --- |
| Deterministic | Rule-based diagnosis, no model |
| Single agent | One tool-using agent (the Phase 7 workflow) |
| Multi-agent | Specialists, synthesis, and verification (the Phase 9 workflow) |

Every variant gets the same evidence cutoff, tools, dataset, and **total token budget**. Model runs are repeated, and the report includes sample size and variance. The multi-agent variant stays only if it improves quality, robustness, or useful parallelism enough to justify its cost.

## Metrics

| Metric | Definition |
| --- | --- |
| Detection precision and recall | Correct incident windows and affected services vs labels, with one-to-one matching |
| Root-cause accuracy | Correct service **and** failure type on diagnosable cases |
| Evidence precision | Supported cited claims ÷ all audited cited claims |
| Unsupported diagnosis rate | Diagnoses asserting causes the available evidence doesn't contain |
| Abstention quality | Correct `insufficient_evidence` on underdetermined cases |
| Diagnosis latency | Diagnosis persisted minus incident detected (p50 and p95) |
| Cost per investigation | Provider-attributed, including retries and ambiguous calls |
| Recovery correctness | Checkpoints reused, no duplicate logical jobs or links |
| Data quality | Invalid, unresolved, late, duplicate, and quarantined events per source |

Deterministic labels are used wherever possible. Human audit samples cover semantic support. Judge-model scores are never presented as ground truth.

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
