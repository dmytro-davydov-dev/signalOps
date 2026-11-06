# Data model

> Status: the source and evidence tables (`0002_source_evidence`, P02-T01), the incident and workflow tables (`0003_incident_workflow`, P02-T02), and the graph and reliability tables (`0004_graph_reliability`, P02-T03) are migrated; ORM models are in `backend/signalops/persistence/models/`. The evaluation tables are still logical design from [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §5 and become migrations in Phase 10.

## Conventions

- UUID primary keys. `timestamptz` in UTC everywhere.
- JSONB only for source-specific attributes. Core fields are typed columns.
- Explicit foreign keys. Polymorphic IDs are never left unchecked.
- Money is stored as integer micro-USD (`*_microusd bigint`).
- `CHECK` constraints cover enumerations (kind, severity, status) and reject negative counters and invalid windows.
- Every workspace-owned relationship is validated against `workspace_id`, through composite foreign keys or transactional validation.
- Constraints and indexes are added only through reviewed Alembic revisions.

## Entity overview

```mermaid
erDiagram
    workspaces ||--o{ sources : has
    workspaces ||--o{ services : has
    sources ||--o{ signals : emits
    services ||--o{ signals : "attributed to"
    signals ||--o| deployments : "is"
    services ||--o{ signal_aggregates : windows
    incidents ||--o{ incident_signals : cites
    incidents ||--o{ incident_services : affects
    incidents ||--o{ investigations : "investigated by"
    investigations ||--|| jobs : "executed by"
    investigations ||--o{ agent_steps : contains
    agent_steps ||--o{ tool_calls : makes
    agent_steps ||--o{ llm_calls : makes
    investigations ||--o{ progress_events : emits
    graph_nodes ||--o{ graph_edges : from
    graph_nodes ||--o{ graph_edges : to
```

## Sources and evidence

| Table | Key fields and constraints |
| --- | --- |
| `workspaces` | `id`, `name`, `created_at` |
| `sources` | `workspace_id`, `name`, `kind` (`structured_log`, `trace_summary`, `deployment`), `config` JSONB object (secret **references** only), `enabled`, `created_at`. `UNIQUE(workspace_id, name)` |
| `services` | `workspace_id`, `name`, `environment`, `attributes`, `created_at`. `UNIQUE(workspace_id, name, environment)` |
| `signals` | `workspace_id`, `source_id`, nullable `service_id`, `source_event_id` (1–512 chars), `kind` (`log`, `trace`, `deployment`), `occurred_at`, `ingested_at`, `severity` (`debug`, `info`, `warning`, `error`, `critical`), `message`, `trace_id`, `attributes` (object), `quality_status` (`accepted`, `degraded`, `quarantined`), `quality_flags` (array; flag effects in [events.md](events.md#normalizer-normalizer-v1)), `replay_scope` (1–128 chars, default `'live'`). `UNIQUE(source_id, replay_scope, source_event_id)` |
| `deployments` | `workspace_id`, `service_id`, `signal_id` UNIQUE, `replay_scope`, `revision`, `deployed_at`, `attributes`. `(workspace_id, replay_scope, service_id, signal_id)` must match the cited signal |
| `signal_aggregates` | `workspace_id`, `service_id`, `replay_scope`, `metric_name`, `window_start`, `window_end` (> start), `sample_count`, `error_count` (`0 ≤ error_count ≤ sample_count`), `value_sum`, `histogram`, `late_count` ≥ 0, `closed_at`, `closed_by` (`wall_clock`, `simulated_clock`, `final_flush`, `backfill`; set together with `closed_at`). `UNIQUE(service_id, replay_scope, metric_name, window_start)` |
| `detection_samples` | PK `(signal_id, metric_name)`; `signal_id` references `signals` `ON DELETE CASCADE`; `workspace_id`, `service_id`, `replay_scope`, `window_start`, `is_error`, `value` (NULL or ≥ 0), `late`, `recorded_at` |
| `detection_clocks` | PK `(service_id, replay_scope)`; `workspace_id`, `clock_at`. Replay scopes only (`replay_scope <> 'live'`) |
| `detection_triggers` | `workspace_id`, `service_id`, `replay_scope` (1–128 chars), `rule_id`, `rule_version` ≥ 1, `metric_name`, `window_start`, `window_end` (> start), `observed`, `threshold`, `sample_count`, `error_count` (`0 ≤ error_count ≤ sample_count`), `triggered_at`. `UNIQUE(service_id, replay_scope, rule_id, window_start)` named `uq_detection_triggers_window` |

**Workspace safety:** each workspace-owned reference is a composite foreign key that includes `workspace_id` (targets expose `UNIQUE(workspace_id, id)`), so a signal cannot cite a source or service from another workspace. A NULL `service_id` skips the service check. Enumerations live in `signalops.contracts.evidence`; adding a value requires a migration that widens the CHECK constraint. Foreign keys use the default `RESTRICT` behaviour: evidence is never cascade-deleted.

**Deviations from the plan (P02-T01):** `deployments` gains `replay_scope` so deployment queries can be scoped without joining `signals`, and `deployments` and `signal_aggregates` gain `workspace_id` for the composite keys. The aggregate unique constraint is named `uq_signal_aggregates_window` because the conventional name exceeds PostgreSQL's 63-character limit.

**Detector state (P05-T01, revision `0009_detection_state`):** the plan lists only `signal_aggregates`. `detection_samples` is the business key of a contribution, so a signal counts once per series even under another event ID, and it lets a window be recomputed from its members (P05-T02). Samples are derived from their signal and the only cascade from evidence; the aggregate keeps the counts after a signal expires. `detection_triggers` records each rule firing for the incident writer (P05-T03). Aggregate `metric_name` is `{metric}.{window_seconds}s`, and `histogram` maps bucket upper bounds in milliseconds (string keys) to counts. Semantics in [events.md](events.md#detector-detector-v1).

**Incident correlation (P05-T03, revision `0011_incident_correlation`):** `detection_triggers.incident_id` links a trigger to its incident through the composite scope key; triggers from before this revision stay null. `incident_services.healthy_through` is the recovery input. `ix_incidents_fingerprint_last_seen_at` serves the correlation lookup. Semantics in [events.md](events.md#incidents) and ADR-017.

**Window closure (P05-T02, revision `0010_window_closure`):** resolves open question Q7 with columns on the window row rather than a separate table, because a window has one closure. `sample_count`, `error_count`, `value_sum`, and `histogram` hold on-time samples only; `late_count` holds the rest. `detection_clocks` is durable replay state, since a Redis or in-memory clock would lose its position on restart. See ADR-016.

**Identity:**

- `source_event_id` must stay stable across retries.
- Never hash message text alone to build it, because two identical messages can be separate events.
- A source without IDs needs a documented, adapter-specific identity strategy.
- A signal with unresolved service identity is quarantined or kept as degraded evidence. It is never assigned to an arbitrary service. The normalizer's rules and the `signals.attributes` layout (`source`, `record`, `provenance`) are in [events.md](events.md#normalizer-normalizer-v1).

## Incidents and investigations

| Table | Key fields and constraints |
| --- | --- |
| `incidents` | `workspace_id`, `replay_scope` (1–128 chars), `fingerprint`, `correlation_window_start`, `title`, `severity` (`low`, `medium`, `high`, `critical`), `status` (`open`, `resolved`), `detected_at`, `first_seen_at` ≤ `last_seen_at`, `resolved_at` (set exactly when resolved), `version` ≥ 1. `UNIQUE(workspace_id, replay_scope, fingerprint, correlation_window_start)` named `uq_incidents_correlation` |
| `incident_services` | PK `(incident_id, service_id)`, `workspace_id`, `role` (`primary`, `affected`), `healthy_through` (end of the latest window that showed the service healthy) |
| `incident_signals` | PK `(incident_id, signal_id)`, `workspace_id`, `replay_scope`, `relevance` in [0, 1] |
| `investigations` | `workspace_id`, `incident_id`, `replay_scope`, `request_key` UNIQUE (1–256 chars), `status`, `workflow_version`, `evidence_cutoff_at`, `budget_microusd`, `reserved_microusd`, `spent_microusd` (all ≥ 0), `max_steps` > 0, `created_at` < `deadline_at`, `completed_at` (set exactly in terminal states), `diagnosis` JSONB object whose `status` matches the investigation status (see below), `version` ≥ 1 |
| `jobs` | `investigation_id` UNIQUE, `status`, `available_at`, `0 ≤ attempts ≤ max_attempts` (default 3), `lease_owner` and `lease_until` (set together, exactly while `running`), `lease_generation` ≥ 0, `last_error` JSONB object |
| `agent_steps` | `investigation_id`, `step_key` (1–256 chars), `agent_role`, `status`, `attempt` ≥ 0, `input`, `output`, `checkpoint` (JSONB objects), `lease_generation`, `started_at` (required once not `pending`), `completed_at` (set exactly when `completed`, `failed`, or `skipped`); `completed` requires `output`. `UNIQUE(investigation_id, step_key)` |
| `tool_calls` | `agent_step_id`, `call_key` (1–256 chars), `tool_name` (the five read-only tools), `arguments` object, `result_ref`, `status` (`running`, `succeeded`, `failed`), `duration_ms` ≥ 0. `UNIQUE(agent_step_id, call_key)` |
| `llm_calls` | `agent_step_id`, `attempt` ≥ 1, `provider`, `model`, `prompt_version`, `request_hash`, `status` (`pending`, `succeeded`, `failed`, `unknown`), tokens ≥ 0, `reserved_microusd`, `cost_microusd`, `cost_basis` (`actual`, `estimated`, `unknown`), `latency_ms`, `provider_request_id`. `UNIQUE(agent_step_id, attempt)` |
| `progress_events` | `id bigint identity` (the SSE cursor), `investigation_id`, `event_type` (1–128 chars), `payload` object, `created_at` |

**Scope safety:** `incident_signals` and `investigations` reference `incidents(workspace_id, replay_scope, id)`, and `incident_signals` also references `signals(workspace_id, replay_scope, id)`, so an incident cannot cite evidence from another workspace or replay run and an investigation always shares its incident's scope. `incident_services` uses `(workspace_id, …)` keys to both parents. Jobs, steps, calls, and progress events inherit scope through `investigation_id`.

**Budget:** all three amounts are nonnegative. No CHECK enforces `reserved + spent ≤ budget`: the gateway's reservation is a conditional `UPDATE` that enforces the ceiling, and an actual provider charge above a reservation must still be recordable.

**LLM call cost:** a `pending` call has no `cost_basis`; any outcome has one. `actual` and `estimated` carry `cost_microusd`; `unknown` (always used for the ambiguous `unknown` status) carries none and the investigation keeps the reservation. `succeeded` requires a known cost.

**Optimistic versions:** `incidents.version` and `investigations.version` are the SQLAlchemy `version_id_col`, so ORM updates include `WHERE version = :old` and raise `StaleDataError` when another writer got there first. Core updates must add the same predicate.

**JSONB NULLs:** nullable JSONB columns use `none_as_null=True`; otherwise SQLAlchemy writes Python `None` as the JSON value `null`, which fails the object checks.

**Deviations from the plan (P02-T02):** `workspace_id` and `replay_scope` on `investigations` and `incident_signals` and `workspace_id` on `incident_services` (composite keys); `investigations.evidence_cutoff_at` (tools and the UI need the cutoff; the plan states it elsewhere but not as a column); `llm_calls.cost_basis` (actual versus estimated versus unknown cost); `created_at` on jobs, steps, tool and LLM calls; `agent_steps.started_at` nullable because pending steps have not started. Incident status, severity, and service role values are not specified by the plan; these are initial choices that P05-T03 may widen with a new revision.

### State machines

Allowed transitions are enforced in PostgreSQL by the `enforce_status_transition()` trigger function (one `status_transition` trigger per table, firing only when `status` changes) and mirror the maps in `signalops.contracts.workflow`. A rejected change raises `check_violation` with constraint name `ck_<table>_status_transition`. Unchanged status is always allowed (for example a reclaimed lease stays `running`). Alembic autogenerate does not see triggers; `test_*_transitions` compares every state pair in the database against the Python maps.

| Entity | Allowed changes |
| --- | --- |
| Incident | `open → resolved`, `resolved → open` |
| Investigation | `queued → running / failed / cancelled`; `running →` any terminal state (`completed`, `insufficient_evidence`, `failed`, `cancelled`, `budget_exceeded`). Terminal states are final |
| Job | `queued → running / failed / cancelled`; `running → queued` (delayed retry) `/ succeeded / failed / cancelled`. Terminal states are final |
| Agent step | `pending → running / skipped`; `running → completed / failed`; `failed → running` (retry). `completed` and `skipped` are final |

Store structured findings, never unrestricted private model reasoning.

Diagnosis status and investigation status map one to one (`DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS` in `contracts/investigations.py`): `completed` stores a `diagnosed` diagnosis, `insufficient_evidence` stores an `insufficient_evidence` diagnosis, and every other status stores none. `ck_investigations_diagnosis_status` (revision `0005_diagnosis_status_mapping`) enforces it; an integration test checks every status and diagnosis combination against the Python map. The workflow derives the terminal status from the validated diagnosis (`terminal_status_for`) rather than choosing it separately.

## Temporal knowledge graph

| Table | Key fields and constraints |
| --- | --- |
| `graph_nodes` | `workspace_id`, `replay_scope`, `node_type` (`service`, `deployment`, `incident`, `runbook`), `external_key` (1–512 chars), nullable `service_id` / `deployment_id` / `incident_id`, `properties` object, `created_at`. `UNIQUE(workspace_id, replay_scope, node_type, external_key)` named `uq_graph_nodes_identity` |
| `graph_edges` | `workspace_id`, `replay_scope`, `from_node_id` ≠ `to_node_id`, `relation` (`DEPENDS_ON`, `DEPLOYED_TO`, `AFFECTS`, `DOCUMENTED_BY`), `valid_from`, `valid_to` (NULL or > `valid_from`), `confidence` in [0, 1], `provenance_signal_id`, `properties` object, `created_at` |

**Scope safety:** both edge endpoints reference `graph_nodes(workspace_id, replay_scope, id)` (`fk_graph_edges_from_node`, `fk_graph_edges_to_node`), so an edge cannot dangle, cross workspaces, or join two replay runs; deleting a node with edges is rejected. `provenance_signal_id` references `signals(workspace_id, replay_scope, id)`.

**Entity nodes:** `node_type` is polymorphic, so each entity type has its own foreign key: `service_id` to `services(workspace_id, id)`, `deployment_id` to `deployments(workspace_id, replay_scope, id)`, `incident_id` to `incidents(workspace_id, replay_scope, id)`. The `ck_graph_nodes_entity_reference` CHECK requires exactly the column matching `node_type` (none for `runbook`) and makes `external_key` equal to that row's ID, so an entity has at most one node per replay scope. Runbooks are not database rows; their `external_key` is the runbook path.

- Traversals are bounded recursive CTEs: at most **3 hops**, with cycle prevention and row limits.
- Queries are "as of" the incident time, filtered by `valid_from` and `valid_to`.
- Several observations can support one edge. The projector coalesces duplicates and keeps provenance.
- An observed dependency or a correlation is not proof of causation.
- The schema rejects self-loops only; longer cycles are prevented by the traversal.

**Deviations from the plan (P02-T03):** `replay_scope` on nodes and edges, so replay graphs stay isolated from the live graph (CLAUDE.md requires replay scope across graph traversals); the typed entity reference columns, because the conventions forbid unchecked polymorphic IDs; `created_at` on nodes and edges; `deployments` gains `UNIQUE(workspace_id, replay_scope, id)` as the node reference target. Edge uniqueness and coalescing of repeated observations are left to the projector (P05-T04), which owns the merge rule.

## Reliability and evaluation

| Table | Key fields and constraints |
| --- | --- |
| `consumer_receipts` | PK `(consumer_group, event_id)`, `consumer_group` 1–256 chars, `processed_at` |
| `outbox_events` | `aggregate_id`, `aggregate_version` ≥ 1, `topic` (1–256 chars), `message_key` (nonempty), `envelope` object, `created_at`, `published_at` (NULL while pending, otherwise ≥ `created_at`), `attempts` ≥ 0, `next_attempt_at` (retry deferral), `quarantined_at` (never set together with `published_at`), `last_error` (1–2048 chars). `UNIQUE(aggregate_id, aggregate_version, topic)` |
| `evaluation_scenarios` | `name`, `version`, `dataset_ref`, `injection_spec`, `expected_outcome`. `UNIQUE(name, version)` |
| `evaluation_runs` | `scenario_id`, `system_variant`, `workflow_version`, `model_config`, `prompt_versions`, `dataset_hash`, `seed`, `metrics` |
| `evaluation_cases` | `run_id`, `case_key`, `investigation_id`, `expected_outcome`, `observed_outcome`, `scores`. `UNIQUE(run_id, case_key)` |

Receipts and outbox rows are infrastructure records without a workspace: the event they describe carries its scope in the envelope. The publisher's pending query (`WHERE published_at IS NULL ORDER BY created_at LIMIT n`) is served by the partial index alone; an integration test checks the plan with 20,000 rows. No `processed_at` index yet; receipt retention cleanup adds one when it exists.

The investigation runtime's database role has **no access** to evaluation ground truth (`expected_outcome`, `injection_spec`).

## Initial indexes

- `signals(workspace_id, replay_scope, occurred_at, id)` for scoped keyset pages, `signals(service_id, replay_scope, occurred_at, id)` per service, `signals(source_id, replay_scope, ingested_at)` for per-source quality counts, and a partial index on `trace_id` where it is not null. Revision `0008_evidence_query_indexes` replaced the plan's `signals(service_id, occurred_at DESC)` and `signals(source_id, ingested_at DESC)`, which lacked `replay_scope` and the ID tiebreaker. Columns are ascending: newest-first pages scan backward, and the `(occurred_at, id) < (…)` row comparison is an index condition (checked with `EXPLAIN`).
- `incidents(status, detected_at DESC)`, `incidents(workspace_id, replay_scope, detected_at DESC)` for scoped listing.
- `investigations(incident_id, created_at DESC)`.
- `jobs(status, available_at)`, and a partial index on `jobs(lease_until)` for running jobs.
- `agent_steps(investigation_id, status)`, `progress_events(investigation_id, id)`.
- A partial index on `outbox_events(created_at)` `WHERE published_at IS NULL`.
- A partial index on `outbox_events(aggregate_id, aggregate_version)` `WHERE published_at IS NULL`, for holding back an aggregate behind a deferred or quarantined row.
- `graph_edges(from_node_id, relation)`, `graph_edges(to_node_id, relation)`.
- Foreign-key support (revision `0006_baseline_indexes`): `incident_services(service_id)`, `incident_signals(signal_id)`, partial indexes on `graph_nodes(service_id)`, `(deployment_id)`, `(incident_id)` and `graph_edges(provenance_signal_id)` where not null. Rule, enforced by `test_every_foreign_key_has_a_supporting_index`: every foreign key to a parent other than the workspace has an index led by one of its identifying (non-`workspace_id`, non-`replay_scope`) columns, so parent-side key checks and reverse lookups avoid sequential scans. Workspace-only references are exempt; workspaces are not deleted row by row.
- Partial `ix_signal_aggregates_open(replay_scope, window_end) WHERE closed_at IS NULL` for finding windows due for closure.
- `detection_samples(service_id, replay_scope, metric_name, window_start)` for recomputing a window; the trigger unique constraint serves the cooldown lookup.
- JSONB indexes are added only after query profiling.

## Retention (initial local targets)

| Data | Retention |
| --- | --- |
| Kafka raw events | 3 days |
| Kafka normalized events | 7 days |
| PostgreSQL detailed signals | 7 days |
| Aggregates and detection triggers | 30 days |
| Detection samples | With their signal (cascade) |
| Consumer receipts | At least the replay horizon |

Before deleting evidence that a diagnosis cites, keep its redacted excerpt, content hash, and provenance, or mark it expired in the UI. Date partitioning, object storage, or a telemetry analytics database come only after a benchmark shows PostgreSQL falling short.
