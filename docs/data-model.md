# Data model

> Status: logical design, not migrations. The source is [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §5. Migrations come in roadmap Phase 2 (P02-T01 to P02-T06). Fields may be refined when migrations and integration checks validate them.

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
| `sources` | `workspace_id`, `name`, `kind`, `config` JSONB (secret **references** only), `enabled`. `UNIQUE(workspace_id, name)` |
| `services` | `workspace_id`, `name`, `environment`, `attributes`. `UNIQUE(workspace_id, name, environment)` |
| `signals` | `source_id`, nullable `service_id`, `source_event_id`, `kind`, `occurred_at`, `ingested_at`, `severity`, `message`, `trace_id`, `attributes`, `quality_status`, `quality_flags`, `replay_scope` (default `'live'`). `UNIQUE(source_id, replay_scope, source_event_id)` |
| `deployments` | `service_id`, `signal_id` UNIQUE, `revision`, `deployed_at`, `attributes` |
| `signal_aggregates` | `service_id`, `replay_scope`, `metric_name`, `window_start`, `window_end`, `sample_count`, `error_count`, `value_sum`, `histogram`. `UNIQUE(service_id, replay_scope, metric_name, window_start)` |

**Identity:**

- `source_event_id` must stay stable across retries.
- Never hash message text alone to build it, because two identical messages can be separate events.
- A source without IDs needs a documented, adapter-specific identity strategy.
- A signal with unresolved service identity is quarantined or kept as degraded evidence. It is never assigned to an arbitrary service.

## Incidents and investigations

| Table | Key fields and constraints |
| --- | --- |
| `incidents` | `replay_scope`, `fingerprint`, `correlation_window_start`, `title`, `severity`, `status`, `detected_at`, `first_seen_at`, `last_seen_at`, `resolved_at`, `version`. `UNIQUE(workspace_id, replay_scope, fingerprint, correlation_window_start)` |
| `incident_services` | PK `(incident_id, service_id)`, `role` |
| `incident_signals` | PK `(incident_id, signal_id)`, `relevance` |
| `investigations` | `incident_id`, `request_key` UNIQUE, `status`, `workflow_version`, `budget_microusd`, `reserved_microusd`, `spent_microusd`, `max_steps`, `deadline_at`, `diagnosis` JSONB, `version` |
| `jobs` | `investigation_id` UNIQUE, `status`, `available_at`, `attempts`, `max_attempts`, `lease_owner`, `lease_until`, `lease_generation`, `last_error` |
| `agent_steps` | `step_key`, `agent_role`, `status`, `attempt`, `input`, `output`, `checkpoint`, `lease_generation`. `UNIQUE(investigation_id, step_key)` |
| `tool_calls` | `call_key`, `tool_name`, `arguments`, `result_ref`, `status`, `duration_ms`. `UNIQUE(agent_step_id, call_key)` |
| `llm_calls` | `attempt`, `provider`, `model`, `prompt_version`, `request_hash`, `status`, tokens, `reserved_microusd`, `cost_microusd`, `latency_ms`, `provider_request_id`. `UNIQUE(agent_step_id, attempt)` |
| `progress_events` | `id bigint identity` (the SSE cursor), `investigation_id`, `event_type`, `payload`, `created_at` |

### State machines

The persistence layer enforces these states and their allowed transitions:

| Entity | States |
| --- | --- |
| Investigation | `queued`, `running`, `completed`, `insufficient_evidence`, `failed`, `cancelled`, `budget_exceeded` |
| Job | `queued`, `running`, `succeeded`, `failed`, `cancelled` |
| Agent step | `pending`, `running`, `completed`, `failed`, `skipped` |

Store structured findings, never unrestricted private model reasoning. Roadmap task P02-T05 defines how diagnosis status maps to investigation terminal status.

## Temporal knowledge graph

| Table | Key fields and constraints |
| --- | --- |
| `graph_nodes` | `node_type` (`service`, `deployment`, `incident`, `runbook`), `external_key`, `properties`. `UNIQUE(workspace_id, node_type, external_key)` |
| `graph_edges` | `from_node_id`, `to_node_id`, `relation` (`DEPENDS_ON`, `DEPLOYED_TO`, `AFFECTS`, `DOCUMENTED_BY`), `valid_from`, `valid_to`, `confidence`, `provenance_signal_id`, `properties` |

- Traversals are bounded recursive CTEs: at most **3 hops**, with cycle prevention and row limits.
- Queries are "as of" the incident time, filtered by `valid_from` and `valid_to`.
- Several observations can support one edge. The projector coalesces duplicates and keeps provenance.
- An observed dependency or a correlation is not proof of causation.

## Reliability and evaluation

| Table | Key fields and constraints |
| --- | --- |
| `consumer_receipts` | PK `(consumer_group, event_id)`, `processed_at` |
| `outbox_events` | `aggregate_id`, `aggregate_version`, `topic`, `message_key`, `envelope`, `published_at`, `attempts`. `UNIQUE(aggregate_id, aggregate_version, topic)` |
| `evaluation_scenarios` | `name`, `version`, `dataset_ref`, `injection_spec`, `expected_outcome`. `UNIQUE(name, version)` |
| `evaluation_runs` | `scenario_id`, `system_variant`, `workflow_version`, `model_config`, `prompt_versions`, `dataset_hash`, `seed`, `metrics` |
| `evaluation_cases` | `run_id`, `case_key`, `investigation_id`, `expected_outcome`, `observed_outcome`, `scores`. `UNIQUE(run_id, case_key)` |

The investigation runtime's database role has **no access** to evaluation ground truth (`expected_outcome`, `injection_spec`).

## Initial indexes

- `signals(service_id, occurred_at DESC)`, `signals(source_id, ingested_at DESC)`, and a partial index on `trace_id` where it is not null.
- `incidents(status, detected_at DESC)`.
- `jobs(status, available_at)`, and a partial index on `jobs(lease_until)` for running jobs.
- `agent_steps(investigation_id, status)`, `progress_events(investigation_id, id)`.
- A partial index on `outbox_events(created_at)` `WHERE published_at IS NULL`.
- `graph_edges(from_node_id, relation)`, `graph_edges(to_node_id, relation)`.
- JSONB indexes are added only after query profiling.

## Retention (initial local targets)

| Data | Retention |
| --- | --- |
| Kafka raw events | 3 days |
| Kafka normalized events | 7 days |
| PostgreSQL detailed signals | 7 days |
| Aggregates | 30 days |
| Consumer receipts | At least the replay horizon |

Before deleting evidence that a diagnosis cites, keep its redacted excerpt, content hash, and provenance, or mark it expired in the UI. Date partitioning, object storage, or a telemetry analytics database come only after a benchmark shows PostgreSQL falling short.
