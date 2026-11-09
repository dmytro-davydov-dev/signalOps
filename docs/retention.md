# Retention, backup, and recovery

Local single-node procedures (P11-T05). Retention never removes cited evidence or deduplication history that a redelivery could still need; a backup of PostgreSQL holds every durable fact the system owns.

## Retention

```bash
uv run signalops retention                       # report only (the default)
uv run signalops retention --apply               # delete in batches of 5,000
uv run signalops retention --scope replay:<run>  # signals and aggregates of one replay scope only
docker compose run --rm --no-deps backend retention   # the same inside Compose
```

The report lists each category with its cutoff, how many rows are old enough (`eligible`), how many of those stay because something references them (`kept`), and how many were deleted.

| Category | Deleted when | Never deleted | Setting (default) |
| --- | --- | --- | --- |
| Signals | Stored (`stored_at`, wall clock) longer ago than the bound | Signals cited by an incident's evidence, a deployment, a graph edge's provenance, or a stored diagnosis (root cause or contradiction). They stay whole, so citations keep resolving and their content hash is unchanged | `SIGNALOPS_RETENTION_SIGNALS_DAYS` (7) |
| Detection samples | With their signal (cascade) | | |
| Aggregates | Closed windows whose `window_end` and `closed_at` are both older than the bound | Open windows, and windows a replay closed recently | `SIGNALOPS_RETENTION_AGGREGATES_DAYS` (32) |
| Consumer receipts | Older than the group's bound (below) | Anything a duplicate could still reach | computed |
| Outbox | Published rows older than the bound | Unpublished and quarantined rows | `SIGNALOPS_RETENTION_OUTBOX_DAYS` (7) |
| Progress history | Events of investigations that ended before the bound | The investigation, its steps, calls, and diagnosis | `SIGNALOPS_RETENTION_PROGRESS_DAYS` (30) |
| Redis | Keys under `cache:` or `limit:` with no expiry (normally none) | Keys with an expiry, which Redis drops itself | |

Receipt bounds are derived, so no setting can shorten deduplication:

| Consumer group | Bound | Why |
| --- | --- | --- |
| `normalizer-v1` | 32 days: the larger of `signals.raw.v1` retention (3 days) and the oldest `occurred_at` ingestion accepts (30 days) plus how far ahead it accepts (1 day), plus 1 day | A client may re-send a record up to the ingestion age bound; it gets the same raw event ID, and the receipt makes it a no-op even after its signal is deleted |
| `detector-v1`, `graph-projector-v1` | 8 days: `signals.normalized.v1` retention plus 1 day | Kafka cannot redeliver older events |
| `dispatcher-v1` | 8 days: `investigations.requested.v1` retention plus 1 day | Likewise |
| any other | the longest bound above | Unknown groups are treated conservatively |

The command refuses to run (exit 2) when a configured bound would break a rule: aggregates must outlive the ingestion age and future bounds, or a late record could reopen a deleted window and fire a rule on partial data; signals must outlive an investigation's deadline plus its six-hour tool window.

Ages are wall-clock storage times. A replay stores signals with a simulated `ingested_at` that can be weeks old, so `ingested_at` would let cleanup delete a replay that is still running; `signals.stored_at` (migration 0019, default `now()`; rows stored earlier carry the migration time) and the wall-clock `closed_at` of windows avoid that.

Deleting a signal that nothing cites removes it from evidence queries and tool reads. Incident timelines, diagnoses, and audits only reference cited signals, so they are unaffected. Kafka topics apply their own retention (`streaming/topics.py`): raw 3 days, everything else 7.

Checks: `tests/integration/test_retention.py` (dry run changes nothing; only uncited old signals go, and a replayed signal with an old `ingested_at` stays; windows go only when both window and closure are old; diagnosis citations resolve afterwards; receipts within their bounds stay and still deduplicate; pending outbox rows and live investigations stay; unsafe bounds are refused).

## Backup and restore

PostgreSQL holds evidence, incidents, the graph, jobs, checkpoints, budgets, progress history, receipts, the outbox, and evaluation results. Redis holds nothing durable. Kafka holds records in flight, which replay from it after a restore.

**Back up** (a consistent snapshot; the stack keeps running):

```bash
set -a; . ./.env; set +a
docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" -Fc -d "$POSTGRES_DB" > signalops-$(date -u +%Y%m%dT%H%M%SZ).dump
```

Login roles are cluster-wide and not in the dump; `signalops migrate` recreates them from `SIGNALOPS_DB_RUNTIME_PASSWORD` and `SIGNALOPS_DB_EVALUATOR_PASSWORD`.

**Restore** into a new, empty database:

```bash
docker compose stop api normalizer detector graph-projector dispatcher investigation-worker detector-closer outbox-publisher
docker compose exec -T postgres createdb -U "$POSTGRES_USER" signalops_restored
docker compose exec -T postgres pg_restore -U "$POSTGRES_USER" --exit-on-error -d signalops_restored < signalops-<time>.dump
```

Then point `POSTGRES_DB` at it (or rename databases while nothing is connected), run `docker compose run --rm migrate` (a no-op at the same head, and it provisions the login roles), and start the services.

**Records after the backup:** consumer groups have committed offsets past what the restored database holds. Before starting the consumers, rewind each group to a little before the dump time; receipts make the overlap harmless:

```bash
for pair in normalizer-v1:signals.raw.v1 detector-v1:signals.normalized.v1 graph-projector-v1:signals.normalized.v1 dispatcher-v1:investigations.requested.v1; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --group "${pair%%:*}" --topic "${pair#*:}" --reset-offsets --to-datetime 2026-11-08T12:00:00.000 --execute
done
```

The rewind can only reach back as far as topic retention (raw records 3 days), so take backups at least daily. Work that started after the backup and did not come from Kafka is lost: operator investigation requests and evaluation runs must be requested again. Outbox rows restored as unpublished are published again, and consumers deduplicate them.

Checks: `tests/integration/test_backup_restore.py` dumps a database with evidence, an incident, a completed investigation, receipts, and outbox and progress rows using the commands above, restores it into an empty database, and finds every row of every table in both schemas identical; a redelivered event is still a receipt hit afterwards. The consumer-group rewind is documented but was not exercised against the shared stack (it would rewind the development groups).

## Fixture recovery

Everything a demonstration or evaluation needs can be rebuilt from the repository:

- Scenario datasets are generated from scenario, seed, and variant (`signalops demo-generate`, `evaluation/catalog.py`); committed label fixtures in `fixtures/` carry their dataset hashes, and `tests/unit/test_demo_generator.py` fails if generation drifts.
- Contract schemas and the OpenAPI document regenerate with `uv run signalops export-schemas`; `--check` fails when the committed files are stale.
- The demo catalog (`signalops seed`) and runbooks are idempotent: rerunning verifies the existing rows.
- Evaluation results live in PostgreSQL (`evaluation` schema) and are covered by the backup; a run can also be repeated from its recorded split, seed, and versions.

## Single-node durability limits

- One Kafka broker with replication factor 1 and local volumes: losing the host or its disk loses records in flight and everything since the last backup. `acks=all` means only that one broker wrote the record.
- One PostgreSQL instance without WAL archiving or replicas: recovery is to the last `pg_dump`, not to a point in time.
- Redis runs without persistence guarantees; losing it loses caches and rate-limit windows only.
- Docker Compose restarts crashed processes (`restart: unless-stopped`) but provides no failover; a stopped dependency stops the work that needs it (docs/reliability.md#outage-checks-p11-t03).
- These are development-environment properties. Multi-broker Kafka, PostgreSQL replication or WAL archiving, and off-host backups are deferred (roadmap deferred backlog).
