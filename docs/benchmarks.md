# Benchmarks

Throughput measurements of the Compose stack on one development host (P11-T04). Each run writes a JSON report to `docs/benchmarks/`; the figures below are copied from those reports. Nothing here is a production capacity claim: one broker, one PostgreSQL, one host, fake model provider.

## How to run

```bash
set -a; . ./.env; set +a
export SIGNALOPS_DATABASE_URL="postgresql+psycopg://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_HOST_PORT/$POSTGRES_DB"
export SIGNALOPS_KAFKA_BOOTSTRAP_SERVERS="localhost:$KAFKA_HOST_PORT"
export SIGNALOPS_REDIS_URL="redis://localhost:$REDIS_HOST_PORT/0"
docker compose --profile observability up -d --wait --scale normalizer=3 --scale detector=2 --scale graph-projector=2
uv run signalops benchmark replay --records 1000000 --out docs/benchmarks/replay-1m.json
uv run signalops benchmark sustained --rate 100 --duration-s 1800 --out docs/benchmarks/sustained-100.json
```

Start from an idle pipeline (all consumer lag 0, outbox empty): a backlog ahead of the run is counted as the run's own time. The process exits 1 when the pipeline did not drain within `--drain-timeout-s` or reconciliation found missing records; the report is written either way.

- **replay** publishes N synthetic records (healthy traffic from the demo generator overlaid `--density` times per 6-hour window, the first window carrying one downstream timeout) straight to `signals.raw.v1` through `ingest_batch`, in their own replay scope, and waits until PostgreSQL holds every signal, all lag is 0, and the outbox is empty. It skips HTTP, so it measures the streaming pipeline.
- **sustained** posts live records through `POST /v1/ingest/{source}` at `--rate` records per second for `--duration-s`, three sources posting each second, timing every request.

Both sample consumer lag and outbox backlog every 5 s, CPU and memory per container (`docker stats`), and the latency of `GET /v1/incidents?limit=20` every 0.5 s, then reconcile every accepted record (`streaming/reconcile.py`) and record PostgreSQL and Kafka storage before and after, host hardware, image tags, replica counts, and the git revision.

## Results

Host: AMD Ryzen 5 7640HS (12 logical CPUs), 24.7 GiB RAM, Linux 7.0, Docker 29.9.3; images `apache/kafka:4.3.0`, `postgres:17.11-alpine`, `redis:7.4.12-alpine`, `signalops-backend:local`. Replicas: normalizer 3, detector 3, graph projector 3, everything else 1. Fake model provider. Both reports record git revision `dff8287` with a dirty tree: the outbox change below was applied but not yet committed (it is part of the P11-T04 commit). Records are demo-generator JSON of 346 bytes mean, 394 max.

### Replay: one million records ([`benchmarks/replay-1m.json`](benchmarks/replay-1m.json))

2026-11-09 10:14–11:59 UTC, replay scope `replay:a9f803cf-dc6a-42e3-b996-acf3f8d2dbea`, density 10.

| Measure | Result |
| --- | --- |
| Records accepted / rejected / failed | 1,000,000 / 0 / 0 |
| Publish to Kafka (`ingest_batch`, 4,289 batches) | 83.8 s, 11,938 records/s; batch p95 26 ms |
| Drained (every signal stored, lag 0, outbox empty) | yes, 6,299 s after the first publish |
| End-to-end throughput | **158.7 records/s** |
| Reconciliation | 1,000,000 processed, 0 quarantined, 0 missing, 1,000,000 signals |
| Peak lag | normalizer 976,938; detector 140,314; graph projector 63,842 |
| Peak outbox backlog | 168 |
| Operator read `GET /v1/incidents` (12,424 probes) | p50 4.4 ms, **p95 5.7 ms**, p99 7.6 ms, max 74 ms, 0 errors |
| CPU mean (% of one core) | PostgreSQL 57.7 (max 143); busiest normalizer 50.4 (the other two 17.5 and 6.4); graph projectors up to 54.5; Kafka 30.1 |
| Peak memory | Kafka 1,444 MiB; PostgreSQL 563 MiB; each backend process ≤ 220 MiB |
| Storage growth | PostgreSQL +3.96 GB (signals +1.72 GB, outbox +1.33 GB, receipts +0.65 GB, detection samples +0.25 GB); Kafka logs +1.58 GB |
| Incidents in the scope | 16: the injected `bank-gateway` latency fault, and 15 "High error rate on inventory" in healthy windows |

The normalizer was the limit throughout: one member owned raw partition 0 (70% of the records) and ran at ~119 records/s while the other two finished their partitions early. Detection kept up once normalized events arrived. About 4 KB of PostgreSQL per signal, outbox rows included, until retention removes published outbox rows after 7 days (docs/retention.md).

**Found:** with ten healthy datasets overlaid, the inventory error-rate rule fired 15 times in about 18 hours of event time. Not investigated; a likely but unverified cause is that ten times the volume meets the rule's minimum sample count while healthy inventory traffic carries some errors. It is a detection-quality question for evaluation, not a throughput result, and nothing was changed.

### Sustained: 100 records/s target for 30 minutes ([`benchmarks/sustained-100.json`](benchmarks/sustained-100.json))

2026-11-09 12:00–12:30 UTC, live scope, through the HTTP API.

| Measure | Result |
| --- | --- |
| Records generated / accepted | 178,558 / 178,558 |
| Achieved rate | **99.2 records/s** (5,870–6,037 per minute, every minute) |
| Target reached | **no, short by 0.8%**: the harness generated 178,558 records instead of 180,000 (below) |
| Ingest requests | 153,639, all HTTP 200; p50 3.5 ms, **p95 14.8 ms**, p99 15.9 ms, max 260 ms |
| Sender behind schedule | p95 10.6 ms, max 330 ms |
| Drained after posting stopped | 0.6 s |
| Reconciliation | 178,558 processed, 0 quarantined, 0 missing |
| Peak lag | normalizer 22, detector 79, graph projector 84; peak outbox backlog 81 |
| Operator read (3,572 probes) | p50 4.7 ms, **p95 7.4 ms**, max 260 ms, 0 errors |
| CPU mean (% of one core) | API 25.4, PostgreSQL 33.4, busiest normalizer 33.4, busiest graph projector 35.4, detectors 23–27, Kafka 29.9 |
| Storage growth | PostgreSQL +440 MB, Kafka logs +269 MB |
| Live incidents | 1 (inventory error rate, as above); its automatic investigation completed on the fake provider |

**Shortfall cause:** `live_records` estimated how many healthy datasets to overlay from one probe dataset, and the overlay came out 0.8% short. It now adds datasets until the target is covered and thins them evenly across the duration (`tests/unit/test_benchmark.py`). The run was not repeated, so 100 records/s for 30 minutes is not claimed. The headroom shown (lag under 100, drain in under a second) says nothing about capacity above 100/s beyond what the replay measured.

## Profile and tuning

Measured on 2026-11-09 before the runs, with a scratch script that ran the normalizer's `TransactionalProcessor` against Compose PostgreSQL on 1,000 fresh raw envelopes (cProfile) and timed synchronous offset commits on a throwaway group, while the stack drained a backlog.

| Measurement | Result |
| --- | --- |
| Normalizer transaction per record | 10.29 ms mean, 9.94 p50, 12.56 p95 |
| of which waiting on PostgreSQL (`epoll`) | ~4.5 ms; the rest is Python (SQLAlchemy statement handling, Pydantic) |
| Statements per record | receipt insert, source select, service select, signal insert, outbox insert, outbox `created_at` refresh, inside BEGIN/COMMIT |
| Synchronous offset commit | 0.16 ms p50, 0.23 p95 (not a bottleneck) |
| Consumer rate per member (Prometheus, during the drain) | normalizer 114–119/s, detector 108–115/s, graph projector 102–107/s |
| Raw partition sizes (end offsets) | 773,142 / 89,585 / 243,288: partition 0 holds 70% |
| Normalized partition sizes | 399,692 / 404,786 / 0: partition 2 is unused |

Changes made:

- `OutboxRepository.add` now issues one `INSERT … RETURNING` instead of a flush followed by a `created_at` refresh: one round trip fewer for every outbox row. Mean transaction time went from 10.29 ms to 9.84 ms in the same setup (about 4%, measured under background load).
- Detector and graph projector run 3 replicas in the benchmark. With 2, Kafka's range assignor gave one member both loaded normalized partitions and the other the empty one, so the second replica did nothing (observed in `kafka-consumer-groups --describe`).

Not changed, with reasons:

- **Raw partition key.** `{source_id}:{service_hint}` puts 70% of the demo traffic on one raw partition, so normalizer replicas beyond one add little: the busiest partition's single member at ~119 records/s caps the replay at about 170 records/s. Keying by `{source_id}:{source_event_id}` would spread records evenly, but it would also spread one service's records across partitions that progress at different speeds. In a replay the detector closes windows on a per-service simulated clock, so records from a slower partition would arrive after their window closed, count as late, and drop out of detection. That ordering assumption needs its own review (docs/events.md requires one for any key change) before the key moves.
- **Batching records per transaction.** Per-record BEGIN/COMMIT and statement overhead dominate. Processing a contiguous run of records from one partition in one transaction, committing the last offset after the database commit, and falling back to one record at a time on failure would keep the offset and receipt rules. It changes the consumer's retry and quarantine paths, which have their own crash tests, so it is a separate task.
- **Caching catalog lookups.** The source and service selects are two of the seven statements per record. A process cache would have to honour a source being disabled, so it needs an expiry rule; deferred with batching.
- **Connection pools.** Each consumer processes one record at a time and holds one connection; pool size is not the limit.
