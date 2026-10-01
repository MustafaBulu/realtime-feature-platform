# Realtime Feature Platform

Realtime Feature Platform is a Java 21 backend project for computing low-latency, entity-based features from streaming events.

The project is intentionally scoped as a streaming infrastructure portfolio project, not a payment, billing, banking, ledger, or fraud decision system.

## Problem

Online systems often need request-time features such as per-entity request volume, error rate, or latency averages. Computing those values from historical SQL rows on every read is simple, but it becomes expensive and hard to keep predictable as read rate, window count, and entity cardinality grow.

## Approach

The platform consumes validated `PlatformEvent` records from Kafka, matches them to declarative feature definitions, updates local RocksDB aggregation state, and materializes serving-ready values into Redis. Feature definitions live in an in-memory or PostgreSQL-backed registry, so the implemented aggregation types can be added through metadata instead of new domain-specific Java processors.

The design is intentionally bounded: state is local to a worker, deduplication is retention-based, and Redis is a serving cache that can be republished from RocksDB. This repository documents those boundaries explicitly instead of claiming production-grade exactly-once or distributed state migration guarantees.

## Evidence

- Golden tests cover the prototype feature behavior for `request_count_total`, `entity_event_count_10m`, `entity_error_rate_10m`, and `entity_avg_latency_ms_5m`.
- Unit tests cover the feature DSL, registry lifecycle, generic aggregators, window boundaries, sliding-window behavior, late-event correction, duplicate handling, recovery, and rebalance listener behavior.
- Docker Compose runs Kafka, PostgreSQL, Redis, Prometheus, Grafana, the Feature API, the stream worker, and the workload generator locally.
- Prometheus and Grafana expose worker throughput, ingestion lag, RocksDB state size, restore duration, API freshness, serving latency, and Redis read status.
- The benchmark publisher/probe helper can produce local workload reports with event/read rates, entity cardinality, uniform or Zipf distributions, warm-up and measurement phases, bounded raw samples, correctness sampling, update-to-availability probes, and PostgreSQL SQL report queries.

## Supported / Not Supported Guarantee Matrix

| Area | Supported | Not Supported |
| --- | --- | --- |
| Feature definition | Declarative metadata for the implemented `count`, `sum`, `avg`, `ratio`, and exact `distinct_count` aggregators and filter DSL. | Arbitrary UDFs, joins, schema-registry evolution, or a general-purpose feature store DSL. |
| Windowing | Event-time aligned tumbling windows and sliding windows based on definition `windowSize` and `slide`. | A watermark-based full event-time engine or read-time trailing rolling windows. |
| Latest window reads | Reads without `windowStart` return the latest materialized event-time window observed by the worker for that feature/entity. Historical corrections do not move this alias back to older windows. | "Last N minutes from now" semantics or automatic recomputation of the latest alias from older corrected windows. |
| Allowed lateness | Events are accepted when `eventTime >= processingTime - rfp.event-time.allowed-lateness`; accepted late events correct historical window-specific keys. | Watermark-driven lateness. Long downtime or large backlog can cause old-but-valid events to be rejected because the cutoff is wall-clock based. |
| Deduplication | Event-id deduplication scoped by Kafka topic-partition namespace while the RocksDB marker is retained. | Permanent global deduplication. Replays after marker expiry can be processed again. |
| Atomicity | The worker orders validation, lateness check, dedup marker write, aggregation, Redis materialization, checkpoint, and Kafka ack in the implemented processing path. | A single atomic transaction across dedup state, aggregate state, checkpoint metadata, Redis writes, and Kafka offsets. |
| Exactly-once | Bounded duplicate suppression, checkpoint metadata, restart restore, deterministic Redis republish, and tests for the prototype recovery path. | End-to-end exactly-once processing. |
| State ownership | RocksDB is local worker state, intended to be used with Kafka partition ownership. | Multi-worker state migration, changelog topics, or moving RocksDB state between workers automatically. |
| Serving freshness | `metadata.freshnessMillis` is data age from the materialized value's worker update timestamp to the API read time. Benchmark probes separately measure Kafka broker ack to first visible API read. | Treating `freshnessMillis` itself as update-to-availability latency. |
| Benchmarks | Local configurable workload generation, request-time PostgreSQL baseline reads, and SQL report queries for methodology development. | Published production benchmark claims. Those require real runs with environment manifests and error-rate data. |

## Modules

| Module | Purpose |
| --- | --- |
| `event-model` | Domain-independent event records |
| `feature-model` | Feature definition and aggregation model |
| `feature-registry` | PostgreSQL feature registry repository and migration |
| `feature-api` | Online feature serving API |
| `stream-worker` | Kafka stream processing worker |
| `workload-generator` | Synthetic event publisher, benchmark runner, and freshness/correctness probe helper |

## Documentation

- [Architecture](docs/architecture.md)
- [Feature DSL](docs/feature-dsl.md)
- [Event-time semantics](docs/event-time-semantics.md)
- [Dedup semantics](docs/dedup-semantics.md)
- [Recovery and rebalance](docs/recovery-rebalance.md)
- [Benchmark methodology](docs/benchmark-methodology.md)
- [Known limitations](docs/known-limitations.md)

## Requirements

- Java 21
- Docker Desktop or a compatible Docker Engine
- Maven Wrapper is included

## Build And Test

```powershell
.\mvnw.cmd -B verify
```

On Unix-like shells:

```bash
./mvnw -B verify
```

The Testcontainers E2E test is marked `disabledWithoutDocker`, so it is skipped when Docker is not available locally.

## Run Infrastructure And Apps

```powershell
docker compose up --build
```

This starts:

| Service | URL |
| --- | --- |
| Feature API | http://localhost:8080 |
| Stream Worker | http://localhost:8081 |
| Workload Generator | http://localhost:8082 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Kafka | localhost:9092 |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

Grafana defaults:

- Username: `admin`
- Password: `admin`

## Health Checks

Application health endpoints:

```text
GET http://localhost:8080/internal/health
GET http://localhost:8081/internal/health
GET http://localhost:8082/internal/health
```

Spring Actuator health endpoints:

```text
GET http://localhost:8080/actuator/health
GET http://localhost:8081/actuator/health
GET http://localhost:8082/actuator/health
```

Prometheus metrics:

```text
GET http://localhost:8080/actuator/prometheus
GET http://localhost:8081/actuator/prometheus
GET http://localhost:8082/actuator/prometheus
```

Worker feature-definition debug endpoint:

```text
GET http://localhost:8081/internal/feature-definitions
```

The endpoint exposes the active definitions loaded by the worker. When `rfp.feature-registry.store=postgres`, the worker polls the PostgreSQL registry and keeps using the last successful definition snapshot if a later reload fails.

Windowed features are materialized twice:

- The feature key without `windowStart` stores the latest materialized event-time window observed by the worker.
- The window-specific key stores the value for the requested `windowStart` and gets a Redis TTL.
- The latest alias is not a trailing rolling "last N minutes from now" query.

Allowed-lateness behavior:

- The allowed-lateness cutoff is based on worker wall-clock processing time: `eventTime < processingTime - rfp.event-time.allowed-lateness` is rejected.
- Late events inside the allowed-lateness window update their historical window-specific bucket.
- Historical corrections do not move the latest-window alias back to an older window.
- Long downtime or a large Kafka backlog can make older events miss the wall-clock cutoff even when they are valid in the source system.

Deduplication behavior:

- Event IDs are remembered in RocksDB before feature updates.
- Dedup marker keys include the Kafka topic-partition namespace.
- Markers expire after `rfp.dedup.retention`.
- Expired markers are cleaned in bounded batches controlled by `rfp.dedup.cleanup-interval` and `rfp.dedup.cleanup-batch-size`.
- The dedup marker, aggregate update, checkpoint metadata, Redis write, and Kafka offset acknowledgement are not one atomic transaction.

Feature registry endpoints:

```text
GET  http://localhost:8080/registry/definitions
POST http://localhost:8080/registry/definitions
POST http://localhost:8080/registry/definitions/{name}/versions/{version}/activate
POST http://localhost:8080/registry/definitions/{name}/versions/{version}/deactivate
```

Feature serving endpoints:

```text
GET  http://localhost:8080/features/{entityType}/{entityId}/{featureName}
GET  http://localhost:8080/features/{entityType}/{entityId}?featureNames=request_count_total&featureNames=entity_event_count_10m
POST http://localhost:8080/features/batch
```

Serving responses include `metadata.status`, `metadata.updatedAt`, `metadata.freshnessMillis`,
`metadata.definitionVersion`, and `metadata.ttlSeconds`. Status values distinguish `PRESENT`,
`MISSING`, `STALE`, and `UNAVAILABLE`; a materialized zero value remains `PRESENT`.
`freshnessMillis` is the age of the materialized data at read time, computed from the worker update timestamp.
It is not a Kafka publish-to-API-visibility latency measurement.

Request-time PostgreSQL baseline endpoints:

```text
GET  http://localhost:8080/baseline/features/{entityType}/{entityId}/{featureName}
GET  http://localhost:8080/baseline/features/{entityType}/{entityId}?featureNames=request_count_total&featureNames=entity_event_count_10m
POST http://localhost:8080/baseline/features/batch
```

The baseline endpoints return the same response shape as realtime feature reads, but compute the value on each
request from `historical_events` through `JdbcTemplate` and the pooled Feature API PostgreSQL datasource.
Optional `benchmarkRunId` and `benchmarkPhase` query parameters restrict reads to a benchmark run; when
`benchmarkRunId` is provided without `benchmarkPhase`, the baseline defaults to the `measurement` phase.

Benchmark endpoint:

```text
POST http://localhost:8082/benchmarks/request-count
```

Example benchmark request:

```json
{
  "warmupDuration": "PT5S",
  "measurementDuration": "PT30S",
  "eventRatePerSecond": 100,
  "readRatePerSecond": 20,
  "entityCardinality": 1000,
  "distribution": "ZIPF",
  "zipfSkew": 1.1,
  "correctnessSampleSize": 20,
  "freshnessProbeCount": 5,
  "freshnessProbePollInterval": "PT0.1S",
  "freshnessProbeTimeout": "PT10S",
  "rawResultLimit": 1000,
  "includeRawResults": false
}
```

The benchmark response includes warm-up and measurement summaries, publish/read latency
percentiles, target and completed throughput, SLO flags, HTTP error and timeout counts,
feature-status counts, bounded optional raw measurements, sampled baseline-vs-realtime
correctness results, update-to-availability probe percentiles, an environment manifest,
and PostgreSQL SQL baseline query results when `spring.datasource.url` is configured for
the workload generator.
The workload-generator SQL baseline values are post-run report queries. Request-time baseline serving is exposed by
Feature API under `/baseline/features/**`.

Published benchmark tables are intentionally absent until a real run is saved under
`benchmarks/results/<date>-<commit>/` with its environment manifest.

## Happy Path

Produce a small request-count workload:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8082/workloads/request-count-total `
  -ContentType application/json `
  -Body '{"entityType":"service","entityId":"catalog-api","samples":[{"count":100,"statusCode":200,"latencyMs":80},{"count":300,"statusCode":200,"latencyMs":120},{"count":50,"statusCode":500,"latencyMs":300}]}'
```

Read the materialized feature:

```text
GET http://localhost:8080/features/service/catalog-api/request_count_total
```

Example response shape:

```json
{
  "entityType": "service",
  "entityId": "catalog-api",
  "featureName": "request_count_total",
  "windowStart": null,
  "value": 450,
  "metadata": {
    "status": "PRESENT",
    "updatedAt": "2026-08-28T12:10:14.200Z",
    "freshnessMillis": 250,
    "definitionVersion": 1,
    "ttlSeconds": null
  }
}
```

Read the latest 10-minute entity event-count feature:

```text
GET http://localhost:8080/features/service/catalog-api/entity_event_count_10m
```

Expected `value` field:

```json
{
  "entityType": "service",
  "entityId": "catalog-api",
  "featureName": "entity_event_count_10m",
  "value": 3
}
```

Window-specific reads are also supported with a `windowStart` query parameter:

```text
GET http://localhost:8080/features/service/catalog-api/entity_event_count_10m?windowStart=2026-08-28T12:10:00Z
```

Read the latest 10-minute entity error-rate feature:

```text
GET http://localhost:8080/features/service/catalog-api/entity_error_rate_10m
```

Expected `value` field:

```json
{
  "entityType": "service",
  "entityId": "catalog-api",
  "featureName": "entity_error_rate_10m",
  "value": 0.1111111111111111
}
```

Read the latest 5-minute average-latency feature:

```text
GET http://localhost:8080/features/service/catalog-api/entity_avg_latency_ms_5m
```

Expected `value` field:

```json
{
  "entityType": "service",
  "entityId": "catalog-api",
  "featureName": "entity_avg_latency_ms_5m",
  "value": 131.11111111111111
}
```

## Prototype Golden Feature Behavior

The prototype currently materializes these features for the `request.completed` event stream:

| Feature | Path | Behavior |
| --- | --- | --- |
| `request_count_total` | Generic engine | Adds the payload `count` field into an unwindowed total per entity. |
| `entity_event_count_10m` | Generic engine | Counts accepted events per entity in a 10-minute tumbling event-time window and also publishes the latest window value. |
| `entity_error_rate_10m` | Generic derived ratio | Computes `server_error_count / request_count` over payload `count` in a 10-minute tumbling event-time window. Server errors are `statusCode >= 500`. |
| `entity_avg_latency_ms_5m` | Generic engine | Computes weighted average latency as `sum(latencyMs * count) / sum(count)` in a 5-minute tumbling event-time window. |

Input validation, duplicate detection, and too-late discard happen before feature updates. Duplicate events are identified by `eventId`. Too-late events are discarded when `eventTime` is older than the configured allowed-lateness cutoff. Out-of-order events inside the allowed-lateness window are accepted as corrections.

The in-memory registry currently seeds metadata for `request_count_total`, `entity_event_count_10m`, `entity_error_rate_10m`, and `entity_avg_latency_ms_5m`.

Sliding windows use the feature definition `slide` as the bucket granularity. Each accepted event updates every aligned sliding window that contains its event time; the latest alias is updated only from the newest open sliding window.

## Roadmap

Next work is intentionally focused on final presentation, not on widening the distributed-systems scope:

- Add a short demo script.
- Move long endpoint walkthroughs lower or into docs.
- Add a top-level architecture flow and benchmark summary only after a real result artifact exists.

Performance and reliability claims should be added only when backed by tests or benchmark output.

## Correctness Claim Boundary

This project does not claim end-to-end exactly-once processing. It implements bounded
event-id deduplication, checkpoint metadata, restart restore, and deterministic Redis
republish behavior for the prototype scope. Worker state is local RocksDB state; multi-worker
state migration and changelog-backed state movement are not implemented.
