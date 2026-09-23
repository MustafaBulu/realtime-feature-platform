# Realtime Feature Platform

Realtime Feature Platform is a Java 21 backend project for computing low-latency, entity-based features from streaming events.

The project is intentionally scoped as a streaming infrastructure portfolio project, not a payment, billing, banking, ledger, or fraud decision system.

## Implemented Prototype Capabilities

- Maven multi-module project structure
- Java 21 build setup
- Spring Boot application skeletons
- Shared event and feature model modules
- JUnit 5 test setup
- Testcontainers dependency setup
- Docker Compose infrastructure
- Kafka, PostgreSQL, Redis, Prometheus, and Grafana services
- Health endpoints
- Structured console logging
- GitHub Actions CI
- Event contract validation
- Worker counters for processed, invalid, and ignored events
- Tumbling 10-minute entity event-count feature
- Entity error-rate and average-latency features
- EventId-based deduplication before feature updates
- Event-time allowed-lateness policy for late event discard
- In-memory feature definition registry
- PostgreSQL-backed feature definition registry
- Feature registry persistence, versioning, and draft/active lifecycle
- Registry endpoints for definition save, activation, and deactivation
- Worker feature-definition reload with last-successful snapshot fallback
- Worker debug endpoint for active feature definitions

## Planned Platform Capabilities

The current worker computes the prototype features through declarative feature definitions and the generic aggregation engine. The next implementation stages harden window correctness, event-time behavior, serving semantics, recovery, and benchmark evidence.

- Adding standard features without adding Java processor classes
- Generic `count`, `sum`, `avg`, and exact `distinct_count` aggregators
- Tumbling and sliding windows driven by event time
- Bounded deduplication with retention and cleanup
- Late-event correction inside allowed lateness
- Batch serving, freshness metadata, and definition-version metadata
- Worker restart and Kafka rebalance recovery semantics
- PostgreSQL on-demand benchmark baseline

## Modules

| Module | Purpose |
| --- | --- |
| `event-model` | Domain-independent event records |
| `feature-model` | Feature definition and aggregation model |
| `feature-registry` | PostgreSQL feature registry repository and migration |
| `feature-api` | Online feature serving API skeleton |
| `stream-worker` | Stream processing worker skeleton |
| `workload-generator` | Synthetic event generator skeleton |

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

Feature registry endpoints:

```text
GET  http://localhost:8080/registry/definitions
POST http://localhost:8080/registry/definitions
POST http://localhost:8080/registry/definitions/{name}/versions/{version}/activate
POST http://localhost:8080/registry/definitions/{name}/versions/{version}/deactivate
```

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

Expected response value:

```json
{
  "entityType": "service",
  "entityId": "catalog-api",
  "featureName": "request_count_total",
  "value": 450
}
```

Read the latest 10-minute entity event-count feature:

```text
GET http://localhost:8080/features/service/catalog-api/entity_event_count_10m
```

Expected response value:

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

Expected response value:

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

Expected response value:

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

Input validation, duplicate detection, and too-late discard happen before feature updates. Duplicate events are identified by `eventId`. Too-late events are discarded when `eventTime` is older than the configured allowed-lateness cutoff.

The in-memory registry currently seeds metadata for `request_count_total`, `entity_event_count_10m`, `entity_error_rate_10m`, and `entity_avg_latency_ms_5m`.

## Roadmap

The platform will grow toward:

- Kafka-backed event ingestion
- Entity-key partitioning
- Event-time windowing
- Tumbling and sliding windows
- RocksDB local state
- Redis feature materialization
- Worker restart and Kafka rebalance recovery
- PostgreSQL on-demand aggregation baseline
- Reliability and benchmark evidence

Performance and reliability claims should be added only when backed by tests or benchmark output.
