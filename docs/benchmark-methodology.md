# Benchmark Methodology

The benchmark endpoint is intended to provide repeatable local evidence, not a universal performance claim.

## Endpoint

```text
POST http://localhost:8082/benchmarks/request-count
```

Default behavior:

- 5 second warm-up
- 15 second measurement
- 10 events per second
- 2 reads per second
- 100 entities
- uniform entity distribution
- PostgreSQL history write enabled when `spring.datasource.url` is configured
- SQL baseline enabled when PostgreSQL is configured
- request-time PostgreSQL baseline reads available from Feature API when PostgreSQL mode is enabled

## Request Fields

| Field | Meaning |
| --- | --- |
| `warmupDuration` | ISO-8601 warm-up duration |
| `measurementDuration` | ISO-8601 measured duration |
| `eventRatePerSecond` | Target Kafka event publish rate |
| `readRatePerSecond` | Target Feature API read rate |
| `entityCardinality` | Number of synthetic service entities |
| `distribution` | `UNIFORM` or `ZIPF` |
| `zipfSkew` | Positive skew used when distribution is `ZIPF` |
| `featureNames` | Features selected by read traffic |
| `writeHistory` | Whether to insert events into PostgreSQL historical storage |
| `runSqlBaseline` | Whether to run equivalent SQL feature queries after measurement |
| `includeRawResults` | Whether to include per-operation raw measurements in the response |

## Report Format

The response contains:

- `runId`
- `startedAt` and `finishedAt`
- normalized request parameters
- one summary for warm-up and one for measurement
- event publish and feature read latency summaries
- SQL baseline query timings and sample rows
- optional raw per-operation measurements

## Request-Time Baseline

Feature API exposes request-time PostgreSQL baseline endpoints:

```text
GET  http://localhost:8080/baseline/features/{entityType}/{entityId}/{featureName}
GET  http://localhost:8080/baseline/features/{entityType}/{entityId}?featureNames=request_count_total&featureNames=entity_event_count_10m
POST http://localhost:8080/baseline/features/batch
```

The response shape matches realtime Feature API reads so benchmark clients can compare realtime Redis reads and
request-time SQL reads without response normalization. The optional `benchmarkRunId` and `benchmarkPhase` query
parameters restrict reads to one benchmark run; `benchmarkRunId` defaults the phase to `measurement` when no phase is
provided.

## Post-Run SQL Report

The historical event table stores the same generated events sent to Kafka. Baseline queries compute:

- total request count per entity
- 10-minute event count per entity
- 10-minute server error rate per entity
- 5-minute weighted average latency per entity

Benchmark comparisons should use the measurement phase only; warm-up exists to reduce cold-start noise.

The workload-generator SQL baseline report is a post-run sample report. It is useful for inspecting aggregate query
timings and sample rows, but the fair request-time serving baseline is the Feature API `/baseline/features/**` path.
