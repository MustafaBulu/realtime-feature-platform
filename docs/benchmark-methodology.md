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

## SQL Baseline

The historical event table stores the same generated events sent to Kafka. Baseline queries compute:

- total request count per entity
- 10-minute event count per entity
- 10-minute server error rate per entity
- 5-minute weighted average latency per entity

Benchmark comparisons should use the measurement phase only; warm-up exists to reduce cold-start noise.
