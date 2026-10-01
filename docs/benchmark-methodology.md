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
- benchmark entities use a run-specific namespace
- Kafka publish latency waits for broker acknowledgement
- raw measurement export is disabled by default and bounded when enabled
- update-to-availability probes are enabled with a small default sample

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
| `rawResultLimit` | Maximum raw measurements returned when raw export is enabled |
| `readSloMillis` | Read latency SLO used for sustained-throughput flags |
| `correctnessSampleSize` | Number of sampled realtime-vs-baseline comparisons after measurement |
| `freshnessProbeCount` | Number of update-to-availability probe events |
| `freshnessProbePollInterval` | Poll interval for freshness probes |
| `freshnessProbeTimeout` | Timeout for each freshness probe |

## Report Format

The response contains:

- `runId`
- `startedAt` and `finishedAt`
- normalized request parameters
- one summary for warm-up and one for measurement
- event publish and feature read latency summaries
- target RPS, completed RPS, and sustained-under-SLO flags
- HTTP non-2xx counts, timeout counts, and feature status counts
- sampled baseline-vs-realtime mismatch counts
- update-to-availability latency p50/p95/p99
- environment manifest data
- SQL baseline query timings and sample rows
- optional bounded raw per-operation measurements

## Entity Namespace

Benchmark-generated entities use a run-specific namespace:

```text
bench-<runId>-<phase>-entity-000000
```

Probe entities use a separate namespace:

```text
probe-<runId>-<index>
```

This keeps background workload entities separate from update-to-availability probes and makes PostgreSQL baseline reads filterable by `benchmarkRunId` and `benchmarkPhase`.

## Publish Latency

Kafka publish latency is measured from send start until the producer future completes with broker acknowledgement. It is not local enqueue latency. The workload producer config sets `acks=all`.

## Result Artifacts

Store benchmark artifacts under:

```text
benchmarks/results/<date>-<commit>/
```

Use UTC-like sortable timestamps such as `20261001-130000` and the short commit SHA. Keep at least:

- benchmark response JSON
- k6 realtime read output
- k6 baseline read output
- environment manifest
- notes on any failed or interrupted run

An example environment manifest is provided at `benchmarks/environment-manifest.example.json`.

## CPU And Memory

Use one measurement source consistently per run. The default methodology is `docker stats` sampled during the measurement phase. Prometheus container metrics are also acceptable if the exact metric names and scrape interval are recorded.

Record CPU and memory for:

- Feature API
- Stream Worker
- Workload Generator
- PostgreSQL
- Redis
- Kafka

Also record host CPU/RAM and any Docker/container limits in the environment manifest.

## k6 Read Load

Realtime read load:

```bash
k6 run benchmarks/k6/realtime-read.js \
  -e BASE_URL=http://localhost:8080 \
  -e RATE=100 \
  -e DURATION=1m \
  -e ENTITY_PREFIX=bench-<runId>-measurement \
  -e ENTITY_CARDINALITY=1000 \
  -e DISTRIBUTION=UNIFORM
```

PostgreSQL request-time baseline read load:

```bash
k6 run benchmarks/k6/baseline-read.js \
  -e BASE_URL=http://localhost:8080 \
  -e RATE=20 \
  -e DURATION=1m \
  -e ENTITY_PREFIX=bench-<runId>-measurement \
  -e ENTITY_CARDINALITY=1000 \
  -e DISTRIBUTION=ZIPF \
  -e BENCHMARK_RUN_ID=<runId>
```

Both scripts support `DISTRIBUTION=UNIFORM` or `DISTRIBUTION=ZIPF`; use `ZIPF_SKEW` for hot-key skew.

## Freshness Probe

The update-to-availability probe measures:

```text
T4 - T0
```

where:

- `T0` is the time when Kafka broker acknowledgement is received for a unique probe event.
- `T4` is the first Feature API read where the new value is visible.

Probe entities are separate from background workload entities. The benchmark response reports p50/p95/p99 for successful probes, timeout count, poll interval, and probe timeout.

The poll interval is part of the measurement error bound. For example, `PT0.1S` means the observed latency can be inflated by up to roughly one polling interval.

## Correctness Sampling

After measurement, the benchmark samples realtime Feature API reads and request-time PostgreSQL baseline reads for the same run-specific entities and feature names. The response reports sample count, matches, mismatches, unavailable reads, and a bounded list of mismatch examples.

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

Do not publish benchmark tables from dry runs, failed runs, or local estimates. README benchmark tables should only use saved result artifacts with manifests.
