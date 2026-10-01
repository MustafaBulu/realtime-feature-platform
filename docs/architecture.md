# Architecture

Realtime Feature Platform computes entity-scoped feature values from Kafka events and materializes serving-ready values in Redis.

## Runtime Flow

1. Producers publish validated `PlatformEvent` JSON records to Kafka.
2. `stream-worker` consumes events by entity key.
3. Event validation, event-time assessment, bounded deduplication, and feature-definition matching run before aggregation.
4. The generic aggregation engine updates RocksDB-backed local state.
5. `FeatureRedisMaterializer` writes latest aliases and window-specific keys to Redis.
6. `feature-api` serves single, subset, and batch reads from Redis with freshness and definition metadata.
7. `feature-api` can also serve request-time PostgreSQL baseline reads from `historical_events` under `/baseline/features/**` for benchmark comparison.
8. `workload-generator` acts as a benchmark publisher/probe helper: it publishes events, drives read traffic, samples correctness, and probes update-to-availability latency.

## Registry

Feature metadata is stored in PostgreSQL when `rfp.feature-registry.store=postgres`. The same repository interface also has in-memory support for local tests and default bootstrapping.

Workers poll active definitions and keep the last successful snapshot if the registry is temporarily unavailable.

## State

- Kafka is the ingestion log.
- RocksDB is worker-local aggregation, dedup, and checkpoint state.
- Redis is the online serving store.
- PostgreSQL stores feature definitions and benchmark historical events.

The request-time baseline path uses the Feature API PostgreSQL datasource and `JdbcTemplate`, so it goes through the same pooled datasource used by the registry when PostgreSQL mode is enabled. It does not use `DriverManager` on the read hot path.

RocksDB state is local to the worker process and is tied to Kafka partition ownership. The current implementation does not provide a changelog topic, remote state store, or automatic state migration between workers. Rebalance handling flushes local state and republishes assigned partition state from the local RocksDB view; it is not a distributed state-transfer protocol.

The dedup marker, aggregation update, checkpoint metadata, Redis materialization, and Kafka offset acknowledgement are ordered in the worker processing path, but they are not one atomic transaction across RocksDB, Redis, and Kafka.

## Observability

Prometheus metrics cover worker throughput, ingestion lag, state size, restore duration, serving latency, freshness, and Redis read status. Grafana provisioning includes overview, correctness, and recovery dashboards.
