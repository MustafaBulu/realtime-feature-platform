# Architecture

Realtime Feature Platform computes entity-scoped feature values from Kafka events and materializes serving-ready values in Redis.

## Runtime Flow

1. Producers publish validated `PlatformEvent` JSON records to Kafka.
2. `stream-worker` consumes events by entity key.
3. Event validation, event-time assessment, bounded deduplication, and feature-definition matching run before aggregation.
4. The generic aggregation engine updates RocksDB-backed local state.
5. `FeatureRedisMaterializer` writes latest aliases and window-specific keys to Redis.
6. `feature-api` serves single, subset, and batch reads from Redis with freshness and definition metadata.

## Registry

Feature metadata is stored in PostgreSQL when `rfp.feature-registry.store=postgres`. The same repository interface also has in-memory support for local tests and default bootstrapping.

Workers poll active definitions and keep the last successful snapshot if the registry is temporarily unavailable.

## State

- Kafka is the ingestion log.
- RocksDB is worker-local aggregation, dedup, and checkpoint state.
- Redis is the online serving store.
- PostgreSQL stores feature definitions and benchmark historical events.

## Observability

Prometheus metrics cover worker throughput, ingestion lag, state size, restore duration, serving latency, freshness, and Redis read status. Grafana provisioning includes overview, correctness, and recovery dashboards.
