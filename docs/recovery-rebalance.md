# Recovery And Rebalance

The worker keeps local recovery metadata in RocksDB.

## Checkpoints

Checkpoint metadata tracks last processed and last safe Kafka offsets. RocksDB state is flushed during the processing acknowledgement lifecycle.

Checkpoint metadata lives in local RocksDB with the aggregation and dedup state. It is recovery metadata for the local worker state, not a transactional Kafka offset commit protocol.

## Startup Restore

On startup, the worker restores RocksDB state and deterministically republishes materialized feature values to Redis. Recovery readiness is exposed through health checks.

## Rebalance

Partition assignment triggers deterministic republish for the assigned partitions. Partition revoke/loss is recorded through metrics and listener tests cover assignment and revoke lifecycle behavior.

The current rebalance behavior assumes local RocksDB state and Kafka partition ownership. It does not migrate RocksDB state between workers, does not use a changelog topic to rebuild state on a different worker, and does not provide distributed state ownership coordination beyond the Kafka consumer group lifecycle.
