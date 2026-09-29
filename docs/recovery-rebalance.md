# Recovery And Rebalance

The worker keeps local recovery metadata in RocksDB.

## Checkpoints

Checkpoint metadata tracks last processed and last safe Kafka offsets. RocksDB state is flushed during the processing acknowledgement lifecycle.

## Startup Restore

On startup, the worker restores RocksDB state and deterministically republishes materialized feature values to Redis. Recovery readiness is exposed through health checks.

## Rebalance

Partition assignment triggers deterministic republish for the assigned partitions. Partition revoke/loss is recorded through metrics and listener tests cover assignment and revoke lifecycle behavior.
