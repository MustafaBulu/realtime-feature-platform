# Dedup Semantics

Deduplication is event-id based and happens before feature updates.

## Key Shape

Dedup marker keys include Kafka topic and partition namespace so independent partitions do not collide.

## Retention

Markers expire after `rfp.dedup.retention`. Expired markers are removed in bounded cleanup batches controlled by:

- `rfp.dedup.cleanup-interval`
- `rfp.dedup.cleanup-batch-size`

## Guarantees

The worker prevents duplicate feature updates while the marker is retained in RocksDB. After marker expiration, a replayed event can be processed again.

Deduplication is bounded and partition-scoped. It is not permanent global exactly-once processing.

The dedup marker is written before feature updates, but the dedup marker, aggregate state update, checkpoint metadata, Redis materialization, and Kafka offset acknowledgement are not committed as one atomic transaction. A crash between those steps can still rely on the implemented recovery and replay behavior, but the system does not claim transactional end-to-end exactly-once semantics.
