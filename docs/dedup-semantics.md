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
