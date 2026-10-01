# Known Limitations

- The platform is a portfolio prototype, not production infrastructure.
- There is no claim of end-to-end exactly-once processing.
- Dedup marker writes, aggregation updates, checkpoint metadata, Redis materialization, and Kafka offset acknowledgement are not one atomic transaction.
- RocksDB state is worker-local and assumes Kafka partition ownership; multi-worker state migration and changelog-backed state movement are not implemented.
- Redis materialization can be republished deterministically from RocksDB, but Redis itself is still an external serving cache.
- Deduplication is bounded by retention; duplicates after marker expiry can be processed again.
- Latest-window reads return the latest materialized event-time window observed by the worker, not a trailing rolling "last N minutes from now" query.
- Allowed lateness uses worker wall-clock processing time, so downtime or backlog can cause older source-valid events to be rejected.
- Feature API `freshnessMillis` is data age at read time, not update-to-availability latency.
- Exact distinct count uses exact state and is not memory-efficient for very high cardinality.
- The benchmark endpoint is single-process and local-environment oriented.
- The workload-generator PostgreSQL SQL report is post-run only; request-time baseline serving is implemented separately under Feature API `/baseline/features/**`.
- No README benchmark result table is published until a real run is saved with `benchmarks/results/<date>-<commit>/` artifacts and an environment manifest.
- Feature definitions are declarative but limited to the implemented aggregator and filter DSL.
- Multi-worker behavior is covered at listener and lifecycle level; broad distributed load validation is still limited.
