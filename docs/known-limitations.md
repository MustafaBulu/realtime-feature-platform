# Known Limitations

- The platform is a portfolio prototype, not production infrastructure.
- There is no claim of end-to-end exactly-once processing.
- Redis materialization can be republished deterministically from RocksDB, but Redis itself is still an external serving cache.
- Deduplication is bounded by retention; duplicates after marker expiry can be processed again.
- Exact distinct count uses exact state and is not memory-efficient for very high cardinality.
- The benchmark endpoint is single-process and local-environment oriented.
- The PostgreSQL SQL baseline is for comparison and validation, not a replacement serving path.
- Feature definitions are declarative but limited to the implemented aggregator and filter DSL.
- Multi-worker behavior is covered at listener and lifecycle level; broad distributed load validation is still limited.
