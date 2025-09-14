# Changelog

## Unreleased

### Added
- SlateDB backend (Beta) as an alternative state backend alongside RocksDB.
  - Feature parity with RocksDB for:
    - KV iteration (inclusive lower, exclusive upper; forward and reverse ordering)
    - Windowed store (get_windows forward/reverse, expire semantics, empty-range behavior)
    - Timestamped store (latest retrieval; same-timestamp overwrite when keep_duplicates=False; latest value when keep_duplicates=True)
    - Changelog offsets (persist/read, recovery idempotency)
    - Locking (exclusive .lock with retry/backoff)
  - Corruption handling:
    - Map driver open errors indicating corruption to SlateDBCorruptedError
    - Optional auto-recreate on open with SlateDBOptions(on_corrupted_recreate=True) with INFO logs on detect/recreate
  - Selection:
    - Application(state_backend="slatedb", slatedb_options=...)
    - StateStoreManager(default_store_type=SlateDBStore, slatedb_options=...)

### Tests
- Added parity tests for KV iteration, windowed reverse and empty-range, timestamped keep_duplicates True/False.
- Added corruption handling tests and migrated locking test into main tests tree.
- Relocated smoke tests to tests/smoke (excluded from default discovery).

### Docs
- Updated AGENTS.md and docs/architecture/slatedb-integration.md with parity scope and corruption handling.
- Updated docs/steering/state-backends.md to mark SlateDB as Beta and document selection and options.
