# Steering: Pluggable State Backends in Quix Streams

This steering document outlines the direction for supporting multiple embedded state backends (RocksDB, SlateDB, …) under a stable, shared abstraction without breaking current users.

## Principles

- User-facing stability: keep public APIs for state usage unchanged.
- Pluggability: multiple backends implement the same base interfaces; selection is a configuration concern.
- Incremental adoption: default remains RocksDB; other backends can be opted-in per store or app-wide.
- Test parity: state tests run against all supported backends.
- Observability: consistent logging and error semantics across backends.

## Current Architecture Snapshot

- Base abstractions in `quixstreams/state/base`: `Store`, `StorePartition`, `PartitionTransaction`.
- RocksDB in `quixstreams/state/rocksdb` implements base contracts and windowed/timestamped features.
- State is registered through `StateStoreManager`, defaulting to `RocksDBStore`.
- Application passes `rocksdb_options` which currently doubles as serialization options.

## Target Architecture

- Introduce a `slatedb` backend mirroring the RocksDB structure.
- Generalize Application and StateStoreManager to accept a “default backend” and backend-specific options, keeping `rocksdb_options` for backward compatibility.
- Maintain a union type of supported store classes to pass into `register_store()`.
- Ensure windowed/timestamped stores are implemented per backend (sharing logic where possible).

## API Surface Changes (Non-Breaking)

- Application
  - Add `state_backend: Literal["rocksdb", "slatedb"] = "rocksdb"`
  - Add `slatedb_options: Optional[SlateDBOptionsType] = None`
- StateStoreManager
  - Add `slatedb_options` and allow `default_store_type=SlateDBStore`
- Keep `rocksdb_options` intact and preferred if backend is RocksDB.

## Testing Strategy

- Extend the test matrix to parameterize backend under test for all state tests.
- Add engine-specific tests for:
  - Column family handling (native vs. prefix emulation)
  - Reverse iteration bounds correctness
  - Corruption detection/recovery path
  - Open retry/locking behavior

## Rollout Plan

- Phase backend behind a feature flag (`state_backend="slatedb"`) and mark beta.
- Gather early feedback, iterate on perf and corner cases.
- Consider moving to a unified `state_options` once both backends are stable; keep `rocksdb_options` as a compatibility alias.

## Risks and Mitigations

- Binding gaps: If SlateDB lacks needed APIs, prioritize a minimal `pyo3` binding for batch and iteration.
- Performance variance: Provide tuning docs and sensible defaults; benchmark common workloads.
- CF emulation edge cases: Centralize prefix format and filtering; add robust tests.

## Status

Operational notes (Beta)
- Locking uses a JSON lock with PID and timestamp to detect stale locks; stale locks are cleaned on open.
- If SlateDB open indicates corruption and `on_corrupted_recreate=True`, the path is cleaned and open is retried once with INFO logs emitted before/after.
- If bounded iteration is not supported by the driver, an INFO log is emitted and a full scan is used with bounds enforced in Python.
- CI uses Python 3.12 for smoke testing with `uv`; tests for SlateDB are invoked in a fast subset.

- SlateDB backend: Beta
- Parity achieved: KV iteration (bounds, fwd/rev), Windowed get_windows and expire semantics (including empty range), Timestamped latest semantics (same-timestamp overwrite when keep_duplicates=False and latest when True), Changelog offsets persistence and recovery, Locking with retry/backoff.
- Selection: Application(state_backend="slatedb") or StateStoreManager(default_store_type=SlateDBStore). Pass SlateDBOptions(dumps/loads, open_max_retries, open_retry_backoff, on_corrupted_recreate) as needed.
- Smoke tests: `pytest -q tests/smoke -k slatedb` (not collected by default).

## Success Criteria

- All state tests pass for both backends.
- Documented selection and configuration with clear migration guidance.
- No regressions in user workflows; RocksDB users unaffected.

