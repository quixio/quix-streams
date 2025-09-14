# SlateDB State Backend — Requirements, Spec, and Integration Plan

This document proposes integrating SlateDB as a drop‑in replacement for RocksDB as the state provider in Quix Streams. It captures requirements, design constraints, the proposed API, and a phased implementation plan with validation.

## Context

Quix Streams currently ships a RocksDB-backed state store (via `rocksdict`) and an in-memory store. The RocksDB state implementation lives under `quixstreams/state/rocksdb` and implements the base abstractions in `quixstreams/state/base`:

- `Store` manages partitions and exposes transactions
- `StorePartition` wraps a single persistent DB instance
- `PartitionTransaction` batches updates/deletes then flushes atomically

Stateful features depend on capabilities provided by the underlying DB implementation:

- Multiple column families (CF) to logically separate datasets (user data, metadata, window internals, etc.)
- Ordered iteration with bounded range scans (lower/upper bounds) and reverse iteration
- Atomic batched writes for updates and deletes (RocksDB `WriteBatch`)
- Independent instances per Kafka partition
- Pluggable serialization (`dumps`/`loads`)
- Metadata persistence for changelog offsets

Windowed and timestamped stores rely on fast range scans and iteration order. Recovery relies on reading/writing a persisted changelog offset and applying idempotent upserts/deletes.

## Goals and Non-Goals

Goals
- Add SlateDB as an alternative state backend with feature parity to RocksDB from the perspective of Quix Streams users.
- Preserve existing public APIs and defaults. RocksDB remains default unless overridden.
- Make backend selectable per-store and configurable at the Application level.
- Reuse existing abstractions (`Store`, `StorePartition`, `PartitionTransaction`) so higher-level code doesn’t change.
- Ensure changelog-backed recovery works identically with SlateDB.

Non-Goals
- Rewriting the state abstraction layers or changing semantics of stateful operations.
- Changing Kafka/changelog logic or delivery guarantees.
- Removing RocksDB; users must continue to use it unchanged.

## Functional Requirements

- Operations
  - get/set/delete/exists for byte keys/values
  - Column-family style logical separation: default CF, metadata CF, and additional CFs (window/timestamped internals)
  - Bounded forward and backward iteration for ordered key ranges
  - Atomic batched writes for updates and deletes
  - Persist/read a changelog offset per partition

- Transactions
  - In-memory update cache behavior stays unchanged
  - Prepare() produces changelog messages; Flush() writes atomically to the DB (including optional changelog offset)

- Windowed/timestamped features
  - Prefix-based keyspaces and ordered range scans
  - Efficient iteration and deletion of bounded ranges

- Recovery
  - Apply changelog upserts/deletes idempotently
  - Update stored changelog offset as messages are applied or skipped

- Configuration
  - Serialization hooks (`dumps`/`loads`) identical to RocksDB usage
  - Backend-specific options for SlateDB, including open retry/backoff and fsync-like durability settings where available
  - Backward compatible Application constructor; new options do not break the existing `rocksdb_options`

## Non-Functional Requirements

- Performance: no more than 20% regression vs. RocksDB on common workloads (read/update-heavy with range scans). Provide benchmarks and tuning hints.
- Durability: worst-case loss limited to updates not yet flushed by a prepared transaction; honor fsync/durability knobs if supported by SlateDB.
- Concurrency: separate processes must fail to open a locked DB and retry with backoff; single-writer per partition model preserved.
- Observability: logs for open/retry, corruption handling, recovery progress match existing patterns.

## Capability Mapping (RocksDB → SlateDB)

Parity with RocksDB
- KV iteration parity: inclusive lower, exclusive upper; forward and reverse order identical to RocksDB (reverse bounds enforced in Python if driver doesn’t support).
- Windowed parity: forward and reverse get_windows; empty ranges (start_from == start_to) return empty; expire semantics identical.
- Timestamped parity: latest retrieval equal; same-timestamp overwrite when keep_duplicates=False; with keep_duplicates=True, latest value at the timestamp is returned as in RocksDB.
- Changelog offsets parity: stored and recovered using the same key and serialization.
- Locking parity: exclusive lock with retry/backoff, raising a backend-specific error when exhausted.

- Column Families: if SlateDB lacks native CFs, emulate via keyspace prefixes `__cf::<name>::` and maintain small internal cache for handles/names.
- Write Batch: use SlateDB’s atomic batch/transaction API if available; otherwise stage changes and write them atomically via provided API or emulate with best-effort transactional write (must be atomic to meet guarantees).
- Ordered Iteration: rely on SlateDB’s sorted key iteration with lower/upper bounds; if reverse iteration lacks bound checks, filter in Python (as already done for backward iteration edge cases in RocksDB path).
- Metadata: store `CHANGELOG_OFFSET_KEY` within the metadata CF (or metadata prefix) exactly as with RocksDB.
- Corruption Handling: detect open errors that imply corruption (error-class/strings TBD), optionally destroy/recreate if `on_corrupted_recreate=True` and recovery enabled.
- Locking: retry open when DB is locked by another process (error-class/strings TBD) honoring `open_max_retries`/`open_retry_backoff`.

## Proposed API and Configuration

New modules
- `quixstreams/state/slatedb/` mirroring the structure of `rocksdb/`:
  - `store.py` → `SlateDBStore(Store)`
  - `partition.py` → `SlateDBStorePartition(StorePartition)`
  - `transaction.py` → `SlateDBPartitionTransaction(PartitionTransaction)`
  - `options.py` → `SlateDBOptions(Protocol | dataclass)` (provides dumps/loads and backend knobs)
  - `windowed/` and `timestamped.py` equivalents, reusing the RocksDB logic but backed by SlateDB iteration/writes

Application changes (backward-compatible)
- Add `state_backend: Literal["rocksdb", "slatedb"] = "rocksdb"`
- Add `slatedb_options: Optional[SlateDBOptionsType] = None`
- Default remains RocksDB; if `state_backend == "slatedb"`, pass `slatedb_options` to stores.

StateStoreManager changes (backward-compatible)
- New constructor args: `slatedb_options: Optional[SlateDBOptionsType] = None`, `default_store_type: StoreTypes = RocksDBStore` remains; allow `SlateDBStore`.
- `register_store()` accepts `store_type=SlateDBStore` and wires correct options.

Serialization
- Continue to use `dumps`/`loads` from backend options. If not provided, fall back to JSON, matching existing default behavior.

## Drop‑in Behavior Guarantees

- Public API surface for state usage (State/WindowedState) remains unchanged.
- Changelog topics and recovery semantics unchanged.
- Per-partition isolation and store naming layout unchanged (on disk path differs only by engine directory name).
- Existing docs and tutorials continue to work; add a new “State Backends” page with SlateDB instructions.

## Error Handling and Edge Cases

Corruption handling
- The SlateDB driver maps open errors that contain corruption-indicative phrases (e.g., "corrupt", "invalid manifest", "invalid sst") to SlateDBCorruptedError.
- If SlateDBOptions.on_corrupted_recreate is True, the partition removes the DB path and retries open once, logging INFO before and after.
- If recreate is disabled or the error is unrelated, the lock is released and the original error is raised.

- Invalid/Corrupted DB: raise `SlateDBCorruptedError` (new) with messages mirroring RocksDB behavior; honor `on_corrupted_recreate`.
- Lock contention: detect and retry; escalate after configured attempts.
- Reverse iteration bounds: enforce lower bound manually if SlateDB iterator lacks reverse bound enforcement.
- CF emulation: CF names sanitized and isolated via prefixes; ensure no key collisions.

## Migration Plan

Phase 1 — Scaffolding and Feature Parity
- Implement `slatedb` package mirroring `rocksdb` structure.
- Implement CF emulation and batch write semantics.
- Wire `StateStoreManager` to support `SlateDBStore` via `store_type` and add Application flags to select default backend.
- Add documentation and examples.

Phase 2 — Tests and Compatibility
- Parameterize state tests to run against both RocksDB and SlateDB.
- Add targeted tests for CF emulation, reverse iteration bounds, and corruption handling.

Phase 3 — Benchmarks and Tuning
- Add micro-benchmarks for get/set/delete/exists and range scans.
- Document performance tuning options for SlateDB.

Phase 4 — Adoption
- Mark SlateDB backend as beta; gather feedback and stabilize.

## Acceptance Criteria

- All state-related tests pass against both backends (unit + windowed/timestamped).
- Recovery and changelog consistency verified by existing recovery tests.
- No public API breaking changes; RocksDB remains default.
- Benchmarks within 20% of RocksDB for common patterns or documented with mitigation.

## Open Questions/Risks

- Python bindings maturity: Confirm SlateDB Python API coverage (batch, iteration bounds, durability). If bindings are missing, plan for a `pyo3`/`maturin` crate.
- CF support: If native CFs exist, use them; else, prefix emulation is required and must be vetted for iteration/path correctness.
- Durability semantics: Ensure fsync/ WAL configuration equivalents exist; otherwise, document differences.
- On-disk size and compaction: Assess compaction behavior relative to range-delete heavy workloads (window deletion).

## Work Breakdown (summary)

1) Add SlateDB backend package and options
2) Integrate with StateStoreManager and Application
3) Implement windowed/timestamped variants
4) Parameterize tests and add backend-specific tests
5) Docs, examples, and migration notes
6) Benchmarks and tuning guidance

