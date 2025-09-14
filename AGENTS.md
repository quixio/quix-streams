# AGENTS: Development Guide for Coding Agents in quix-streams

This document provides the rules, workflow, and conventions for autonomous coding agents contributing to this repository. Follow these guidelines strictly to keep changes safe, reviewable, and aligned with the project architecture.

Principles (must)
- Plan, then execute: write the intended changes and tests first (TDD), then implement the smallest slice to turn tests green.
- Functional requirements (FRs) over non-functionals (NFRs): prioritize correctness and feature parity; defer performance work unless explicitly requested.
- Public API stability: do not break existing user-facing APIs. Prefer opt-in additions and backward-compatible changes.
- No cross-backend shims: implement backend-specific behavior by extending existing base interfaces, not by introducing a new abstraction layer that spans backends.
- Consistent naming: use clear and specific names. For SlateDB, use SlateDBStore, SlateDBStorePartition, SlateDBPartitionTransaction, SlateDBOptions, WindowedSlateDBStore, TimestampedSlateDBStore, etc.
- Small, atomic diffs: keep PRs focused and tests co-located. If the change touches multiple subsystems, split into sequential PRs.
- Safety: never add code that puts secrets into logs; never suggest or execute destructive commands without explicit user approval.

Repository architecture (high-level)
- quixstreams/state/base: Core state interfaces (Store, StorePartition, PartitionTransaction) and transaction cache logic.
- quixstreams/state/rocksdb: Existing production backend (rocksdict-based) including windowed and timestamped logic.
- quixstreams/state/slatedb: New backend integration.
  - driver.py: runtime handle for the SlateDB Python package (slatedb). Includes a fallback in-memory driver for smoke tests.
  - options.py: serialization and backend options (JSON by default).
  - store.py / partition.py / transaction.py: core SlateDB backend classes.
  - windowed/ and timestamped.py: SlateDB variants mirroring RocksDB behavior.
  - exceptions.py: backend-specific exceptions (e.g., SlateDBLockError).
- quixstreams/state/manager.py: Wires stores, recovery, and backend selection for the Application.
- tests/test_quixstreams/test_state: fast, dependency-light backend tests (unit and parity) used for TDD.
- tests/smoke: quick end-to-end checks for both backends; not collected by default during full runs.

SlateDB backend rules
- Parity with RocksDB: The SlateDB backend mirrors RocksDB semantics for KV, windowed, and timestamped stores. This includes:
  - KV iteration: inclusive lower and exclusive upper bounds; identical order in forward and reverse scans.
  - Windowed store: get_windows forward/reverse iteration and expire semantics match RocksDB; empty range [start_from == start_to) yields no windows.
  - Timestamped store: latest retrieval matches RocksDB; when keep_duplicates=False, a set at the same timestamp overwrites; when keep_duplicates=True, latest at the timestamp is returned, matching RocksDB behavior.
  - Changelog offsets: persisted under __meta__::changelog_offset and validated as in RocksDB.
  - Locking: exclusive .lock file with retry/backoff; errors raise SlateDBLockError.
- CF emulation: emulate column families using namespaced keys: __cf::<name>::<user_key>. Keep this logic private to the SlateDB backend.
- Changelog offsets: store as bytes via int_to_bytes/int_from_bytes under __meta__::changelog_offset.
- Iteration: ensure inclusive lower and exclusive upper bounds; reverse iteration must respect lower bound even if driver doesn’t enforce it natively.
- Locking: acquire an exclusive lock file (<db_path>.lock) on open; use retry/backoff from SlateDBOptions; release on close; raise SlateDBLockError on failure.
- Driver: prefer RealSlateDBDriver (backed by the slatedb PyPI package). The in-memory driver exists only for smoke tests.
- Corruption handling: on open, map known driver errors to SlateDBCorruptedError. If SlateDBOptions.on_corrupted_recreate=True, remove the DB path and reopen once; log INFO before and after recreate.
- CF emulation: emulate column families using namespaced keys: __cf::<name>::<user_key>. Keep this logic private to the SlateDB backend.
- Changelog offsets: store as bytes via int_to_bytes/int_from_bytes under __meta__::changelog_offset.
- Iteration: ensure inclusive lower and exclusive upper bounds; reverse iteration must respect lower bound even if driver doesn’t enforce it natively.
- Locking: acquire an exclusive lock file (<db_path>.lock) on open; use retry/backoff from SlateDBOptions; release on close; raise SlateDBLockError on failure.
- Driver: prefer RealSlateDBDriver (backed by the slatedb PyPI package). The in-memory driver exists only for smoke tests.

TDD workflow (using uv)
1) Write (or extend) tests first under tests/test_quixstreams/test_state (for unit/parity) or tests/smoke (for quick end-to-end checks).
2) Implement the smallest code change to satisfy the tests.
3) Sync environment with uv (no pip):
   - uv venv .venv
   - uv sync --extra slatedb (if SlateDB tests are included)
4) Run only the relevant tests (fast inner loop) inside the venv via uv run:
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_kv.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_cf_and_recovery.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_iteration.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_windowed_basic.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_windowed_ops.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_windowed_expire.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_timestamped_basic.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_parity_windowed_timestamped.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_backend_param_smoke.py
   - uv run pytest -q tests/test_quixstreams/test_state/test_slatedb_locking.py
5) Refactor after green; keep diffs small.

Backends and dependencies
- RocksDB backend: requires rocksdict (already listed in requirements.txt).
- SlateDB backend: requires slatedb (added to requirements.txt) and available as optional dependency: pip install quixstreams[slatedb].
- When writing tests that should run in minimal CI: avoid testcontainers and external services; keep tests under devtests/ and rely on in-memory or local backing stores.

StateStoreManager wiring
- Default backend remains RocksDB unless explicitly set to SlateDB via Application state_backend or by constructing StateStoreManager with default_store_type=SlateDBStore.
- When SlateDB is the default, windowed and timestamped stores must be WindowedSlateDBStore and TimestampedSlateDBStore respectively.

Coding conventions for agents
- Do not edit files via shell redirections; always use the repository’s code-edit mechanism. Keep patches minimal and scoped to files/functions under change.
- Tests should be deterministic and fast. Parametrize when appropriate; prefer small smoke tests to large end-to-end flows.
- Use JSON serialization by default (options.dumps/options.loads) unless explicitly overridden.
- Use type hints and keep public docstrings concise and accurate. Avoid leaking internals into public docs unless intentional.

Common pitfalls
- Reverse iteration lower bound: validate and filter keys explicitly.
- Changelog offsets: never regress offset checks during flush; respect existing semantics.
- Windowed ranges: enforce [start_from_ms, start_to_ms) semantics unless documented otherwise. For expire_windows minimal path, include windows where start == max_start_time.
- Timestamped latest lookup: ensure merge order between cache and store when picking the latest entry <= timestamp.

How to add a new backend feature
- Write a dev test capturing the behavior and constraints.
- Implement the feature in SlateDB backend, reusing patterns from RocksDB where possible, and respecting CF emulation and iteration rules.
- Add parity smoke tests (if applicable) comparing RocksDB and SlateDB outputs at clear checkpoints.

CI guidance (high-level)
- Smoke job should run:
  - tests/smoke (fast set)
  - optionally a targeted SlateDB/RocksDB parity subset under tests/test_quixstreams/test_state
- Heavy tests (requiring containers) should remain in the main test suite and run in full CI as configured by maintainers.

UV workflows and best practices
- Install uv (Linux/macOS):
  - curl -LsSf https://astral.sh/uv/install.sh | sh
  - Ensure ~/.local/bin (or installer output) is on PATH
- Create and use a project venv (no global installs):
  - uv venv .venv
  - Prefer uv run <cmd> to avoid manual activation: uv run pytest -q devtests
- Install dependencies using uv-native commands only (no pip/uv pip):
  - Primary: uv sync (reads pyproject.toml and uv.lock if present)
  - With extras (e.g., SlateDB): uv sync --extra slatedb
  - If test/dev groups are defined: uv sync --group test --group dev
  - Note: If the project still uses requirements.txt for some deps, prefer migrating them to pyproject optional-dependencies/groups. Do not use pip or uv pip unless explicitly authorized.
- Lockfile and reproducible installs:
  - Generate/refresh lock intentionally: uv lock
  - Install from lock: uv sync
  - Agents must not rewrite locks unless requested (e.g., dependency upgrade cycle)
- Running linters/formatters with uvx (uv tool runner):
  - uvx ruff check --fix .
  - uvx black .
  - uvx pre-commit run -a
- CI caching:
  - Cache ~/.cache/uv (default UV cache) between jobs for speed
- CI example (GitHub Actions):
  ```yaml
  jobs:
    slate-smoke:
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v4
        - name: Install uv
          run: |
            curl -LsSf https://astral.sh/uv/install.sh | sh
            echo "$HOME/.local/bin" >> $GITHUB_PATH
        - name: Cache uv
          uses: actions/cache@v4
          with:
            path: ~/.cache/uv
            key: uv-${{ runner.os }}-${{ hashFiles('pyproject.toml', 'uv.lock') }}
        - name: Sync env with extras
          run: |
            uv venv .venv
            uv sync --extra slatedb
        - name: Smoke tests (backend param)
          run: |
            uv run pytest -q tests/smoke
  ```
- Best practices:
  - Use uv run so commands always execute inside the project venv
  - Avoid global installs and --user
  - Keep locks up-to-date only during planned upgrade cycles
  - Prefer small smoke suites for SlateDB in CI; run full suite in scheduled or main-branch workflows

Security & safety
- Never print secrets or tokens.
- Do not add network calls in tests that rely on external availability.
- Keep failure modes clear: raise backend-specific exceptions with helpful messages.

Contact & ownership
- Primary maintainers: @quixstreams team (see repository README for contact channels).
- For new features: open an issue with a short RFC (scope, tests, acceptance criteria) before large changes.
