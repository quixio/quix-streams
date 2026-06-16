# Memory backend — recovery parity with RocksDB (header detection + Rule 4)

**Status:** Draft
**Project:** quix-streams
**Created:** 2026-06-16
**Planned with:** Buddy
**Branch (implements):** `feature/sc-73191/udpate-state-behavior-of-dedup-feature-to`
**Companion of:** `dev-planning/state-ttl-legacy-backfill/spec.md` (§8.7 header signal),
`dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md` (Rule 4)
**Resolves:** OP-2 in `dev-planning/state-ttl-legacy-backfill/open-points.md`
**Sequencing note:** §8.7 (RocksDB) edits `metadata.py` + `base/transaction.py` +
`recovery.py` — those shared files must land first; this memory-backend change is
purely local to `quixstreams/state/memory/partition.py` and its tests, and
*consumes* the `ttl_stamped` kwarg those shared edits already thread.

---

## 1. Summary

Bring the in-memory state backend (`MemoryStorePartition`) to recovery parity with
the RocksDB backend for the State-TTL feature. Two changes, both mirroring decisions
already made for RocksDB:

1. **Header-based stamped-vs-legacy detection** — replace the memory backend's
   value-content flip-discovery (`_looks_like_stamped_value`) in
   `recover_from_changelog_message` with the out-of-band `__ttl_stamped__` changelog
   header (spec §8.7), exactly as RocksDB does.
2. **Rule 4 wallclock-at-rebuild filter (OP-2)** — port the stamp-ratchet removal /
   wallclock filter so a uniform-expiry backfilled store does not collapse to ~1
   survivor on cold-restore.

This is **parity, not urgency**: the memory backend is out of the Maxio/RocksDB
production path. Ludvík wants both backends fixed on this one branch.

> **Implementation state at spec time (important — read the code first).** The
> memory backend's `recover_from_changelog_message`
> (`quixstreams/state/memory/partition.py:221-290`) has **already had the Rule 4
> wallclock filter ported** (lines 262-290: lazy session-`now`, drop iff
> `stamp != SENTINEL_NEVER and stamp <= self._recovery_now_ms`, post-recovery
> high-water seeded to that `now`, `_now_ms()` test seam at line 140). The
> **stamp-ratchet is already gone.** What is **NOT** done is the header swap: the
> flip-discovery gate (lines 236-244) still calls `_looks_like_stamped_value` on
> the value bytes, and the `ttl_stamped` kwarg is accepted but **explicitly unused**
> (`_ = ttl_stamped`, lines 229-234). So the bulk of OP-2's *mechanism* is in
> place; the remaining defect is the **value-content false-flip** (the same root
> cause as OP-3 for RocksDB). The work below is therefore mostly the §8.7 header
> swap plus verification that the already-ported Rule 4 is correct and tested.

## 2. Goals

- Memory backend flip-discovery routes on the `__ttl_stamped__` changelog header,
  never on value content — eliminating the legacy-8-byte-epoch-ms false-flip.
- Memory backend cold-restore of a uniform-expiry store retains all in-window
  records (no stamp-ratchet collapse) — OP-2 fixed.
- Behavior is a faithful mirror of the RocksDB recovery path; no divergent
  semantics, no value-format change, no produce-side change.
- Deterministic, sleep-free tests via the existing `_now_ms()` seam.

## 3. Non-goals

- No produce-side change. The base `_prepare` already emits `__ttl_stamped__` for
  memory partitions (it probes `getattr(self._partition, "uses_ttl_stamps", False)`,
  spec §8.7.3 step 2; `MemoryStorePartition.uses_ttl_stamps` exists and flips). Do
  **not** touch `base/transaction.py` for memory.
- No change to Rule 3, `legacy_records_ttl`, the value codec (`8B‖value`,
  `SENTINEL_NEVER`), or the index format.
- No persistence layer for memory metadata (it has none — see §6). No on-disk
  format-version handling.
- No change to RocksDB. This spec consumes the shared `ttl_stamped` kwarg only.

## 4. User stories / scenarios

(Memory-backend analogues of the RocksDB scenarios; the memory store is used in
dev/test and when a deployment selects the in-memory backend.)

1. **Cold-restore of a stamped memory store.** A memory partition recovers from a
   changelog whose default-CF records carry `__ttl_stamped__`. Recovery flips into
   TTL mode on the first header-true record, strips the 8-byte prefix, rebuilds the
   in-RAM `__ttl_index__`, and judges survivors against wallclock-at-rebuild
   (Rule 4). A uniform-expiry store rebuilt within its TTL window keeps **all**
   records (not ~1).
2. **Legacy memory store, values are 8-byte epoch-ms.** A changelog of un-stamped
   legacy dedup values (each value an 8-byte BE epoch-ms timestamp, **no**
   `__ttl_stamped__` header) recovers. Recovery sees header-absent on every record,
   **stays legacy**, and replays every value verbatim into the default CF — the
   false-flip that the value-content heuristic caused is gone.
3. **Mixed legacy-then-stamped changelog.** Un-stamped history followed by stamped
   backfill supersessions (same keys, later offsets). Replay latches into TTL mode
   on the first header-true record; trailing stamped values overwrite the earlier
   verbatim ones; Rule 4 selects survivors. (Existing behavior; only the flip
   recognition changes.)

## 5. Proposed design

Mirror RocksDB's `recover_from_changelog_message`
(`quixstreams/state/rocksdb/partition.py:211-308`) verbatim in semantics, adapted to
the memory backend's RAM-dict structure.

**Recovery routing rule (one line):** flip-discovery keys off the `ttl_stamped`
header argument — on the first `cf_name == "default"` record with `ttl_stamped` true
(and the partition not yet flipped, and `type(self).uses_ttl_stamps` true), flip
into TTL mode and latch for the session; header absent → replay verbatim (legacy).

**Wallclock filter rule (one line, already in code):** for a stamped default-CF
replay value, drop iff `stamp != SENTINEL_NEVER and stamp <= self._recovery_now_ms`,
where `self._recovery_now_ms` is captured once (lazily, on the first stamped
default-CF replay) via `self._now_ms()` and also seeds `self._high_water_ms`.

## 6. Sub-features / work breakdown

### 6.1 Header-based flip-discovery (replace value-content heuristic) — TO DO
- **What it does:** memory recovery flips into TTL mode based on the `__ttl_stamped__`
  header (surfaced as the `ttl_stamped` kwarg), not on value-byte inspection.
- **Exact change:** in `MemoryStorePartition.recover_from_changelog_message`
  (`quixstreams/state/memory/partition.py:221-290`):
  - **Remove** the no-op `_ = ttl_stamped` and the comment block at lines 229-234
    that says the kwarg is deliberately unused.
  - **Replace** the flip gate at lines 236-244:
    ```python
    # FROM (value-content heuristic — false-positives on legacy 8B epoch-ms):
    if (
        type(self).uses_ttl_stamps
        and not self.uses_ttl_stamps
        and cf_name == "default"
        and value
        and self._looks_like_stamped_value(value)
    ):
        self.uses_ttl_stamps = True
        self._state.setdefault(TTL_INDEX_CF_NAME, {})

    # TO (header signal — mirrors RocksDB partition.py:237-252):
    if (
        type(self).uses_ttl_stamps
        and not self.uses_ttl_stamps
        and cf_name == "default"
        and ttl_stamped
    ):
        logger.info(
            "Recovery: __ttl_stamped__ header on default-CF replay; flipping "
            "in-memory partition into TTL mode for the rest of recovery."
        )
        self.uses_ttl_stamps = True
        self._state.setdefault(TTL_INDEX_CF_NAME, {})
    ```
  - Everything downstream (the `if not self.uses_ttl_stamps:` legacy verbatim
    branch at lines 246-252, the `LOCAL_ONLY_CFS` branch 254-256, the stamped
    main-CF branch + Rule 4 at 258-290) is **unchanged**.
- **`_looks_like_stamped_value` (memory, lines 356-366):** after this change it has
  **no remaining live caller in the memory backend.** Mirror the RocksDB §8.7.3
  step 4 decision: grep for other callers; if none, either delete it or leave it as
  dead-but-documented with a comment that it is no longer on the recovery path. Do
  **not** leave a live recovery caller. `_normalize_replay_value` (lines 347-354) is
  retained — it is still used by the stamped branch.
- **Touchpoints:** `quixstreams/state/memory/partition.py` only.
- **Owner:** ArchDev. **Dependencies:** §8.7 shared edits (recovery.py threads
  `ttl_stamped`) must be on the branch. **Status:** to do.

### 6.2 Rule 4 wallclock filter (OP-2) — ALREADY PORTED, verify only
- **What it does:** drops a replayed stamped entry iff
  `stamp != SENTINEL_NEVER and stamp <= wallclock_now`; `wallclock_now` captured
  once per session via `self._now_ms()`; no stamp-ratchet; post-recovery high-water
  seeded to `wallclock_now`.
- **State:** **already implemented** at `partition.py:262-290` and the `_now_ms()`
  seam at `partition.py:140-147`, `self._recovery_now_ms` field at line 112. The
  old stamp-ratchet (`recovery_now = self._high_water_ms` + `advance_high_water(
  stamp)`) is **not present** in the current code. No code change required for the
  filter itself; the work is to make it **reachable correctly** by fixing 6.1 (the
  false-flip currently routes legacy stores into this filter and drops them) and to
  add the test coverage in §8.
- **Owner:** Tester (coverage), ArchDev (no-op / confirm). **Status:** ported;
  verify under header routing.

### 6.3 Doc cross-link — TO DO
- Add a one-line pointer from this companion file in spec.md §8.7's memory note and
  in OP-3's "Memory backend" bullet (those reference a "pending OP-2 decision" — now
  resolved here). **Owner:** Buddy/DocuGuy. **Status:** OP-2 updated (see §7 of
  open-points.md edit); spec.md §8.7 note may be left as-is (RocksDB ArchDev owns
  that file concurrently — do NOT edit spec.md).

## 7. Data & interface contracts

- **Consumed kwarg:** `recover_from_changelog_message(..., ttl_stamped: bool = False)`
  — already in the memory signature (line 227) and already threaded from
  `recovery.py:199, 216-220` (`ttl_stamped = bool(headers.get(
  CHANGELOG_TTL_STAMPED_HEADER))`). No signature change.
- **Header:** `CHANGELOG_TTL_STAMPED_HEADER = "__ttl_stamped__"` in
  `quixstreams/state/metadata.py` (added by §8.7). Memory consumes it identically:
  present/true → stamped; absent → legacy. Default `False` covers pre-header
  messages (back-compat option (a), §8.7.4).
- **Value codec (unchanged):** `8B(stamp)‖value`, `SENTINEL_NEVER` for never-expires.
  `encode_ttl_value` / `decode_ttl_value` / `encode_index_key` / `decode_index_key`
  from `quixstreams/state/rocksdb/ttl_codec.py` (memory already imports these).
- **No new fields, routes, or message shapes.**

## 8. Structural adaptation (memory vs RocksDB) — RAM dicts, no CFs, no on-disk metadata

The memory backend stores all state in nested Python dicts
(`self._state: Dict[str, Dict[bytes, Any]]`), keyed by CF name. There is **no**
RocksDB `WriteBatch`, **no** column-family handles, and **no** persistent on-disk
metadata. The mirror adapts as follows (all already reflected in the current code,
called out so ArchDev does not re-introduce a RocksDB idiom):

| RocksDB | Memory equivalent (current code) |
|---|---|
| `WriteBatch` + `batch.put/delete` + `self._write(batch)` | direct dict writes: `self._state.setdefault(cf_name, {})[key] = value` / `.pop(key, None)` |
| `get_or_create_column_family(TTL_INDEX_CF_NAME)` | `self._state.setdefault(TTL_INDEX_CF_NAME, {})` |
| `__ttl_index__` on-disk CF | in-RAM dict `self._state[TTL_INDEX_CF_NAME]`, rebuilt from survivors during replay (lines 283-286) — same `encode_index_key(stamp, key) -> b""` entries |
| `_stamp_flip_metadata()` (persists `__ttl_enabled__` + format version to disk so a restart re-opens in TTL mode) | **NO equivalent and none needed.** Memory partitions are not persistent — each open starts fresh and re-recovers from the changelog, re-deriving the flip via the header. So the recovery flip is **session-local**: `self.uses_ttl_stamps = True` (in-RAM) is sufficient. Do **not** add a `_stamp_flip_metadata` mirror to memory. (The RocksDB flip metadata call at `partition.py:252` has no memory counterpart in the recovery path — the memory flip block must NOT call it.) |
| `_now_ms()` seam | identical `_now_ms()` at `partition.py:140-147` |
| `self._recovery_now_ms` | identical field at `partition.py:112` |

**Index rebuild from survivors:** because the memory `__ttl_index__` is a RAM dict
rebuilt entirely during replay, a dropped (expired) entry writes neither the main
value nor the index entry (lines 280, 283-286), so the index never references a
filtered-out key — same invariant as RocksDB. The live sweep (`_run_sweep`, lines
368-417) reads that rebuilt index; no change.

## 9. Back-compat

Same as spec §8.7 option (a): first enablement must happen on the header build;
header-absent records are treated as legacy with **no heuristic fallback** (the
`ttl_stamped` default of `False` and the removal of `_looks_like_stamped_value` from
the recovery path together guarantee no value-content inference). For a non-persistent
memory store this is even safer than RocksDB — there is no on-disk store that could
already hold header-less stamped values; every memory recovery re-reads the
changelog from scratch.

## 10. Test plan

Extend `tests/test_quixstreams/test_state/test_memory/test_recovery_wallclock.py`
(it already exists and exercises the wallclock filter). Mirror the RocksDB cases in
`test_state/test_rocksdb/` (`TestRecoveryWallclock`,
`test_first_enablement_cold_restore.py::TestColdRestoreFalseFlipRepro`). Use the
`_now_ms` lambda seam already used in the file (`recovered._now_ms = lambda: now_ms`).

1. **OP-2 regression — uniform-expiry survives rebuild (no collapse).** Recover N
   header-true stamped records all with the same expiry `E`, with `now_ms < E`.
   **Assert all N retained** in `default`, N matching `__ttl_index__` entries
   (was: ~1). *(Likely already present — confirm it routes via `ttl_stamped=True`,
   not the old heuristic.)*
2. **Uniform-expiry rebuilt after window.** Same store, `now_ms >= E`. **Assert
   none retained**, `__ttl_index__` empty.
3. **Mixed expiries, order-independent.** Header-true records with varied stamps,
   `now_ms` between them. **Assert exactly `stamp > now_ms` survive**, independent
   of replay order.
4. **`SENTINEL_NEVER` always survives.** Sentinel-stamped header-true records;
   rebuild at far-future `now_ms`. **Assert all retained** (never compared).
5. **Header routing — stamped flips, legacy does not.** (a) First header-true
   default-CF record flips `uses_ttl_stamps` to True and creates the index CF;
   (b) an all-header-absent changelog leaves `uses_ttl_stamps` False and the index
   CF absent/empty.
6. **False-flip regression (the OP-3/memory analogue — the headline new test).**
   Replay legacy values that are **8-byte BE epoch-ms timestamps** with **no**
   `__ttl_stamped__` header. **Assert** the partition stays legacy
   (`uses_ttl_stamps == False`), **all** records land verbatim in `default` (none
   dropped), and the values are byte-identical (not 8B-stripped). This is the case
   the old `_looks_like_stamped_value` got wrong; pin it.
7. **Post-recovery high-water seeding.** After a stamped recovery, assert
   `high_water_ms == now_ms`; feed a live record with event-time `< now_ms` →
   high-water stays pinned (monotonic); `> now_ms` → advances. (Mirror
   `spec-recovery-wallclock.md` §8 case 5.)
8. **Determinism within a session.** Fixed `now_ms`, replay same changelog twice,
   assert identical survivor sets + index.

**Owner:** Tester.

## 11. Risks, constraints, open questions

- **Shared-file sequencing (constraint, handled by main thread).** §8.7 must land
  the `CHANGELOG_TTL_STAMPED_HEADER` constant + `recovery.py` threading before this
  change is testable. recovery.py already threads `ttl_stamped` in the read code
  (line 199) — confirm the constant import exists in metadata.py before ArchDev
  starts. If §8.7 is still mid-flight, the memory change compiles (kwarg defaults
  `False`) but tests need the header to be produced.
- **`_looks_like_stamped_value` shared-bound coupling.** As with RocksDB §8.7.3
  step 4, do not delete `_MAX_PLAUSIBLE_STAMP_MS`-style constants if the read-side
  validator still uses them; only remove the recovery-path call. Grep first.
- **No open questions** — this is a faithful mirror of decided RocksDB behavior;
  every decision (header signal, Rule 4, back-compat (a), no flip-metadata
  persistence for memory) is inherited.

## 12. Alternatives considered

- **Keep the value-content heuristic for memory only.** Rejected — it carries the
  exact OP-3 false-flip defect; parity demands the header.
- **Persist memory flip metadata (`_stamp_flip_metadata` mirror).** Rejected —
  memory is non-persistent; there is nothing to persist to and nothing reads it on
  re-open (every open re-recovers). Adding it would be dead code.
- **Defer the memory fix to a separate branch.** Rejected by Ludvík — both backends
  fixed on this branch.

## 12a. Implementation note (ArchDev, 2026-06-16)

Implemented on `feature/sc-73191/...`, working tree (uncommitted):

- **§6.1 done.** `recover_from_changelog_message`
  (`quixstreams/state/memory/partition.py`) now routes on the `ttl_stamped`
  header kwarg, mirroring RocksDB `partition.py:237-252`: first `cf_name ==
  "default"` record with `ttl_stamped` true flips + latches `uses_ttl_stamps =
  True` (session-local, RAM only — no `_stamp_flip_metadata` mirror). The
  `_ = ttl_stamped` no-op and the value-content flip gate were removed.
- **`_looks_like_stamped_value` (memory): RETAINED, off the recovery path.**
  No live caller remains in the memory backend after the gate removal, and no
  test patches memory's copy. Kept (with a docstring noting it is dead on the
  recovery path) to stay symmetric with RocksDB, which retains its copy because
  a test patches it. Deleting it would diverge the two backends for no gain.
- **§6.2 wallclock filter: confirmed already-correct (y).** Memory lines
  `262-290` are semantically byte-identical to RocksDB `280-308`: drop iff
  `stamp != SENTINEL_NEVER and stamp <= self._recovery_now_ms`, `_recovery_now_ms`
  captured once via `_now_ms()` on the first stamped default-CF replay, seeds
  `_high_water_ms`. Index rebuilt from survivors only (sentinels excluded).
  No change made.
- **Tests** (`tests/.../test_memory/test_recovery_wallclock.py`): existing
  `TestMemoryRecoveryWallclock` cases updated to thread `ttl_stamped=True`
  (their replays are genuinely stamped). New `TestMemoryRecoveryRoutesOnHeaderOnly`:
  `test_header_absent_stamp_shaped_value_stays_legacy` (false-flip repro —
  headline), `test_header_true_flips_and_filters`,
  `test_first_header_true_latches_for_session`. All spy-assert
  `_looks_like_stamped_value` is not called.
- **Suites green:** memory + rocksdb state suites = **155 passed** (152 RocksDB
  baseline + 3 net-new memory tests; 10 total in the wallclock module).

## 13. References

- `dev-planning/state-ttl-legacy-backfill/spec.md` §8.7 (header signal), §8.6 (cold
  restore), Rule 4 summary.
- `dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md` (Rule 4 full).
- `dev-planning/state-ttl-legacy-backfill/open-points.md` OP-2 (resolved here),
  OP-3 (RocksDB analogue).
- Code: `quixstreams/state/memory/partition.py:221-290` (recover),
  `:140-147` (`_now_ms`), `:356-366` (`_looks_like_stamped_value`, to remove from
  recovery path); `quixstreams/state/rocksdb/partition.py:211-308` (mirror);
  `quixstreams/state/recovery.py:199,216-220` (`ttl_stamped` threading).
- Tests: `tests/test_quixstreams/test_state/test_memory/test_recovery_wallclock.py`.
