# Architecture — State TTL legacy-store backfill

**Shortcut:** sc-73191
**Branch:** `feature/sc-73191/udpate-state-behavior-of-dedup-feature-to`
**Spec:** `dev-planning/state-ttl-legacy-backfill/spec.md`
**Implementation notes / spike:** `dev-planning/state-ttl-legacy-backfill/implementation-notes.md`

## What it does

Adds an opt-in `RocksDBOptions(legacy_records_ttl: Optional[timedelta] = None)`.
When set, enabling per-write TTL on a **populated legacy store** (one that
already holds un-stamped records) no longer raises
`IncompatibleStateStoreError`. Instead, on the first `state.set(..., ttl=...)`
write, the partition re-stamps every pre-existing record with a uniform expiry
of `high_water + legacy_records_ttl` (event-time at the enable moment),
populates the secondary `__ttl_index__`, writes the flip metadata, and flips
into TTL mode — all in place, with no state deletion. New records continue to
get their true per-write event-time expiry. With the default `None`, behavior is
byte-for-byte identical to the prior reject-on-populated-store behavior.

## Why this architecture

- **Additive, single source of truth.** The feature is one new config field,
  one new partition method (`backfill_legacy_records`), and one new branch in
  the existing flush-time flip/reject decision (`_maybe_flip_or_reject`). It
  reuses the existing value/index codecs (`encode_ttl_value`,
  `encode_index_key`), the existing flip metadata keys, and the existing
  changelog + recovery paths. No format-version bump (the on-disk layout is the
  existing v2 layout; backfill only changes *which records* enter it).
- **Event-time reference clock (spec §8.1).** The whole TTL feature is
  event-time driven (read-time filter and sweep both compare against the
  partition high-water, not wall-clock). Backfill therefore stamps
  `high_water + legacy_records_ttl`, so legacy records expire consistently with
  new records. We never invent a wall-clock expiry — if no event-time is
  available at flip we hard-error.
- **Bulk-at-flip, single atomic batch (spec §8.2-8.4).** All re-stamped values,
  index entries, and the `__ttl_enabled__` flag are staged into the one
  transaction cache and committed by the partition's existing single-`WriteBatch`
  `write()`. RocksDB `WriteBatch` is atomic, so the store is either fully
  backfilled-and-flipped or untouched. Because `__ttl_enabled__` is written last
  (in the same batch), a crash before commit leaves the store legacy and the
  backfill re-runs cleanly — no double-stamping, exactly-once backfill.
- **Changelog via option (a) (spec §8.5).** Re-stamped pre-existing keys are
  staged into the **transaction update cache**, so the existing changelog
  producer emits them naturally and cold-restore recovery rebuilds from the same
  bytes with zero recovery-side code change. The `__ttl_index__` CF is
  local-only and is rebuilt during recovery, not produced to the changelog.
  Spike confirmed (a) is viable: a 500k-key store re-stamps in ~0.4 s with a
  ~27 MB WriteBatch; memory scales linearly and is dominated by the cache, which
  is acceptable for the dedup workload. Chunking was deliberately rejected
  because it would break the single-atomic-batch idempotency guarantee.

## Data flow

```
state.set(k, v, ttl=...) on an unflipped partition
  └─ RocksDBPartitionTransaction.set / set_bytes        (transaction.py)
       ├─ stages raw value in the update cache (legacy layout)
       ├─ records pending stamp in self._pending_stamps
       └─ advance_high_water(record_timestamp)

tx.prepare()                                            (transaction.py)
  └─ _maybe_flip_or_reject()                            <-- the decision
       1. no TTL writes            -> return (legacy, unchanged)
       2. already flipped          -> return (inline-stamped path)
       3. populated + opt-in unset -> reject_ttl_on_populated_store() (raise)
       4. empty                    -> flip (empty-store fast path)
       5. populated + opt-in set   -> BACKFILL:
            expires = _compute_legacy_expiry(legacy_records_ttl)
                    = high_water + _ttl_to_ms(legacy_records_ttl)
            backfilled_keys = partition.backfill_legacy_records(expires, cache)
                 └─ iterate default CF on disk (user data only; spike Q3)
                    for each key NOT already staged this batch:
                       cache.set(key, encode_ttl_value(expires, value))   [default CF]
                       cache.set(encode_index_key(expires, key), b"")     [__ttl_index__]
            _restamp_default_cf_cache_for_flip(skip_keys=backfilled_keys)
                 └─ stamp the genuine in-batch writes with their own stamps
            _write_flip_metadata_to_cache()   (__ttl_enabled__, format version)
            partition.uses_ttl_stamps = True
  └─ super().prepare()  -> produces every non-local-only cache entry to changelog
                           (re-stamped legacy keys + new keys reach the changelog)

tx.flush()                                              (base transaction.py)
  └─ partition.write(cache, offset)                     (partition.py)
       └─ one WriteBatch: all cache entries + high-water + sweep + offset
       └─ self._write(batch)   <-- single atomic on-disk commit (flag included)
```

Recovery (cold restore) is unchanged: the existing flag-discovery
(`partition.py:167-183`) flips a recovering partition into TTL mode on the first
stamped default-CF replay, and `_normalize_replay_value` round-trips stamped
values, so the backfilled stamps replay verbatim.

## File inventory

Created:

- `docs/architecture-state-ttl-legacy-backfill.md` — this document.
- `tests/test_quixstreams/test_state/test_rocksdb/test_legacy_backfill.py` —
  unit tests for spec §11 cases 1-9 (validation, backfill stamps, reject
  message, empty store, idempotency, event-time clock, windowed opt-out,
  recovery wiring, crash-before-flag re-run).
- `dev-planning/state-ttl-legacy-backfill/implementation-notes.md` — spike
  findings (Q1-Q3) and chosen approach.
- `dev-planning/state-ttl-legacy-backfill/open-points.md` — OP-1 (recovery
  high-water vs uniform expiry; see Caveats).

Modified:

- `quixstreams/state/rocksdb/options.py` — `legacy_records_ttl` field +
  docstring; `__post_init__` validation (strictly positive or `ValueError`).
- `quixstreams/state/rocksdb/types.py` — `legacy_records_ttl` on the
  `RocksDBOptionsType` protocol.
- `quixstreams/state/rocksdb/partition.py` — thread the option into `__init__`
  (`self._legacy_records_ttl`), add the `legacy_records_ttl` property, add
  `backfill_legacy_records(expires_at_ms, cache)`, and rewrite
  `reject_ttl_on_populated_store` (operator-callable message;
  `operator_action="set_legacy_records_ttl"`). **OP-1 recovery-clock fix
  (spec-recovery-wallclock.md):** `recover_from_changelog_message` now judges
  expiry against a wallclock captured once per recovery session
  (`self._recovery_now_ms`, lazily on the first stamped default-CF replay via
  the `_now_ms()` test seam) instead of a stamp-ratcheted high-water; the old
  `advance_high_water(stamp)` recovery ratchet is removed and the post-recovery
  `high_water_ms` is seeded to that captured wallclock.
- `quixstreams/state/rocksdb/transaction.py` — 4th (backfill) branch in
  `_maybe_flip_or_reject`; new `_compute_legacy_expiry`; `skip_keys` parameter
  on `_restamp_default_cf_cache_for_flip` to prevent double-stamping backfilled
  keys; top-level import of `IncompatibleStateStoreError`.

## Integration with neighboring features

- **Per-write TTL (v3.24, the base feature).** Backfill is strictly a new branch
  in that feature's flip/reject decision. It shares the codecs, metadata keys,
  index CF, high-water, read-time filter, and sweep unchanged. No-TTL and
  already-flipped workloads are untouched.
- **Windowed / timestamped stores.** These set `uses_ttl_stamps = False` at the
  class level, so `set()`/`set_bytes` take the always-legacy path and
  `_maybe_flip_or_reject` is never reached with TTL writes. `legacy_records_ttl`
  is stored but inert for them (test case 7).
- **Application API.** `app.py` already forwards `rocksdb_options`; no
  Application-level change is needed. Operators set
  `rocksdb_options=RocksDBOptions(legacy_records_ttl=timedelta(...))`.

## Caveats / things to sanity-check

- **OP-1 (recovery vs uniform expiry) — RESOLVED (wallclock-at-recovery).** The
  old recovery filter advanced its high-water by each entry's expiry stamp and
  dropped entries with `stamp <= high_water`, so N records sharing ONE backfill
  expiry collapsed to ~1 survivor on cold restore. The recovery path now judges
  expiry against the **current wallclock** captured once per recovery session
  (`drop iff stamp != SENTINEL_NEVER and stamp <= wallclock_now`), and no longer
  ratchets a stamp-derived clock. A backfilled store rebuilt within its TTL
  window now retains all N records; one rebuilt after the window drops the
  genuinely-expired ones. `SENTINEL_NEVER` entries always survive. The live
  read-time path (`transaction.py`) is unchanged and stays event-time.
  **Accepted trade-off:** expiry-on-rebuild is now tied to *when* the rebuild
  runs (wallclock), not the data's event-time — correct for live/near-real-time
  streams, but a store rebuilt long after its data was written drops entries
  whose expiry is in the wallclock past even under historical reprocessing. See
  `spec-recovery-wallclock.md` §5 for the full trade-off and `open-points.md`
  (OP-1, now resolved). **NOTE:** the equivalent recovery filter in
  `quixstreams/state/memory/partition.py:recover_from_changelog_message` still
  uses the old stamp-ratchet logic; the fix was scoped to the RocksDB partition
  only (per spec), so the in-memory store retains the OP-1 collapse — flag for a
  follow-up if the memory backend is used with TTL + changelog recovery.
- **Large stores.** Backfill loads the whole default CF into the transaction
  cache (option a) before flushing. Fine for the dedup workload (short keys,
  tiny values). For pathological huge-value stores this is the memory knob to
  revisit (see implementation-notes §8.3 Q1) — out of scope for Phase 1.
