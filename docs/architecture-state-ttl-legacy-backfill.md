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

TTL is **strictly per-write**: only `state.set(..., ttl=...)` sets an expiry. A
write with no explicit `ttl=` is always never-expires (`SENTINEL_NEVER`),
regardless of flip state or `legacy_records_ttl`. `legacy_records_ttl` is ONLY a
one-time migration knob for the pre-existing legacy records (the backfill above);
it imposes no store-wide default and never floors a steady-state write. (An
earlier design — "Rule 3" — floored no-`ttl=` writes to `legacy_records_ttl`;
that was removed by design to keep the per-write-only contract intact.)

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
- **Census-then-chunk re-stamp, flag-last (spec `spec-backfill-completeness.md`
  §3 — Fix A; supersedes `spec-chunked-backfill.md` §3.2 iterate-while-write and
  §4 inference).** The single-shot design held several whole-store-sized copies
  and was **OOM-killed on a real 500 MB Quix Cloud deployment at ~165k records**;
  the first chunked design fixed memory but **iterated the default CF while
  writing re-stamped values back into it**, which at real scale (200k+ keys, SST
  flushes/compactions mid-iteration) can **skip or duplicate keys** — and a
  single skipped key flipped a store into TTL mode with an un-stamped value,
  which the read path then mis-stripped → **data corruption / crash-loop on a
  live deployment**. Fix A makes the backfill **provably complete**: it
  **censuses the full default-CF key list first** (`sorted(default_cf.keys())`
  into a Python `list[bytes]`, keys only, ~80 B/key live) with no concurrent
  writes, then **chunks over that frozen list**, **point-getting** each value
  fresh (`default_cf.get(key)`) and wrapping it whole. The read driver (the
  frozen list) is independent of the CF's live structure, so every census key is
  visited **exactly once**. Per chunk: build a chunk-sized `WriteBatch`
  (default-CF puts + `__ttl_index__` puts), produce the chunk's re-stamped
  default-CF records to the changelog, `flush()` the producer (bounding its
  in-flight queue), advance the persisted cursor **in the same batch**, commit
  with the **raw** writer `self._write(batch)`, then release. Peak transient
  memory is the key-list census (~16 MB at 200k) plus one chunk (~30-80 MB), flat
  in chunk terms. The work happens out of the transaction cache; only the small
  genuine in-batch user writes still flow through the cache. **No inference
  anywhere** — `_looks_like_stamped_value` is no longer called by the backfill
  (it survives only for recovery flag-discovery), and the
  `_restamp_one_for_backfill` per-record skip/re-stamp helper is removed.
- **Flag-last atomicity + persisted cursor (spec §3.3, §3.4 — Fix A).** A single
  `WriteBatch` can no longer wrap the whole migration, so atomicity is anchored
  by: (1) the `__ttl_enabled__` + format-version flag written **last** by the
  caller (`_maybe_flip_or_reject`) after the cursor reaches `len(key_list)`; and
  (2) a persisted integer cursor `__ttl_backfill_progress__` in the metadata CF,
  advanced in the **same WriteBatch** as each chunk's puts. A crash mid-backfill
  (or before the flag) ⇒ the partition opens **legacy** (flag absent) and reads
  pre-existing values raw (no stripping) in the meantime ⇒ the next `ttl=` write
  re-enters backfill, **re-censuses** (producing the **identical sorted list**,
  so the integer cursor resumes exactly), and stamps from the cursor onward.
  Keys below the cursor are **known done** (skipped via the cursor, not
  byte-sniffed) and are **never re-read** (no double-wrap by construction). Note
  the convergence semantics: keys stamped by an interrupted run keep that run's
  `expires_at_ms`; keys stamped by the completing run get the completing run's
  expiry. If the high-water advanced between runs the two differ — the store is
  fully stamped (the invariant holds) but not strictly uniform across the crash
  boundary. The backfill stays **sequential** inside `prepare()` (no threads), so
  processing is paused for the whole migration; no interleaving.
- **Census memory (spec §5, OP-BC-2).** The frozen key list is bounded but
  linear in key count: ~16 MB at 200k, ~80 MB at 1M, ~800 MB at 10M. Above
  `_CENSUS_SPILL_WARN_THRESHOLD` (3M keys) a one-line WARNING flags the future
  need for a disk-spill census; the backfill still proceeds in memory. Disk
  spill is **not** built yet (OP-BC-2).
- **Fail-safe read — degrade, never corrupt (spec §4 — Fix B).** The flipped-
  partition read path (`transaction.py:_get_bytes`) previously **unconditionally
  stripped** the first 8 bytes of every value, relying on the invariant "flipped
  ⇒ every value stamped". When the buggy backfill violated that invariant, a
  long legacy JSON value (`{"status":"ON",...}`) had its first 8 bytes
  (`{"status`) chopped off → `StateSerializationError` → crash-loop. Fix B adds a
  strict module-level validator `_safe_decode_stamp(value) -> (stamp, payload) |
  None`: it returns a decode **only** when `len >= 8` **and** the leading 8 BE
  bytes are `SENTINEL_NEVER` or a plausible epoch-ms expiry (`0 < stamp < 10**15`,
  the same bound as `_looks_like_stamped_value`). `_get_bytes` now strips only
  when the validator confirms a stamp; otherwise it **returns the value RAW**
  (treated as never-expires) and logs a once-per-transaction WARNING. The
  short-blob (`< 8` bytes) → `Marker.UNDEFINED` handling is preserved unchanged.
  Genuine stamps always decode (they are `>= 8` bytes and carry the sentinel or
  a sub-cap expiry), so genuinely-stamped values still expiry-filter normally —
  no regression. The only residual (spec §4.3): a legacy value whose first 8
  bytes coincidentally decode to a plausible expiry is still mis-stripped — but
  Fix A's completeness guarantee means a Fix-A-backfilled store has **no
  un-stamped values left** to mis-classify, so this only bites pre-Fix-A stores.
  No per-value format marker added (spec §4.4; the marker is the designated
  escalation, OP-BC-1, if a future feature must mix legacy + stamped values).
  **Shared-path note:** the change is confined to the flipped branch of
  `_get_bytes`; recovery (Rule 4 wallclock) and the expiry sweep call
  `decode_ttl_value` directly and are **not** routed through `_safe_decode_stamp`
  — recovery/sweep behavior is unchanged.
- **Changelog via option (a), per chunk (spec §5, §8.5).** Re-stamped
  pre-existing default-CF keys are produced to the changelog directly per chunk
  (not via the transaction cache), so cold-restore recovery rebuilds from the
  same bytes with zero recovery-side code change. The `__ttl_index__` CF is
  local-only and is rebuilt during recovery, not produced. Recovery replays in
  Kafka offset order regardless of how producing was batched, so per-chunk
  production is recovery-identical to single-shot. **Changelog offset
  correctness:** the per-chunk `_write(batch)` does NOT advance the persisted
  `__changelog_offset__`; that is still written once in the final
  `partition.write()`. `InternalProducer._on_delivery` records the max produced
  offset per topic-partition across *all* `produce()` calls (chunk + in-batch),
  and `Checkpoint.commit` reads `producer.offsets` after its flush, so the final
  persisted offset already includes the chunk-produced messages — verified
  against `internal_producer.py:206-232`.

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
       5. populated + opt-in set   -> CHUNKED BACKFILL:
            staged_default_keys = serialized default-CF keys in this batch
            expires = _compute_legacy_expiry(legacy_records_ttl)
                    = high_water + _ttl_to_ms(legacy_records_ttl)
            restamped = partition.backfill_legacy_records(
                            expires, changelog_producer, processed_offsets,
                            staged_default_keys, chunk_size)
                 # Fix A: census FIRST, then chunk over the frozen list.
                 key_list = sorted(k for k in default_cf.keys()
                                   if k not in staged_default_keys)  # frozen census
                 cursor = _load_backfill_progress()    # resume point (0 first run)
                 while cursor < len(key_list):
                    chunk_keys = key_list[cursor : cursor + chunk_size]
                    for key in chunk_keys:
                       value = default_cf.get(key)      # FRESH point-get (no iter)
                       if value is None: continue        # deleted since census
                       stamped = encode_ttl_value(expires, value)  # WRAP WHOLE
                       batch.put(key, stamped)                       [default]
                       batch.put(encode_index_key(expires, key), b"") [__ttl_index__]
                    changelog_producer.produce(chunk's re-stamped default recs)
                    changelog_producer.flush()        # bound producer queue
                    cursor += len(chunk_keys)
                    batch.put(__ttl_backfill_progress__, cursor)  # SAME batch
                    self._write(batch)                # raw writer, atomic per-chunk
                    release batch                      # peak = census + 1 chunk
            _restamp_default_cf_cache_for_flip()       # stamp in-batch writes only
            _write_flip_metadata_to_cache()            # __ttl_enabled__, format ver
            partition.uses_ttl_stamps = True           # (reached only if no crash)
  └─ super().prepare()  -> produces the (small) in-batch cache entries to changelog

tx.flush()                                              (base transaction.py)
  └─ partition.write(cache, offset)                     (partition.py)
       └─ one WriteBatch: in-batch cache entries + flip metadata + high-water
                          + sweep-eligible + __changelog_offset__
       └─ self._write(batch)   <-- final atomic commit; this is where the FLAG
                                    lands (flag-last). Crash before here ⇒ legacy.
```

Pre-existing records are persisted+produced by the chunk loop *before* this
final batch; the final batch carries only the genuine in-batch user writes and
the flip metadata. The `__changelog_offset__` is advanced only in this final
write, using the checkpoint's `produced_offsets` (which already reflects the
chunk-produced messages).

Recovery (cold restore) is unchanged: the existing flag-discovery flips a
recovering partition into TTL mode on the first stamped default-CF replay, and
the wallclock-at-recovery filter (Rule 4) decides survivors. Chunk boundaries
are invisible to recovery — messages replay in Kafka offset order.

## File inventory

Created:

- `docs/architecture-state-ttl-legacy-backfill.md` — this document.
- `tests/test_quixstreams/test_state/test_rocksdb/test_legacy_backfill.py` —
  unit tests for spec §11 cases 1-9 (validation, backfill stamps, reject
  message, empty store, idempotency, event-time clock, windowed opt-out,
  recovery wiring, crash-before-flag re-run) + the wallclock-recovery cases.
- `tests/test_quixstreams/test_state/test_rocksdb/test_chunked_backfill.py` —
  unit tests for the chunked backfill (`spec-chunked-backfill.md` §10): chunk
  size validation, store >> chunk completes with one `_write` per chunk + final
  flip batch, store < chunk, empty store, crash-before-flag cursor-resumed re-run
  with no double-wrap, first-run wrap-whole (no inference), per-chunk changelog
  rebuild-identical. (Updated for Fix A: the prior mixed-already-stamped/stale-
  index re-stamp test is replaced by the first-run wrap-whole test, since Fix A
  retires the byte-sniffing recognizer from the backfill path.)
- `tests/test_quixstreams/test_state/test_rocksdb/test_backfill_completeness.py` —
  unit tests for Fix A + Fix B (`spec-backfill-completeness.md` §9): multi-chunk
  completeness (every key stamped, zero un-stamped), crash-resume via the
  persisted cursor (cursor-skipped keys not re-read, `_looks_like_stamped_value`
  never called, no double-wrap), chunk-count formula + full 100%-stamped scan,
  census excludes `staged_default_keys`, deleted-since-census key skipped;
  fail-safe read (un-stamped legacy value returns raw not corrupted — the live
  crash-loop regression — genuinely-stamped values still filter, the §4.3
  plausible-prefix residual documented).
- `dev-planning/state-ttl-legacy-backfill/implementation-notes.md` — spike
  findings (Q1-Q3) and chosen approach.
- `dev-planning/state-ttl-legacy-backfill/open-points.md` — OP-1 (recovery
  high-water vs uniform expiry; see Caveats).

Modified:

- `quixstreams/state/rocksdb/options.py` — `legacy_records_ttl` field +
  docstring; `legacy_backfill_chunk_size: int = 10_000` field + docstring;
  `__post_init__` validation (both: strictly positive or `ValueError`).
- `quixstreams/state/rocksdb/types.py` — `legacy_records_ttl` and
  `legacy_backfill_chunk_size` on the `RocksDBOptionsType` protocol.
- `quixstreams/state/rocksdb/partition.py` — thread both options into `__init__`
  (`self._legacy_records_ttl`, `self._legacy_backfill_chunk_size`), add the
  `legacy_records_ttl` and `legacy_backfill_chunk_size` properties, **rewrite
  `backfill_legacy_records` as the census-then-cursor loop (Fix A)** (signature
  `(expires_at_ms, changelog_producer, processed_offsets, staged_default_keys,
  chunk_size) -> int`; no longer takes/uses the transaction cache): census
  `sorted(default_cf.keys())` (excluding `staged_default_keys`) into a frozen
  list, resume from the persisted cursor `__ttl_backfill_progress__`
  (`_load_backfill_progress`), point-get + wrap-whole each chunk, advance the
  cursor in the same batch, raw-write. The `_restamp_one_for_backfill` per-record
  inference helper and the live forward iterator are **removed**;
  `_looks_like_stamped_value` is no longer called by the backfill (kept for
  recovery flag-discovery). `_CENSUS_SPILL_WARN_THRESHOLD` constant added. Rewrite
  `reject_ttl_on_populated_store` (operator-callable message;
  `operator_action="set_legacy_records_ttl"`). **OP-1 recovery-clock fix
  (spec-recovery-wallclock.md):** `recover_from_changelog_message` now judges
  expiry against a wallclock captured once per recovery session
  (`self._recovery_now_ms`, lazily on the first stamped default-CF replay via
  the `_now_ms()` test seam) instead of a stamp-ratcheted high-water; the old
  `advance_high_water(stamp)` recovery ratchet is removed and the post-recovery
  `high_water_ms` is seeded to that captured wallclock.
- `quixstreams/state/rocksdb/transaction.py` — backfill branch in
  `_maybe_flip_or_reject` re-plumbed for the chunked method: `prepare()` now
  forwards `processed_offsets` into `_maybe_flip_or_reject`, which computes
  `staged_default_keys` (the in-batch default-CF keys) and passes the changelog
  producer + offsets + chunk size into `backfill_legacy_records`; the
  pre-existing keys no longer pass through `_restamp_default_cf_cache_for_flip`
  (it now stamps only the in-batch writes, called with no `skip_keys`); new
  `_compute_legacy_expiry`. `_compute_stamp` returns `SENTINEL_NEVER`
  unconditionally for `ttl is None` (TTL strictly per-write; the former Rule-3
  no-`ttl=` floor was removed — `legacy_records_ttl` is migration-only).
  **Fix B:** new module-level `_safe_decode_stamp` strict validator +
  `_MAX_PLAUSIBLE_STAMP_MS` constant; `_get_bytes`'s flipped branch now degrades
  to raw (never-expires) on a non-stamp instead of unconditionally stripping,
  with a once-per-transaction WARNING (`self._unstamped_read_warned`). Short-blob
  → `UNDEFINED` handling preserved.
- `quixstreams/state/rocksdb/metadata.py` — new `TTL_BACKFILL_PROGRESS_KEY`
  metadata key (the Fix A cursor; local-only, never produced; no format-version
  bump).

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
- **Large stores — OOM fixed (chunked backfill).** The single-shot backfill held
  several whole-store-sized copies and was OOM-killed at ~165k records on a
  500 MB Quix Cloud deployment. The backfill is now chunked
  (`legacy_backfill_chunk_size`, default 10_000); peak transient memory is one
  chunk (~30-80 MB at the default), flat in total store size. Lower the knob on
  tight-memory deployments. See `spec-chunked-backfill.md`.
- **Per-chunk `producer.flush()` cost.** One network round-trip per chunk (~17
  for the 165k store at the default) — negligible. If profiling ever shows flush
  latency dominating for very large stores, flush every K chunks instead (trades
  a bounded multiple of producer memory for fewer round-trips); not currently
  needed.
- **Data corruption fixed (Fix A completeness + Fix B fail-safe read).** The
  first chunked backfill iterated the default CF while writing re-stamped values
  back into it; at real scale that can skip a key, flipping a populated store
  into TTL mode with an un-stamped value, which the read path then mis-stripped
  (`{"status":"ON",...}` → `":"ON",...}` → `StateSerializationError` →
  crash-loop on a live deployment). Fix A makes the backfill provably complete
  (census-then-cursor, no iterate-while-write, no inference); Fix B makes the
  read fail-safe (degrade to raw on a non-stamp, never strip 8 good bytes). Both
  shipped together. The earlier OP-CB-1 re-run double-wrap hazard is **retired**:
  the persisted cursor resumes deterministically with no byte-sniffing, so a
  resumed key is wrapped exactly once. The only residual is Fix B's §4.3
  plausible-prefix corner (a legacy value whose first 8 bytes coincidentally
  decode to a plausible expiry is mis-stripped) — emptied in practice by Fix A's
  completeness; it bites only pre-Fix-A stores.
- **Crash-boundary expiry non-uniformity (Fix A).** Keys stamped by an
  interrupted run keep that run's `expires_at_ms`; keys stamped by the completing
  run get the completing run's expiry. If the high-water advanced between runs
  the two differ. The store is fully stamped (the read-path invariant holds) but
  not strictly uniform across the crash boundary — a benign deviation for the
  dedup workload (a small set of legacy keys expire slightly earlier/later than
  the rest). Documented; not worth re-reading already-done keys to "fix".
- **No customer-callable repair for stores already corrupted by the buggy
  build (OP-BC-1 / MEMORY `quix-cloud-no-state-reset`).** The buggy build never
  shipped, so no repair pass is built (per the brief). Fix B alone stops the
  crash-loop on any such store (degrade-to-raw). If a flipped store with
  mis-stripping values is ever found in the wild, a one-shot repair pass
  (re-census + re-stamp any value `_safe_decode_stamp` rejects) is the designated
  follow-up.
