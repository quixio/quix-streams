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
- **Chunked re-stamp, flag-last (spec `spec-chunked-backfill.md` §3, §6).** The
  earlier single-shot design staged the *entire* store into the transaction
  cache and committed it in one `WriteBatch`. That held several whole-store-sized
  copies co-resident (cache + serialized batch + changelog producer queue) and
  was **OOM-killed on a real 500 MB Quix Cloud deployment at ~165k records**. The
  backfill now iterates the populated default CF with a **single forward
  iterator** in bounded chunks of `legacy_backfill_chunk_size` (default 10_000).
  Per chunk: re-stamp into a chunk-sized `WriteBatch` (default-CF puts +
  `__ttl_index__` puts), produce the chunk's re-stamped default-CF records to the
  changelog, `flush()` the producer (bounding its in-flight queue to one chunk),
  commit the batch with the **raw** writer `self._write(batch)`, then release the
  chunk's structures. Peak transient memory is therefore **one chunk**, flat
  regardless of total store size (~30-80 MB at the default). The work happens out
  of the transaction cache entirely; only the small genuine in-batch user writes
  still flow through the cache.
- **Flag-last atomicity invariant (spec §3.3, §6).** A single `WriteBatch` could
  no longer wrap the whole migration, so atomicity is anchored differently:
  `backfill_legacy_records` persists+produces every chunk first, then the caller
  writes `__ttl_enabled__` + the format version **last**, in the final
  transaction batch via `partition.write()`. Until that flag lands the on-disk
  partition is still legacy. A crash mid-backfill (or before the flag) ⇒ the
  partition opens legacy ⇒ the backfill re-runs from a fresh iterator and
  converges. Each chunk is itself atomic (its default+index puts commit
  together). The backfill stays **sequential** inside `prepare()` (no threads),
  so processing is paused for the whole migration and there is no interleaving —
  the atomic-flip correctness (convert all → flag → resume) is preserved.
- **Re-run / already-stamped safety (spec §4, OP-CB-1 — the correctness core).**
  Because chunks commit incrementally, a re-run after a partial backfill re-reads
  already-stamped records. For each record the loop reuses
  `_looks_like_stamped_value`: if it decodes to *exactly* the current run's
  `expires_at_ms` it is **skipped** (no-op put avoided); if it decodes to a
  *different* stamp (a prior partial run with a different high-water-derived
  expiry) it is **re-stamped from the stripped payload** (`decode_ttl_value` →
  payload, never the stamped blob) and the stale `__ttl_index__` pointer is
  deleted — so there is no double-wrap and the store converges to one uniform
  expiry; otherwise (first run / genuine legacy value) the whole value is
  wrapped. The only residual hazard is a genuine legacy value whose leading 8
  bytes coincidentally decode to exactly `expires_at_ms` (skipped → left
  un-stamped) — a specific 64-bit collision, the same heuristic class already
  accepted in `_looks_like_stamped_value` / `_normalize_replay_value`. Accepted
  and documented; no per-record format marker added.
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
                 └─ single forward iterator over the default CF; loop:
                    chunk = islice(iterator, chunk_size)   (bounded read)
                    for key, value in chunk (skip staged_default_keys):
                       # already-stamped detection (re-run safety):
                       #   stamp == expires        -> skip (no-op)
                       #   stamp != expires (prior) -> re-stamp from PAYLOAD,
                       #                               delete stale index key
                       #   not stamped (legacy)     -> wrap whole value
                       batch.put(key, encode_ttl_value(expires, payload))  [default]
                       batch.put(encode_index_key(expires, key), b"")      [__ttl_index__]
                    changelog_producer.produce(chunk's re-stamped default recs)
                    changelog_producer.flush()        # bound producer queue
                    self._write(batch)                # raw writer, per-chunk commit
                    release chunk + batch              # peak memory = 1 chunk
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
  flip batch, store < chunk, empty store, crash-before-flag re-run convergence
  with no double-wrap, mixed already-stamped/legacy re-stamp + stale-index
  cleanup, per-chunk changelog rebuild-identical.
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
  `backfill_legacy_records` as the chunked loop** (new signature
  `(expires_at_ms, changelog_producer, processed_offsets, staged_default_keys,
  chunk_size) -> int`; no longer takes/uses the transaction cache) with the
  `_restamp_one_for_backfill` per-record helper (already-stamped detection,
  payload re-stamp, stale-index delete), and rewrite
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
  `_compute_legacy_expiry`; Rule-3 `_compute_stamp` floor (prior work).

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
- **Re-run double-wrap hazard (OP-CB-1).** See "Re-run / already-stamped safety"
  above: a genuine legacy value whose leading 8 bytes coincidentally equal the
  exact current `expires_at_ms` would be skipped and left un-stamped. Negligible
  64-bit collision; accepted, no per-record marker added.
