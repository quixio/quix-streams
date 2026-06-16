# Implementation notes — State TTL legacy-store backfill

**Branch:** `feature/sc-73191/udpate-state-behavior-of-dedup-feature-to`
**Status:** spike complete, feature implemented.

## Spike results (spec §12 open questions)

Spike script run against the bundled `rocksdict` (raw_mode, same options the
partition uses). Seeded 500k user keys in the `default` CF, plus a populated
`__metadata__` CF, then re-stamped the whole `default` CF + built the
`__ttl_index__`.

### Q1 — single WriteBatch of millions of entries?

- 500k keys → re-stamp batch = **1,000,000 puts** (500k value re-stamps +
  500k index entries), **26.7 MB** in the WriteBatch, built in 0.20 s and
  written in 0.22 s. Linear in key count: ~27 bytes/entry of WriteBatch
  memory. Extrapolated to 10M keys ≈ 530 MB WriteBatch.
- **Decision: do NOT chunk the write batch.** A single atomic
  `db.write(batch)` is the *only* way to keep the idempotency invariant
  (§8.4): `__ttl_enabled__` lands in the same batch as every re-stamped
  value, so either the store is fully backfilled-and-flipped or untouched.
  Chunking would reintroduce partial-backfill / double-stamp risk (see Q3).
- Memory caveat: option (a) for changelog (below) already requires holding
  all re-stamped values in the Python `PartitionTransactionCache` dict, which
  dominates memory regardless of the WriteBatch. For the Maxio dedup
  workload (keys are short hashes, values tiny) this is comfortably within a
  normal deployment's RAM. If a future workload has huge values this is the
  knob to revisit — but it is out of scope for Phase 1, and the spec
  explicitly says backfill must complete in full (not bounded by
  `max_evictions_per_flush`).

### Q2 — changelog wiring

- **Decision: option (a)** — stage the re-stamped pre-existing keys (and their
  `__ttl_index__` entries, and the flip metadata) into the **transaction
  update cache** in `_maybe_flip_or_reject`, *before* `super().prepare()`
  runs. Consequences, all verified against current code:
  - `super().prepare()` (`base/transaction.py:602-641`) iterates the update
    cache and produces every non-`LOCAL_ONLY_CFS` entry to the changelog →
    pre-existing re-stamped values reach the changelog naturally. The
    `__ttl_index__` CF is in `LOCAL_ONLY_CFS`, so index entries are *not*
    produced (correct — recovery rebuilds the index locally).
  - `partition.write()` (`partition.py:242-306`) iterates the same cache and
    stages every entry into one `WriteBatch`, then a single `self._write`.
    So the on-disk commit and the changelog production share the cache as the
    single source of truth — no out-of-band writes, no divergence.
  - Recovery flag-discovery (`partition.py:167-183`) flips a recovering
    partition into TTL mode on the first stamped default-CF replay, and
    `_normalize_replay_value` round-trips stamped values, so cold restore
    rebuilds an identical store. **No recovery-side code change needed.**
- Option (b) (out-of-band changelog production inside `backfill_legacy_records`)
  was rejected: it would duplicate the changelog producer logic, bypass the
  `LOCAL_ONLY_CFS` filter, and risk on-disk / changelog divergence. Option (a)
  reuses the existing path verbatim.
- Because of option (a), `backfill_legacy_records` is implemented to stage into
  the **transaction cache**, not to take a raw `WriteBatch`. The spec §6.3
  signature `backfill_legacy_records(expires_at_ms, batch)` assumed direct
  batch writes; option (a) (spec's own recommendation, §8.5) supersedes that.
  Final signature: `backfill_legacy_records(expires_at_ms, cache)` where
  `cache` is the `PartitionTransactionCache`. This is the one deliberate
  deviation from the literal §6.3 signature, made to honor §8.5.

### Q3 — iterator content + double-stamp

- **`default` CF iteration yields ONLY user data.** Spike: 500k user keys +
  a populated `__metadata__` CF → iterating `default` returned exactly 500k
  keys, **0** with a `__` prefix. Metadata, global-counter and index keys all
  live in *separate* column families, never in `default`. So
  `backfill_legacy_records` needs **no key filtering**.
- **Double-stamp is impossible with the chosen single-atomic-batch design.**
  RocksDB `WriteBatch` is atomic: a crash before `db.write` writes nothing
  (next open is still legacy → clean re-run), and a crash during `db.write`
  cannot be observed as partial. Because `__ttl_enabled__` is in the same
  batch/cache, a re-run only happens on a store that was never flipped, whose
  `default` values are therefore still un-stamped. We never iterate an
  already-stamped store to re-stamp it. (A double-stamped value would decode
  to an outer expiry wrapping 8 bytes of garbage + payload — confirmed in the
  spike — which is exactly why we forbid chunking.)

## Chosen architecture (final)

1. `RocksDBOptions.legacy_records_ttl: Optional[timedelta] = None` + protocol
   field. Validated strictly-positive in `__post_init__` (frozen dataclass) via
   `ValueError`. Threaded to `partition.__init__` as `self._legacy_records_ttl`.
2. Transaction reads it through `self._partition._legacy_records_ttl`.
3. `_maybe_flip_or_reject` 4th branch: populated-legacy + opt-in set → compute
   `expires_at_ms = high_water_ms + _ttl_to_ms(ttl)` (fallback: max in-batch
   pending stamp's base = batch ts; hard-error if neither) → call
   `partition.backfill_legacy_records(expires_at_ms, cache)` which stages all
   pre-existing keys (re-stamped) + index entries into the cache → then the
   existing `_restamp_default_cf_cache_for_flip` + `_write_flip_metadata_to_cache`
   handle the in-batch keys and flag → flip runtime flag.
4. `backfill_legacy_records` iterates `default` CF, skips keys already present
   in the cache (those are handled by `_restamp_default_cf_cache_for_flip` with
   their true pending stamp), re-stamps the rest with `expires_at_ms`, and
   stages matching `__ttl_index__` entries.
5. Reject message rewritten to point at `legacy_records_ttl`;
   `operator_action` → `set_legacy_records_ttl`.

### BACKFILL-RECOVERY finding (needs Ludvík's decision)

Implementing case 8 surfaced a real interaction between **uniform-expiry
backfill** and the **existing changelog-recovery filter** that the spec did not
anticipate:

- `recover_from_changelog_message` (`partition.py:229-247`) reads
  `recovery_now = self._high_water_ms` per message and, for a stamped
  default-CF value, **drops it** if `stamp <= recovery_now`. After writing each
  message it advances the recovery high-water by that message's **expiry stamp**
  (`partition.py:237-240`, `advance_high_water(stamp)`).
- Backfill produces **N records that all share one expiry** `E =
  enable_time + legacy_records_ttl`. On replay, the first lands (high-water was
  below `E`) and advances the recovery high-water to `E`; **every subsequent
  backfilled record then satisfies `E <= E` and is dropped as "already
  expired".** So a cold-restore of a backfilled store can lose all but one of
  the backfilled records.
- This is a property of the **recovery layer**, not the backfill writer: two
  ordinary steady-state TTL writes that happen to share the maximum expiry hit
  the same drop. But backfill makes it the *common* case (uniform expiry by
  design), so it matters here.

Root cause layer: `code` (recovery filter), exposed by `architecture`
(uniform-expiry backfill). It is **out of the additive scope** of this feature
(the spec said "reuse the existing recovery path verbatim"), so I did **not**
change the recovery filter. The unit test (case 8) is written to exercise the
backfill→changelog→recovery *wiring* with a single legacy record (distinct
stamps), which passes and proves option (a) is correct. The N-uniform-records
recovery drop is flagged for Ludvík:

- **Option A (recommended):** change the recovery high-water advancement to use
  the record **event-time** (carried in changelog headers / processed-offsets)
  instead of the **expiry stamp**, and/or change the drop comparison to `<`
  (strict) so an entry expiring exactly at the current high-water is retained
  during replay. Either is a small, localized recovery-side change but it
  alters documented recovery semantics for ALL TTL stores, so it needs a
  spec/Buddy decision before I touch it.
- **Option B:** accept the limitation and document that a cold-restore of a
  backfilled store collapses the uniform-expiry cohort to (at most) one record.
  For the dedup workload this means most legacy dedup keys stop blocking
  duplicates after a volume-loss restore — likely unacceptable.

I recommend escalating to Buddy/Ludvík and, if Option A is approved, doing it as
a **follow-up** so this feature stays additive. An open-points entry has been
added.

### enable_time reference clock

`expires_at_ms = high_water_ms + _ttl_to_ms(legacy_records_ttl)`. high_water is
advanced by the triggering TTL write (`transaction.py:190-191, 234-235`) so it
is non-None in the normal path. Fallback: the max base timestamp among the
batch's pending stamps (i.e. the TTL write's record timestamp). If neither is
available → `IncompatibleStateStoreError` (do NOT invent wall-clock — spec §8.1).
