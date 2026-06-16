# Chunked legacy-records backfill (OOM fix)

**Status:** Draft
**Project:** quix-streams
**Created:** 2026-06-15
**Planned with:** Buddy
**Addendum to:** `dev-planning/state-ttl-legacy-backfill/spec.md`
(§8.2 "bulk re-stamp at flip", §8.3 "large-store concern", §8.5 "changelog
durability option (a)") and `spec-recovery-wallclock.md` (Rule 4 recovery).
**Resolves:** OP-1 follow-up in `spec.md` §12 ("OP-1 — batch size for millions of
keys"): single `WriteBatch` vs chunked write-batches.
**Branch (implements):** additive follow-up to `feature/sc-73191`.

---

## 1. Summary

The legacy-records backfill (`spec.md` §6.2/§6.3, §8.2-§8.5) was OOM-killed on a
real Quix Cloud deployment. The deployment logged

```
Backfilled 165445 legacy records and flipped state store partition into TTL mode
```

…then was hard-killed with no traceback (the classic OOM signature). The user
confirmed: the kill happens **while setting TTLs on old records**, not during
new-message processing. The container limit was 500 MB; only ~165k records
(each user value ≈ 800 bytes padded) were enough to blow it.

Root cause: the current backfill (`backfill_legacy_records`,
`partition.py:411-485`) re-stamps the **entire** default CF in one pass and
**stages every re-stamped record into the transaction update cache**. That cache
is then (a) walked again by the parent `_prepare` to produce every record to the
changelog, and (b) walked a third time by `partition.write()` to build one
giant `WriteBatch`. So at peak the process holds, simultaneously, multiple
full-store-sized structures (see §3). Memory ≈ O(whole store) × several copies.

This addendum replaces the single-shot backfill with a **chunked** one:
re-stamp + persist (`WriteBatch`) + produce-to-changelog in bounded batches of
`N` records, **releasing each chunk's memory before reading the next**. Peak
memory becomes O(one chunk) regardless of total store size. The backfill stays
**sequential** with processing and the **atomic-flip correctness** is preserved
by writing the `__ttl_enabled__` / format-version flag **last**, only after every
chunk is durably persisted and produced.

---

## 2. Problem — why even 165k records OOMs at 500 MB

`backfill_legacy_records` today (`partition.py:455-485`):

```python
for key, value in default_cf.items():        # iterate WHOLE default CF
    stamped = encode_ttl_value(expires_at_ms, value)
    cache.set(key=key, value=stamped, prefix=b"", cf_name="default")     # (1)
    cache.set(key=encode_index_key(expires_at_ms, key), value=b"", ...)  # (2)
```

It is called from `_maybe_flip_or_reject` inside `prepare()`
(`transaction.py:438`, `480-490`). After it returns, **still inside the same
transaction**:

- the parent `_prepare` (`base/transaction.py:602-641`) iterates the cache and
  calls `ChangelogProducer.produce` for **every** re-stamped record, enqueuing
  them all into the producer's internal librdkafka queue;
- later `flush()` → `partition.write()` (`partition.py:275-339`) iterates the
  cache a third time and stages every record into a single `WriteBatch`.

Co-resident peak (per `spec.md` §8 memory breakdown, confirmed):

1. **The transaction update cache** — the re-stamped default-CF entries (1) plus
   one index-CF entry each (2). These are Python `dict`/`bytes` objects; per-entry
   overhead is ~1.5-2 KB (Python object headers + two `bytes` payloads + dict
   slots), *not* the 800 B on-disk size. For 165k records that alone is
   ≈ 250-330 MB before anything else.
2. **The serialized `WriteBatch`** — a second serialized copy of every put
   (default + index), held until `_db.write()` returns.
3. **The changelog producer queue** — librdkafka buffers the produced messages
   (key+value+headers) until flushed; another full-store-sized copy.
4. **RocksDB block cache + the source iterator's pinned blocks**, recovery
   residual — stacked on top.

Four-plus simultaneous whole-store-sized copies against a 500 MB ceiling → OOM
at 165k × 800 B (≈ 130 MB of raw value bytes, but multiplied several-fold). The
fix must bound (1), (2), and (3) to one chunk and release them between chunks.

---

## 3. Chunked algorithm (step-by-step)

The backfill moves **out of the shared transaction cache** and into a
self-contained loop on the partition that does its own per-chunk produce +
commit, then writes the flip flag last. This is the central design change: the
transaction cache is the wrong vehicle because everything staged in it is held
for the whole transaction lifetime (cache → produce-all → write-all).

### 3.1 New shape of `backfill_legacy_records`

`RocksDBStorePartition.backfill_legacy_records(...)` is rewritten to loop in
chunks. Suggested signature (additive; caller in `transaction.py` adapts):

```
def backfill_legacy_records(
    self,
    expires_at_ms: int,
    changelog_producer: Optional[ChangelogProducer],
    processed_offsets_header: Optional[str],   # for changelog headers, see §5
    staged_default_keys: set[bytes],           # in-batch keys to skip (see §3.4)
    chunk_size: int,                           # = self._legacy_backfill_chunk_size
) -> int:                                      # returns count of records re-stamped
```

It no longer takes `cache` and no longer returns `backfilled_keys` for the
caller to skip in `_restamp_default_cf_cache_for_flip` (the in-batch keys are
now passed *in* as `staged_default_keys`; see §3.4 / §6).

### 3.2 The loop

```
default_handle = self.get_column_family_handle("default")
index_handle   = self.get_column_family_handle(TTL_INDEX_CF_NAME)
restamped = 0

iterator = default_cf.items()       # single forward iterator over the default CF
while True:
    # 1. READ one chunk: pull up to `chunk_size` (key, value) pairs from the
    #    iterator into a local list. Stop when the iterator is exhausted.
    chunk = take(iterator, chunk_size)   # excludes in-batch staged_default_keys
    if not chunk:
        break

    # 2. RE-STAMP + build this chunk's WriteBatch (default puts + index puts).
    batch = WriteBatch(raw_mode=True)
    for key, value in chunk:
        if _already_stamped_with(value, expires_at_ms):   # §4 idempotency
            continue
        stamped = encode_ttl_value(expires_at_ms, value)
        batch.put(key, stamped, default_handle)
        batch.put(encode_index_key(expires_at_ms, key), b"", index_handle)

    # 3. PRODUCE this chunk's re-stamped DEFAULT-CF records to the changelog.
    #    (Index CF is LOCAL_ONLY — never produced; rebuilt on recovery, §5.)
    if changelog_producer is not None:
        for key, value in chunk:
            changelog_producer.produce(key=stamped_value_for(key),
                                       value=..., headers=default_cf_headers)
        # Flush the producer for this chunk so the queue does not grow
        # unbounded across chunks (bounds producer memory to one chunk, §5).
        changelog_producer.flush()

    # 4. COMMIT this chunk atomically (default + index puts in one batch).
    self._write(batch)

    # 5. RELEASE: drop `chunk`, `batch`, and any per-chunk temporaries so the
    #    next iteration starts at one-chunk memory. (Local scope + the loop
    #    boundary handle this; do not accumulate anything across iterations.)
    restamped += len(chunk)

# 6. After ALL chunks are persisted+produced, the CALLER writes the flip flag
#    last (see §3.3, §4). backfill_legacy_records does NOT write the flag.
return restamped
```

Notes:
- **Single forward iterator** over the default CF for the whole backfill (do not
  re-seek per chunk). RocksDB iterators read consistent snapshot data; the
  per-chunk `_write` calls put *new* (stamped) values, but they sort to the same
  keys and the iterator continues forward past them — re-stamped keys are behind
  the cursor and are not revisited within a single run. (A crash + re-run uses a
  fresh iterator and relies on §4 idempotency.)
- `take(iterator, chunk_size)` materializes at most `chunk_size` `(key, value)`
  pairs; that bounds (1) in §2 to one chunk.
- The chunk's `WriteBatch` bounds (2); flushing the producer per chunk bounds
  (3). All three OOM contributors are now O(chunk).

### 3.3 Flag-last ordering (atomicity anchor)

`__ttl_enabled__` and `__ttl_format_version__` **must be written in a final,
separate `WriteBatch` after the chunk loop returns**, by the caller
(`_maybe_flip_or_reject`), together with the in-batch re-stamped user writes and
the high-water (those are small — one transaction's worth). Concretely the flush
path becomes:

1. `backfill_legacy_records(...)` runs the chunk loop (steps 1-5 above): all
   pre-existing records are durably committed **and** produced, chunk by chunk.
2. The transaction then proceeds as today for the **in-batch** writes only:
   `_restamp_default_cf_cache_for_flip()` stamps the (small) in-batch cache,
   `_write_flip_metadata_to_cache()` queues the flag, and the normal
   `super().prepare()` → `flush()` → `partition.write()` commits the in-batch
   cache + the flag in one final batch.

So the flag lands **after** every chunk. Until that final batch commits, the
partition's on-disk `__ttl_enabled__` is still absent → on any crash mid-backfill
the partition opens **legacy** and the backfill re-runs from the start (§4).

### 3.4 In-batch keys (the triggering transaction's own writes)

The triggering flush also contains the genuine `state.set(..., ttl=...)` write(s)
that flipped the partition, staged in the transaction cache. Those must keep
their **own true pending stamp**, not the uniform legacy expiry. Today
`backfill_legacy_records` returns `backfilled_keys` and
`_restamp_default_cf_cache_for_flip` skips them. Chunked version inverts this:
the caller computes `staged_default_keys` (the serialized default-CF keys present
in `cache.get_updates("default")`) and **passes them into**
`backfill_legacy_records`, which skips them in step 2 (does not re-stamp or
produce them). `_restamp_default_cf_cache_for_flip` then stamps them with their
true stamp as today. Net behavior identical to current §8.5 skip logic, just
re-plumbed because the loop no longer touches the cache.

---

## 4. Idempotency / crash-safety + already-stamped-on-rerun handling

**Invariant (unchanged from `spec.md` §8.4):** the flag is written last; a crash
before the flag leaves the partition legacy and the backfill re-runs cleanly on
the next open.

But chunked commits introduce a NEW situation the single-shot version never had:
**a re-run after a partial backfill will re-read records that a prior partial run
already stamped.** Example: 50 of 165 chunks committed, then crash. On restart
`__ttl_enabled__` is absent → legacy → next `ttl=` write triggers backfill again
→ the iterator now sees 50 chunks of **already-8-byte-stamped** values mixed with
115 chunks of un-stamped legacy values.

### 4.1 Decision: detect-and-skip already-stamped values

In step 2 of the loop, before re-stamping, the chunk loop must decide whether a
read value is already a valid backfill stamp. Use the existing recognizer
`partition.py:_looks_like_stamped_value` (already present, `872-903`) — it
decodes the 8-byte BE prefix and accepts `SENTINEL_NEVER` or a plausible
epoch-ms expiry (`0 < stamp < 10**15`). Refine for backfill:

```
def _already_backfilled(value: bytes, expires_at_ms: int) -> bool:
    if len(value) < TTL_STAMP_BYTES:
        return False
    stamp, _ = decode_ttl_value(value)        # may raise ValueError → not stamped
    return stamp == expires_at_ms             # exact match to THIS run's expiry
```

Rationale for matching the **exact** `expires_at_ms` rather than "any plausible
stamp":

- A partial prior run stamped its records with `expires_at_ms = high_water_at_
  that_run + legacy_records_ttl`. On the re-run, `expires_at_ms` is recomputed
  from the **current** high-water (`_compute_legacy_expiry`,
  `transaction.py:517-541`). If the high-water advanced between runs, the two
  expiries differ. **Decision:** treat a record whose stamp differs from the
  current run's `expires_at_ms` as needing (re-)stamping to the current value, so
  the whole store ends up uniform at the value of the run that *completes*. This
  keeps the post-condition simple: "after a successful backfill every
  pre-existing record carries the same `expires_at_ms`, equal to the completing
  run's high-water + ttl." A record already carrying exactly that value is
  skipped (no-op put avoided). A record carrying a *different* prior-run stamp is
  re-stamped to the current value — idempotent and convergent.
- This also safely handles a value that merely *looks* stamped but is legacy user
  data whose first 8 bytes coincidentally decode to a number: if it does not
  equal `expires_at_ms` it is (re-)stamped, wrapping it with a fresh 8-byte
  prefix. **WARNING / open question (§7, OP-CB-1):** wrapping a value that was
  *actually* already an 8-byte-stamped value from a prior run with a *different*
  expiry is correct (re-stamp). But wrapping a *legacy un-stamped* value whose
  leading 8 bytes happen to decode to a plausible number, when that number is not
  `expires_at_ms`, is also correct (it gets stamped). The only hazard is a legacy
  value whose leading 8 bytes coincidentally equal `expires_at_ms` exactly — it
  would be skipped and left un-stamped. Probability is negligible (must match a
  specific 64-bit value) and the same class of heuristic risk already exists in
  `_normalize_replay_value` / `_looks_like_stamped_value` (documented in
  `spec.md` §6.8). Accept and document; do not add a per-record format marker.

### 4.2 Why re-stamping is idempotent even without skip

Even if the recognizer were absent, re-stamping an already-stamped value would
**double-wrap** it (`encode_ttl_value(E, b"\x..stamp..||payload")`), corrupting
it. So the skip in §4.1 is **required for correctness**, not just efficiency —
this is the key correctness detail of the whole addendum. A re-run MUST detect
already-stamped records and either skip them (stamp == current expiry) or
re-stamp the *payload* (never the already-stamped blob). Because §4.1 compares
the decoded `stamp` and, on mismatch, re-stamps using `decode_ttl_value`'s
**payload** (strip the old 8-byte prefix, re-wrap with the current expiry), there
is no double-wrap on the re-stamp branch either:

```
stamp, payload = decode_ttl_value(value)
if stamp == expires_at_ms:
    skip                                 # already at the target expiry
else:
    batch.put(key, encode_ttl_value(expires_at_ms, payload), default_handle)
    # also fix the index entry: delete the stale encode_index_key(stamp, key)
    # and put encode_index_key(expires_at_ms, key). See §4.3.
```

For a genuinely un-stamped legacy value, `decode_ttl_value` on a value `< 8`
bytes raises `ValueError` (treat as un-stamped, wrap whole value); a value `>= 8`
bytes is assumed stamped per the existing convention — but on the very first run
the store is all-legacy, and the existing format-version guard
(`_enforce_format_version`) plus the fact that legacy values were written without
a stamp means we **must wrap the whole value on the first run**. **Decision:**
distinguish first-run from re-run by the partition's flip state at backfill
entry — but the partition is always legacy at backfill entry (that is the
precondition). Instead, distinguish per-record: on the **first** run no record
carries `expires_at_ms`, so §4.1's exact-match test is simply never true and
every record is wrapped whole — correct. On a **re-run**, prior-run records carry
a decodable stamp; §4.1 strips+re-wraps or skips. The single code path in §4.1
handles both **provided** the wrap-vs-rewrap uses payload-after-decode only when
the value actually decodes to a stamped form. ArchDev: implement the recognizer
so that a value is treated as "stamped" (→ strip prefix, compare, re-wrap
payload) only when `len >= TTL_STAMP_BYTES` **and** decoding yields a stamp that
is either `SENTINEL_NEVER` or `< 10**15` (reuse `_looks_like_stamped_value`);
otherwise treat as legacy and wrap the whole value. This matches the existing
recovery heuristic exactly, so behavior is consistent with `spec.md` §6.8.

### 4.3 Index-CF consistency on re-run

When a record is re-stamped from a prior-run `stamp_old` to `expires_at_ms`, the
prior run also wrote `encode_index_key(stamp_old, key)` into `__ttl_index__`. The
re-run must **delete** that stale index key and put `encode_index_key(
expires_at_ms, key)` in the same chunk batch, else the sweep would later find a
dangling index entry. The sweep already GCs ghost index entries
(`_run_sweep`, `partition.py:781-794`: "key was overwritten with a fresh expiry
stamp" → `batch.delete(index_key)`), so a missed stale index entry is
**self-healing** and non-fatal — but deleting it in the backfill batch is cheaper
and cleaner. **Decision:** delete the stale index key in the re-stamp branch when
`stamp_old != expires_at_ms and stamp_old != SENTINEL_NEVER`; rely on the sweep
as a backstop. (On the common first-run path there is no prior index entry, so no
delete is needed.)

---

## 5. Changelog durability per chunk (recovery correctness)

`spec.md` §8.5 (option a) requires the re-stamped values reach the changelog so a
cold-restore rebuilds them. Chunking changes *when* they are produced (per chunk)
but not *what*:

- Each chunk produces its re-stamped **default-CF** records via
  `ChangelogProducer.produce` with the standard default-CF headers (cf name +
  the processed-offsets header — pass the same `processed_offsets` string the
  transaction uses; see `base/transaction.py:611-624`). The `__ttl_index__` CF is
  in `LOCAL_ONLY_CFS` and is **never** produced — it is rebuilt on recovery from
  the stamped default-CF values exactly as today (`partition.py:264-268`).
- **Flush the producer once per chunk** (step 3). This bounds the producer's
  in-flight queue to one chunk (OOM contributor (3) in §2). It is the same
  `producer.flush()` the checkpoint path calls (`checkpoint.py:249`); calling it
  per chunk is safe and just adds a network round-trip per chunk.
- **Recovery replays in offset order regardless of how producing was batched.**
  Kafka assigns monotonically increasing offsets to the produced messages in the
  order `produce()` is called; chunk K's records get lower offsets than chunk
  K+1's. On rebuild, `recover_from_changelog_message` replays them in offset
  order and the wallclock-at-rebuild filter (Rule 4,
  `spec-recovery-wallclock.md`) decides survivors per record. Batching boundaries
  are invisible to recovery. **Confirmed: per-chunk production is
  recovery-identical to single-shot production.**
- The **flip metadata** is local-only (metadata CF is in `LOCAL_ONLY_CFS`) and is
  never produced; recovery infers TTL mode from the first stamped default-CF
  replay (`partition.py:201-217`). So flag-last on the local store does not affect
  the changelog stream at all — the changelog just carries the stamped records.

**Changelog offset bookkeeping:** the per-chunk `_write(batch)` in step 4 does
**not** advance the persisted `__changelog_offset__` (it writes only default +
index puts). The changelog offset is still written once, in the final
`partition.write()` for the in-batch cache (`write()` →
`_update_changelog_offset`, `partition.py:330-331`), using the
`changelog_offset` the checkpoint passes after its producer flush
(`checkpoint.py:287-290`). This is acceptable because: a crash before the final
flag-batch leaves both `__ttl_enabled__` **and** `__changelog_offset__`
un-advanced for the backfill, so the partition re-runs from a consistent legacy
state. The chunk-produced changelog messages that were already published become
**superseded/duplicated** by the re-run's re-produced stamped values (same keys,
later offsets win on replay) — idempotent on recovery. ArchDev: confirm the
checkpoint's `produced_offsets` correctly reflects the highest offset including
the chunk-produced messages (it should, since they went through the same
producer); flag if the per-chunk `flush()` interacts oddly with
`producer.offsets`.

---

## 6. Atomicity nuance — new invariant

Single-shot backfill committed everything (all re-stamps + flag) in one
`WriteBatch` → strictly atomic. Chunked backfill cannot. New invariant:

> **Each chunk is atomically persisted (its default+index puts commit together in
> one `WriteBatch`) and produced. The partition is considered "flipped" ONLY once
> `__ttl_enabled__` is written in the final batch after the last chunk. Until
> then the partition is legacy.** A crash at any point before the final flag-batch
> leaves a partially-restamped-but-still-legacy store that re-runs the backfill
> cleanly (§4). New-message processing is blocked for the entire backfill (it runs
> synchronously inside `prepare()`); there is no interleaving.

**Processing-stays-paused confirmation (concurrency constraint):** the backfill
runs inside `RocksDBPartitionTransaction.prepare()` →
`_maybe_flip_or_reject()` → `backfill_legacy_records()`
(`transaction.py:429-490`), which is called synchronously from
`Checkpoint.commit()` step 2 (`checkpoint.py:244`). The partition transaction
model is single-threaded per partition; `commit()` is a blocking call on the
processing thread and no new records are consumed/processed for this partition
until it returns. The chunk loop is ordinary synchronous Python inside that call.
Therefore **chunking does not introduce any interleaving** — new messages cannot
process against a half-migrated store. The only change vs single-shot is that the
work is split into multiple `_write` + `flush` calls *within the same blocking
`prepare()`*. The atomic-flip correctness (convert all → flag → resume) is
preserved; chunking only bounds memory. **This is the concurrency constraint from
the brief, satisfied: backfill stays sequential with processing.**

One subtlety to call out for ArchDev: the chunk loop issues `_db.write()` per
chunk **before** the transaction's own `flush()`. That is a deliberate departure
from the "transaction writes once at flush" model, but it is sound because (a)
the writes are to keys the transaction is not otherwise touching in its cache
(pre-existing keys, minus `staged_default_keys`), and (b) they are
forward-only-convergent (§4). Document this in the method docstring.

---

## 7. Config knob + default — DECISION

Add a chunk-size knob. **Decision: a `RocksDBOptions` field**, not a bare module
constant — consistent with the sibling `max_evictions_per_flush` knob
(`options.py:83`) and with `legacy_records_ttl` itself, and it lets an operator
on a tight memory limit (the exact failure here) tune it down without a code
change.

```python
# quixstreams/state/rocksdb/options.py  (RocksDBOptions, frozen dataclass)
legacy_backfill_chunk_size: int = 10_000
```

- **Default 10_000.** Justification: at the observed ≈1.5-2 KB per cached/produced
  record (§2), 10k records ≈ 15-30 MB of transient Python + a similar-sized
  `WriteBatch` + one chunk of producer queue ≈ well under 100 MB peak — comfic
  inside a 500 MB container with RocksDB block cache (default 128 MB,
  `options.py:70`) and recovery residual. It also matches the existing
  `max_evictions_per_flush` default (10_000), so the two bounded-batch mechanisms
  share a mental model.
- **Validation:** strictly positive `int`; `<= 0` raises `ValueError` in
  `__post_init__` (mirror the `legacy_records_ttl` validation, `options.py:86-94`).
- **Plumbing:** add to `RocksDBOptionsType` protocol (`types.py`), read in
  `partition.__init__` into `self._legacy_backfill_chunk_size`, pass into
  `backfill_legacy_records`. Same wiring pattern as `legacy_records_ttl`
  (`partition.py:103-105`) and `max_evictions_per_flush` (`partition.py:97`).
- **Inertness:** like `legacy_records_ttl`, it only matters on the one backfilling
  flush; ignored otherwise and on windowed/timestamped stores.

---

## 8. Memory target

**Peak memory ≈ `chunk_size` × per-record-cost, independent of total store
size.** With the default `chunk_size = 10_000` and the observed per-record cost
(≈1.5-2 KB live Python in the chunk list + the chunk's `WriteBatch` ≈ on-disk
size × ~2 + one chunk of producer queue), the transient backfill working set is
on the order of **30-80 MB**, flat regardless of whether the store holds 165k or
50M records. The 165k-record store that OOM'd at 500 MB would now back-fill in
≈17 chunks, each releasing its memory before the next, never exceeding the
one-chunk working set on top of the steady-state RocksDB footprint.

Contrast: single-shot held ≥4 whole-store copies → ≈ O(store) and unbounded.

**Test assertion (if feasible, §9):** with a small `chunk_size` (e.g. 100) and a
store of, say, 10k records, assert that the number of `_write` calls ==
`ceil(10_000 / 100)` and (if a memory probe is available) that peak RSS growth
during backfill is bounded and does not scale with total record count when
`chunk_size` is held fixed across two store sizes.

---

## 9. Edge cases

- **Empty store:** `default_cf.items()` yields nothing; the chunk loop's first
  `take` returns empty → `break` immediately; `restamped == 0`. The caller then
  takes the in-batch flip path exactly as the empty-store fast path. (In practice
  `_maybe_flip_or_reject` only calls backfill when `main_cf_has_user_data()` is
  true, so empty never reaches here — but the loop is empty-safe regardless.)
- **Store smaller than one chunk:** one `take` returns all records (< chunk_size),
  one `WriteBatch`, one produce+flush, one `_write`; flag written last. Identical
  outcome to single-shot, just via the loop.
- **Chunk boundary mid-iteration:** the single forward iterator is held across
  `take` calls; partial reads do not lose or duplicate keys. Per-chunk `_write`s
  put stamped values behind the cursor; they are not revisited in the same run
  (§3.2).
- **Crash between chunk K and the flag:** `__ttl_enabled__` absent → legacy on
  reopen → backfill re-runs; §4 skips/re-stamps the K already-committed chunks and
  finishes the rest; flag written last on the completing run. Convergent.
- **Interaction with the bounded eviction sweep
  (`max_evictions_per_flush`):** independent mechanism. The sweep runs in
  `partition.write()` (`partition.py:325-326`) only when `uses_ttl_stamps` is
  True. During the chunk loop the partition is **still legacy**
  (`uses_ttl_stamps` flips only when the final flip-metadata/runtime flag is set
  by the caller after the loop), so the per-chunk `_write(batch)` calls go through
  the bare `self._write(batch)` (step 4 calls `_write` directly, **not**
  `write()`), bypassing the sweep entirely — no sweep runs during backfill. The
  first sweep happens on the *next* normal flush after the flip, against the
  freshly-built index, bounded by `max_evictions_per_flush` as usual. Confirm
  ArchDev uses `self._write(batch)` (the raw writer, `partition.py:523-528`) in
  the chunk loop, not `self.write(...)`.
- **Windowed / timestamped partitions:** `uses_ttl_stamps = False` at the class
  level; they never reach `_maybe_flip_or_reject`'s backfill branch
  (`spec.md` §10). Unaffected; `legacy_backfill_chunk_size` ignored.
- **No changelog producer (changelog disabled):** step 3 is skipped
  (`changelog_producer is None`); chunks still persist locally via `_write`.
  Matches today's behavior where `_prepare` no-ops without a producer
  (`base/transaction.py:603-604`).
- **`high_water_ms` is None at flip:** unchanged hard-error in
  `_compute_legacy_expiry` (`spec.md` §8.1, `transaction.py:534-540`) — happens
  before the loop starts.

---

## 10. Test plan

### Unit (under `tests/.../state/rocksdb/test_legacy_backfill.py`)

1. **Chunked backfill of a store >> chunk size completes fully.** Seed a legacy
   store with `M` records (e.g. M=1000), set `legacy_backfill_chunk_size=100`,
   trigger backfill. Assert: every pre-existing key is `decode_ttl_value`-able
   with `expires_at == high_water + ttl_ms`; a matching `__ttl_index__` entry per
   key; `__ttl_enabled__` + format version set; `restamped == M`; and the loop
   issued `ceil(M / 100)` chunk `_write`s (spy/count `_write` or `_db.write`).
2. **Crash-before-flag re-runs cleanly.** Run the chunk loop but simulate a crash
   after, say, 3 chunks and **before** the flag batch (inject via a patched
   `_write` that raises on the 4th call, or stop before `_write_flip_metadata`).
   Reopen the partition: assert it loads **legacy** (`uses_ttl_stamps` False,
   `__ttl_enabled__` absent). Trigger backfill again: assert it converges — every
   record ends stamped with the **completing** run's `expires_at_ms`, no
   double-wrapped values (decode twice would fail / payload intact), index
   consistent, flag set.
3. **Already-stamped detection / no double-wrap (the §4 correctness core).**
   Construct a store where some default-CF values are already
   `encode_ttl_value(E_prior, payload)` (simulating a partial prior run) and the
   rest are legacy un-stamped. Run backfill with current `expires_at_ms = E_now`
   (E_now != E_prior). Assert: legacy values → wrapped once with E_now; prior
   E_prior values → re-stamped to E_now with payload intact (single 8-byte
   prefix, original payload recovered by one `decode_ttl_value`); values already
   at E_now → untouched (skipped); no value decodes to a nested stamp.
4. **Per-chunk changelog production recovers identically.** Back-fill with a
   changelog producer and `chunk_size=100` over M records; capture the produced
   messages. Rebuild a fresh partition from the changelog (with
   `legacy_records_ttl` **absent** from config, per `spec.md` Rule 2). Assert the
   recovered stamped values + `__ttl_index__` are identical to a single-shot
   backfill's recovery (same survivor set under the Rule 4 wallclock filter,
   same index). Assert chunk boundaries leave no gap/duplication in offset-order
   replay.
5. **Empty store / sub-chunk store.** Empty → no chunks, no `_write` from the
   loop, in-batch flip only. M < chunk_size → exactly one chunk `_write` + flag.
6. **Config validation.** `legacy_backfill_chunk_size <= 0` raises `ValueError`
   at `RocksDBOptions` construction; default is `10_000`.
7. **Sweep not run during backfill.** Spy `_run_sweep`; assert it is **not**
   called during the chunk loop (partition still legacy), and **is** eligible on
   the next normal flush after the flip.
8. **Memory-bound assertion (if feasible).** With fixed `chunk_size`, back-fill
   two stores of very different sizes (e.g. 1k vs 20k records) under a memory
   probe (`tracemalloc` peak around the loop, or `resource.getrusage` RSS delta).
   Assert peak transient allocation during the loop does **not** scale with total
   record count (within a tolerance) — i.e. it tracks `chunk_size`, not store
   size. If a reliable probe is impractical cross-platform, fall back to asserting
   the **chunk count** scales (test 1) and that no full-store list/dict is ever
   materialized (e.g. assert the cache is never populated with backfill keys —
   `cache.get_updates("default")` contains only the in-batch keys, not the
   pre-existing ones).

### Integration (uses `C:\repos\TTL_test_environment`)

Seed a large legacy dedup state (target the ~165k-record, 800-B-value shape that
OOM'd) on the pre-TTL build under a constrained memory limit; upgrade with
`RocksDBOptions(legacy_records_ttl=..., legacy_backfill_chunk_size=10_000)`;
assert the app **starts and completes the backfill without OOM**, logs the
re-stamped count, existing dedup keys block duplicates until enable_time+ttl in
stream time, a redeploy does not re-backfill, and a wiped-volume restart recovers
identically from the changelog with the config field removed.

---

## 11. Sub-features / work breakdown

1. **`legacy_backfill_chunk_size` config field + validation + plumbing.**
   `options.py`, `types.py`, `partition.__init__`. Owner: ArchDev.
2. **Rewrite `backfill_legacy_records` as the chunk loop** (§3): single iterator,
   per-chunk re-stamp/produce/flush/`_write`, return count; no longer uses the
   transaction cache; skips `staged_default_keys`; idempotent already-stamped
   detection (§4); index-CF re-stamp consistency (§4.3). Owner: ArchDev.
3. **Re-plumb the caller** `_maybe_flip_or_reject` (`transaction.py:480-490`):
   compute `staged_default_keys`, pass producer + headers + chunk_size into
   `backfill_legacy_records`, keep flag-last ordering, keep
   `_restamp_default_cf_cache_for_flip` for in-batch keys only. Owner: ArchDev.
4. **Tests** (§10). Owner: Tester.
5. **Docs:** note the new knob, the chunked migration behavior, and the
   set-once-then-remove migration story (unchanged). Owner: DocuGuy.

---

## 12. Risks & open questions

- **OP-CB-1 (the hardest correctness point):** the already-stamped recognizer on
  re-run (§4.1/§4.2). It must (a) never double-wrap an already-stamped value,
  (b) re-stamp a prior-run stamp to the current expiry using the *payload* (post
  `decode_ttl_value`), and (c) wrap a genuine legacy value whole. The single
  hazard is a legacy value whose leading 8 bytes coincidentally equal the exact
  current `expires_at_ms` (skipped → left un-stamped). Probability is negligible
  (specific 64-bit collision) and is the same heuristic class already accepted in
  `_looks_like_stamped_value` / `_normalize_replay_value` (`spec.md` §6.8).
  **Decision: accept and document; do not add a per-record format marker.**
  ArchDev must get this branch exactly right — it is the correctness core of the
  re-run path.
- **Per-chunk `producer.flush()` cost:** adds a network round-trip per chunk. With
  10k-record chunks this is ≈17 flushes for the 165k store — negligible. If
  profiling shows flush latency dominates for huge stores, the flush could be made
  every K chunks instead of every chunk, trading a bounded multiple of producer
  memory for fewer round-trips. **Default: flush every chunk** (simplest, tightest
  memory bound). Flag if integration shows a latency problem.
- **`producer.offsets` / changelog-offset interaction (§5):** confirm the
  checkpoint's `produced_offsets` (`checkpoint.py:280`) reflects the chunk-produced
  messages so the final `flush()` persists a correct `__changelog_offset__`.
  ArchDev to verify against `InternalProducer.offsets`.
- **Memory backend (`quixstreams/state/memory/partition.py`):** out of scope here
  (it has no `backfill_legacy_records` and its recovery defect is OP-2 in
  `spec.md` §12, pending Ludvík). Do not touch without sign-off.

---

## 13. References

- Base spec: `dev-planning/state-ttl-legacy-backfill/spec.md`
  (§6.2/§6.3 backfill, §8.2 bulk-at-flip, §8.3 large-store/OP-1, §8.4 idempotency,
  §8.5 changelog option a, §10 edge cases)
- Recovery spec: `dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md`
  (Rule 4 wallclock-at-rebuild — unchanged here)
- Backfill to rewrite: `quixstreams/state/rocksdb/partition.py:411-485`
  (`backfill_legacy_records`)
- Caller: `quixstreams/state/rocksdb/transaction.py:441-515`
  (`_maybe_flip_or_reject`, `_compute_legacy_expiry`,
  `_restamp_default_cf_cache_for_flip`, `_write_flip_metadata_to_cache`)
- Already-stamped recognizer to reuse: `partition.py:872-903`
  (`_looks_like_stamped_value`), `929-960` (`_normalize_replay_value`)
- Raw writer: `partition.py:523-528` (`_write`); full write path `275-339`
- Sweep (independent): `partition.py:713-803` (`_run_sweep`)
- Codecs: `quixstreams/state/rocksdb/ttl_codec.py`
  (`encode_ttl_value`, `decode_ttl_value`, `encode_index_key`, `TTL_STAMP_BYTES`,
  `SENTINEL_NEVER`)
- Changelog producer: `quixstreams/state/recovery.py:283-334`
  (`ChangelogProducer.produce` / `.flush`)
- Cache → produce → write flow: `quixstreams/state/base/transaction.py:580-689`;
  checkpoint commit: `quixstreams/checkpointing/checkpoint.py:244-290`
- Config home: `quixstreams/state/rocksdb/options.py`
  (`max_evictions_per_flush:83`, `legacy_records_ttl:84`)
- Metadata keys: `quixstreams/state/rocksdb/metadata.py`
  (`TTL_ENABLED_KEY`, `STATE_FORMAT_VERSION_KEY`, `TTL_INDEX_CF_NAME`)
- Real OOM evidence: deployment log "Backfilled 165445 legacy records…" + hard
  kill, 500 MB limit, 800-B values (user-confirmed: OOM during TTL set on old
  records).
- Shortcut 73191; MEMORY: `quix-cloud-no-state-reset`,
  `state-ttl-legacy-modes-design`
