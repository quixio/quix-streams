# Backfill completeness + fail-safe read (data-corruption fix)

**Status:** Draft
**Project:** quix-streams
**Created:** 2026-06-15
**Planned with:** Buddy
**Addendum to:** `dev-planning/state-ttl-legacy-backfill/spec-chunked-backfill.md`
(supersedes its §4 "already-stamped recognizer" decision — see §3.3 below) and
`dev-planning/state-ttl-legacy-backfill/spec.md` (§6.3/§8.2 backfill, §8.4
idempotency, §8.5 changelog durability).
**Companion (unchanged):** `spec-recovery-wallclock.md` (Rule 4 wallclock-at-rebuild).
**Branch (implements):** additive follow-up to `feature/sc-73191`.
**Severity:** DATA-CORRUPTION — proven on a live Quix Cloud deployment.

---

## 1. Summary

The chunked legacy-records backfill (`spec-chunked-backfill.md`) flipped a
populated store into TTL mode while leaving **at least one default-CF record
un-stamped**. Because the read path
(`transaction.py:_get_bytes`, ~312-339) treats a flipped partition as
"every value is `[8-byte expiry][payload]`" and **unconditionally strips the
first 8 bytes**, reading the un-stamped record chopped 8 bytes off a valid
payload and handed corrupted bytes to the deserializer.

**Real corruption evidence (live deployment):** a stored value

```
{"status":"ON","pad":"xxxxxxxxxxxx…"}
```

came back as

```
":"ON","pad":"xxxxxxxxxxxx…"}
```

— the leading 8 bytes (`{"status`) were silently removed. The JSON
deserializer then raised `StateSerializationError`, and because the bad read sits
on the hot path the deployment entered a **crash-loop** (every restart re-reads
the same un-stamped key and re-fails).

The current `_get_bytes` has a `try/except ValueError → Marker.UNDEFINED` guard
(lines 333-339), but it only fires for values **shorter than 8 bytes**
(`decode_ttl_value` raises only on `len < TTL_STAMP_BYTES`,
`ttl_codec.py:76-81`). A long legacy JSON value is `>= 8` bytes, so it does
**not** raise — it is silently mis-stripped. The guard is therefore not
fail-safe against the failure that actually happened.

Two independent defects combined to produce the corruption; **both must be
fixed**:

- **Defect 1 (completeness):** the backfill did not stamp every pre-existing
  record before writing the flip flag. The store invariant the read path relies
  on — *"partition flipped ⇒ every default-CF value is stamped"* — was violated.
- **Defect 2 (fail-safe):** the read path is not defensive — an un-stamped value
  in a flipped store **corrupts** (strips 8 good bytes) instead of **degrading**
  (returning the value intact).

This addendum specifies **Fix A** (a provably complete backfill that guarantees
the invariant without inferring stamped-ness) and **Fix B** (a fail-safe read
that degrades instead of corrupting). Fix A removes the cause; Fix B removes the
blast radius if the invariant is ever violated again (by this bug's residue on
already-deployed stores, a future regression, or an externally-mutated store).

---

## 2. Root cause — you cannot infer stamped-ness from the bytes

This is the central insight the whole design is built around:

> **There is no reliable way to tell, from a value's bytes alone, whether it is
> already stamped (`[8-byte expiry][payload]`) or a genuine legacy value
> (`[payload]`).**

- **No marker.** `encode_ttl_value` (`ttl_codec.py:55-64`) prepends a raw 8-byte
  big-endian stamp with **no tag, magic, or version byte**. A stamped value and a
  legacy value are byte-sequences from the same universe.
- **Content-sniffing is a guess.** `_looks_like_stamped_value`
  (`partition.py:1010-1041`) asks "do the first 8 bytes decode to a *plausible*
  epoch-ms expiry (`0 < stamp < 10**15`) or `SENTINEL_NEVER`?" That is a
  heuristic. A legacy JSON value beginning `{"status` decodes (as a BE uint64) to
  a number, and that number can easily fall in the plausible range — a false
  positive that strips 8 good bytes. Conversely a stamped value whose payload
  starts oddly is fine, but the recognizer can never be *certain*.
- **Length doesn't work.** Payload sizes vary; there is no fixed baseline length
  to compare against.

**Design consequence:** the fix must **not** try to infer stamped-ness anywhere
on a correctness-critical path. Instead it must **guarantee the invariant** that
makes inference unnecessary:

> **INVARIANT (the contract `_get_bytes` relies on): a partition is flipped
> (`__ttl_enabled__` set) ⇒ every default-CF value is stamped.**

Fix A guarantees the *write* side of that invariant by construction (visit every
key, track completion with a persisted cursor, write the flag last). Fix B makes
the *read* side robust so that even a single violation degrades safely rather
than corrupting. The two are complementary: A prevents the hole, B ensures a hole
is never catastrophic.

---

## 3. Fix A — provably complete backfill (no inference, no skipped keys)

### 3.1 The two structural causes of skipped keys

1. **Iterating while writing the same CF.** The current loop
   (`spec-chunked-backfill.md` §3.2) holds a **single live forward iterator** over
   the default CF (`islice` over `default_cf.items()`) **while** per-chunk
   `self._write(batch)` puts re-stamped values **back into that same CF**.
   RocksDB iterators read from a point-in-time view, but mutating the CF under a
   long-lived iterator across many `_write`s at real scale (200k+ keys, multiple
   SST flushes/compactions triggered mid-iteration) is exactly the kind of
   read-while-write pattern that can **skip or duplicate keys** — and a single
   skipped key is enough to corrupt (§1). This is the prime suspect for the
   un-stamped record.
2. **Inference on re-run.** The re-run path relies on `_looks_like_stamped_value`
   to decide skip-vs-restamp (the OP-CB-1 hazard). Per §2 that is a guess; a
   mis-guess either leaves a value un-stamped (→ corruption) or double-wraps it
   (also corruption).

Fix A eliminates both: **never iterate a CF you are writing**, and **never infer
— track progress explicitly.**

### 3.2 Mechanism: snapshot the key list, then chunk over a fixed list

**Step 0 — capture a stable key list (keys only).**
Before any writing, materialize the complete list of pre-existing default-CF
**keys** (not values) from a single stable read:

- Iterate `default_cf.keys()` once and collect into a Python `list[bytes]` (or
  iterate `.items()` but **keep only the key**, discarding the value). Exclude
  the metadata-CF entries (already a separate CF) and exclude `staged_default_keys`
  (the triggering transaction's own in-batch writes, handled by
  `_restamp_default_cf_cache_for_flip` — see `spec-chunked-backfill.md` §3.4).
- This is a one-time read with **no concurrent writes** to the default CF (the
  backfill is sequential inside `prepare()`; processing is paused —
  `spec-chunked-backfill.md` §6), so the key list is a complete, consistent
  census of every key that must be stamped.

**Step 1 — chunk over the fixed key list, reading each value fresh.**
For each slice of `chunk_size` keys from the captured list:

```
for chunk_keys in batched(key_list, chunk_size):
    batch = WriteBatch(raw_mode=True)
    produced = []
    for key in chunk_keys:
        value = default_cf.get(key)        # fresh point read, NOT an iterator
        if value is None:
            continue                       # deleted since census — skip (§5)
        stamped = encode_ttl_value(expires_at_ms, value)   # wrap WHOLE value
        batch.put(key, stamped, default_handle)
        batch.put(encode_index_key(expires_at_ms, key), b"", index_handle)
        produced.append((key, stamped))
    # produce this chunk's default-CF records to the changelog, then flush
    for key, stamped in produced:
        changelog_producer.produce(key=key, value=stamped, headers=default_cf_headers)
    if changelog_producer is not None:
        changelog_producer.flush()
    self._write(batch)                     # atomic: default + index puts together
    _advance_cursor(self, count_done)      # PERSIST progress (§3.3)
```

**Why this guarantees completeness:** the set of keys to stamp is **frozen up
front**. The write loop reads each key by **point get** (`default_cf.get(key)`)
— it never relies on a live iterator over the CF it is mutating. Every key in the
census is visited **exactly once**, deterministically, regardless of SST
flushes, compactions, or the re-stamped values landing back in the CF. There is
no read-while-write skip hazard because the read driver (the frozen list) is
independent of the CF's live structure.

**No inference anywhere.** On the first run, every value is wrapped **whole**
(`encode_ttl_value(expires_at_ms, value)`) — we never ask "is this already
stamped?", because a not-yet-flipped store is **all legacy by precondition** (the
flip flag is absent; the partition opened legacy). The recognizer
`_looks_like_stamped_value` is **not used** in the backfill correctness path.

### 3.3 Persisted cursor — deterministic, inference-free re-run

A crash mid-backfill must resume **without guessing** which keys are already
stamped. Track progress with a **persisted cursor** in the metadata CF:

- **New metadata key `__ttl_backfill_progress__`** (additive; see §7). It records
  the number of keys stamped so far **and** enough to resume deterministically.
  Two viable encodings (ArchDev picks; recommend the second):
  - (a) an integer count `N` — resume by re-stamping the **same frozen key list**
    from index `N` onward. Requires the key list to be **reproducible in the same
    order** across runs. RocksDB `default_cf.keys()` yields keys in sorted
    byte order deterministically, so re-censusing on re-run produces the **same
    ordered list**, and skipping the first `N` is exact. **Recommended** — small,
    simple, and the re-census is cheap (keys only).
  - (b) the **last-stamped key** `K_last` — resume by re-censusing and dropping
    every key `<= K_last` (sorted order). Equivalent guarantee; slightly more
    robust if a key is deleted between runs (a count could then over/under-shoot
    by the deleted key). Acceptable alternative.
- The cursor is written **in the same `WriteBatch` as each chunk's puts** (or
  immediately after via `_write`) so cursor-advance and stamped-data commit
  **atomically per chunk**. Crash semantics: on reopen the partition is still
  legacy (flag absent, §3.4), the next `ttl=` write re-enters backfill, the
  census reproduces the identical ordered list, and the cursor says "resume at
  N" — every key from N to the end is stamped, keys before N are **known** done
  (not guessed). The store converges with **zero reliance on byte-sniffing**.
- A re-run never re-stamps an already-stamped key (the cursor excludes it), so
  there is **no double-wrap risk** and no need to detect double-wraps. This is the
  key improvement over `spec-chunked-backfill.md` §4, whose re-run correctness
  rested on `_looks_like_stamped_value` (the OP-CB-1 hazard). **This addendum
  retires that hazard for the backfill path.**

### 3.4 Flag-last ordering (unchanged anchor, now cursor-gated)

`__ttl_enabled__` + `__ttl_format_version__` are still written **last**, only
after the cursor reaches the end of the key list (every census key stamped + every
chunk produced + flushed). Concretely:

1. Census the key list (§3.2 step 0).
2. Chunk loop stamps + produces + advances the cursor (§3.2 step 1, §3.3).
3. Only when the cursor == `len(key_list)` does the caller
   (`_maybe_flip_or_reject`) proceed to stamp the in-batch cache
   (`_restamp_default_cf_cache_for_flip`) and write the flip metadata in the final
   batch (`spec-chunked-backfill.md` §3.3).

A crash at **any** point before the final flag-batch leaves `__ttl_enabled__`
absent → the partition reopens **legacy** → the read path takes the
`_stamps_enabled(cf_name) is False` branch (`transaction.py:319-322`) and returns
values **raw** (no stripping) — so a partially-backfilled-but-not-flipped store
**reads correctly** in the meantime. The cursor lets the eventual re-run finish
the job deterministically. The flag-last + cursor pair is what makes re-runs safe
**without inference**.

### 3.5 What is removed / retired

- The single live forward iterator over the default-CF-being-written is removed.
- `_looks_like_stamped_value` is **no longer called by the backfill** (it remains
  for the recovery flag-discovery path, `partition.py` recovery — out of scope
  here, see §6).
- `spec-chunked-backfill.md` §4's exact-match-on-`expires_at_ms` re-stamp logic
  is **superseded**: with a persisted cursor a re-run never re-reads a stamped key
  at all, so there is no already-stamped-detection branch to get right.

---

## 4. Fix B — fail-safe read (degrade, never corrupt)

### 4.1 The current unsafe read

`_get_bytes` on a flipped partition (`transaction.py:327-339`):

```python
raw = super()._get_bytes(...)
if raw is Marker.UNDEFINED or raw is Marker.DELETED:
    return raw
try:
    stamp, payload = decode_ttl_value(cast(bytes, raw))   # strips 8 bytes
except ValueError:
    return Marker.UNDEFINED        # ONLY fires when len(raw) < 8
# ... filter on stamp, return payload
```

`decode_ttl_value` raises only for `len < 8` (`ttl_codec.py:76-81`). A long
legacy JSON value is `>= 8` bytes → no raise → **silent mis-strip → corruption**.

### 4.2 The fail-safe rule

> **On a flipped partition, only treat a value as stamped if it robustly decodes
> to a stamp. Otherwise treat it as legacy / never-expires and return it RAW.**

Concretely, replace the strip-or-`UNDEFINED` logic with a strict validator
applied **before** stripping:

```python
raw = super()._get_bytes(...)
if raw is Marker.UNDEFINED or raw is Marker.DELETED:
    return raw
decoded = _safe_decode_stamp(cast(bytes, raw))   # (stamp, payload) | None
if decoded is None:
    # NOT a recognizable stamp → this is (or may be) a legacy un-stamped value.
    # Degrade: return it RAW and never-expires, do NOT strip. Log once.
    return cast(bytes, raw)
stamp, payload = decoded
# ... existing expiry filter on `stamp`, return `payload`
```

`_safe_decode_stamp(value)` returns `(stamp, payload)` only when the value
**robustly looks like a stamp**, else `None`:

- `len(value) >= TTL_STAMP_BYTES`, **and**
- the leading 8 BE bytes decode to either `SENTINEL_NEVER` **or** a plausible
  epoch-ms expiry (`0 < stamp < 10**15` — the same bound as
  `_looks_like_stamped_value`, `partition.py:1040`). This excludes the most common
  false positives (`SENTINEL`-collisions and absurd magnitudes) while accepting
  every real stamp.

If `_safe_decode_stamp` returns `None`, the value is returned **raw** — i.e. the
deployment that hit this bug would now return `{"status":"ON","pad":"…"}`
**intact** (treated as never-expires) instead of crash-looping. A missed/
un-stamped record degrades to **"didn't expire"**, which for the Maxio dedup
workload means at worst a legacy dedup key keeps blocking duplicates slightly
longer than intended — a benign degradation, not a crash and not data loss.

**Emit a single throttled WARNING** when this branch fires, naming the partition
and key prefix, so the condition is observable (it should be impossible once Fix
A ships, but its presence signals either a pre-Fix-A store or an external
mutation).

### 4.3 Residual ambiguity (the honest caveat)

`_safe_decode_stamp` is still a **heuristic** — it is the same class of guess as
§2 warns about, just applied in the **safe direction**. The two residual
mis-classifications:

- **Legacy value whose first 8 bytes decode to a plausible expiry.** Mis-treated
  as stamped → 8 bytes stripped → corruption *for that value*. This is the
  **only** residual corruption path, and it is exactly the original bug's shape.
  Fix A's completeness guarantee makes it vanishing in practice: after a complete
  Fix-A backfill, **every** legacy value has been overwritten by a genuinely
  stamped value, so there are no un-stamped values left for `_safe_decode_stamp`
  to mis-classify. The residual only bites a store that was flipped by the
  **buggy** (pre-Fix-A) backfill and left a hole whose leading bytes happen to
  look like a stamp.
- **Stamped value mis-read as legacy.** Cannot happen for a value that
  `_safe_decode_stamp` accepts; a real stamp is always `>= 8` bytes and decodes to
  `SENTINEL_NEVER` or a plausible expiry, so genuine stamps are always accepted.
  (The only way a real stamp is rejected is if a real expiry exceeds `10**15` ms —
  year ~33658 — which is not a realistic event-time.)

### 4.4 The per-value format marker — evaluation and recommendation

A per-value **format marker** (e.g. a 1-byte tag `0x01` prefixing every stamped
value, or a magic byte distinguishing `[tag][8-byte stamp][payload]` from legacy
`[payload]`) is the **only** way to make stamped-vs-legacy **100% unambiguous** —
it removes the §4.3 residual entirely and would let `_get_bytes` decide with
certainty.

**Tradeoffs:**

- **Pro:** eliminates the residual corruption path completely; no heuristic
  anywhere; future-proof against any backfill bug.
- **Con (significant):** it **changes the on-disk and changelog value format**
  for every TTL store — a format-version bump (`STATE_FORMAT_VERSION` 2 → 3),
  +1 byte per value, and a migration path for stores already flipped under v2
  (which have **stamp-without-marker** values). Adding a marker now would itself
  require a backfill-style migration of every already-stamped store, and a v2
  changelog would replay marker-less values into a v3 reader — re-introducing
  exactly the same "can't tell the format from the bytes" problem at the v2/v3
  boundary that we are trying to escape. It also touches the shared codec
  (`ttl_codec.py`) and therefore the recovery path and the sweep, widening blast
  radius well beyond this fix.

**Recommendation: do NOT add a per-value marker now.** Fix A's completeness
guarantee + Fix B's safe-direction degrade reduce the residual to "a store
flipped by the *buggy* backfill, containing an un-stamped value whose first 8
bytes coincidentally decode to a plausible expiry." That is a narrow, transient
population (only pre-Fix-A stores) and the failure mode under Fix B is a single
mis-stripped value that degrades observably (logged) rather than crash-loops.
The marker's cost — a format-version bump, a second migration of all v2 stores,
and a fresh v2/v3 changelog ambiguity — is not justified to close a residual that
Fix A already empties.

**Record the marker as the designated escalation** (§6, OP-BC-1): if a future
feature needs *new* legacy values to coexist with stamped values in the same
flipped CF (breaking Fix A's "every value stamped" guarantee by design), a
per-value marker with a format-version bump becomes the correct answer, and this
section is the prior art.

### 4.5 Shared-path impact (per the brief)

`_get_bytes` is the shared default-CF read path. The change touches **only the
flipped-partition branch** (the `_stamps_enabled` is-True path, lines 327-339);
the legacy branch (319-322) is untouched, and unflipped stores are byte-identical
as before. **Recovery (Rule 4) and the sweep (Rule 3 / `_run_sweep`) call
`decode_ttl_value` directly, not `_get_bytes`** — confirm ArchDev does **not**
route recovery/sweep through `_safe_decode_stamp` unless deliberately hardening
them too. The recovery flag-discovery heuristic (`_looks_like_stamped_value`) and
`_normalize_replay_value` are **separate** and out of scope here; if ArchDev
chooses to consolidate the "is this a plausible stamp" logic into one shared
`_safe_decode_stamp` used by both read and recovery, that is acceptable **as a
refactor only** — it must not change recovery behavior, and must be called out in
the PR. Default: keep the read-path validator local and leave recovery as-is.

---

## 5. Memory analysis

Fix A adds **one new bounded structure**: the frozen key list.

- **Key list cost.** Keys only (`bytes`), held for the whole backfill. Per the
  dedup workload, a serialized key is ~30 B on disk; as a live Python `bytes`
  object the cost is ~30 B payload + ~50 B object/list-slot overhead ≈ **~80 B
  per key** (conservative).
  - **200k keys:** 200_000 × ~80 B ≈ **~16 MB** (the brief's ~6 MB is the raw
    30 B/key figure; ~16 MB is the realistic live-Python figure with overhead).
    Comfortably inside a 500 MB container.
  - **1M keys:** ≈ **~80 MB**. Still bounded and acceptable.
  - **10M keys:** ≈ **~800 MB** — this would itself blow a 500 MB container.
    For stores in the multi-million-key range the key list must be **chunked from
    disk too** (see below).
- **Per-chunk cost** is unchanged from `spec-chunked-backfill.md` §8: one chunk's
  values + `WriteBatch` + one chunk of producer queue ≈ **30-80 MB** at
  `chunk_size=10_000`, released between chunks.

**Total Fix-A peak ≈ key-list + one-chunk ≈ (16 MB + 30-80 MB) ≈ ~50-100 MB** for
the 200k store — flat in chunk terms, plus the linear-in-keycount key list.

**Multi-million-key safeguard (additive, recommend specifying):** if
`len(key_list) × ~80 B` would be a concern, census the keys to a **spill file on
disk** (`.tmp`-style temp, sorted append) instead of an in-memory list, and stream
the cursor over the file. RocksDB's own sorted iteration already gives ordered
keys, so an alternative is to **re-derive the ordered key stream by point-seeking**
rather than holding the full list — but that re-introduces iterate-while-write
risk unless seeking a **read-only snapshot handle** taken before the first write.
**Recommendation:** for the current Maxio scale (hundreds of k) hold the key list
in memory (≤~80 MB at 1M). Specify the disk-spill / snapshot-handle path as the
documented mechanism for ≥ several-million-key stores, gated behind the same
`legacy_backfill_chunk_size` knob (when the list would exceed, say, a configurable
key-count threshold, spill). Flag this as OP-BC-2 (§6) for ArchDev to size.

---

## 6. Risks, constraints, open questions

- **OP-BC-1 — per-value marker deferred.** Decision in §4.4: do **not** add a
  marker now. The residual (§4.3) is a pre-Fix-A-store-only, single-value,
  observable-degradation hazard. Marker is the escalation if a future design
  must mix legacy + stamped values in one flipped CF.
- **OP-BC-2 — key-list memory at multi-million scale (§5).** In-memory list is
  fine to ~1M keys (~80 MB). For ≥ several million, spill the key census to disk
  or use a read-only snapshot handle for ordered re-seek. ArchDev to size against
  a realistic max and pick a threshold; default in-memory for now.
- **Already-corrupted deployed stores (the live incident).** A store flipped by
  the buggy backfill already has an un-stamped record on disk. Fix B makes its
  reads **degrade instead of crash** (the crash-loop stops). Fix A does **not**
  retroactively re-stamp it (the store is already flipped; backfill won't re-run).
  **Open question for Ludvík:** do we need a one-shot **repair pass** for stores
  flipped by the bug (re-census + re-stamp any value `_safe_decode_stamp` rejects,
  guarded by a new repair flag)? Recommend: Fix B alone unblocks the crash-loop;
  add the repair pass only if a flipped store is found to have mis-stripping
  values that Fix B's heuristic can't catch. (Cross-ref MEMORY
  `quix-cloud-no-state-reset`: customers can't reset, so a code-side repair is the
  only operator-callable recovery.)
- **Constraint — sequential / no parallelism.** Unchanged: backfill runs
  synchronously inside `prepare()`, processing paused
  (`spec-chunked-backfill.md` §6). The census + chunk loop is ordinary synchronous
  Python.
- **Constraint — Rule 3 / recovery unchanged** except the shared-read-path note in
  §4.5. Recovery (wallclock filter) and the sweep call `decode_ttl_value`
  directly and are not touched by Fix B unless ArchDev deliberately consolidates
  (must not change behavior; PR call-out required).
- **Cursor + changelog-offset interaction.** The per-chunk cursor advance writes
  to the metadata CF (LOCAL_ONLY, not produced). The changelog-offset bookkeeping
  is unchanged from `spec-chunked-backfill.md` §5. Confirm the new metadata key is
  in `LOCAL_ONLY_CFS`-protected metadata and never produced.

---

## 7. Data & interface contracts

- **New metadata key `__ttl_backfill_progress__`** in the metadata CF
  (`metadata.py`, alongside `TTL_ENABLED_KEY` / `STATE_FORMAT_VERSION_KEY`).
  Value: either an int count `N` (recommended, §3.3 option a) or the last-stamped
  key bytes (§3.3 option b). Written/updated per chunk **atomically with that
  chunk's puts**. Deleted (or ignored) once `__ttl_enabled__` is set; on an
  already-flipped store it is irrelevant. **No format-version bump** (additive
  metadata key; legacy and v2 stores simply never have it).
- **No change to the value codec or `STATE_FORMAT_VERSION`** (the marker is
  rejected, §4.4). On-disk value layout stays `[8-byte stamp][payload]` for
  stamped values; legacy values stay `[payload]`.
- **`backfill_legacy_records` signature** gains nothing required beyond what
  `spec-chunked-backfill.md` §3.1 already specifies; it now (a) censuses keys
  first, (b) reads each value by point-get, (c) advances the persisted cursor. It
  no longer holds a live forward iterator over the default CF and no longer calls
  `_looks_like_stamped_value`.
- **New read helper `_safe_decode_stamp(value) -> tuple[int, bytes] | None`** in
  `transaction.py` (or a shared module), used by `_get_bytes` (§4.2).

---

## 8. Edge cases

- **The bug's exact case (un-stamped record in a flipped store):** Fix B returns
  it raw + never-expires, logs once; **no crash, no corruption**. (Test §9.1.)
- **Key deleted between census and stamping:** `default_cf.get(key)` returns
  `None`; skip it (§3.2). It was deleted, so it needs no stamp; the index has no
  entry for it. No correctness impact.
- **Key inserted after census:** impossible — processing is paused during
  backfill, so no new default-CF keys appear (`spec-chunked-backfill.md` §6). Any
  in-batch write is in `staged_default_keys`, excluded from the census and stamped
  by `_restamp_default_cf_cache_for_flip`.
- **Crash mid-chunk (before that chunk's `_write`):** cursor not advanced for the
  chunk; flag absent; reopen legacy; re-run re-censuses (identical ordered list)
  and resumes from the cursor — the partial chunk is redone from scratch (its
  earlier puts, if any landed, are overwritten with identical stamped values —
  idempotent because the expiry is recomputed from the **completing** run's
  high-water and re-stamping a legacy value is deterministic). (Test §9.3.)
- **Crash after cursor advance but before flag (between chunks):** reopen legacy;
  re-run resumes at the cursor; converges; flag written last. (Test §9.3.)
- **Empty store / sub-chunk store:** census yields 0 / <chunk_size keys; loop runs
  0 / 1 chunks; flag last. Identical outcome to `spec-chunked-backfill.md` §9.
- **Legacy value whose first 8 bytes look like a stamp, in a Fix-A-completed
  store:** cannot occur — Fix A overwrote every legacy value with a genuine stamp.
  The §4.3 residual only applies to pre-Fix-A stores.
- **Changelog disabled:** chunks persist locally via `_write`; no produce. Cursor
  still advances. Unchanged.
- **`high_water_ms` None at flip:** hard error before the census, as today.

---

## 9. Test plan

Under `tests/.../state/rocksdb/test_legacy_backfill.py`.

1. **Fail-safe read — flipped store with a deliberately un-stamped record does
   NOT corrupt (the core Fix-B regression).** Construct a flipped partition
   (`__ttl_enabled__` set), then write a default-CF value **without a stamp**
   directly (bypass the stamping path) using a realistic JSON payload
   (`{"status":"ON","pad":"x"*64}`) whose first 8 bytes are `{"status`. Read it
   back via the normal `get`/`get_bytes` path. **Assert:** the returned value is
   **byte-identical to the original** (no 8-byte strip), **no
   `StateSerializationError`**, and a WARNING was logged. Repeat with a payload
   whose first 8 bytes coincidentally decode to a plausible expiry to document the
   §4.3 residual (this one *will* strip — assert the test captures/expects that as
   the known residual, not as a pass for the common case).
2. **Multi-chunk backfill stamps EVERY key — no skips.** Seed a legacy store with
   `M` keys (e.g. `M=1000`), `chunk_size=100`. Run backfill. **Assert:** for
   **every** one of the `M` keys, the on-disk value `_safe_decode_stamp`-decodes to
   `expires_at == high_water + ttl_ms` with the original payload intact; a
   matching `__ttl_index__` entry per key; **count of stamped keys == M with zero
   un-stamped keys** (iterate the whole default CF and assert *every* value
   decodes to a valid stamp); `__ttl_enabled__` + format version set;
   `__ttl_backfill_progress__` cleared/at end.
3. **Crash-mid-backfill resumes via cursor and converges.** Patch `_write` (or the
   cursor-advance) to raise after, say, 3 chunks and **before** the flag batch.
   **Assert:** reopened partition loads **legacy** (`__ttl_enabled__` absent),
   `__ttl_backfill_progress__` reflects ~3 chunks, and reads of the first 3
   chunks' keys return correctly (raw/legacy via Fix B — store not flipped yet).
   Trigger backfill again. **Assert:** it resumes from the cursor (re-census yields
   the identical ordered list; keys before the cursor are **not** re-read/re-
   stamped — spy `default_cf.get` to confirm), converges so **every** key ends
   stamped with the **completing** run's `expires_at_ms`, **no double-wrapped
   value** (each value decodes with exactly one `decode_ttl_value` to the original
   payload), index consistent, flag set.
4. **No reliance on `_looks_like_stamped_value` in backfill.** Spy/patch
   `_looks_like_stamped_value` to raise if called during `backfill_legacy_records`;
   assert it is **never invoked** by the backfill path (it may still be used by
   recovery, which this test does not exercise).
5. **200k-scale coverage assertion (completeness at scale).** Seed a store at a
   representative large scale (e.g. 200k keys, smaller padding to keep the test
   tractable, or a scaled proxy with a fixed seed), run backfill with
   `chunk_size=10_000`. **Assert:** a full final scan of the default CF finds
   **zero** values that `_safe_decode_stamp` rejects (i.e. 100% stamped),
   `restamped == 200_000`, chunk-`_write` count == `ceil(200_000/10_000) == 20`,
   and the key-list peak allocation tracks key-count not value-size (tie to the
   §5 memory model; `tracemalloc` peak around the census ≈ key-count × ~80 B
   within tolerance). If 200k is too slow for CI, run the strict 100%-stamped
   assertion at the largest tractable size and assert the chunk-count formula at
   200k via a mocked census.
6. **Census excludes `staged_default_keys`.** With an in-batch `ttl=` write that
   triggers the flip, assert the triggering key is **not** in the census (not
   double-handled) and is stamped by `_restamp_default_cf_cache_for_flip` with its
   **own** true stamp, while every other pre-existing key gets the uniform legacy
   expiry.
7. **Key deleted between census and stamp is skipped cleanly.** Simulate a key
   present at census time but `default_cf.get` → `None` at stamp time; assert it is
   skipped, no exception, no dangling index entry.
8. **Recovery parity (regression guard).** Back-fill with a changelog producer
   (`chunk_size=100`), rebuild a fresh partition from the changelog with
   `legacy_records_ttl` **absent** (Rule 2). Assert the recovered stamped store +
   index are identical to a single-pass backfill's recovery (same survivor set
   under the Rule 4 wallclock filter). Confirms Fix A's per-chunk production is
   recovery-identical and Fix B's read change did not alter recovery.

### Integration (uses `C:\repos\TTL_test_environment`)

Reproduce the live incident shape: seed a large legacy dedup store (~165k+
records, 800-B padded JSON values like `{"status":"ON","pad":"…"}`) on the
pre-TTL build under a constrained memory limit; upgrade with
`RocksDBOptions(legacy_records_ttl=…, legacy_backfill_chunk_size=10_000)`.
**Assert:** the app starts and **completes** the backfill with **zero un-stamped
keys** (no crash-loop), reads of every dedup key return intact values,
`StateSerializationError` does **not** appear in logs, a redeploy does not
re-backfill, and a wiped-volume restart recovers identically from the changelog
with the config field removed. Additionally, seed a store with a known
hand-crafted un-stamped record in an already-flipped store and confirm Fix B
degrades (raw return + WARNING) rather than crash-loops.

---

## 10. Sub-features / work breakdown

1. **Fix A — census-then-chunk backfill with persisted cursor.** Rewrite
   `backfill_legacy_records` (`partition.py`): census keys → chunk over frozen
   list → point-get each value → wrap whole → per-chunk `WriteBatch` + produce +
   flush + cursor-advance. Remove the live forward iterator; remove the
   `_looks_like_stamped_value` dependency. Add `__ttl_backfill_progress__`
   handling. Owner: ArchDev.
2. **`__ttl_backfill_progress__` metadata key + plumbing.** `metadata.py`; ensure
   it is local-only and never produced. Owner: ArchDev.
3. **Fix B — fail-safe read.** Add `_safe_decode_stamp`; change `_get_bytes`'s
   flipped-branch to degrade-to-raw on non-stamp, with a throttled WARNING.
   Owner: ArchDev.
4. **Tests (§9).** Owner: Tester. Cases 1, 2, 3, 5 are mandatory (the brief's
   named tests); 4, 6, 7, 8 are guards.
5. **Docs.** Note the fail-safe read behavior, the cursor-based resume, and (if
   accepted) the repair-pass story for pre-Fix-A stores. Owner: DocuGuy.
6. **(Open) Repair pass for already-corrupted deployed stores** (OP-BC-1 follow-up,
   §6) — pending Ludvík. Owner: TBD.

---

## 11. Alternatives considered

- **Keep the iterate-while-write loop, just "fix" the recognizer.** Rejected —
  any recognizer is a guess (§2); a skipped key still corrupts under the current
  read path. The structural cause (iterate-while-write) must go.
- **Infer stamped-ness on re-run via `_looks_like_stamped_value`
  (`spec-chunked-backfill.md` §4).** Superseded — the persisted cursor makes
  re-runs deterministic with no inference (§3.3, §3.5).
- **Per-value format marker (§4.4).** Rejected for now — eliminates the residual
  but forces a `STATE_FORMAT_VERSION` bump, a second migration of all v2 stores,
  and a fresh v2/v3 changelog ambiguity. Designated escalation if a future feature
  must mix legacy + stamped values in one flipped CF.
- **Read-path only (Fix B without Fix A).** Rejected — Fix B degrades reads but
  leaves the store with un-stamped holes and an unreliable backfill; the dedup
  semantics would silently drift. Both fixes are required.
- **Backfill-only (Fix A without Fix B).** Rejected — Fix A prevents new holes but
  does nothing for stores **already** flipped by the bug (they're past the
  idempotency gate and won't re-backfill). Fix B is what stops the live
  crash-loop. Both required.

---

## 12. References

- This addendum's parents: `dev-planning/state-ttl-legacy-backfill/spec.md`,
  `spec-chunked-backfill.md` (supersedes its §4), `spec-recovery-wallclock.md`
  (unchanged), `open-points.md` (OP-1, resolved).
- Read path to fix (Fix B): `quixstreams/state/rocksdb/transaction.py:312-339`
  (`_get_bytes`).
- Backfill to rewrite (Fix A): `quixstreams/state/rocksdb/partition.py`
  (`backfill_legacy_records`, `_restamp_one_for_backfill`).
- Heuristic being retired from the backfill path: `partition.py:1010-1041`
  (`_looks_like_stamped_value`); related `_normalize_replay_value:1067-1098`.
- Value codec (no marker, no version bump): `quixstreams/state/rocksdb/ttl_codec.py`
  (`encode_ttl_value:55-64`, `decode_ttl_value:67-83`, `TTL_STAMP_BYTES:41`,
  `SENTINEL_NEVER:47`).
- Metadata keys (new `__ttl_backfill_progress__`):
  `quixstreams/state/rocksdb/metadata.py` (`TTL_ENABLED_KEY`,
  `STATE_FORMAT_VERSION_KEY`, `TTL_INDEX_CF_NAME`).
- Flip-metadata writer: `partition.py:991-1008` (`_stamp_flip_metadata`).
- Real corruption evidence: live Quix Cloud deployment — flipped store with an
  un-stamped record; `{"status":"ON","pad":"…"}` read back as
  `":"ON","pad":"…"}` (first 8 bytes stripped) → `StateSerializationError` →
  crash-loop. 500 MB container, 800-B padded values.
- Shortcut 73191; MEMORY: `quix-cloud-no-state-reset`,
  `state-ttl-legacy-modes-design`, `feedback-operator-callable-upgrades`.
```