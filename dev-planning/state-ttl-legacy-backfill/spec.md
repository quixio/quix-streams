# State TTL — Legacy-Store Backfill on Enable

**Status:** Converged (final) — Rule 3 REMOVED by design (see §0 / §6.7); §8.7 stamped-vs-legacy header signal added (OP-3 resolved)
**Project:** quix-streams
**Created:** 2026-06-15
**Planned with:** Buddy
**Shortcut:** 73191 — "update state behavior of dedup feature to treat old states"
**Customer:** Maxio (dedup workload)
**Companion spec:** `dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md`
(recovery-clock change; §8.x recovery rules below are the final, agreed form)

---

## 0. Converged design (final) — the four rules

These are **decisions**, not options. All are implemented. Rule 3 was briefly
built and then **REMOVED by design** (see Rule 3 below) — TTL is strictly
per-write.

1. **Activation gate — TTL only when used in code.** `legacy_records_ttl`
   (`RocksDBOptions`) set in config does **nothing on its own**. The partition
   flips into TTL mode **only** when application code performs a real
   `state.set(..., ttl=...)` write. The flush-time detector
   (`transaction.py:_maybe_flip_or_reject`, ~443-446) returns early unless the
   batch actually contains a TTL write (`_batch_has_ttl_writes`). Backfill and
   every other TTL behavior are gated on that flip. **If the code never uses
   `ttl=`, the store stays legacy / byte-identical to v3.23.6 and
   `legacy_records_ttl` is inert — everything below is skipped.** Status:
   implemented.

2. **Backfill is a durable one-time migration.** On the flipping write, if the
   default CF is populated **and** `legacy_records_ttl` is set → re-stamp every
   pre-existing record with a uniform expiry **and produce the re-stamped values
   into the changelog** (option (a), §8.5). Because the stamps are now durable in
   the changelog, **`legacy_records_ttl` does NOT need to remain configured after
   backfill** — it is a *set-once-then-remove* migration trigger. Idempotency:
   once `__ttl_enabled__` is set the partition is flipped and never re-backfills,
   whether or not the flag stays in config. Status: implemented. (This retracts
   the earlier "config must persist for recovery correctness" claim — see §8.1,
   §8.5, §9.)

3. **~~No never-expires data once active~~ — REMOVED by design.** This rule
   would have floored a no-`ttl=` write on a flipped, `legacy_records_ttl`-set
   partition to `event_time + legacy_records_ttl` instead of `SENTINEL_NEVER`,
   making `legacy_records_ttl` an effective store-wide default TTL. **It was
   removed (shortcut 73191, decision by Ludvík).** Rationale: TTL must stay
   **strictly per-write** — only `state.set(..., ttl=...)` ever sets an expiry.
   The team explicitly avoided a store-wide / constructor default TTL, and a
   no-`ttl=` floor reintroduces exactly that through the back door.
   `legacy_records_ttl` is now **ONLY a one-time migration knob** for
   pre-existing legacy records (applied via the backfill in Rule 2); it must NOT
   affect any steady-state write. **Current behavior:**
   `transaction.py:_compute_stamp` returns `SENTINEL_NEVER` whenever `ttl is
   None`, unconditionally — byte-identical to the pre-Rule-3 / v3.23.6 semantics.
   A no-`ttl=` write is never-expires regardless of flip state or
   `legacy_records_ttl`. (For the Maxio dedup workload this is moot either way —
   every dedup write passes `ttl=`.) **Status: REMOVED — implemented (see §6.7).**

4. **Recovery — wallclock-at-rebuild filter (final).** Changelog rebuild drops a
   stamped entry **iff `stamp != SENTINEL_NEVER and stamp <= wallclock_now`**,
   where `wallclock_now` is captured **once** per rebuild session
   (`partition.py:255-258`). The old stamp-ratchet recovery clock
   (`recovery_now = high_water`, `advance_high_water(stamp)`) is **removed**.
   Un-stamped legacy values encountered during recovery are replayed **verbatim**
   (legacy / never-expires); but post-backfill the changelog carries the
   **stamped** versions, which supersede them, so backfilled records come back
   stamped. Full rationale and trade-offs live in
   `spec-recovery-wallclock.md`. Status: implemented (RocksDB only — see open
   item OP-2 in §12 for the unfixed memory backend).

> **Stamped-vs-legacy discovery is no longer value-content-based (§8.7).** Recovery
> originally inferred "this replayed value is stamped → flip the partition into TTL
> mode" by *peeking at the value bytes* (`_looks_like_stamped_value`). That heuristic
> is **unsound** — a legacy dedup value that is an 8-byte BE epoch-ms timestamp is
> numerically indistinguishable from an expiry stamp — and is the confirmed root
> cause of the §8.6 cold-restore first-enablement bug (OP-3). **§8.7 replaces it with
> a per-record changelog message header that carries the stamped/legacy bit
> out-of-band.** §8.7 supersedes the "no new recovery signal" / "no new keys" wording
> in §7.2 and §8.6.2 **for this one header** (it is transport metadata, not a value-
> format or on-disk-key change).

> **First-ever enablement on a cold-restored store** — when the changelog holds
> ONLY un-stamped legacy records and no prior backfill has run, the recovered
> records must land in the default CF so the Rule 2 backfill fires on the first
> `ttl=` write. This is a **gap** Rule 2/4 above did not cover; see the new
> **§8.6**. The unambiguous flip signal that makes this correct is **§8.7**.

---

## 1. Summary

The per-write State TTL feature (v3.24.0+) refuses to enable TTL on a populated
legacy store: the first `state.set(..., ttl=...)` write on a default CF that
already holds un-stamped data raises `IncompatibleStateStoreError`. That path is
unusable in Quix Cloud, which has no customer-callable state reset. This spec
adds an opt-in **backfill** mode driven by a single config field
`legacy_records_ttl` on `RocksDBOptions`.

The converged semantics (see §0):

- **Nothing happens unless the application code actually uses `ttl=`.** Config
  alone never touches a legacy store; the partition only flips when a real TTL
  write lands (Rule 1). No-`ttl=` workloads remain byte-identical to v3.23.6 and
  `legacy_records_ttl` is inert.
- When a TTL write lands on a populated legacy store **and** `legacy_records_ttl`
  is set, the partition re-stamps its pre-existing un-stamped records with a
  uniform expiry, populates the secondary expiry index, produces the re-stamped
  values to the changelog, and flips into TTL mode — all in place, no state
  deletion (Rule 2). Because the stamps are durable on the changelog, the config
  field is a **one-time migration trigger** that can be removed after the
  backfill commits.
- A write with **no** `ttl=` is **always never-expires** (`SENTINEL_NEVER`),
  regardless of flip state or `legacy_records_ttl`. TTL is strictly per-write;
  `legacy_records_ttl` never floors steady-state writes (Rule 3 REMOVED).
- Cold-restore recovery judges expiry against **wallclock-at-rebuild**, not a
  stamp-derived clock (Rule 4 / companion spec). Recovery learns whether a
  replayed default-CF record is stamped from a **per-record changelog header**
  (§8.7), never from value content. **First-ever enablement on a cold-restored
  store** (changelog has only un-stamped legacy records) is covered in §8.6 and
  made correct by §8.7.

## 2. Goals

- Let an operator enable TTL on a populated legacy store without deleting state,
  by setting `legacy_records_ttl` and letting the application's existing `ttl=`
  writes trigger an in-place backfill.
- Apply a well-defined, documented uniform expiry to the legacy records since
  their true age is unrecoverable (event-time reference clock, §8.1).
- Keep the change additive: one config field, one new branch in the flip/reject
  decision, one new partition method. (`_compute_stamp` is unchanged from
  v3.23.6 for the `ttl is None` path — Rule 3 was removed.)
- Preserve byte-for-byte backward compatibility whenever the application code
  never performs a `ttl=` write (Rule 1 — the activation gate).
- Keep the backfill idempotent and **durable on the changelog**, so the config
  field does not have to persist after the migration (Rule 2).
- Keep cold-restore recovery correct against real time and free of the
  uniform-expiry collapse (Rule 4 / companion spec).
- **Make first-ever enablement work on a cold-restored store** whose changelog
  holds only un-stamped legacy records, by ensuring those records land in the
  active default CF so the Rule 2 backfill fires (§8.6).
- **Make stamped-vs-legacy discovery during recovery reliable and unambiguous**
  via a per-record changelog header, eliminating the value-content heuristic that
  false-positives on 8-byte legacy timestamp values (§8.7 — resolves OP-3).
- Replace the "delete the state directory" error text with operator-callable
  guidance.

## 3. Non-goals

- Recovering the *true* age of legacy records (impossible — they carry no
  timestamp). We assign a uniform reference expiry instead.
- Touching windowed / timestamped partitions (they opt out of TTL stamps at the
  class level via `uses_ttl_stamps = False`; backfill must skip them entirely —
  they never reach the flip path).
- A general "wipe state" mode (evaluated in §9, not recommended).
- Per-key or per-prefix custom TTLs for legacy records.
- A store-wide default TTL for no-`ttl=` writes. TTL is strictly per-write;
  no-`ttl=` writes are always never-expires (`SENTINEL_NEVER`). This is why
  Rule 3 (the no-`ttl=` floor) was removed.
- Stamping legacy records during recovery replay (Option 2, rejected in §8.6.2);
  recovered legacy records are replayed verbatim and stamped only by the
  existing first-`ttl=`-write backfill.
- Changing the on-disk value codec or format version. §8.7 adds a **transport**
  header to changelog messages only; the stored value layout (`8B‖value`) is
  unchanged (OP-3 option B explicitly rejected, §8.7 / §9).

## 4. User stories / scenarios

1. **Maxio dedup upgrade.** Maxio runs a production dedup store on
   quix-streams 3.23.x and upgrades. They cannot reset state in Quix Cloud. They
   set `legacy_records_ttl=timedelta(days=7)` and redeploy. Their dedup code
   already calls `state.set(..., ttl=...)`, so on the first such write the
   partition backfills all existing dedup keys with a single expiry 7 days after
   the enable moment (in stream time) and produces the re-stamped values to the
   changelog; new dedup keys get true event-time expiry. **They can then remove
   `legacy_records_ttl` from config** — the migration is durable. No state
   deletion, no manual ops.
2. **Config set but code never uses `ttl=` (Rule 1).** An operator sets
   `legacy_records_ttl` but the application never performs a `ttl=` write. **The
   store is untouched** — no flip, no backfill, byte-identical to v3.23.6. The
   config field is inert.
3. **Operator forgets the opt-in.** Application performs a `ttl=` write on a
   populated legacy store but `legacy_records_ttl` is unset. They get the loud
   rejection — but the new message points at setting `legacy_records_ttl`, not at
   deleting state.
4. **Redeploy after backfill.** The deployment restarts. The partition opens,
   reads `__ttl_enabled__`, and resumes TTL mode. Backfill does **not** run
   again, regardless of whether `legacy_records_ttl` is still in config.
5. **Cold restore.** The state volume is lost; the partition recovers from the
   changelog. The backfilled stamps were produced into the changelog at flip
   time, **each carrying the `__ttl_stamped__` header (§8.7)**, so recovery
   rebuilds an identical stamped store + index — **without needing
   `legacy_records_ttl` in config and without sniffing value content**. Survivors
   are judged against wallclock-at-rebuild (Rule 4).
5a. **First-ever enablement on a cold-restored store (§8.6 / §8.7).** A fresh
   consumer group / wiped volume restores a store whose changelog holds **only
   un-stamped legacy records** (no prior backfill ever ran — those records carry
   **no** `__ttl_stamped__` header). Recovery sees header-absent on every record,
   keeps the partition legacy, and replays the records verbatim into the default
   CF; the first live `ttl=` write then sees a **populated legacy** store and runs
   the normal backfill — re-stamping the recovered records `high_water +
   legacy_records_ttl` and producing stamped (header-bearing) values to the
   changelog. (Live-observed gap, 2026-06-16; §8.6. Made correct by the §8.7
   header — pre-§8.7 the value-content heuristic false-flipped these records and
   dropped them.)
6. **Permanent key while active (Rule 3 REMOVED).** In a flipped partition with
   `legacy_records_ttl` set, the app calls `state.set(k, v)` with no `ttl=`. It
   **lives forever** (`SENTINEL_NEVER`) — `legacy_records_ttl` does NOT floor it.
   TTL is strictly per-write. (Earlier Rule 3 would have floored this; removed by
   design.) Note: this write still **carries the `__ttl_stamped__` header**
   because the partition is in TTL mode — the value is `SENTINEL_NEVER`-stamped on
   the wire (§8.7 matrix, no-`ttl=` post-flip row).
7. **Fresh / empty store.** Operator sets `legacy_records_ttl` on an empty store;
   the first `ttl=` write flips via the empty-store fast path (backfill finds
   nothing).

## 5. Proposed design

The flip-vs-reject decision lives in one place:
`RocksDBPartitionTransaction._maybe_flip_or_reject()`
(`transaction.py:418-492`), called from `prepare()` before the parent produces
changelog records.

**Activation gate (Rule 1):** the method returns immediately unless
`self._batch_has_ttl_writes` is true and the partition is not already flipped
(`transaction.py:443-446`). This is the gate that makes config alone inert.

Terminal cases once the gate passes:

1. default CF **empty** → flip (empty-store fast path); stamp the in-batch cache.
2. default CF **populated** and `legacy_records_ttl` is `None` → **reject**
   (with the operator-callable message).
3. default CF **populated** and `legacy_records_ttl` is set → **backfill**:
   re-stamp every pre-existing on-disk default-CF record with a uniform expiry,
   stage the re-stamped values into the transaction update cache (so the existing
   changelog producer emits them — durability per Rule 2), populate
   `__ttl_index__`, write the flip metadata, then flip.

**Rule 3 (REMOVED):** `_compute_stamp` is unchanged from v3.23.6 for the
`ttl is None` path — it always returns `SENTINEL_NEVER`. `legacy_records_ttl`
never touches a steady-state write; it is consumed only by the backfill (§8.1).
See §6.7 for the removal rationale.

**Recovery (Rule 4)** is the wallclock-at-rebuild filter in
`recover_from_changelog_message` (`partition.py:183-273`), specified in full in
`spec-recovery-wallclock.md`. **Recovery decides stamped-vs-legacy from the
per-record `__ttl_stamped__` changelog header (§8.7), not from value content.**
**For the recovered records to be reachable by the populated-store backfill on
first-ever enablement, see §8.6.**

Why this shape: it is additive, keeps a single source of truth for the flip
metadata and value layout, and the backfilled values reach the changelog through
the ordinary producer path so recovery needs no special-case wiring.

## 6. Sub-features / work breakdown

### 6.1 Config parameter on `RocksDBOptions` — DONE
- **What it does:** exposes the opt-in. Default `None` preserves the reject.
- **Touchpoints:** `quixstreams/state/rocksdb/options.py` (field + docstring);
  `quixstreams/state/rocksdb/types.py` (`RocksDBOptionsType` protocol); threaded
  into `partition.py:__init__` (`self._legacy_records_ttl`,
  `partition.py:103-105`) and exposed via the `legacy_records_ttl` property
  (`partition.py:156-162`).
- **Owner:** ArchDev. **Status:** implemented.

### 6.2 Backfill branch in the flip/reject decision — DONE
- **Touchpoints:** `transaction.py:_maybe_flip_or_reject` (the populated +
  `legacy_records_ttl` set branch, ~457-467); reuses
  `_restamp_default_cf_cache_for_flip` and `_write_flip_metadata_to_cache`.
- **Owner:** ArchDev. **Status:** implemented.

### 6.3 On-disk re-stamp of pre-existing records — DONE
- **Touchpoints:** `RocksDBStorePartition.backfill_legacy_records(expires_at_ms,
  cache)` (called from `transaction.py:463-466`); iterates the default CF, wraps
  each value with `encode_ttl_value`, writes the index entry, stages re-stamped
  values into the transaction cache (Rule 2 durability).
- **Owner:** ArchDev. **Status:** implemented.

### 6.4 Idempotency guard — DONE
- **What it does:** backfill runs exactly once. After it runs `__ttl_enabled__`
  is set; on any later open the partition loads already-flipped
  (`partition.py:127`), so `_maybe_flip_or_reject` returns at the already-flipped
  branch (`transaction.py:445-446`). Holds whether or not `legacy_records_ttl`
  stays in config (Rule 2).
- **Owner:** Tester (assertion), ArchDev (no-op). **Status:** implemented.

### 6.5 Error-message fix — DONE
- **Touchpoints:** `reject_ttl_on_populated_store` in `partition.py` — message +
  `extra` log fields point at `legacy_records_ttl`, not deleting state.
- **Owner:** ArchDev; DocuGuy updates docs. **Status:** implemented.

### 6.6 Docs & upgrade note
- **What it does:** documents the opt-in, the activation gate (Rule 1), the
  set-once-then-remove migration nature (Rule 2), the per-write-only TTL contract
  (no store-wide default — Rule 3 removed), the reference-clock semantics, the
  first-enablement-on-cold-restore path (§8.6), the `__ttl_stamped__` recovery
  header (§8.7), and the Maxio upgrade path.
- **Touchpoints:** TTL feature docs under `docs/`, `RocksDBOptions` docstring.
- **Owner:** DocuGuy. **Status:** ready (Rule 3 removed, code is stable).

### 6.7 No-`ttl=` writes while active (Rule 3) — **REMOVED by design**
- **History:** Rule 3 briefly floored a no-`ttl=` write on a flipped,
  `legacy_records_ttl`-set partition to `event_time + legacy_records_ttl` instead
  of `SENTINEL_NEVER`. **It was removed** (decision by Ludvík, shortcut 73191).
- **Rationale:** TTL must stay **strictly per-write** — only
  `state.set(..., ttl=...)` ever sets an expiry. The team explicitly avoided a
  store-wide / constructor default TTL; the no-`ttl=` floor reintroduces exactly
  that store-wide default through the back door. `legacy_records_ttl` is ONLY a
  one-time migration knob for pre-existing legacy records (the §6.2 backfill); it
  must not affect any steady-state write.
- **Current behavior:** `transaction.py:_compute_stamp` returns `SENTINEL_NEVER`
  unconditionally when `ttl is None` — byte-identical to pre-Rule-3 / v3.23.6:
  ```python
  def _compute_stamp(self, ttl, timestamp) -> int:
      if ttl is None:
          return SENTINEL_NEVER
      ...
  ```
  No flip-state or `legacy_records_ttl` check in the `ttl is None` branch. The
  explicit-`ttl=` path and its `timestamp is None` validation are unchanged.
- **Unaffected:** backfill (§6.2 / §8.1) still stamps pre-existing legacy records
  via `_compute_legacy_expiry`, which uses `legacy_records_ttl` directly and is
  not routed through the `ttl is None` branch. Recovery, fail-safe-read, and
  chunked/completeness are untouched.

### 6.8 Recovery-landing of legacy records for first-ever enablement — DESIGN (delivered via §8.7)
- **What it does:** ensures that un-stamped legacy records recovered from the
  changelog are present in the active default CF such that
  `main_cf_has_user_data()` returns True at flip time, so the existing §6.2
  backfill branch fires on the first `ttl=` write. **No new stamping logic** —
  the change is making the recovered records reachable by the existing machinery.
- **Root cause (CONFIRMED by ArchDev, OP-3):** the records DID write to the
  default CF; they were then **dropped** because the value-content flip-discovery
  heuristic false-flipped the partition into TTL mode and the Rule 4 wallclock
  filter dropped the (past-dated) 8-byte legacy values. The corrective fix is the
  per-record header signal — see **§8.7**, which subsumes this sub-feature.
- **Touchpoints:** see §8.7.3.
- **Dependencies:** §6.2 backfill (must remain the stamping path), Rule 4
  recovery filter (unchanged for the stamped-restore case).
- **Owner:** ArchDev (implements §8.7), Tester (regression). **Status:** root
  cause confirmed; fix specified in §8.7.

## 7. Data & interface contracts

### 7.1 Config field — DONE
```python
# quixstreams/state/rocksdb/options.py  (RocksDBOptions, frozen dataclass)
legacy_records_ttl: Optional[timedelta] = None
```
- **Default `None`** = preserve current reject-on-populated behavior. Parameter
  name confirmed by Ludvík — do **not** rename.
- **Inertness (Rule 1):** a non-`None` value does nothing until the application
  performs a `ttl=` write; on an unflipped store with no `ttl=` writes the field
  has zero effect.
- **Migration lifetime (Rule 2):** required only for the one backfilling flush.
  After `__ttl_enabled__` is durable, the field may be removed from config.
- **Validation:** strictly positive `timedelta` if set; reject `<= 0` with
  `ValueError`. Reuse `_ttl_to_ms`.

### 7.2 On-disk layout (unchanged formats, reused)
- Backfilled value: `encode_ttl_value(expires_at_ms,
  original_value_bytes)`. Identical to a normal stamped write.
- Index entry: `encode_index_key(expires_at_ms, user_key)` → `b""` in
  `__ttl_index__`.
- Metadata: `__ttl_enabled__ = b"\x01"`, `__ttl_format_version__ = 2`.
- No new keys, no new CFs, no format-version bump **on disk**.

> **Amended by §8.7 (transport only).** §8.7 adds a new **changelog message
> header** `__ttl_stamped__`. This is **not** an on-disk key, CF, or value-format
> change — the stored value layout (`8B‖value`) and the on-disk metadata keys are
> unchanged. The "no new recovery signal" constraint that §8.6.2 carried is
> **explicitly superseded for this header** (it rides the changelog transport, it
> is not stamped into RocksDB). See §8.7 for the full contract and rationale.

### 7.3 Expiry formulas
- **Backfill (Rule 2):** `expires_at_ms = enable_time_ms + _ttl_to_ms(legacy_records_ttl)`,
  `enable_time_ms = high_water_ms` (§8.1). For the §8.6 cold-restore case,
  `high_water_ms` is the **first live `ttl=` write's** event-time after recovery
  (§8.6.4) — a full fresh window, not immediate expiry.
- **Explicit `ttl=` (steady state, unchanged):**
  `expires_at_ms = record_event_time_ms + _ttl_to_ms(ttl)`.
- **No-`ttl=` write (always, Rule 3 removed):** `SENTINEL_NEVER`, regardless of
  flip state or `legacy_records_ttl`. TTL is strictly per-write.

## 8. Reference-clock semantics, idempotency, recovery

### 8.1 enable_time reference clock — DECISION: event-time high-water

`enable_time_ms = self._partition.high_water_ms` (`_compute_legacy_expiry`,
`transaction.py:494-518`), hard-erroring if `None` rather than inventing a
wall-clock expiry.

Rationale: the live TTL read filter and the sweep both compare against
event-time `high_water_ms`, not wall-clock, so backfill must too. For dedup,
"a legacy key stops blocking duplicates `legacy_records_ttl` of *stream time*
after enable" is the right semantic.

**Documented consequence:** existing records expire `legacy_records_ttl` after
the enable moment in stream time, not after their true (unknown) age.

> **RETRACTION (Rule 2).** Earlier drafts of this section / §8.5 / §9 stated that
> `legacy_records_ttl` "must persist in config for recovery correctness." **That
> is no longer true and is retracted.** Backfill produces the re-stamped values
> into the changelog (§8.5 option (a)), so the stamps are durable independent of
> config. `legacy_records_ttl` is a **set-once-then-remove migration trigger**;
> idempotency is anchored on the durable `__ttl_enabled__` flag, not on the
> config field. Recovery (Rule 4) reads stamps from the changelog and needs no
> config field at all. (Note: keeping `legacy_records_ttl` set after migration
> has **no ongoing effect** — it only ever drives the one-time backfill, which is
> idempotent. Rule 3 was removed, so it never floors steady-state writes.)

### 8.2 Backfill timing — DECISION: bulk re-stamp at flip (not lazy)

Re-stamp all existing records in one operation at the flip flush. Atomicity +
idempotency: the re-stamp and the `__ttl_enabled__` flag commit together.
Recovery simplicity: bulk-at-flip produces all stamped values into the changelog
so recovery rebuilds identically (§8.5). Read-path cost: lazy stamping would add
a permanent hot-path branch; bulk pays once.

### 8.3 Large-store concern (Maxio may have millions of keys)

Bulk-at-flip iterates the whole default CF once. Stream the iterator and stage
puts; do not hold both old and new copies of every value. Backfill must complete
in full before `__ttl_enabled__` is written (flag-last ordering preserves
idempotency). Emit a single INFO log with the re-stamped key count
(`transaction.py:480-486`). Open item OP-1 (§12): batch-size strategy for very
large CFs.

### 8.4 Idempotency

Once backfill completes, `__ttl_enabled__` is persisted. On the next open the
partition loads already-flipped, so `_maybe_flip_or_reject` returns at the
already-flipped branch and never re-enters backfill. **Invariant: backfill runs
only on a not-yet-flipped partition, and `__ttl_enabled__` is written last in the
flip batch.** This invariant does **not** depend on `legacy_records_ttl`
remaining in config (Rule 2).

### 8.5 Changelog / recovery consistency — DECISION: option (a)

Backfilled stamped values for pre-existing keys are **staged into the transaction
update cache** so the existing changelog producer emits them naturally
(`backfill_legacy_records` stages them; `_restamp_default_cf_cache_for_flip`
skips those keys via `skip_keys` to avoid double-wrapping,
`transaction.py:520-565`). This reuses the existing recovery path verbatim:
recovery flag-discovery flips a recovering partition on the first **header-bearing**
stamped default-CF replay (§8.7 — was: on the first value-content match,
`partition.py:201-217`), and the wallclock filter (Rule 4) decides survivors.

> **RETRACTION (Rule 2), continued.** Because the backfilled stamps are on the
> changelog, recovery reconstructs the stamped store **without** reading
> `legacy_records_ttl`. The field is therefore not required at recovery time, and
> the earlier "config must persist for recovery correctness" wording is
> withdrawn here too. Un-stamped legacy values may still appear in the changelog
> for the pre-migration history, but the later stamped versions supersede them on
> replay, so backfilled keys come back stamped.

### 8.6 First-ever enablement on a cold-restored store (changelog holds ONLY un-stamped legacy records) — NEW

> **Scope of this section.** §8.5 and Rule 2 assumed the changelog already
> carries **stamped** values produced by a prior in-place backfill, so a later
> cold restore merely inherits them via the flag-discovery + Rule 4 path. The
> case **never covered**: the *first-ever* TTL enablement happens **after** a
> cold restore (state volume wiped / fresh consumer group), so the changelog
> holds **only un-stamped legacy records** and **no prior backfill has ever
> run**. This section closes that gap. Decision: **Option 1** (Ludvík,
> 2026-06-16). Companion cross-ref: `spec-recovery-wallclock.md` (Rule 4). The
> reliable signal that distinguishes the un-stamped legacy records from stamped
> ones is specified in **§8.7** (this is the OP-3 acceptance fix for §8.6).

#### 8.6.1 Problem statement

Live observation (Quix Cloud, 2026-06-16, fresh consumer group v7.4):

1. A genuine `quixstreams==3.23.6` seeder filled a v7.4 store with ~214,879
   **un-stamped legacy** records; the changelog likewise holds ~214,879
   un-stamped records.
2. The sc-73191 TTL build was deployed against a **wiped local volume** → full
   cold-restore recovery of all 214,879 changelog messages → log line
   "Recovery successful".
3. On the first live `ttl=` write, the flip logged **"empty-store fast path"**
   (`transaction.py:593-598`), **NOT** "Backfilled 214879 legacy records"
   (`transaction.py:587-592`).
4. `rocksdb_exact_keys` read **0** during/after recovery; post-flip the key
   count grew from 1 with only new live writes.

Two distinct failures, in order:

- **(a) Records did not land** in the active default CF that
  `main_cf_has_user_data()` (`partition.py:372-381` → `_main_cf_has_user_data`,
  `partition.py:1062-1070`) and live processing read. **CONFIRMED root cause
  (OP-3):** the records DID write to the default CF, but the value-content
  flip-discovery heuristic (`_looks_like_stamped_value`) false-positived on the
  8-byte BE epoch-ms legacy values, flipped the recovering partition into TTL
  mode, and the Rule 4 wallclock filter then dropped every (past-dated) legacy
  value as expired. So at flip time the default CF was effectively empty → the
  flip took the empty-store fast path.
- **(b) No backfill stamp.** Because the flip saw "empty", the populated-store
  backfill branch (`transaction.py:547-569`) never fired, so the legacy records
  received **no** TTL stamp and **no** `__ttl_index__` entry — and worse, they
  had already been dropped.

Why §8.5 / Rule 2 did not cover it: Rule 2's idempotency-and-durability
reasoning is anchored on the changelog **already** carrying stamped values (from
an in-place backfill that ran before any restore). The recovery flag-discovery
path only flips when it judges a default-CF replay value "stamped" — and the
**value-content** judge (`_looks_like_stamped_value`) cannot tell a legacy
8-byte epoch-ms value apart from a real stamp. **§8.7 replaces the value-content
judge with a per-record header**, which is the correct, unambiguous signal.

#### 8.6.2 Required behavior (Option 1)

The legacy un-stamped records recovered from the changelog **MUST** be present in
the active default CF such that `main_cf_has_user_data()` returns **True** at flip
time. Then the **existing** populated-store backfill branch
(`transaction.py:547-569`, `backfill_legacy_records`, `partition.py:438+`) fires
on the first live `ttl=` write **exactly as in the in-place path** — re-stamping
each record `high_water + legacy_records_ttl`, producing stamped values to the
changelog, chunked, idempotent.

**Option 1 mechanism (one line):** recovered legacy records (which carry **no**
`__ttl_stamped__` header, §8.7) land **verbatim** (un-stamped) in the default CF
during replay → the first live `ttl=` write sees a populated legacy store and
runs the normal backfill, which stamps them.

Explicitly **NOT** Option 2: do **not** stamp during recovery replay. Rejected
because it needs a recovery-time clock the changelog does not carry, it
duplicates the backfill stamping logic, and it fights Rule 4.

> **Constraint update.** This subsection previously stated "no new recovery
> signal." §8.7 **supersedes that** for the single `__ttl_stamped__` changelog
> header: the header IS the new recovery signal, and it is exactly what makes
> Option 1 correct (header-absent → land verbatim; header-present → stamped path).
> No stamping logic is added to recovery — only a header *read* replaces the
> value-content peek.

#### 8.6.3 Root-cause (CONFIRMED — OP-3, 2026-06-16)

ArchDev confirmed and reproduced the cause. Of the five original hypotheses, the
mechanism is **none of 1-4** (CF-handle identity, recovery-vs-processing instance,
flush visibility, and `group_by` were all refuted) — it is a **sixth**: the
**value-content flip-discovery heuristic** `_looks_like_stamped_value`
(`partition.py:1029-1060`) false-positives on legacy values whose first 8 bytes
are a big-endian epoch-ms timestamp (`0 < stamp < 10^15`). That flips the
recovering partition into TTL mode (`partition.py:228-244`); subsequent legacy
records take the stamped main-CF branch and the Rule 4 wallclock filter
(`partition.py:285`) drops them as already-expired. A genuine expiry stamp and a
legacy epoch-ms value occupy the **same** numeric range, so threshold tuning
cannot fix it. The corrective design is **§8.7** (out-of-band header signal). The
two `xfail(strict=True)` repro tests in
`tests/.../test_rocksdb/test_first_enablement_cold_restore.py::TestColdRestoreFalseFlipRepro`
pin this bug; flipping them to passing is the §8.7 acceptance gate.

#### 8.6.4 high_water semantics (settles the immediate-expiry concern)

The backfill stamp is computed at the **first live `ttl=` write after recovery**,
in `_compute_legacy_expiry` (`transaction.py:600-624`):
`expires_at = high_water_ms + legacy_records_ttl`. At that moment `high_water_ms`
is the **live triggering record's event-time** ("now" in stream time), because
the `ttl=` write advanced the high-water with its own timestamp before the flip
(`advance_high_water`, `partition.py:191-200`). Therefore the recovered legacy
records receive a **full fresh** `legacy_records_ttl` window from the enable
moment — **NOT** immediate expiry. This is the same backfill validated on the
in-place 175k store yesterday (drained gradually, not en masse).

> **Residual risk (document-only, not a blocker).** The backfill stamps in
> **event-time** (`high_water`); Rule 4 recovery judges survivors in **wallclock**
> (`spec-recovery-wallclock.md` §3). These two clocks diverge only when replaying
> **far-historical** data — i.e. a later cold restore of the now-stamped store,
> run long after the stamps' event-time, could drop records the live event-time
> stream would still consider current (the §5 trade-off of the companion spec).
> For the live Maxio dedup case the two clocks track each other and there is no
> divergence. Surface in DocuGuy's recovery docs; no code change.

#### 8.6.5 Idempotency on re-restore (backfill is once-ever)

After this first-enablement backfill runs, it **produces stamped values to the
changelog** — and, per §8.7, **each carries the `__ttl_stamped__` header**.
Consequently a **subsequent** wipe + restore replays header-bearing stamped
values, so the recovery flag-discovery path fires on the first such replay (now
keyed on the header, not value content), flips the recovering partition, and the
Rule 4 wallclock filter judges survivors — **the already-working path, now made
reliable by §8.7**.

Required invariants for ArchDev to preserve:

- The second restore **MUST NOT** re-backfill or double-stamp. Idempotency is
  anchored on the durable stamped values + the `__ttl_enabled__` / format-version
  metadata stamped at flip — not on `legacy_records_ttl` staying in config
  (Rule 2). With §8.7 the double-stamp guard becomes a **header lookup**: a
  header-present record is already stamped and must route to Rule 4, never into
  backfill (§8.7.5).
- A record stamped by the first backfill and then re-stamped would be wrong (it
  would reset the expiry window). The flag-discovery flip and the
  `_normalize_replay_value` round-trip must leave already-stamped values
  byte-identical on the second restore.
- The §8.7 header read must **only** route the all-un-stamped-changelog case to
  the legacy verbatim branch. It must not change how a stamped-changelog restore
  lands, and it must not cause a stamped restore to be classified as "populated
  legacy" (which would wrongly re-enter backfill).

#### 8.6.6 Interaction matrix (NEW row + confirmation of unchanged rows)

| Restore / store state | Changelog contents | First `ttl=` write outcome | Path | Status |
|---|---|---|---|---|
| **cold-restore, wiped local** | **ONLY un-stamped legacy** (no `__ttl_stamped__` header) | recovered records land in default CF → flip sees **populated legacy** → **backfill + flip** | §8.6.2 / §8.7 → existing `backfill_legacy_records` | **NEW (this section)** |
| cold-restore, wiped local | has **stamped** values (`__ttl_stamped__` header present) | recovery flag-discovery flips on first header-bearing replay; Rule 4 wallclock filter selects survivors; first `ttl=` write is a normal flipped write | §8.7 header read + Rule 4 | **unchanged behavior, reliable signal** |
| in-place, populated local legacy | n/a (local data present) | flip sees populated legacy → **backfill + flip** | §6.2 / §8.5 | **unchanged** |
| empty store (cold or fresh) | empty / none | empty-store fast path flip | `transaction.py:593-598` | **unchanged** |

#### 8.6.7 Test plan (first-enablement-on-cold-restore)

**Integration (cold-restore scenario, the regression that caught this):**

1. Seed a changelog with **N un-stamped legacy** records (real/simulated v3.23.6
   producer path — un-stamped values, **no `__ttl_stamped__` header**), **empty
   local state**.
2. Deploy the TTL build with `RocksDBOptions(legacy_records_ttl=timedelta(...))`;
   let recovery replay all N messages.
3. Perform a single live `ttl=` write.
4. **Assert** the flip logged **"Backfilled N legacy records"**, **NOT**
   "empty-store fast path".
5. **Assert** all N recovered records now `decode_ttl_value`-able with
   `expires_at == high_water + legacy_records_ttl` (event-time of the triggering
   write), and each has a matching `__ttl_index__` entry.
6. **Assert** the records expire ~`legacy_records_ttl` after the first live
   `ttl=` write **in stream time**, confirming the full-fresh-window semantics of
   §8.6.4 (not immediate expiry).
7. **Assert** a **second** wipe + restore (now replaying the stamped,
   header-bearing values produced in steps 3-5) does **NOT** re-backfill (no
   "Backfilled" log on the second enable), survivors selected by Rule 4, and no
   record is double-stamped (expiry unchanged from step 5).

**Unit (recovery-landing fix, under `tests/.../state/rocksdb/`):** see §8.7.7 for
the header-specific cases (these supersede the earlier "hypotheses 1-3" probes,
now that the root cause is confirmed).

### 8.7 Reliable stamped-vs-legacy signal — per-record changelog header (OP-3 RESOLUTION) — NEW

> **Decision (Ludvík, 2026-06-16): OP-3 option A, per-record-header variant.**
> The stamped/legacy bit is carried out-of-band in a **per-record changelog
> message header**, set on every default-CF record produced while the partition is
> in TTL mode. This supersedes the value-content heuristic `_looks_like_stamped_value`
> in the recovery flip-discovery path. **This section explicitly authorizes the
> new changelog header that §7.2 ("no new keys, no format-version bump") and
> §8.6.2 ("no new recovery signal") previously forbade — for this header only, and
> only on the changelog transport. The on-disk value codec and metadata keys are
> unchanged.**

#### 8.7.1 Problem recap

The recovery flip-discovery gate inferred "this replayed default-CF value is
stamped → flip into TTL mode" by **peeking at the value bytes**
(`_looks_like_stamped_value`, `partition.py:1029-1060`): it accepts any value
whose leading 8 bytes decode as `SENTINEL_NEVER` or `0 < stamp < 10^15`. A legacy
dedup value is commonly an **8-byte big-endian epoch-ms timestamp** (~1.7e12),
which lands squarely inside that range, so the heuristic **false-positives** and
flips a recovering legacy partition into TTL mode. The Rule 4 wallclock filter
then drops every (past-dated) legacy record as expired, emptying the default CF;
the first `ttl=` write sees "empty" and skips backfill — the §8.6 cold-restore
bug. Content-based detection is **fundamentally insufficient**: a genuine expiry
stamp and a legacy epoch-ms value occupy the **same** numeric range, so no
threshold can separate them. An **out-of-band, unambiguous** signal is required.
(Full confirmed root cause: OP-3 in `open-points.md`; §8.6.3.)

#### 8.7.2 Decided design

Add a per-record boolean **changelog message header**:

```python
# quixstreams/state/metadata.py  (alongside the two existing changelog headers)
CHANGELOG_TTL_STAMPED_HEADER = "__ttl_stamped__"
```

**Why a per-record header, not a single dedicated marker message** (OP-3 option A
came in two shapes; the per-record variant is chosen): a one-off marker message
("the partition flipped at offset X") can **age out** of the changelog via topic
retention or be removed by **log compaction** (compaction keeps only the latest
value per key, so a marker under a unique key could be the sole record for that
key and survive, but a marker that shares semantics with data is fragile, and any
retention-based eviction loses it entirely). A **per-record header rides every
default-CF record** and therefore survives compaction (each surviving record
carries its own bit) and retention (whatever records remain carry their own bit).
The signal can never be separated from the data it describes.

**Why not OP-3 option B (per-value format byte):** rejected — it is a value-format
change. It bumps the codec / format version (§7.2 forbids), and it breaks
byte-identity with stores already stamped by the current build. The header is pure
transport metadata; the stored bytes are untouched.

**When the header is set (produce side):** on **every** `default`-CF record
produced while the partition is in TTL mode (`uses_ttl_stamps == True` at produce
time) — i.e. from the flip onward. This is **broader** than "this particular write
carried `ttl=`": after the flip, even a no-`ttl=` write is `SENTINEL_NEVER`-stamped
and carries the 8-byte prefix on the wire, so it too must be marked stamped.

Produce-side matrix:

| Record produced when | value on wire | `__ttl_stamped__` header |
|---|---|---|
| pre-flip / legacy (3.23.6 or unflipped) | raw, no 8-byte prefix | **ABSENT** → recovery replays verbatim |
| backfill flush (re-stamped legacy) | `8B‖value` | **present / true** |
| post-flip `ttl=` write | `8B(expiry)‖value` | **present / true** |
| post-flip no-`ttl=` write | `8B(SENTINEL)‖value` | **present / true** |

The header is set **only for the `default` CF**. Other CFs (and `LOCAL_ONLY_CFS`,
which are not produced at all) never carry it; recovery only consults it for
`default`.

**Recovery rule (recovery side):** flip-discovery keys off the header, never value
content:

- `__ttl_stamped__` **present / true** → the value is stamped → strip the 8-byte
  prefix + apply the Rule 4 wallclock filter (the existing stamped main-CF branch,
  `partition.py:268-295`). On the first such header-true default-CF record, flip
  the recovering partition into TTL mode and latch (below).
- `__ttl_stamped__` **absent** → the value is legacy / un-stamped → replay
  **verbatim** (the existing legacy branch, `partition.py:246-256`). Never strip,
  never index, never filter.

`_looks_like_stamped_value` is **removed from the recovery flip-discovery path
entirely**. (See §8.7.3 for its other callers — it must NOT be deleted outright if
still referenced elsewhere; only the recovery-path call is removed.)

**Per-partition latching:** once any header-true default-CF record is seen during
a recovery session, the partition is in TTL mode for the rest of that session
(`uses_ttl_stamps = True`, the index CF is created, flip metadata is stamped) —
matching today's flag-discovery latch. So the per-record header and the latch
agree: every subsequent record (header-true or, in a mixed changelog, the trailing
stamped supersessions) routes through the stamped branch. A purely legacy
changelog (all header-absent) never latches and stays legacy — exactly the §8.6
Option 1 requirement.

> **Mixed changelog ordering note.** A real first-enablement changelog is
> *legacy-then-stamped*: the un-stamped history precedes the backfill's stamped
> supersessions (same keys, later offsets). Replay applies in offset order, so a
> key's final state is its last (stamped, header-true) write — the partition
> latches into TTL mode when the first stamped record arrives and the trailing
> stamped values overwrite the earlier verbatim ones. This is the existing,
> correct behavior; the header only changes *how* "stamped" is recognized.

#### 8.7.3 Exact touchpoints

1. **New metadata constant** — `quixstreams/state/metadata.py`:
   add `CHANGELOG_TTL_STAMPED_HEADER = "__ttl_stamped__"` alongside
   `CHANGELOG_CF_MESSAGE_HEADER` / `CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER`
   (lines 6-7).

2. **Set the header on produce** — `quixstreams/state/base/transaction.py`,
   `_prepare` (lines 602-641). The per-CF `headers` dict is built at lines
   621-624. Add the `__ttl_stamped__` header **only when `cf_name == "default"`
   and the partition is in TTL mode**.

   **Seam (base transaction learning the rocksdb flip-state):** `_prepare` lives
   in the **base** `PartitionTransaction`, but the base class already holds
   `self._partition` (`base/transaction.py:216`) and the rocksdb partition
   exposes the `uses_ttl_stamps` attribute (`RocksDBStorePartition`, read at
   `transaction.py:165/198/211/535/584`). The minimal, clean seam is therefore a
   **read-only attribute probe on the partition the base already owns** — no new
   plumbing, no subclass override of `_prepare`:

   ```python
   # base/transaction.py _prepare, inside the per-cf loop:
   headers: Headers = {
       CHANGELOG_CF_MESSAGE_HEADER: cf_name,
       CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: source_tp_offset_header,
   }
   if cf_name == "default" and getattr(self._partition, "uses_ttl_stamps", False):
       headers[CHANGELOG_TTL_STAMPED_HEADER] = b"\x01"
   ```

   `getattr(..., False)` keeps the base class agnostic of the rocksdb subclass and
   is a no-op for any partition type that has no `uses_ttl_stamps` (it stays
   header-less → byte-identical legacy behavior). **ArchDev:** prefer this
   attribute probe; only if a cleaner explicit seam is warranted, expose a tiny
   `StorePartition` method/property (e.g. `produces_ttl_stamped_changelog`/the
   existing `uses_ttl_stamps`) on the base partition protocol and call that
   instead of `getattr`. Do not override `_prepare` in the rocksdb transaction
   solely for this — keep one changelog-production path.

   **Header presence vs value.** Use **presence** as the boolean (a non-empty
   `b"\x01"` value when set; header omitted when false). Recovery treats
   "header key present and truthy" as stamped. Choose `b"\x01"`/absent for
   symmetry with `__ttl_enabled__`; ArchDev to confirm the changelog
   producer/serializer round-trips `Headers` byte values intact.

3. **Read the header on recover** — two files:
   - `quixstreams/state/recovery.py`, `recover_from_changelog_message`
     (lines 162-214). It already parses `headers` (line 179) and threads
     `cf_name`/`key`/`value`/`offset` into the partition call (lines 209-214).
     **Parse the new header and pass it through** as a new keyword argument
     (e.g. `ttl_stamped: bool`): `ttl_stamped = bool(headers.get(
     CHANGELOG_TTL_STAMPED_HEADER))`. Add the import to the existing
     `from ..metadata import (...)` block (recovery.py line 24).
   - `quixstreams/state/rocksdb/partition.py`,
     `recover_from_changelog_message` (lines 210-300). Add the
     `ttl_stamped: bool = False` parameter. **Replace** the value-content
     flip-discovery gate (lines 228-244) so the flip condition is
     `... and cf_name == "default" and ttl_stamped` (drop the
     `value and self._looks_like_stamped_value(value)` clause). Everything
     downstream (legacy verbatim branch 246-256, stamped branch + Rule 4
     268-295) is unchanged — only the *gate predicate* changes from value-content
     to header. **Default the param to `False`** so any caller that does not pass
     it (and any pre-header changelog message lacking the header) is treated as
     legacy/un-stamped — see §8.7.4 back-compat.

4. **`_looks_like_stamped_value` other callers** — before removing/deleting,
   note (confirmed via grep) it is **no longer called by the backfill** path
   (`spec-backfill-completeness.md`; `partition.py:488` comment) and the
   docstring at `partition.py:1029` says recovery is its consumer. The only live
   production caller in the flip-discovery path is `partition.py:233` (and the
   mirrored memory backend `memory/partition.py:230`, see §8.7.6). After §8.7
   removes the recovery-path call, **`_looks_like_stamped_value` has no remaining
   production caller** — but `transaction.py:41` references it in a *comment* tied
   to the shared `_MAX_PLAUSIBLE_STAMP_MS` bound, and `_normalize_replay_value` /
   the read-side strict validator may still use that bound. **Recommendation:**
   keep the `_MAX_PLAUSIBLE_STAMP_MS` constant (read-side strict validation still
   wants it) and either (a) delete `_looks_like_stamped_value` if grep confirms no
   other live caller, or (b) leave it as dead-but-documented with a comment that
   it is no longer on the recovery path. ArchDev to grep and pick; do not leave a
   live recovery caller.

#### 8.7.4 Back-compat / migration story (REQUIRED — OP-3 called this out)

**The hazard:** a store already flipped by the **current** (heuristic) build has
**stamped values on its changelog with NO `__ttl_stamped__` header**. On a
header-build restore those records read header-absent → treated as legacy → the
8-byte prefix is **not** stripped → the stored value is `8B‖value` returned as if
it were the raw user value = **corruption**.

Options evaluated:

- **(a) Require that first enablement happen on the header build.** Accept that
  any store stamped before the header build must be re-enabled fresh on the header
  build (the changelog is re-seeded). **Justification for choosing this:** the test
  environment is **re-seeding fresh on v7.5** (stated by Ludvík for this branch),
  and the live Maxio store has **not** successfully flipped yet — the §8.6 bug
  means it never produced header-less stamped values to its changelog at all (its
  changelog is still all-un-stamped legacy). So there is **no real population of
  header-less stamped changelogs** to be compatible with. Zero migration code,
  zero risk surface.
- **(b) Fallback to the heuristic only when the header is absent AND the value
  plausibly decodes as a stamp.** **Rejected:** this **re-introduces the exact
  false-positive** OP-3 is eliminating — a legacy 8-byte epoch-ms value with no
  header would still be misclassified as stamped. It defeats the purpose of the
  header and re-opens the §8.6 bug for the all-legacy case. Do not do this.
- **(c) Re-emit headers on next flush for already-flipped stores.** A flipped
  store on the header build will, on its **next** transaction, produce all
  *changed* keys with the header (because `uses_ttl_stamps == True`). But it will
  **not** retroactively re-emit *unchanged* keys, so a cold restore between deploy
  and the next full rewrite still hits header-less stamped records. Full coverage
  would require a one-time "rewrite every default-CF key to the changelog with the
  header" migration — substantial, and unnecessary given (a) applies here.
  Rejected for this branch.

**DECISION: option (a).** Require first enablement on the header build; re-seed
the test environment fresh on v7.5; no header-less-stamped-changelog compatibility
path is built. Document the constraint (DocuGuy): *if* a future deployment is found
to have header-less stamped changelogs, the supported recovery is delete-state +
re-restore on the header build (the changelog's later writes will carry the
header). The `ttl_stamped` recovery param defaults to `False` so a genuinely
header-less message is treated as legacy — which is correct for the all-legacy
case and the documented-unsupported case alike (it will not silently corrupt a
mixed store because, per option (a), no such store exists on this branch).

#### 8.7.5 Idempotency / interaction with §8.6 landing + §8.6.5 double-stamp guard

With the header, the §8.6.5 double-stamp guard becomes a **header lookup** rather
than a value-content guess:

- A second wipe + restore of a store backfilled under §8.7 replays
  header-true stamped values → the recovery gate flips on the first such record
  → the stamped main-CF branch + Rule 4 wallclock filter run → survivors land
  stamped, **no record re-enters backfill**, **no double-stamp**. Confirmed: a
  stamped-changelog restore (header-true) flips via the header → Rule 4, never via
  `main_cf_has_user_data()`-as-legacy → backfill.
- A first-enablement restore (all header-absent) never latches, lands verbatim,
  is classified populated-legacy at the first `ttl=` write, and backfills exactly
  once (§8.6.2).

These two cases are now distinguished by an **unambiguous** bit, so the
"un-stamped-only is populated-legacy; stamped-only is already-flipped"
classification (§8.6.5) is guaranteed, not heuristic.

#### 8.7.6 Memory backend scope

`quixstreams/state/memory/partition.py:recover_from_changelog_message`
(lines 221-254) carries the **same** value-content flip-discovery heuristic
(`_looks_like_stamped_value`, called at line 230; defined line 345) and therefore
shares this bug (OP-2-adjacent: OP-2 is the memory backend's *Rule 4* gap;
this is the memory backend's *flip-discovery* gap).

**Recommendation: scope §8.7 to the RocksDB backend for this branch; flag the
memory backend for Ludvík's call.** Rationale: (1) the live Maxio incident and the
acceptance tests are RocksDB-only; (2) the memory backend already has an
**unresolved, pending-Ludvík** open item (OP-2) covering its recovery semantics —
the flip-discovery header fix is naturally bundled with that decision rather than
split across branches; (3) the memory backend has no on-disk persistence, so its
cold-restore story differs and deserves its own once-over. The produce-side header
(set in the **base** `_prepare`) will be emitted for **any** backend whose
partition has `uses_ttl_stamps == True`, including memory — so the *signal* is
already on the changelog for free; only the memory backend's *recovery read* would
remain on the heuristic until OP-2 is decided. **Flag for Ludvík:** decide OP-2 +
this memory-backend recovery-read fix together. Do not change the memory backend
recovery path on this branch without sign-off.

#### 8.7.7 Test plan (acceptance gate + unit coverage)

**Acceptance gate (the two existing `xfail(strict=True)` repro tests flip to
passing):**
`tests/test_quixstreams/test_state/test_rocksdb/test_first_enablement_cold_restore.py::TestColdRestoreFalseFlipRepro`
— `test_legacy_timestamp_values_must_not_flip_recovery` and
`test_first_ttl_write_must_backfill_timestamp_valued_legacy`. These drive
`recover_from_changelog_message` with 8-byte epoch-ms legacy values **and no
`__ttl_stamped__` header**; after §8.7 the partition must stay legacy
(`uses_ttl_stamps is False`), all 50 records land verbatim, `main_cf_has_user_data()`
is True, and the first `ttl=` write backfills (not empty-store fast path).
**ArchDev:** remove the `@pytest.mark.xfail(strict=True)` decorators as the last
step; the suite must go from `145 passed, 2 xfailed` to `147 passed`. The
existing `TestFirstEnablementColdRestoreHappyPath` and
`TestStampedRestoreNotMisclassified` must remain passing.

**Unit (add, under `tests/.../state/rocksdb/`):**

1. **Header set on post-flip no-`ttl=` (SENTINEL) writes.** On a flipped
   partition, a `state.set(k, v)` with no `ttl=` produces a default-CF changelog
   record whose value decodes to `SENTINEL_NEVER` **and** carries
   `__ttl_stamped__` (the broad-trigger matrix row). Assert the header is present
   on the produced message.
2. **Header set on backfill + post-flip `ttl=` writes;** assert present on every
   default-CF record produced after the flip (backfill chunk records and live
   `ttl=` writes).
3. **Header absent on pre-flip legacy.** An unflipped legacy partition's writes
   produce default-CF changelog records with **no** `__ttl_stamped__` header
   (byte-identical to v3.23.6 — assert header key absent).
4. **Header absent on non-default CFs** even when flipped (signal is default-CF
   only).
5. **Recovery routes purely on header, not value content.** Drive
   `recover_from_changelog_message` with: (a) header-absent + a value whose
   8-byte prefix decodes as a plausible stamp (the OP-3 false-positive shape) →
   partition stays legacy, value replays verbatim; (b) header-true + a stamped
   value → partition flips, stamped branch + Rule 4 run. Assert
   `_looks_like_stamped_value` is **not** consulted on the recovery path (spy /
   patch-to-raise, mirroring `test_backfill_completeness.py`).
6. **Back-compat (chosen option a).** A header-absent message carrying a stamped
   value (the unsupported "pre-header stamped changelog" shape) is treated as
   legacy (verbatim, not stripped) — i.e. the recovery param defaults to `False`
   and there is no heuristic fallback. Document this as the
   delete-and-re-restore-on-header-build contract; assert no silent strip.
7. **First-enablement end-to-end (header path).** Mirror §8.6.7 integration cases
   1-7 but assert the records carry **no** header on the seed changelog and the
   re-stamped records carry the header on the post-backfill changelog (so the
   second restore flips via the header).

## 9. Alternatives considered

- **Mode enum `legacy_state_on_ttl_enable = "reject" | "backfill" | "wipe"`.**
  Not chosen: `"backfill"` still needs a duration (so a second field is
  unavoidable), `"reject"` is already the `None` default, and `"wipe"` is a
  footgun in Quix Cloud. The single `legacy_records_ttl: Optional[timedelta]`
  carries opt-in + duration with the safest default.
- **Require `legacy_records_ttl` to stay configured for correctness.**
  **Rejected / retracted** (Rule 2): durable changelog stamps make the field a
  one-time trigger. Persisting it has no ongoing effect (Rule 3 removed).
- **Store-wide default TTL for no-`ttl=` writes (the Rule 3 floor).**
  **Rejected / removed** by design: TTL is strictly per-write. Only
  `state.set(..., ttl=...)` sets an expiry; a no-`ttl=` write is always
  never-expires. A store-wide default (whether via constructor or via
  `legacy_records_ttl` flooring) was explicitly avoided.
- **Wall-clock enable_time for backfill.** Rejected — inconsistent with the
  event-time live filter and sweep (§8.1).
- **Lazy stamp-on-read backfill.** Rejected — permanent hot-path cost, never
  converges the changelog, recovery hazard (§8.2).
- **Stamp-ratchet recovery clock.** Rejected — collapses uniform-expiry stores
  (see `spec-recovery-wallclock.md`); replaced by wallclock-at-rebuild (Rule 4).
- **Stamp legacy records during recovery replay (Option 2 for §8.6).**
  **Rejected** (Ludvík, 2026-06-16): needs a recovery-time clock the changelog
  does not carry, duplicates the backfill stamping logic, and fights Rule 4. The
  chosen Option 1 lands records verbatim and lets the existing first-`ttl=`-write
  backfill stamp them (§8.6.2).
- **Value-content flip-discovery heuristic (`_looks_like_stamped_value`).**
  **Rejected / removed from the recovery path** (OP-3, §8.7): a legacy 8-byte
  epoch-ms value and a real expiry stamp share the same numeric range, so content
  cannot distinguish them — the confirmed root cause of the §8.6 bug. Replaced by
  the per-record `__ttl_stamped__` header (§8.7).
- **OP-3 option B — per-value structural marker (1-byte format tag).**
  **Rejected** (§8.7.2): it is a value-format / codec change, bumps the format
  version (§7.2 forbids), and breaks byte-identity with already-stamped stores.
- **OP-3 option A, single dedicated flip-marker message.** **Rejected in favor of
  the per-record header** (§8.7.2): a one-off marker can age out via retention or
  be removed by compaction; a per-record header rides every record and survives
  both.
- **OP-3 option C — accept + document (forbid cold-restore first-enable on
  8-byte-prefixed legacy stores).** **Rejected:** unacceptable for the live Maxio
  case that hit this.
- **Keep rejecting, document delete-and-recover.** Rejected — Quix Cloud has no
  customer-callable state reset.

## 10. Edge cases

- **Config set but no `ttl=` write ever (Rule 1):** store untouched, no flip, no
  backfill; `legacy_records_ttl` inert. Byte-identical to v3.23.6. No
  `__ttl_stamped__` header ever produced.
- **Empty store + first `ttl=` write:** empty-store flip; backfill finds nothing.
- **Populated store + `ttl=` write + opt-in unset:** loud reject (new message).
- **Cold-restore, changelog has ONLY un-stamped legacy + first `ttl=` write
  (§8.6 / §8.7):** every record is header-absent → recovery stays legacy →
  records land verbatim in default CF → flip sees populated legacy → backfill
  stamps them. (Was: heuristic false-flip → records dropped → empty-store fast
  path — the live bug.)
- **Cold-restore, changelog has stamped (header-bearing) values:** recovery flips
  on the first header-true record → Rule 4 picks survivors → first write is a
  normal stamped write (unchanged behavior, reliable signal).
- **Windowed / timestamped partitions:** `uses_ttl_stamps = False` at the class
  level; `set`/`set_bytes` take the always-legacy path and never reach the flip
  path, so neither backfill fires and the header is never set.
  `legacy_records_ttl` ignored.
- **No-`ttl=` write on a flipped store with `legacy_records_ttl` set (Rule 3
  removed):** stays `SENTINEL_NEVER` (never-expires) — but is `SENTINEL_NEVER`-
  stamped on the wire and **carries `__ttl_stamped__`** (partition is in TTL
  mode). `legacy_records_ttl` does NOT floor it.
- **No-`ttl=` write on a flipped store with `legacy_records_ttl` UNSET:** same —
  `SENTINEL_NEVER`, header present.
- **`legacy_records_ttl` removed from config after migration (Rule 2):**
  backfill never re-runs (idempotent on `__ttl_enabled__`); recovery still works
  (header-bearing stamps on changelog). No steady-state effect either way.
- **`high_water_ms` is None at flip:** hard error (§8.1).
- **Already-flipped store opened with opt-in set:** no re-backfill (idempotent).
- **Crash mid-backfill (before flag write):** `__ttl_enabled__` not set → next
  open is still legacy → backfill re-runs cleanly. Flag-last ordering is the
  idempotency anchor (§8.4).
- **Second cold restore after a §8.6 first-enablement:** changelog now carries
  header-bearing stamped values → flag-discovery flips on the header, Rule 4
  filters survivors, no re-backfill, no double-stamp (§8.6.5 / §8.7.5).
- **Header-absent message carrying a stamped value (pre-header changelog,
  unsupported per §8.7.4 option a):** treated as legacy (verbatim, not stripped);
  supported recovery is delete-state + re-restore on the header build.
- **Recovery survivors:** judged against wallclock-at-rebuild (Rule 4); sentinel
  entries always survive; index stays consistent with survivors. See companion
  spec §7.

## 11. Test plan

### Unit (under `tests/.../state/rocksdb/`)
1. `legacy_records_ttl <= 0` raises `ValueError`.
2. Populated legacy store + opt-in set + first `ttl=` write → backfills: every
   pre-existing key `decode_ttl_value`-able with `expires_at == high_water + ttl_ms`;
   matching `__ttl_index__` entry per key; `__ttl_enabled__` + format version set.
3. Populated legacy store + opt-in unset + `ttl=` write → raises
   `IncompatibleStateStoreError`; message references `legacy_records_ttl`, not
   "delete the state directory".
4. Empty store + opt-in set → empty-store flip (only in-batch keys stamped).
5. Idempotency: backfill, close, reopen, write again → no second backfill;
   pre-existing keys keep original expiry. Repeat with `legacy_records_ttl`
   **removed from config** on reopen → still no re-backfill (Rule 2).
6. enable_time = event-time high-water, not wall-clock.
7. Windowed/timestamped partition + opt-in set → ignored, no backfill.
8. Recovery: backfill with a changelog producer, rebuild a fresh partition from
   the changelog **with `legacy_records_ttl` absent from config** → identical
   stamped values + index (Rule 2 + Rule 4 wiring). Produced records carry
   `__ttl_stamped__`; recovery flips on the header (§8.7).
9. Crash-before-flag: stamps written without the flag, reopen → legacy mode,
   backfill re-runs and converges.
10. **Rule 3 REMOVED — no-`ttl=` while active is never-expires:** on a flipped
    partition with `legacy_records_ttl` set, `state.set(k, v)` (no `ttl=`) →
    stored value decodes to `SENTINEL_NEVER`, NOT `event_time +
    legacy_records_ttl`; no `__ttl_index__` entry; **but the changelog record
    carries `__ttl_stamped__`** (§8.7.7 case 1). Backfill still stamps
    pre-existing legacy records. Covered by `test_no_default_ttl_floor.py`.
11. **Rule 1 — config inert without `ttl=`:** opt-in set, only no-`ttl=` writes on
    an unflipped legacy store → no flip, values byte-identical to v3.23.6, no
    index CF created, **no `__ttl_stamped__` header**.
12. **§8.7 stamped-vs-legacy header:** see §8.7.7 cases 1-7 (header set on every
    post-flip default-CF record incl. SENTINEL writes; absent pre-flip; recovery
    routes purely on the header; `_looks_like_stamped_value` not consulted on
    recovery; back-compat option-a behavior). The two `TestColdRestoreFalseFlipRepro`
    `xfail` tests flip to passing as the acceptance gate.

### Integration (uses `C:\repos\TTL_test_environment`)
Seed legacy dedup state on the pre-TTL build; upgrade with
`rocksdb_options=RocksDBOptions(legacy_records_ttl=timedelta(...))`; assert app
starts, existing dedup keys block duplicates until enable_time+ttl in stream
time, new keys get true event-time expiry, redeploy does not re-backfill, a
wiped-volume restart recovers identically from the changelog **with the config
field removed**, and a no-`ttl=` write while active is never-expires (Rule 3
removed). **Plus the §8.6.7 / §8.7.7 first-enablement-on-cold-restore scenario:**
seed an all-un-stamped (header-absent) changelog + empty local state, deploy the
header build, assert the first `ttl=` write logs "Backfilled N legacy records"
(not "empty-store fast path"), records carry stamps + index entries, expire
~legacy_records_ttl after the first live `ttl=` write in stream time, and a second
wipe+restore (now replaying header-bearing stamped values) does not re-backfill.
**Re-seed the test environment fresh on v7.5** per the §8.7.4 option-a back-compat
decision.

## 12. Open questions / open items

1. **OP-1 — batch size for millions of keys (§8.3):** single rocksdict
   `WriteBatch` vs chunked write-batches, flag-last either way. Needs a rocksdict
   spike.
2. **OP-2 — memory backend recovery defect (PENDING LUDVÍK DECISION):** the same
   recovery collapse fixed in RocksDB (Rule 4) **still exists** in
   `quixstreams/state/memory/partition.py:recover_from_changelog_message`
   (~lines 203-249): it still uses the stamp-ratchet clock instead of
   wallclock-at-rebuild. **AND** the memory backend's flip-discovery still uses
   the value-content heuristic (`_looks_like_stamped_value`, `memory/partition.py:230`)
   that §8.7 replaces in RocksDB — so it shares the OP-3 bug too. **Recommendation
   (§8.7.6): bundle the memory-backend `__ttl_stamped__` recovery-read fix with the
   OP-2 decision** — decide both together; do not touch the memory backend recovery
   path on this branch without sign-off. (Produce-side header is already emitted for
   memory partitions via the base `_prepare`.)
3. **Rule 3 — REMOVED (§6.7):** the no-`ttl=` floor was built and then removed by
   design (TTL strictly per-write). No open work.
4. **OP-3 — §8.6 landing-bug root cause: RESOLVED (§8.7).** Confirmed cause: the
   value-content flip-discovery heuristic `_looks_like_stamped_value`
   false-positives on 8-byte epoch-ms legacy values. **Decision (Ludvík,
   2026-06-16): OP-3 option A, per-record-changelog-header variant** —
   `__ttl_stamped__` set on every post-flip default-CF record, recovery routes on
   the header, heuristic removed from the recovery path. Back-compat: option (a)
   (first enablement on the header build; re-seed test env fresh on v7.5).
   ArchDev implements per §8.7; the two `xfail(strict=True)` repro tests flipping
   to passing is the acceptance gate. See `open-points.md` (OP-3 RESOLVED).

## 13. Decision-tree (sanity table)

Activation-gate dimension added (Rule 1): "no `ttl=` in code" short-circuits
everything. (Rule 3 floor rows removed — no-`ttl=` is always never-expires.)
Cold-restore-first-enable row added (§8.6). Recovery flip-discovery now keys on
the `__ttl_stamped__` header (§8.7), not value content.

| Code uses `ttl=`? | Store state | `legacy_records_ttl` | Resulting behavior |
|---|---|---|---|
| **never** | any (empty/populated/legacy) | **None** | **inert / skip** — no flip, byte-identical to v3.23.6 (Rule 1) |
| **never** | any | **set** | **inert / skip** — config does nothing without a real `ttl=` write (Rule 1) |
| yes | empty | None | flip (empty-store fast path) on first `ttl=` write |
| yes | empty | set | flip (backfill finds nothing; identical to flip) |
| yes | populated-legacy (in-place local) | None | **reject** (new message: set `legacy_records_ttl`) |
| yes | populated-legacy (in-place local) | set | **backfill** all existing records (durable on changelog), then flip (Rule 2) |
| yes | **cold-restored, changelog ONLY un-stamped legacy (header-absent)** | set | recovery stays legacy (no header) → records land verbatim → flip sees **populated legacy** → **backfill** then flip (§8.6 / §8.7 — NEW) |
| yes | cold-restored, changelog has stamped values (header-bearing) | None or set | recovery flips on first `__ttl_stamped__` record; first write is a normal stamped write; Rule 4 picked survivors (§8.7) |
| yes | already-flipped | None or set | normal TTL writes; opt-in ignored for backfill (idempotent); records carry the header |
| — | already-flipped **(flipped)** | **set** + a **new write with no `ttl=`** | `SENTINEL_NEVER` (never-expires), header **present** — `legacy_records_ttl` does NOT floor; TTL is strictly per-write (Rule 3 removed) |
| — | already-flipped (flipped) | None + new write with no `ttl=` | `SENTINEL_NEVER` (never-expires), header present |
| any | windowed/timestamped | None or set | TTL machinery off at class level; opt-in ignored; no backfill; no header |

**Recovery (Rule 4, all rows above where the store is flipped):** on
cold-restore, the partition is recognized as flipped via the `__ttl_stamped__`
header (§8.7), then a stamped entry is dropped iff
`stamp != SENTINEL_NEVER and stamp <= wallclock_now` (captured once per rebuild);
header-absent legacy values replay verbatim but are superseded by their later
header-bearing stamped versions on the changelog. See `spec-recovery-wallclock.md`.

## 14. References

- Companion recovery spec: `dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md`
- Existing TTL design: `dev-planning/state-ttl/architecture.md`
- Open points (OP-3 RESOLVED): `dev-planning/state-ttl-legacy-backfill/open-points.md`
- Activation gate / flip-reject / backfill: `quixstreams/state/rocksdb/transaction.py:418-518`
- Flip/reject decision (current line layout): `quixstreams/state/rocksdb/transaction.py:501-598`
- `_compute_legacy_expiry`: `quixstreams/state/rocksdb/transaction.py:600-624`
- `_compute_stamp` (no-`ttl=` → `SENTINEL_NEVER`, Rule 3 removed): `quixstreams/state/rocksdb/transaction.py:167-176`
- Flipped inline write paths (route through `_compute_stamp`):
  `transaction.py:238-287`
- **Changelog header production (§8.7 produce-side seam):**
  `quixstreams/state/base/transaction.py:602-641` (`_prepare`), `:216` (`self._partition`)
- **Changelog header parsing/threading (§8.7 recovery-side):**
  `quixstreams/state/recovery.py:162-214`, `:24` (metadata imports), `:179-182`
- Recovery filter (Rule 4) + flag-discovery + legacy verbatim branch:
  `quixstreams/state/rocksdb/partition.py:210-300` (flip-discovery gate 228-244 →
  replaced by header read in §8.7)
- `_looks_like_stamped_value` (removed from recovery path, §8.7):
  `quixstreams/state/rocksdb/partition.py:1029-1060`; memory mirror
  `quixstreams/state/memory/partition.py:230,345`
- Metadata header constants (new `__ttl_stamped__` in §8.7):
  `quixstreams/state/metadata.py:6-9`
- `main_cf_has_user_data` / `_main_cf_has_user_data` (empty/populated gate):
  `quixstreams/state/rocksdb/partition.py:372-381`, `1062-1070`
- CF handle/instance caches: `quixstreams/state/rocksdb/partition.py:828-861`
- Acceptance-gate repro tests (flip `xfail`→pass):
  `tests/test_quixstreams/test_state/test_rocksdb/test_first_enablement_cold_restore.py`
- Codecs: `quixstreams/state/rocksdb/ttl_codec.py`
- Config home: `quixstreams/state/rocksdb/options.py`; property
  `quixstreams/state/rocksdb/partition.py:156-162`
- MEMORY: `quix-cloud-no-state-reset`, `feedback-operator-callable-upgrades`,
  `state-ttl-legacy-modes-design`
- Shortcut 73191
