# Incomplete-migration recovery: complete the backfill for stranded legacy records (OP-4)

**Status:** Draft (spec'd, pending implementation)
**Project:** quix-streams
**Created:** 2026-06-16
**Planned with:** Buddy
**Addendum to:** `dev-planning/state-ttl-legacy-backfill/spec.md` (§8.6 cold-restore
first-enablement, §8.7 `__ttl_stamped__` header) and Rule 2 / Rule 4 (§0)
**Companion:** `dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md`
(Rule 4 wallclock-at-rebuild)
**Resolves:** OP-4 in `dev-planning/state-ttl-legacy-backfill/open-points.md`
**Branch (implements):** follow-up on `feature/sc-73191` (additive, RocksDB only)

> This is `spec.md` **§8.8** by another name. It is filed as a companion document
> for length; `spec.md` should cross-link it from §8.6 / §8.7 and Rule 2. The
> RocksDB recovery path it modifies is `recover_from_changelog_message`
> (`quixstreams/state/rocksdb/partition.py:211-309`, post-§8.7).

---

## 1. Summary

§8.6 / §8.7 made two changelog shapes correct on cold restore: an **all-legacy**
changelog (no `__ttl_stamped__` header on any record) lands verbatim and is
backfilled by the first live `ttl=` write; an **all-stamped** changelog (every
default-CF record header-true) flips on the header and is judged by the Rule 4
wallclock filter. This spec closes the third shape: a **MIXED** changelog that
holds **both** stamped (header-true) and legacy (header-absent) default-CF
records — the on-disk signature of a **migration that started but never
finished**.

On a fresh-volume cold restore of a MIXED changelog, the existing recovery path
flips into TTL mode the instant it replays the first header-true record, then
replays the leftover legacy records verbatim (never-expiring). The first live
`ttl=` write afterwards sees an already-flipped partition, so
`_maybe_flip_or_reject` returns early (Rule 1 gate) and the backfill **never
runs**. The leftover legacy keys are therefore **stranded as never-expiring,
permanently** — for the Maxio dedup workload, those keys block duplicates
forever and never drain.

The fix acts **during recovery**, where the per-record header still exists (on
disk the stamped-vs-legacy bit is unrecoverable — §3). During the full cold-
restore replay we (a) detect that the partition replayed both header-true and
header-absent default-CF records (incomplete migration), (b) collect the keys
that arrived header-absent, and (c) **after** replay completes, backfill exactly
that collected key set — stamp them, write index entries, produce header-bearing
stamped records to the changelog — so the migration finishes and no legacy
record survives un-stamped. This is **approach A, replay-driven completion**
(Ludvík, 2026-06-16).

---

## 2. Problem statement (OP-4)

**Why the gate, not classification, is the gap.** `__ttl_enabled__` is
`LOCAL_ONLY` (never produced to the changelog → lost on volume loss), so cold
restore cannot read it. The §8.7 header fixed *classification* (recovery no
longer mis-reads a legacy 8-byte value as stamped). But the **backfill is gated
on the partition being NOT flipped** (`transaction.py:535-536`), and recovery
flips the partition the moment it replays **any** header-true record
(`partition.py:237-252`). So the gate, not the classifier, strands the records.

**Failure sequence (confirmed by design analysis):**

1. A TTL build is deployed **without** State Management → forced cold restore on
   an empty volume → recovery replays an all-legacy changelog verbatim → first
   `ttl=` write flips and **starts** the chunked backfill, producing stamped +
   header-bearing records to the changelog.
2. That run is **interrupted before the backfill completes** (pod killed,
   redeploy, crash). The changelog is now **MIXED**: the chunks that committed
   carry stamped (header-true) records; the un-backfilled keys remain as their
   original legacy (header-absent) records. Log compaction keeps the latest write
   per key, so a re-stamped key is present as stamped and an untouched key is
   present as legacy.
3. A later redeploy **with** state but on a **fresh volume** (or a wiped one) →
   full cold restore from offset 0 of the MIXED changelog.
4. Recovery flips on the first header-true record (`uses_ttl_stamps = True`,
   metadata stamped) and replays the leftover header-absent legacy records
   **verbatim** into the default CF (the `not self.uses_ttl_stamps` branch is no
   longer taken once flipped — they fall through to the main-CF branch but, being
   header-absent, must NOT be stripped; see §6.2 for the exact routing fix).
5. The first live `ttl=` write sees `uses_ttl_stamps == True` →
   `_maybe_flip_or_reject` returns at line 535-536 → **backfill never runs** →
   the leftover legacy keys are never stamped, never indexed, **never expire**.

**Why §8.7 alone does not close it.** §8.7 routes per-record on the header during
replay, which is necessary but not sufficient: it correctly *recognizes* the
leftover records as legacy, but nothing then *completes* their migration. The
completion logic is new and is the subject of this spec.

**Why we cannot fix it post-recovery on disk.** Once recovery finishes, the
leftover legacy values sit in the default CF as `value` (raw, no 8-byte prefix)
next to stamped values stored as `8B‖value`. The two are **byte-indistinguishable
without the header** (the header is changelog-transport-only; it is never stored
in the value, and `__ttl_enabled__` is a single partition-level flag, not per
record). A byte-sniff would re-introduce exactly the OP-3 false-positive. So the
completion **must** run during recovery, while the per-record header is still
attached to each replayed message.

---

## 3. Decided direction — approach A (replay-driven completion)

During the full cold-restore changelog replay (offset 0 on an empty volume), use
the per-record `__ttl_stamped__` header to:

1. **Detect** that this partition replayed **both** header-true records **and**
   header-absent default-CF records → the migration is incomplete (MIXED).
2. **Collect** the set of default-CF keys that most-recently arrived
   **header-absent** (the leftover legacy keys).
3. **After replay completes**, backfill exactly that collected set: stamp each
   leftover key, write its `__ttl_index__` entry, produce a stamped +
   header-bearing record to the changelog. Do **not** gate this on "partition not
   flipped" — by this point the partition is already flipped.

This leverages log compaction: the changelog keeps the latest write per key, so a
re-stamped key surfaces as stamped (header-true) and an untouched legacy key
surfaces as legacy (header-absent). The collected set is therefore exactly the
keys the interrupted backfill never reached.

---

## 4. Detection mechanics (during replay)

Per **recovering partition**, track two pieces of state for the recovery session
(alongside the existing `self._recovery_now_ms`, `partition.py:290-292`):

- `self._recovery_saw_stamped: bool` — set True on the first header-true
  default-CF record (this is the same condition that flips the partition today,
  `partition.py:237-242`).
- `self._recovery_legacy_keys` — the set of default-CF keys that arrived
  **header-absent** while the partition was (or became) flipped. See §4.1 for the
  memory bound; this is **not** an unbounded in-RAM `set`.

**Per default-CF record during replay:**

- header-true → `self._recovery_saw_stamped = True`; if the key is in
  `_recovery_legacy_keys`, **remove** it (a later stamped write supersedes an
  earlier legacy write for the same key — compaction ordering, §8.7.2 "mixed
  changelog ordering note"). Route through the stamped branch + Rule 4 as today.
- header-absent → **record the key** as a leftover-legacy candidate (§4.1). Route
  verbatim (no strip, no index) as today.

**Completion trigger (end of recovery session):** after the last changelog
message for the partition is applied, if `_recovery_saw_stamped` **and**
`_recovery_legacy_keys` is non-empty → the migration is incomplete → run the
completion backfill (§5) over `_recovery_legacy_keys`. If only one of the two
shapes was seen, do nothing (§7 unchanged-paths).

> **Where "end of recovery session" is.** `recover_from_changelog_message` is
> called once per message and has no natural "done" hook today. ArchDev must add a
> partition-level **recovery-finalize** call (e.g. `complete_recovery()` /
> extend the existing post-recovery seeding seam where `_recovery_now_ms` is
> consumed) invoked by the recovery manager after the partition's changelog
> high-watermark is reached, before the partition is handed to live processing.
> The completion backfill runs there. Touchpoint: the recovery driver in
> `quixstreams/state/recovery.py` that loops `recover_from_changelog_message` —
> add a terminal `partition.complete_recovery()` (name ArchDev's call). This is
> the one piece of **new plumbing** this spec requires.

### 4.1 Memory bound (the collected key set can be large)

The leftover-legacy key set could be a large fraction of a multi-hundred-k store
(an interrupt early in the backfill leaves most keys legacy). Do **not** hold an
unbounded Python `set` of raw key bytes in RAM for the whole replay.

**Decision: persist the candidate set to a dedicated local-only RocksDB CF, not
to RAM.** Introduce `__ttl_backfill_pending__` (a `LOCAL_ONLY` CF, never
produced to the changelog, same class as `__ttl_index__`):

- header-absent default-CF record → `batch.put(key, b"", pending_handle)` in the
  **same WriteBatch** as the verbatim default-CF put (atomic with the replay
  write).
- header-true default-CF record → `batch.delete(key, pending_handle)` in the same
  WriteBatch as the stamped put (supersession — the key is no longer leftover).

At end-of-recovery the pending CF holds **exactly** the leftover-legacy keys (a
key written legacy and never superseded). The completion backfill (§5) then
**iterates `__ttl_backfill_pending__`** rather than an in-RAM set — peak memory is
one chunk, identical to the existing chunked backfill (`spec-backfill-
completeness.md`). `log()` the count of pending keys at the start of completion.

This also gives **free interrupt-safety** (§6): the pending CF is durable on the
local volume, so a completion interrupted partway resumes from whatever pending
keys remain. (On a *fresh-volume* restore the pending CF is rebuilt from scratch
each replay, which is correct — see §6.)

> **Alternative considered (rejected): in-RAM `set` capped at K, spill to disk
> past K.** More code than just always using the local CF, and the local CF is
> already the project's idiom for `LOCAL_ONLY` rebuild-from-changelog state
> (`__ttl_index__`). Use the CF unconditionally.

---

## 5. Completion-backfill clock (wrinkle #2) — DECISION: wallclock-at-rebuild

The completion backfill runs **at/after recovery**, so its expiry **must** be
consistent with Rule 4 (wallclock-at-rebuild), **not** the original event-time
`high_water` path used by the live first-`ttl=`-write backfill (§8.1).

**Formula:**

```
expires_at_ms = wallclock_now + _ttl_to_ms(legacy_records_ttl)
```

where `wallclock_now` is the **same once-per-recovery-session value** already
captured for the Rule 4 filter (`self._recovery_now_ms`, `partition.py:290-292`).
If no stamped record was replayed, `_recovery_now_ms` is None — but the
completion path only runs when `_recovery_saw_stamped` is True, which is exactly
when `_recovery_now_ms` has been captured, so it is always available here. (If
defensively None, capture it via `self._now_ms()` at completion start.)

**Justification (wrinkle #2):**

- The leftover legacy records have **no event-time** — they are the original
  un-stamped values; there is no triggering live `ttl=` write to borrow a
  `high_water` from (the live-write backfill of §8.1/§8.6.4 borrows the
  triggering record's event-time, which does not exist here — completion runs
  before any live write).
- Recovery already judges **every** survivor by `wallclock_now` (Rule 4).
  Stamping the leftover records with `wallclock_now + legacy_records_ttl` gives
  them a **full fresh `legacy_records_ttl` window from the rebuild moment** —
  consistent with how the surrounding stamped records were just judged, and with
  the post-recovery high-water which Rule 4 seeds to `wallclock_now`
  (`spec-recovery-wallclock.md` §3.3). Using event-time `high_water` here would be
  meaningless (it is None / stale at recovery) and would fight Rule 4.
- This matches the §8.6.4 residual-risk note: backfill-at-recovery is a
  wallclock event; the live-write backfill is an event-time event. Completion is
  unambiguously the former.

The produced completion records carry `expires_at_ms = wallclock_now +
legacy_records_ttl` and the `__ttl_stamped__` header, so a **subsequent** restore
sees them as ordinary stamped records (header-true) and never re-enters
completion (§6).

### 5.1 `legacy_records_ttl` must be available at recovery (wrinkle #1)

Completion needs a duration. `legacy_records_ttl` is read from config
(`RocksDBOptions` → `partition.legacy_records_ttl`), available at recovery time
because the partition is constructed before recovery. **But Rule 2 says the field
is "set-once-then-remove" after migration — and an operator who started a
migration, saw it "succeed" (Rule 1 inert / partial), and removed the field would
arrive at completion with `legacy_records_ttl is None`.** See §8 for the decided
behavior in that case, and the Rule 2 wording change.

---

## 6. Idempotency / interrupt-safety (wrinkle #3) — DECISION

Completion mirrors the existing chunked backfill's flag-last + skip-already-done
design (`spec-backfill-completeness.md` §3.3):

1. **Census from the pending CF, chunked.** Completion iterates
   `__ttl_backfill_pending__` (the durable leftover set, §4.1) in byte-sorted
   order, chunk_size keys at a time. For each key: point-get the current
   default-CF value, wrap whole with `encode_ttl_value(expires_at_ms, value)`,
   write the `__ttl_index__` entry, **and delete the key from
   `__ttl_backfill_pending__`** — all in the **same WriteBatch** as the chunk's
   default-CF put. Produce the chunk's stamped records (header-true) to the
   changelog, flush, then read the next chunk.
2. **The pending-CF delete is the progress cursor.** A key is removed from pending
   only once it has been stamped + indexed + produced atomically. A crash
   mid-completion leaves the still-pending keys in the CF; the next recovery (or a
   re-entered completion) resumes over exactly those.
3. **Never double-stamps.** Only keys present in `__ttl_backfill_pending__` are
   stamped. A key already stamped during this recovery (header-true on the wire)
   was either never added to pending or was deleted from it on supersession
   (§4), so it is **not** in the census. A key stamped by a *previous* completion
   is, on the next restore, a header-true record on the changelog → it is never
   added to pending → never re-stamped. The wrap is whole-value-once
   (`encode_ttl_value` on the raw legacy value), never on an already-stamped
   value.

**How a second interruption converges.** Suppose completion stamps keys K1..Km of
N pending and is then interrupted (before the `__ttl_enabled__`-equivalent /
before all pending drained). Two cases:

- **Same volume survives** (warm-ish restart, local data intact): the next open
  reads `uses_ttl_stamps == True` from disk (metadata stamped at the §8.7 flip
  during recovery). On the next *cold* restore, the changelog now has K1..Km as
  stamped (header-true, produced in step 1) and K(m+1)..N still legacy
  (header-absent, never re-produced). Recovery rebuilds the pending CF from
  scratch: K1..Km arrive header-true → not pending (or superseded); K(m+1)..N
  arrive header-absent → pending. Completion runs over exactly K(m+1)..N.
  **Convergent**: each pass strictly shrinks the pending set; the migration
  completes after at most ceil(N / progress-per-pass) restores, and in practice
  one full uninterrupted pass.
- **Fresh volume each time** (pathological repeated wipes): identical reasoning —
  the changelog is the source of truth; each replay reconstructs pending from the
  current changelog state, which monotonically gains stamped records as
  completions produce them. No record is ever double-stamped because the wrap is
  driven off header-absent census only.

**Flag interaction.** There is no separate "completion done" flag — completion is
**done iff `__ttl_backfill_pending__` is empty** at end of recovery. The partition
is already flipped (`__ttl_enabled__` set at §8.7 recovery flip), so the live Rule
1 gate stays correct. Completion is therefore self-describing and needs no new
metadata flag beyond the pending CF.

---

## 7. Interaction with §8.6 / §8.7 — unchanged paths (explicit)

The new completion path triggers **only** in the MIXED case
(`_recovery_saw_stamped and pending non-empty`). The other shapes are unchanged:

| Changelog shape | `_recovery_saw_stamped` | pending CF at end | Completion runs? | Behavior |
|---|---|---|---|---|
| **all-legacy** (no header anywhere) — §8.6 first-enable | False | non-empty (every key) | **NO** (gated on `saw_stamped`) | stays legacy, lands verbatim, live `ttl=` write backfills (§8.6.2) — **unchanged** |
| **all-stamped** (every default record header-true) — §8.7 | True | empty | **NO** (pending empty) | flips on header, Rule 4 filters survivors — **unchanged** |
| **MIXED** (both shapes present) — incomplete migration | True | non-empty (leftovers) | **YES** (this spec) | flips on header, leftovers collected, completion stamps them at recovery end |

- The all-legacy case must **not** run completion: `_recovery_saw_stamped` is
  False, so the trigger short-circuits. The pending CF being non-empty is
  irrelevant without the stamped flag. (The live-write backfill of §8.6 still
  owns this case.)
- The all-stamped case has an **empty** pending CF (no header-absent records), so
  completion is a no-op even though `_recovery_saw_stamped` is True.
- §8.7's per-record routing, the Rule 4 wallclock filter, the §8.6 verbatim-
  landing, and the live `_maybe_flip_or_reject` are **all unchanged** in their
  own paths. This spec **adds** the pending-CF bookkeeping (a few lines in
  `recover_from_changelog_message`) and the terminal completion call.

---

## 8. Config-absent behavior (wrinkle #1) — DECISION: reject loudly

**Scenario:** a MIXED changelog is cold-restored but `legacy_records_ttl` is
**absent** from config (operator removed it after the migration "succeeded", or
never set it on this deployment). Completion needs a duration and has none.

**Options:**

- **(a) Reject loudly** with the operator-callable message: recovery completes the
  replay (so no data is lost / corrupted), then at the completion trigger raises
  `IncompatibleStateStoreError` (the §6.5 operator-callable variant) naming
  `legacy_records_ttl` and stating the migration is incomplete and the field must
  be restored to finish it. The deployment fails to start until the operator
  re-sets `legacy_records_ttl`.
- **(b) Replay verbatim + loud WARN.** Land the leftovers as never-expire, emit a
  loud WARN that the migration is incomplete and the config must be restored, let
  the deployment start.

**DECISION: (a) reject loudly.**

**Trade-off / justification.** The whole point of OP-4 is that silently stranding
legacy records as never-expire is the bug we are fixing. Option (b) reproduces
*exactly that silent never-expire outcome* and merely adds a WARN that an operator
running in Quix Cloud will likely never see — for the Maxio dedup workload that
means duplicates silently never drain, the original symptom. Option (a) turns a
**silent permanent data-correctness bug into a loud, actionable startup failure**:
the operator re-adds `legacy_records_ttl` (a one-line config change they can make
in the Portal — consistent with the operator-callable-upgrades directive) and the
next restore completes the migration. The cost is a hard stop, but a hard stop on
a recoverable misconfiguration is strictly safer than silent never-expiring
dedup state. The reject message must:

- name `legacy_records_ttl` and say it is **required to complete an interrupted
  migration** (distinct from the §6.5 "enable on populated store" message);
- state the count of pending leftover records;
- point at re-setting the field in config and redeploying — **not** at deleting
  state (Quix Cloud has no state reset).

### 8.1 Rule 2 wording change (required)

Rule 2 currently says `legacy_records_ttl` is "set-once-then-remove, durable on
the changelog" and "does NOT need to remain configured after backfill." **Amend
Rule 2 (spec.md §0 Rule 2 and §7.1 "Migration lifetime"):**

> `legacy_records_ttl` may be removed from config only after the migration is
> **COMPLETE** — i.e. every pre-existing legacy record has been stamped and the
> changelog carries no header-absent default-CF records. Removing it while a
> migration is merely **started** (interrupted backfill → MIXED changelog) is
> unsafe: a subsequent cold restore needs the field to complete the leftover
> backfill (§spec-incomplete-migration-recovery §5/§8) and will **reject at
> startup** if it is absent. "Complete" is observable as: a cold restore replays
> an all-stamped changelog (no header-absent default-CF records) and the
> `__ttl_backfill_pending__` CF ends empty.

This is a tightening, not a contradiction: for an *uninterrupted* migration the
changelog is fully stamped and the field can still be removed exactly as before.
Only the interrupted case newly requires keeping it until completion.

---

## 9. Scope and limitations

### 9.1 Cold restore (full replay) only — warm restore OUT of scope

This fix triggers only during a **full cold-restore replay** (offset 0 on an
empty/wiped volume), where every changelog record — including the leftover legacy
ones — is re-encountered with its header. A **warm restore** (local state present,
only a delta of new changelog records replayed) **cannot** re-encounter the
on-disk stranded legacy records: they already sit in the local default CF as raw
values and are not in the replayed delta, so the pending CF is never populated for
them and completion never sees them.

**Documented limitation:** a store that was warm-stranded (interrupted backfill,
then warm restarts that never trigger a full replay) keeps its leftover legacy
records un-stamped indefinitely. **The only recovery is a forced full rebuild**
(wipe the local volume so the next start is a full cold restore, which then runs
completion). DocuGuy must document this, and the operator-callable path is "force
a full state rebuild" — note this depends on Quix Cloud offering a wipe/rebuild
affordance (cross-ref MEMORY `quix-cloud-no-state-reset`; flag to Ludvík as the
one operator-action gap).

### 9.2 Memory backend — follow-up, consistent with OP-2

OP-4 applies to the **memory backend** in principle (its
`recover_from_changelog_message` shares the flip-on-first-stamped structure), but:

- the memory backend is non-persistent — it has **no on-disk volume to lose**, so
  the "fresh-volume cold restore of a MIXED changelog" scenario only arises when a
  memory partition rebuilds from a MIXED changelog produced by an interrupted
  RocksDB-or-memory backfill;
- the memory backend's recovery path is **already pending OP-2** (Rule 4 + §8.7
  header read not yet landed there — `open-points.md` OP-2).

**Decision: scope OP-4 to RocksDB on this branch; bundle the memory-backend
completion with the OP-2 memory-recovery work** (same rationale as §8.7.6). Do not
touch the memory backend recovery path here without Ludvík's sign-off. The
pending-CF mechanism (§4.1) is RocksDB-specific; the memory equivalent would be an
in-RAM pending set bounded by the memory store's own size (acceptable since memory
stores are smaller and non-persistent) — to be specified with OP-2.

---

## 10. Touchpoints (for ArchDev)

1. **New local-only CF constant** — `quixstreams/state/rocksdb/metadata.py`
   (alongside `TTL_INDEX_CF_NAME`): `TTL_BACKFILL_PENDING_CF_NAME =
   "__ttl_backfill_pending__"`. Add to `LOCAL_ONLY_CFS` so it is never produced.
2. **Recovery bookkeeping** — `quixstreams/state/rocksdb/partition.py`
   `recover_from_changelog_message` (211-309):
   - add `self._recovery_saw_stamped` init (with `_recovery_now_ms`, ~`__init__`);
   - in the header-true flip block (237-252) and the stamped main-CF branch
     (280-303): set `_recovery_saw_stamped = True`; `batch.delete(key,
     pending_handle)` on supersession;
   - in the legacy-verbatim landing of a header-absent default-CF record **while
     flipped**: `batch.put(key, b"", pending_handle)`. NOTE the routing subtlety
     (§6.2 of OP-4 problem): once flipped, a header-absent default-CF record must
     still be replayed **verbatim and added to pending** — it must NOT be stripped
     by the stamped main-CF branch. The §8.7 gate already routes header-absent →
     verbatim per-record; confirm that routing holds **after** the partition has
     flipped (the §8.7 design routes on the per-record header, not the latched
     flag, so this should already be correct — ArchDev verify and add the pending
     put).
3. **Recovery-finalize seam (new plumbing)** — add `complete_recovery()` (name
   ArchDev's) on `RocksDBStorePartition`; call it from the recovery driver in
   `quixstreams/state/recovery.py` after the partition reaches its changelog
   high-watermark, before live handoff. It:
   - if not (`_recovery_saw_stamped` and pending CF non-empty) → return;
   - if `legacy_records_ttl is None` → raise the operator-callable reject (§8);
   - else compute `expires_at_ms = self._recovery_now_ms + _ttl_to_ms(
     legacy_records_ttl)` and run the chunked completion over the pending CF (§6),
     reusing `backfill_legacy_records`'s chunk/produce/flush machinery
     (`partition.py:446+`) with the pending-CF census instead of the full
     default-CF census.
4. **Reuse** `encode_ttl_value`, `encode_index_key`, the changelog producer +
   header path (§8.7 produce-side already sets `__ttl_stamped__` because
   `uses_ttl_stamps == True` during completion), and the chunked write/flush loop.

---

## 11. Test plan (RocksDB)

**Primary repro — cold-restore partial-migration (the regression OP-4 names):**

1. Build a MIXED changelog: produce N1 **stamped** (header-true,
   `encode_ttl_value`) default-CF records and N2 **legacy** (header-absent, raw)
   default-CF records for **disjoint** keys (simulating an interrupted backfill —
   N1 keys done, N2 keys leftover). Empty local volume.
2. Cold-restore a fresh partition with
   `RocksDBOptions(legacy_records_ttl=timedelta(...))`; replay all N1+N2
   messages; trigger `complete_recovery()`.
3. **Assert** the partition flipped (`uses_ttl_stamps is True`).
4. **Assert** all N2 leftover keys are now `decode_ttl_value`-able with
   `expires_at == _recovery_now_ms + legacy_records_ttl` (the wallclock-at-rebuild
   value, NOT event-time high-water), each with a matching `__ttl_index__` entry.
5. **Assert** the N1 already-stamped keys are **byte-unchanged** (not re-stamped,
   no double-wrap) and judged by Rule 4 wallclock as usual.
6. **Assert** `__ttl_backfill_pending__` is **empty** after completion (migration
   complete) and there are **no header-absent / never-expire legacy survivors**.
7. **Assert** the completion produced N2 stamped + header-bearing records to the
   changelog (so a second restore sees an all-stamped changelog).

**Config-absent edge (wrinkle #1 / §8):**

8. Same MIXED changelog, but `legacy_records_ttl` **absent** from config. Replay,
   then `complete_recovery()` → **assert** it raises `IncompatibleStateStoreError`
   naming `legacy_records_ttl`, stating migration incomplete + the leftover count,
   and pointing at restoring config (NOT deleting state). Assert the leftover
   records were **not** silently stamped and **not** corrupted (still raw legacy on
   disk; the replay itself succeeded).

**Idempotency / interrupt (wrinkle #3 / §6):**

9. Drive completion to stamp only the first chunk (inject an interrupt after one
   chunk via a test seam), reopen on a fresh volume with the **post-interrupt
   changelog** (first chunk now stamped, rest still legacy), re-run recovery +
   completion → **assert** only the remaining leftovers are stamped, the
   first-chunk keys are not re-stamped (expiry unchanged), pending ends empty, and
   the survivor set equals the single-pass result (convergence).

**Unchanged-path guards (§7):**

10. All-legacy changelog → `complete_recovery()` is a no-op (`_recovery_saw_stamped
    False`); the live `ttl=` write still backfills (§8.6 path intact).
11. All-stamped changelog → pending CF empty → `complete_recovery()` no-op;
    Rule 4 survivors unchanged.

---

## 12. Alternatives considered

- **(approach B) Stamp leftover records inline during replay** (the moment a
  header-absent record is replayed on a flipped partition). Rejected: it needs the
  completion clock available mid-replay (it is — `_recovery_now_ms`), but it
  duplicates the chunked produce/flush machinery inside the per-message hot path,
  and it produces stamped records to the changelog interleaved with replay, which
  complicates offset/header bookkeeping. Approach A (collect-then-complete-at-end)
  reuses the existing chunked backfill verbatim and keeps the per-message path a
  cheap bookkeeping put/delete.
- **(approach C) On-disk byte-sniff post-recovery** to find leftover legacy
  values. Rejected outright: reintroduces the OP-3 false-positive (a legacy
  8-byte epoch-ms value is byte-indistinguishable from a stamp). The whole reason
  the fix lives in recovery is that the header only exists there (§2).
- **(config-absent option b) Land verbatim + WARN.** Rejected (§8): reproduces the
  silent never-expire bug OP-4 exists to kill.
- **In-RAM pending set.** Rejected for the bound (§4.1): a local-only CF is the
  project idiom (`__ttl_index__`), gives durable interrupt-safety for free, and
  bounds RAM to one chunk.

---

## 13. Risks / open questions

- **R1 — recovery-finalize plumbing is new.** There is no existing per-partition
  "recovery done" hook; ArchDev must add one in the recovery driver. Low risk
  (the high-watermark is already known to the recovery manager) but it is the only
  non-additive change. **Flag to Ludvík.**
- **R2 — warm-stranded stores are unrecoverable without a forced full rebuild
  (§9.1),** which depends on a Quix Cloud wipe/rebuild affordance that may not
  exist (MEMORY `quix-cloud-no-state-reset`). **Flag to Ludvík:** is "force full
  rebuild" an action the operator can take in the current Portal? If not, the
  warm-stranded case has no operator-callable recovery and we should say so loudly
  in docs.
- **R3 — reject-on-config-absent (§8) hard-stops a deployment.** Acceptable per
  the decision, but it means an operator who removed `legacy_records_ttl` after a
  *partial* migration gets a startup failure on the next cold restore. Mitigated
  by the precise operator-callable message and the Rule 2 wording change (§8.1).
- **OQ1 — completion clock vs a very old MIXED changelog.** Completion stamps
  leftovers `wallclock_now + legacy_records_ttl` (a fresh window from rebuild).
  This is the intended Rule 4 semantic, but note the leftovers get a *fresh* TTL
  window at every (rare) re-completion, same as the §8.6.4 residual-risk note.
  Document; no code change.

---

## 14. References

- OP-4: `dev-planning/state-ttl-legacy-backfill/open-points.md`
- Base spec §0 (Rule 2 amended here / Rule 4), §8.6, §8.7:
  `dev-planning/state-ttl-legacy-backfill/spec.md`
- Rule 4 wallclock-at-rebuild: `dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md`
- Backfill completeness (chunk/census/flag-last reused for completion):
  `dev-planning/state-ttl-legacy-backfill/spec-backfill-completeness.md`
- Recovery path: `quixstreams/state/rocksdb/partition.py:211-309`
  (`recover_from_changelog_message`)
- Chunked backfill machinery to reuse: `quixstreams/state/rocksdb/partition.py:446+`
  (`backfill_legacy_records`)
- Flip/reject gate: `quixstreams/state/rocksdb/transaction.py:501-598`
  (`_maybe_flip_or_reject`)
- Recovery driver (finalize seam): `quixstreams/state/recovery.py`
- `__ttl_stamped__` header: `quixstreams/state/metadata.py`
- MEMORY: `quix-cloud-no-state-reset`, `feedback-operator-callable-upgrades`
- Shortcut 73191
