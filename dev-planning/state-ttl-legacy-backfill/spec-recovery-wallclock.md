# Recovery-clock change: wallclock-at-recovery TTL filtering (OP-1 fix)

**Status:** Draft
**Project:** quix-streams
**Created:** 2026-06-15
**Planned with:** Buddy
**Addendum to:** `dev-planning/state-ttl-legacy-backfill/spec.md` (§8.1 recovery half, §8.5)
**Branch (implements):** follow-up to `feature/sc-73191` (new branch, additive)
**Resolves:** OP-1 in `dev-planning/state-ttl-legacy-backfill/open-points.md`

---

## 1. Summary

During changelog recovery, TTL-enabled RocksDB partitions currently decide
whether a replayed entry is "already expired" by comparing its expiry stamp
against a **pseudo-clock derived from the replayed stamps themselves**
(`recovery_now = self._high_water_ms`, ratcheted forward by each record's expiry
stamp). With uniform-expiry backfill (spec §8.1) every backfilled record carries
the **same** expiry `E`; the first replay ratchets the pseudo-clock to `E`, and
every later record then satisfies `E <= E` and is dropped — collapsing the store
to ~1 surviving key (OP-1).

This addendum changes the **recovery path only**: during rebuild-from-changelog,
the expiry comparison uses the **current wallclock time** (`now = ms since epoch`,
captured once per recovery session) as the "now" reference, and recovery **no
longer advances the partition high-water by expiry stamps**. The live read-time
path (`transaction.py:288-326`) stays event-time and is **unchanged**. No new
changelog field or message timestamp is introduced.

---

## 2. Problem (ref OP-1)

See OP-1 for full detail. The defective logic is in
`quixstreams/state/rocksdb/partition.py:recover_from_changelog_message`
(~lines 167-258 on `feature/sc-73191`):

1. `recovery_now = self._high_water_ms` (lines 231) — read per replayed default-CF message.
2. Drop a stamped value when `stamp != SENTINEL_NEVER and recovery_now is not None and stamp <= recovery_now` (lines 232-240).
3. After each main-CF message, `advance_high_water(stamp)` — advancing the recovery clock **by the expiry stamp** (lines 252-258).

The clock that step 2 compares against is built from the same stamps it is
filtering. With uniform expiry `E`, step 3 pins the clock at `E` after the first
record, and step 2 then drops the rest. This is the collapse. It is not unique to
backfill — any two steady-state writes sharing the maximum expiry hit it — but
backfill makes uniform expiry the common case (e.g. Maxio dedup loses most legacy
keys after a cold restore).

Why there is no real clock today: `ChangelogProducer.produce`
(`recovery.py:312-331`) sets **no message timestamp** and adds no event-time
header, so changelog messages carry no event-time. Recovering true event-time per
record is not cheaply available — this motivates the wallclock approach over
injecting event-time (see §6).

---

## 3. New recovery semantics

### 3.1 Drop condition

During changelog recovery, for a stamped **default-CF** replay value with decoded
`stamp`:

```
drop iff  stamp != SENTINEL_NEVER  and  stamp <= wallclock_now
```

where `wallclock_now` is `int(time.time() * 1000)` (ms since epoch), captured
**once per recovery session** (see §3.2). `SENTINEL_NEVER` entries are never
compared and always survive (§7). When an entry is kept, its main-CF write **and**
its `__ttl_index__` entry are written exactly as today (`encode_index_key(stamp,
key)`); when dropped, both are skipped and only the changelog offset is rolled
forward. Non-default CFs, tombstones (`value is None`), and `LOCAL_ONLY_CFS`
behave exactly as today.

**Removed:** step 3 above — recovery no longer calls `advance_high_water(stamp)`
to ratchet a stamp-derived recovery clock. The block at `partition.py:252-258` is
deleted/replaced by the high-water seeding in §3.3.

### 3.2 `wallclock_now`: captured once per recovery session — DECISION

`wallclock_now` is captured **once** at the start of a recovery session and reused
for every message in that session, not re-read per message.

- **Determinism within a rebuild:** a per-message clock would let two identical
  replays produce different survivor sets depending on how long the replay takes
  (a record on the expiry boundary could survive early in replay and be dropped
  later). A session-fixed `now` makes the survivor set a pure function of
  (changelog contents, single `now` value), so a rebuild is deterministic and
  test-reproducible (§8).
- **Implementation:** capture lazily on the **first** stamped default-CF message
  of the recovery session (store on the partition, e.g. `self._recovery_now_ms`),
  or eagerly when recovery for the partition begins. Lazy-on-first-stamped is
  preferred because it avoids reading the clock for legacy / non-TTL partitions
  that never enter the stamped branch, and it naturally co-locates with the
  existing first-stamped-replay flip detection (`partition.py:185-201`). The
  captured value is reset/discarded when the recovery session ends (next open is a
  new session).

Cross-rebuild non-determinism (a rebuild tomorrow uses a different `now`) is
**intentional** — see §5.

### 3.3 Post-recovery high-water seeding

After recovery completes, `high_water_ms` must be set so the first **live** reads
behave sanely. Seed it to `wallclock_now` (the session clock) if it was captured;
i.e. `self._high_water_ms = self._recovery_now_ms`. If no stamped default-CF
message was ever replayed (no clock captured), leave `high_water_ms` as loaded by
`_load_high_water()` / `None`, exactly as a cold TTL store.

Rationale and the monotonicity edge:

- The live read filter (`transaction.py:322-323`) compares `stamp <= high_water_ms`
  using the monotonic event-time high-water. Seeding it to `wallclock_now` means
  the very first live reads judge expiry against the same reference recovery just
  used — no discontinuity at the recovery→live boundary.
- `advance_high_water` is monotonic (`partition.py:156-165`): live records advance
  it only when their event-time exceeds the current value. So if incoming live
  **event-time < wallclock_now**, the high-water stays **pinned at
  `wallclock_now`** until live event-time catches up. During that window, expiry
  is judged against wallclock rather than the (lagging) live event-time.
- **This is acceptable and is the same trade-off the recovery filter now makes**
  (§5): for a store that has just been recovered, "now" is wallclock until the
  live stream's own event-time overtakes it. For live/near-real-time streams the
  gap is negligible. For far-historical reprocessing it means recovered entries
  are judged against real time (§5) — by design. **Ludvík must sign off on this
  pinning behavior** (§7 / §9).

---

## 4. Scope / blast radius

> **One-line statement:** This changes cold-restore recovery expiry filtering for
> **every TTL store** (not just backfilled ones): recovered entries are now judged
> against wallclock-at-recovery instead of a stamp-derived pseudo-clock, so far
> fewer entries are dropped during replay and the surviving set depends on *when*
> the rebuild runs.

For a **normal (non-backfill) TTL store** recovered from changelog:

- **Before:** entries were dropped during replay whenever `stamp <= ` the
  stamp-ratcheted recovery high-water. Because the high-water ratchets to the max
  stamp seen so far, any entry whose expiry was `<=` an already-replayed larger
  expiry could be dropped even though, in real time, it had not yet expired —
  conservative-to-wrong, and order-dependent.
- **After:** entries are dropped only if genuinely expired against real wallclock
  at rebuild time (`stamp <= wallclock_now`). A store rebuilt within its records'
  TTL window retains all live entries; one rebuilt long after the window drops the
  expired ones. This matches what the live read filter would have done at that
  wallclock moment, and is order-independent.

Net: recovery becomes **less aggressive** and **correct against real time** for
ordinary TTL stores, while fixing the backfill collapse. The live path,
serialization, codecs, index format, and sweep are untouched.

---

## 5. Documented trade-off (accepted)

Expiry-on-rebuild is now tied to **WHEN you rebuild** (wallclock), not to the
event-time of the data:

- **Live / near-real-time streams:** intuitive and correct — "now" is now.
- **Reprocessing far-historical data, or a long-delayed changelog replay:**
  recovered entries are judged against **real current time**, so entries that are
  old in wallclock terms are dropped on rebuild even if, in event-time terms, the
  reprocessed stream considers them "current". A store rebuilt months after its
  data was written will drop everything whose expiry is in the past.
- **Non-determinism across rebuild times — BY DESIGN:** the same changelog
  rebuilt at two different wallclock moments can yield different survivor sets.
  Within a single rebuild it is deterministic (§3.2); across rebuilds it is not,
  intentionally, because "now" legitimately differs.

This is the explicit, accepted semantic. It is documented here and should be
surfaced in user-facing TTL recovery docs (DocuGuy).

---

## 6. Alternatives considered + decision rationale

- **(a) Inject true event-time onto changelog events.** Add a message timestamp
  or an event-time header in `ChangelogProducer.produce` and have recovery rebuild
  the real event-time high-water, keeping recovery event-time-consistent with the
  live path. **Rejected for now:** more code, a new wire field/timestamp on every
  changelog message, migration/back-compat surface for existing changelog topics,
  and it reopens the uniform-expiry interaction (the backfilled records' true
  event-times may themselves be uniform/`enable_time`). Higher correctness, much
  higher cost.
- **(b) Do nothing (accept OP-1).** Document the collapse. **Rejected:**
  unacceptable for dedup workloads — most legacy keys stop blocking duplicates
  after a restore (OP-1).
- **(c) chosen — wallclock-at-recovery.** Smallest viable change: one localized
  edit in `recover_from_changelog_message` plus high-water seeding; no wire
  change; no codec change. Sound for the dominant live-stream case; the
  historical-reprocessing trade-off (§5) is explicitly accepted.

**Decision:** Ludvík chose **(c) wallclock-at-recovery** for simplicity.

---

## 7. Edge cases

- **`SENTINEL_NEVER` entries always survive recovery.** They are never compared
  to `wallclock_now` (drop condition guards `stamp != SENTINEL_NEVER`). Unchanged
  from today; called out explicitly so the test plan asserts it.
- **`__ttl_index__` rebuild stays consistent with survivors.** An index entry
  (`encode_index_key(stamp, key)`) is written iff the corresponding main-CF entry
  survives recovery; dropped entries write neither. So the index never references a
  key that was filtered out — preserving the sweep invariant. (Unchanged
  mechanism; only the survivor set changes.)
- **Memory / global / non-default CFs unaffected.** Only the `default` CF carries
  stamped values; all other CFs (`LOCAL_ONLY_CFS`, global counters, etc.) replay
  verbatim as today.
- **Windowed / timestamped partitions (`uses_ttl_stamps = False`) unaffected.**
  They take the legacy verbatim replay branch (`partition.py:203-213`) and never
  reach the stamped filter or the clock capture.
- **No stamped message replayed (legacy / empty TTL store).** No `wallclock_now`
  is captured, high-water seeding is skipped, behavior identical to a cold TTL
  store.
- **High-water pinning after recovery** (§3.3): live event-time below
  `wallclock_now` keeps high-water pinned until it catches up. **This is the edge
  Ludvík must explicitly sign off on** — it means immediately-post-recovery live
  reads on a historical-reprocessing stream judge expiry by wallclock, not by the
  reprocessed event-time.

---

## 8. Test plan

1. **OP-1 regression — backfilled store rebuilt WITHIN TTL window:** backfill N
   records with uniform expiry `E`, produce to changelog, cold-restore with
   `wallclock_now < E`. **Assert all N retained** (was: ~1). Assert `__ttl_index__`
   has N matching entries.
2. **Backfilled store rebuilt AFTER TTL window:** same store, cold-restore with
   `wallclock_now >= E`. **Assert none retained** (all genuinely expired);
   `__ttl_index__` empty.
3. **Normal (non-backfill) TTL store, mixed expiries:** records with varied
   stamps, cold-restore at a `wallclock_now` between them. **Assert exactly the
   entries with `stamp > wallclock_now` survive**, order-independent. Contrast with
   old stamp-ratchet behavior in the test docstring.
4. **`SENTINEL_NEVER` entries always survive:** include sentinel-stamped records;
   rebuild at any `wallclock_now` (including far future). **Assert all sentinel
   entries retained.**
5. **Post-recovery high-water seeding:** after recovery, assert
   `high_water_ms == wallclock_now`; then feed a live record with event-time
   `< wallclock_now` and assert high-water stays pinned (monotonicity edge §3.3);
   feed one `> wallclock_now` and assert it advances.
6. **Determinism within a single rebuild session:** freeze/inject a fixed
   `wallclock_now` (clock seam for tests), replay the same changelog twice, assert
   identical survivor sets and identical index contents.

Implementation note for ArchDev: introduce a small seam to inject `wallclock_now`
in tests (e.g. an internal `_now_ms()` helper or constructor-injectable clock) so
cases 1-6 are deterministic without sleeping.

---

## 9. Before / after recovery behavior

| Scenario | Old behavior | New behavior |
|---|---|---|
| Backfilled store (uniform expiry `E`), rebuilt **within** TTL window (`now < E`) | **Collapse** — first record ratchets recovery clock to `E`, rest dropped as `E <= E`; ~1 survivor | **All N retained** — `E > wallclock_now`, none expired |
| Backfilled store, rebuilt **after** TTL window (`now >= E`) | Collapse — ~1 survivor (wrong reason) | **None retained** — all genuinely expired vs wallclock |
| Normal TTL store, mixed expiries, rebuilt within window | Order-dependent drop: entries `<=` max-stamp-so-far dropped even if not truly expired | Entries with `stamp > wallclock_now` survive; order-independent |
| Normal TTL store, rebuilt long after expiries | Drops by ratcheted stamp clock | Drops iff `stamp <= wallclock_now` (correct vs real time) |
| `SENTINEL_NEVER` entry | Survives (never compared) | Survives (never compared) — unchanged |
| Post-recovery `high_water_ms` | Set to max replayed expiry stamp | Seeded to `wallclock_now`; then monotonic event-time advance from live records |

---

## 10. References

- OP-1: `dev-planning/state-ttl-legacy-backfill/open-points.md`
- Base spec §8.1 (event-time clock — recovery half changed here), §8.5 (changelog
  wiring option a): `dev-planning/state-ttl-legacy-backfill/spec.md`
- Recovery filter to change: `quixstreams/state/rocksdb/partition.py:167-258`
  (`recover_from_changelog_message`)
- High-water: `quixstreams/state/rocksdb/partition.py:134-165`
- No-timestamp changelog producer: `quixstreams/state/recovery.py:312-331`
- Live read filter (UNCHANGED): `quixstreams/state/rocksdb/transaction.py:289-326`
