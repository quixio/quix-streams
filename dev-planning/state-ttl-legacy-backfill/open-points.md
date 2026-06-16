# Open points — State TTL legacy-store backfill (sc-73191)

## OP-1: Uniform-expiry backfill collapses on cold-restore recovery

**Raised by:** ArchDev (during implementation, case 8 recovery test).
**Root cause layer:** `code` (recovery filter) exposed by `architecture`
(uniform-expiry design).
**Status:** RESOLVED — fixed on `feature/sc-73191/udpate-state-behavior-of-dedup-feature-to`
per `spec-recovery-wallclock.md` (option C, wallclock-at-recovery). The RocksDB
recovery filter now drops a replayed default-CF entry iff
`stamp != SENTINEL_NEVER and stamp <= wallclock_now`, where `wallclock_now` is
captured once per recovery session; the stamp-ratchet `advance_high_water(stamp)`
in recovery is removed and post-recovery `high_water_ms` is seeded to that
wallclock. Resolution = proposed option A's spirit (real-time reference) via
wallclock rather than per-record event-time (no new changelog field). Regression
tests in `tests/.../test_rocksdb/test_legacy_backfill.py::TestRecoveryWallclock`.

## OP-2: Memory backend — recovery parity (stamp-ratchet collapse + value-content false-flip)

**Raised by:** Ludvík (follow-up to OP-1 — memory backend was originally out of
scope).
**Root cause layer:** `code` (memory `recover_from_changelog_message` carried the
identical stamp-ratchet AND the same value-content flip-discovery as RocksDB).
**Status:** **RESOLVED (2026-06-16, Buddy/Ludvík) — port wallclock-at-rebuild
(Rule 4) + header detection (§8.7) to the memory backend, this branch.** Spec:
**`dev-planning/state-ttl-legacy-backfill/spec-memory-backend-recovery.md`**.

Decision detail:
- **Rule 4 (wallclock-at-rebuild):** already ported in
  `quixstreams/state/memory/partition.py:262-290` (lazy session-`now` via
  `_now_ms()` seam at `:140`, `self._recovery_now_ms` at `:112`, drop iff
  `stamp != SENTINEL_NEVER and stamp <= self._recovery_now_ms`, post-recovery
  high-water seeded to that `now`). The old stamp-ratchet
  (`recovery_now = self._high_water_ms` + `advance_high_water(stamp)`) is **gone**.
  Verify-only + add coverage.
- **Header detection (§8.7):** **TO DO** — the flip-discovery gate
  (`partition.py:236-244`) still calls the value-content heuristic
  `_looks_like_stamped_value` and ignores the threaded `ttl_stamped` kwarg
  (`_ = ttl_stamped`, `:229-234`). Replace the gate so it flips iff
  `cf_name == "default" and ttl_stamped`, mirroring RocksDB
  `partition.py:237-252`. Remove `_looks_like_stamped_value` from the recovery
  path. No produce-side change (base `_prepare` already emits the header for
  memory). No `_stamp_flip_metadata` mirror (memory is non-persistent — see spec
  §8). Test cases: uniform-expiry-survives-rebuild (no collapse), legacy-8-byte-
  epoch-ms-must-NOT-flip, header routing (full list in spec §10). **Owner:**
  ArchDev (gate swap) + Tester (cases). Land after the §8.7 shared edits
  (metadata.py / recovery.py) are on the branch.

### Problem (OP-1, historical)

`backfill_legacy_records` re-stamps every pre-existing record with the SAME
expiry `E = high_water + legacy_records_ttl` (spec §8.1, by design). These
re-stamped values are produced to the changelog (option a, spec §8.5). On a
cold restore the existing recovery filter
(`quixstreams/state/rocksdb/partition.py:229-247`):

1. reads `recovery_now = self._high_water_ms` per replayed message,
2. drops a stamped default-CF value when `stamp <= recovery_now`,
3. advances the recovery high-water by the **expiry stamp** after each message.

Because all backfilled records share expiry `E`, the first replay advances the
recovery high-water to `E`, and every subsequent backfilled record then
satisfies `E <= E` and is **dropped as already-expired**. Result: a
volume-loss restore of a backfilled store can retain only one of the N
backfilled records. For the Maxio dedup workload this means most legacy dedup
keys stop blocking duplicates after such a restore.

This is NOT specific to backfill — two ordinary steady-state TTL writes sharing
the maximum expiry hit the same drop — but backfill makes uniform expiry the
common case. The memory backend (OP-2) shared the identical defect plus the
OP-3 value-content false-flip; both are addressed by the memory companion spec.

### Why not fixed in this feature

The spec scoped the change as additive ("reuse the existing recovery path
verbatim", §8.5) and explicitly did not authorize changing recovery semantics.
Fixing it alters documented recovery behavior for ALL TTL stores, so it needs a
spec decision.

### Proposed resolutions

- **A (recommended):** advance the recovery high-water by record **event-time**
  (available in changelog headers / processed-offsets) rather than the expiry
  stamp, and/or make the drop comparison strict (`<`) so an entry expiring
  exactly at the current high-water survives replay. Small, localized recovery
  change; needs Buddy to update the TTL recovery spec first.
- **B:** accept + document the limitation (likely unacceptable for dedup).

### Ask

Buddy to decide A vs B. If A, spec the recovery-filter change and ArchDev
implements it as a follow-up branch so this feature lands additive.

---

## OP-3: §8.6 cold-restore first-enablement — confirmed root cause is the
## value-content flip-discovery heuristic (needs an unambiguous flip signal)

**Raised by:** ArchDev (Phase 1 root-cause, 2026-06-16).
**Root cause layer:** `architecture` (the recovery flip-discovery *protocol*
relies on an unsound value-content heuristic) exposed as `code` in
`_looks_like_stamped_value` (`quixstreams/state/rocksdb/partition.py`).
**Status:** **RESOLVED (2026-06-16, Buddy/Ludvík) — OP-3 option A,
per-record-changelog-header variant.** The signal is a per-record changelog
message header `CHANGELOG_TTL_STAMPED_HEADER = "__ttl_stamped__"`, set on every
`default`-CF record produced while the partition is in TTL mode
(`uses_ttl_stamps == True`); recovery flip-discovery reads the header and never
peeks at value content; `_looks_like_stamped_value` is removed from the recovery
path. Full design + touchpoints + test plan: **spec.md §8.7**. ArchDev implements
on this branch; the two `xfail(strict=True)` repro tests
(`test_first_enablement_cold_restore.py::TestColdRestoreFalseFlipRepro`) flipping
to passing (`145 passed, 2 xfailed` → `147 passed`) is the acceptance gate.

### Resolution summary (decision record)

- **Chosen:** OP-3 option **A**, *per-record header* (not the single dedicated
  marker message). Rationale: a one-off marker can age out via retention or be
  removed by log compaction; a per-record header rides every record and survives
  both, so the stamped/legacy bit can never be separated from the data.
- **Rejected B** (per-value format byte): it is a value-format/codec change,
  bumps the format version (§7.2 forbids), breaks byte-identity with
  already-stamped stores.
- **Rejected C** (accept + document): unacceptable for the live Maxio case.
- **Produce trigger (one line):** set `__ttl_stamped__ = b"\x01"` on a changelog
  record iff `cf_name == "default"` and the partition has
  `uses_ttl_stamps == True` at produce time (broader than "this write had `ttl=`"
  — post-flip no-`ttl=` SENTINEL writes are stamped too and carry the header).
- **Recovery rule (one line):** header present/true → strip 8B + Rule 4 wallclock
  filter (flip + latch on first such record); header absent → replay legacy
  verbatim.
- **Back-compat:** option **(a)** — require first enablement on the header build;
  the test environment is re-seeded fresh on v7.5 and the live Maxio store never
  successfully flipped (its changelog is still all-un-stamped legacy), so there is
  no real population of header-less *stamped* changelogs to be compatible with.
  Option (b) heuristic-fallback rejected (re-opens the false-positive); option (c)
  re-emit-on-flush rejected (incomplete + unnecessary here). The recovery
  `ttl_stamped` param defaults to `False` so a header-less message is treated as
  legacy; an (unsupported) pre-header stamped changelog's supported recovery is
  delete-state + re-restore on the header build.
- **Memory backend:** §8.7's RocksDB scope is unchanged, but the memory backend's
  matching recovery-read fix is now **specified and DECIDED** (OP-2 RESOLVED
  above): `dev-planning/state-ttl-legacy-backfill/spec-memory-backend-recovery.md`
  ports the same header detection + Rule 4 to the memory partition on this branch.
  Produce-side header is already emitted for memory partitions via the base
  `_prepare`. **(Back-compat note retained.)**

### Confirmed root cause (spec §8.6.3 Q5)

Recovery did **not** stay in the legacy verbatim branch
(`partition.py:246-256`). The flag-discovery gate (`partition.py:228-244`) flips
a recovering partition into TTL mode the moment it sees a default-CF replay value
that `_looks_like_stamped_value` (`partition.py:1029-1060`) accepts — i.e. whose
leading 8 bytes decode as `SENTINEL_NEVER` or `0 < stamp < 10**15`.

A legacy dedup store commonly stores an **8-byte big-endian epoch-ms timestamp**
as its value ("last seen at"). Such a value's first 8 bytes decode as a plausible
epoch-ms expiry (~1.7e12 < 1e15), so the heuristic **false-positives**:

1. The first such legacy record falsely flips the recovery partition
   (`uses_ttl_stamps = True`, `__ttl_enabled__` stamped on disk).
2. Every subsequent legacy record now takes the stamped main-CF branch
   (`partition.py:272`); its 8-byte prefix is read as the "stamp" and the Rule 4
   wallclock filter (`partition.py:285`) drops it as `stamp <= wallclock_now`
   (the epoch-ms values are in the past).
3. The default CF ends up holding ~1 record (only the one whose accidental
   "stamp" was in the future). `rocksdb_exact_keys` reads ~0.
4. The first live `ttl=` write sees `main_cf_has_user_data() == False`
   → empty-store fast path → 214,879 legacy records lost, never stamped.

This is **exactly** the live symptom set (§8.6.1): empty-store fast path, key
count growing from 1, recovery "successful". Refuted hypotheses: Q1 (CF-handle
vs instance) and Q3 (flush/visibility) — a 20k-record probe writing via
`get_column_family_handle("default")` is fully visible via
`get_or_create_column_family("default").items()`/`.keys()`. Q2 (recovery vs
processing instance) — `Store.assign_partition` caches **one** `StorePartition`
instance per partition; recovery and the live transaction share it. Q4
(`group_by` repartition) — not the mechanism here; the records are not in a
sibling partition, they are *dropped*.

### Reproduction

`tests/.../test_rocksdb/test_first_enablement_cold_restore.py::`
`TestColdRestoreFalseFlipRepro` — two `xfail(strict=True)` tests drive
`recover_from_changelog_message` with 8-byte epoch-ms legacy values and assert
the partition stays legacy / all records land. They fail today exactly as above
(1 of N survives). The happy-path tests in the same module
(`TestFirstEnablementColdRestoreHappyPath`) prove that when the legacy values do
**not** resemble stamps, landing + backfill already work — so the defect is
**solely** the flip-discovery heuristic, not the landing or backfill machinery.

### Why no in-scope code fix

`_looks_like_stamped_value` cannot be corrected by threshold tuning: a genuine
expiry stamp and a legacy epoch-ms value occupy the **same** numeric range
(~1.7e12). Narrowing the window to exclude epoch-ms would also reject real
stamps; widening is explicitly forbidden by the spec. Value content is
fundamentally insufficient to distinguish "stamped" from "legacy". A correct fix
needs an **out-of-band, unambiguous flip signal**. **(Resolved by the
out-of-band per-record changelog header — spec.md §8.7.)**

### Proposed resolutions (Buddy/Ludvík to choose) — DECIDED: A (per-record header)

- **A (recommended) — on-changelog flip marker.** At flip time, produce a single
  dedicated changelog marker message (e.g. a reserved key in the default CF, or a
  message header) that recovery flag-discovery keys off, replacing the
  value-content peek entirely. Format of stored values unchanged; recovery flips
  iff it has seen the marker, never on value content. Smallest correct change;
  needs a spec note (it is a new — if tiny — changelog signal, which §8.6.2
  currently forbids). Back-compat: a store flipped by the *current* build has no
  marker on its changelog, so a future restore of *those* changelogs would still
  rely on the heuristic — acceptable only if we accept the heuristic for the
  already-stamped case and use the marker going forward, OR re-emit the marker on
  the next flush. **→ CHOSEN, in the *per-record header* variant** (rides every
  record, survives compaction/retention; back-compat handled via option (a),
  re-seed fresh on the header build — see Resolution summary above and §8.7.4.)
- **B — per-value structural marker.** Add a 1-byte format tag in front of the
  8-byte stamp so a stamped value is unambiguous. Clean and self-describing, but
  it **is** a value-format change (bumps the codec / format version), which §7.2
  forbids and which breaks byte-identity with already-stamped stores. **REJECTED.**
- **C — accept + document.** Tell operators not to enable TTL via cold-restore
  on a legacy store whose values are 8-byte-prefixed; require an in-place
  enable-then-restore order. Unacceptable for the live Maxio case that hit this.
  **REJECTED.**

### Ask — DONE

Buddy/Ludvík picked **A (per-record header)** and spec'd the exact signal + the
back-compat story for already-flipped changelogs in **spec.md §8.7**. ArchDev
implements on this branch; the two `xfail(strict=True)` repro tests flip to
passing as the acceptance gate.
