"""
Invariant tests: the memory backend must never LOAD a persisted high-water.

``MemoryStorePartition`` persists ``TTL_HIGH_WATER_KEY`` into ``__metadata__``
on ``write()`` purely as write-only bookkeeping for RocksDB parity; nothing
ever reads it back. The former ``_load_high_water`` mirror of the RocksDB
clamp was deleted as dead code: it had zero call sites, ``_state`` is rebuilt
fresh per instance, and no ``__metadata__`` record is ever produced to the
changelog (``base/transaction.py`` skips ``LOCAL_ONLY_CFS`` at produce time),
so replay cannot resurrect the entry either. NOTE the replay-side
``LOCAL_ONLY_CFS`` guard is NOT what protects this: on a legacy (un-flipped)
partition the verbatim replay branch in ``recover_from_changelog_message``
writes BEFORE that guard — unreachability rests on the produce-side filter
plus zero readers.

These tests pin the H1 guarantee that replaced the deleted code: a poisoned
persisted high-water (the pre-H1 on-disk artifact shape) must never latch into
the live clock and mass-expire finite-stamped records. Each test plants the
poison in ``_state`` BEFORE the entry point under test runs, so a load path
re-added at that entry point goes red:

- construction/open — the RocksDB-equivalent wiring point
  (``RocksDBStorePartition._load_high_water`` is called at open). The
  ``__setattr__`` seam on the test subclass poisons ``_state`` at the instant
  ``__init__`` creates it (a constructor load has nothing to read before
  then) AND records every non-``None`` assignment to ``_high_water_ms``, so a
  construction-time load is caught even when the later unconditional
  ``self._high_water_ms = None`` (re)initialization clobbers its latch — the
  window the plain ``is None`` assert alone was blind to (finding 4.3).
- the first live write/flush — the exact-assignments assert pins that the
  ONLY value the clock ever latched is the write's own timestamp. This is
  poison-magnitude-independent: a poison BELOW the first write's timestamp
  (also parametrized) slips past every value-comparison arm because
  ``advance_high_water`` keeps the maximum, but not past the recorder.
- recovery-completion (``complete_recovery``) — the post-replay finalize
  seam, the only realistic point where a persisted entry could appear to
  carry information the live instance lacks. Covered in BOTH branches:
  Branch A (this-branch evidence, MIXED census — reached via genuine
  changelog replay, where the survivor-expiry/recovery-clock derivation
  lives; finding 4.2 proved a live-write-built partition never reaches it)
  and Branch B (empty census, hygiene exit).
- the live read path — a poisoned clock feeding the read-expiry filter makes
  ``stamp <= high_water`` true for every finite stamp (mass-expiry).

Honest scope, stated per arm in each test's docstring. Summary of what these
tests can NOT prove:

- The recorder seam exists only on the construction-test subclass. It sees
  loads that assign ``_high_water_ms`` through normal attribute assignment;
  a load that stores the value elsewhere, consumes it without storing, or
  bypasses ``__setattr__`` leaves no trace it can assert on.
- The recovery tests run a plain (seam-less) partition, so they detect a
  load only through its observable latch: any non-``None``
  ``_high_water_ms`` after Branch A (expected ``None`` — magnitude-
  independent), a clobbered ``BASE_TS`` after Branch B, or mass-expiry on
  read-back. A clamped load (``_MAX_PLAUSIBLE_STAMP_MS``-guarded, like the
  RocksDB twin) of the implausibly-large poison is indistinguishable from
  "no load" there; a clamped load of a PLAUSIBLE value would still latch and
  go red in the recovery arms.
- A load wired inside the ``recover_from_changelog_message`` replay loop
  itself is not exercised directly here; it is caught only through its
  effect on the clock at recovery-completion or read time.
"""

from datetime import timedelta

import pytest

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    METADATA_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.rocksdb.metadata import TTL_HIGH_WATER_KEY
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.state.serialization import int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

BASE_TS = 1_780_000_000_000
DAY_MS = 86_400_000
POISONED_HIGH_WATER_MS = 10**18
POISON_BYTES = int_to_bytes(POISONED_HIGH_WATER_MS)
# A poison BELOW the first write's timestamp (and far below the
# _MAX_PLAUSIBLE_STAMP_MS clamp): a load that latches it would be masked by
# any later max-keeping ``advance_high_water`` call, so only arms that observe
# the latch itself (the ``__setattr__`` recorder, or a clock expected to be
# ``None``) can detect it.
LOW_POISONED_HIGH_WATER_MS = BASE_TS - 1
LOW_POISON_BYTES = int_to_bytes(LOW_POISONED_HIGH_WATER_MS)


class PoisonedAtConstructionPartition(MemoryStorePartition):
    """
    Plants the poisoned ``__metadata__`` entry at the exact moment
    ``__init__`` assigns ``self._state``, so the poison pre-exists every
    subsequent construction statement — a constructor load can only read via
    ``_state``, and the poison is in it from the instant it exists.

    Additionally RECORDS every non-``None`` assignment to ``_high_water_ms``
    (into ``self.__dict__["_recorded_high_water_assignments"]``, created
    lazily so the seam works before ``__init__`` runs). This closes the
    clobber window the plain ``_high_water_ms is None`` assert left open: a
    load wired between ``_state``'s creation and the later unconditional
    ``self._high_water_ms: Optional[int] = None`` (re)initialization latches
    the poison and is then silently overwritten — invisible to the final
    value, but visible to the recorder.

    Test-local seam only: it intercepts the plain attribute assignments the
    production code already performs; no production hook is involved. A load
    that bypasses ``__setattr__`` (``object.__setattr__``) or stores the
    value in a different attribute is outside this seam's reach.
    """

    poison_bytes: bytes = POISON_BYTES

    def __setattr__(self, name, value):
        if name == "_state":
            value.setdefault(METADATA_CF_NAME, {})[TTL_HIGH_WATER_KEY] = (
                self.poison_bytes
            )
        elif name == "_high_water_ms" and value is not None:
            self.__dict__.setdefault("_recorded_high_water_assignments", []).append(
                value
            )
        super().__setattr__(name, value)

    @property
    def recorded_high_water_assignments(self) -> list:
        return self.__dict__.get("_recorded_high_water_assignments", [])


class LowPoisonedAtConstructionPartition(PoisonedAtConstructionPartition):
    """Same seam, but the poison is BELOW the first write's timestamp (and
    below the ``_MAX_PLAUSIBLE_STAMP_MS`` clamp), so a latched load would be
    masked by every value-comparison arm — only the assignment recorder and
    the pre-write ``is None`` window can catch it."""

    poison_bytes = LOW_POISON_BYTES


class TestHighWaterLoadClampMemory:
    @pytest.mark.parametrize(
        ("partition_cls", "poison_bytes"),
        [
            (PoisonedAtConstructionPartition, POISON_BYTES),
            (LowPoisonedAtConstructionPartition, LOW_POISON_BYTES),
        ],
        ids=["poison-above-first-write", "poison-below-first-write"],
    )
    def test_construction_and_first_write_ignore_poisoned_high_water(
        self, partition_cls, poison_bytes
    ):
        """No load of the persisted high-water may run at construction or
        anywhere before the first live write latches its own timestamp.

        True scope of this arm (stated precisely, per finding 4.3):

        - The ``__setattr__`` recorder catches a load wired ANYWHERE in
          ``__init__`` after ``_state`` exists — including the window before
          the unconditional ``self._high_water_ms = None`` (re)initialization,
          where a latch is silently clobbered and the plain ``is None`` assert
          alone proved blind. A constructor load cannot run before ``_state``
          exists (there is nothing to read from), so this covers the full
          construction window for loads that assign ``_high_water_ms``.
        - The exact-assignments assert after the first write extends that to
          the construction-to-first-flush window, independent of poison
          magnitude: a LOW poison latched before the write would be masked by
          the max-keeping ``advance_high_water`` (final clock still
          ``BASE_TS``), but the recorder still sees the extra assignment.
        - NOT provable with this seam: a load that consumes the persisted
          value without assigning ``_high_water_ms``, stores it in another
          attribute, or writes via ``object.__setattr__``. Those leave no
          observable trace this test can assert on.
        """
        # The poison is planted DURING construction (see the subclass seam),
        # i.e. before any construction-time load path could run.
        partition = partition_cls(changelog_producer=None)
        assert partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] == poison_bytes

        # RED if a load is wired anywhere in construction: even one whose
        # latch is later clobbered by the ``= None`` (re)initialization was
        # recorded by the seam. (The ``is None`` assert alone only catches a
        # load wired AFTER that line.)
        assert partition.recorded_high_water_assignments == []
        assert partition._high_water_ms is None

        # First live ttl= write: the clock must come from the write's own
        # timestamp.
        ttl = timedelta(days=7)
        with partition.begin() as tx:
            tx.set(key="k0", value="v0", prefix=b"pfx", timestamp=BASE_TS, ttl=ttl)
        assert partition.uses_ttl_stamps is True
        assert partition._high_water_ms == BASE_TS

        # The ONLY non-None assignment the clock ever received is the write's
        # own timestamp — magnitude-independent: a load latching a LOW poison
        # anywhere before this flush leaves [poison, BASE_TS] here even though
        # the final ``== BASE_TS`` above stays green.
        assert partition.recorded_high_water_assignments == [BASE_TS]

        # Read path, with the poison re-planted (the flush above overwrote the
        # persisted entry with the legitimate clock): the finite-stamped
        # record must stay readable. No ``timestamp=`` here, so the read gates
        # purely on the partition's live clock. (Behaviorally meaningful for
        # the above-first-write poison only — a below-first-write poison could
        # never mass-expire this record anyway.)
        partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = poison_bytes
        assert partition.begin().get(key="k0", prefix=b"pfx") == "v0"
        partition.close()

    def test_recovery_completion_ignores_poisoned_high_water(self):
        """Branch A (this-branch evidence, MIXED census) of
        ``complete_recovery`` must not latch the poisoned persisted high-water.

        The partition is driven through changelog REPLAY — one header-true
        stamped record (sets ``_recovery_saw_stamped``, flips the partition,
        captures the recovery clock) plus one header-absent legacy leftover
        (censused, and NOT stamp-shaped, so the census is not-all-stamped and
        completion falls through to the survivor-expiry derivation) — so
        ``complete_recovery`` genuinely enters Branch A and executes the
        no-``legacy_records_ttl`` ``else:`` arm that derives ``now`` /
        ``survivor_expiry``, the natural site for a re-added load. (The
        previous version of this test built the partition via a live
        ``tx.set(..., ttl=...)``, which leaves ``_recovery_saw_stamped`` False
        and the census empty, so it exited via Branch B's hygiene path and a
        load wired inside Branch A was proven undetectable.)

        Replay and completion never touch the live clock on this backend, so
        the post-completion expectation is ``_high_water_ms is None`` — which
        makes this arm poison-magnitude-independent: ANY latched value (the
        implausibly-large poison OR a plausible low one that the RocksDB-style
        clamp would pass) fails the ``is None`` assert.

        True scope: this catches a load anywhere in Branch A (or B) that
        assigns ``_high_water_ms`` a non-``None`` value during
        ``complete_recovery``. A load that reads the poison but discards it,
        or stores it somewhere other than ``_high_water_ms``, has no
        observable effect on this plain (seam-less) partition and is NOT
        detected here.
        """
        partition = MemoryStorePartition(changelog_producer=None)
        partition._now_ms = lambda: BASE_TS  # noqa: E731

        # Step 1: MIXED replay. Header-true stamped survivor first (flips the
        # partition + latches _recovery_saw_stamped + tracks the survivor
        # expiry), then a header-absent legacy leftover (censused verbatim;
        # its value must not decode as a stamp so the census is
        # not-all-stamped and Branch A reaches the completion arm).
        survivor_expiry = BASE_TS + 30 * DAY_MS
        partition.recover_from_changelog_message(
            key=b"pfx|" + json_dumps("survivor"),
            value=encode_ttl_value(survivor_expiry, json_dumps("v-stamped")),
            cf_name="default",
            offset=0,
            ttl_stamped=True,
        )
        partition.recover_from_changelog_message(
            key=b"pfx|" + json_dumps("leftover"),
            value=json_dumps("v-legacy"),
            cf_name="default",
            offset=1,
            ttl_stamped=False,
        )
        # Branch-A preconditions — the exact gate the old version never met.
        assert partition._recovery_saw_stamped is True
        assert partition.uses_ttl_stamps is True
        pending = partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        assert b"pfx|" + json_dumps("leftover") in pending
        # Replay never advances the live clock (the frontier is frozen).
        assert partition._high_water_ms is None

        # Step 2: poison the persisted metadata exactly the way ``write()``
        # persists it (mimicking an on-disk artifact from before the H1 guard
        # existed) — BEFORE the entry point under test runs.
        partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = POISON_BYTES

        # Step 3: completion runs Branch A's survivor-expiry derivation.
        partition.complete_recovery()

        # Branch A really ran: census drained, done-marker recorded in RAM.
        assert TTL_BACKFILL_PENDING_CF_NAME not in partition._state
        assert TTL_MIGRATION_DONE_KEY in partition._state.get(TTL_SYSTEM_CF_NAME, {})

        # The poisoned persisted entry must not have latched. ``None`` is the
        # only legitimate value here, so ANY latch — implausible or plausible
        # — goes red.
        assert partition._high_water_ms is None

        # Step 4: both records are still readable (not mass-expired): a
        # latched poison feeding the read-expiry filter would make
        # ``stamp <= high_water`` true for every finite stamp.
        assert partition.begin().get(key="survivor", prefix=b"pfx") == "v-stamped"
        assert partition.begin().get(key="leftover", prefix=b"pfx") == "v-legacy"
        partition.close()

    def test_recovery_completion_hygiene_branch_ignores_poisoned_high_water(self):
        """Branch B (no this-branch evidence: live-write partition, empty
        census) of ``complete_recovery`` must not latch the poisoned persisted
        high-water either. This preserves the coverage the previous
        recovery-completion test provided (it only ever exercised this
        branch): a load wired in Branch B's hygiene exit would overwrite the
        legitimately-latched ``BASE_TS`` clock and fail the ``== BASE_TS``
        assert — for a HIGH poison via the direct comparison, and for a LOW
        poison too, because a load assigns directly rather than max-keeping.
        """
        # Step 1: one ttl= write; the record reads back and the live clock is
        # set.
        partition = MemoryStorePartition(changelog_producer=None)
        ttl = timedelta(days=7)
        with partition.begin() as tx:
            tx.set(key="k0", value="v0", prefix=b"pfx", timestamp=BASE_TS, ttl=ttl)
        assert partition._high_water_ms == BASE_TS

        # Step 2: poison the persisted metadata BEFORE the entry point runs.
        partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = POISON_BYTES

        # Step 3: with no pending census this is the benign hygiene branch, so
        # the ONLY observable effect a re-added load could have here is on the
        # clock.
        partition.complete_recovery()
        assert partition._high_water_ms == BASE_TS

        # Step 4: the finite-stamped record is still readable (not
        # mass-expired).
        assert partition.begin().get(key="k0", prefix=b"pfx") == "v0"
        partition.close()
