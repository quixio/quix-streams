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
poison in ``_state`` BEFORE the entry point under test runs, so an UNCLAMPED
load path re-added at that entry point goes red:

- construction/open — the RocksDB-equivalent wiring point
  (``RocksDBStorePartition._load_high_water`` is called at open). The
  ``__setattr__`` seam on the test subclass poisons ``_state`` at the instant
  ``__init__`` creates it, before any later construction statement could read
  it; an unclamped construction-time load latches ``POISONED_HIGH_WATER_MS``
  and fails the ``_high_water_ms is None`` assertion.
- the first live write/flush — ``advance_high_water`` keeps the maximum, so a
  poisoned clock latched anywhere before the first flush fails the
  ``== BASE_TS`` assertion.
- recovery-completion (``complete_recovery``) — the post-replay finalize seam,
  the only realistic point where a persisted entry could appear to carry
  information the live instance lacks.
- the live read path — a poisoned clock feeding the read-expiry filter makes
  ``stamp <= high_water`` true for every finite stamp (mass-expiry).

Honest scope: a re-added load that DOES carry the ``_MAX_PLAUSIBLE_STAMP_MS``
clamp (see ``RocksDBStorePartition._load_high_water``, the wired clamped twin)
keeps these tests green by design — the pinned invariant is "poison never
latches", not "no load code may exist". A load wired inside the
``recover_from_changelog_message`` replay loop itself is not exercised
directly here; it is caught only through its effect on the clock at
recovery-completion or read time.
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import METADATA_CF_NAME
from quixstreams.state.rocksdb.metadata import TTL_HIGH_WATER_KEY
from quixstreams.state.serialization import int_to_bytes

BASE_TS = 1_780_000_000_000
POISONED_HIGH_WATER_MS = 10**18
POISON_BYTES = int_to_bytes(POISONED_HIGH_WATER_MS)


class PoisonedAtConstructionPartition(MemoryStorePartition):
    """
    Plants the poisoned ``__metadata__`` entry at the exact moment
    ``__init__`` assigns ``self._state``, so the poison pre-exists every
    subsequent construction statement — including any future load call wired
    where RocksDB wires ``_load_high_water`` (at open, after the state
    container exists). Test-local seam only: it intercepts the plain attribute
    assignment the production ``__init__`` already performs; no production
    hook is involved.
    """

    def __setattr__(self, name, value):
        if name == "_state":
            value.setdefault(METADATA_CF_NAME, {})[TTL_HIGH_WATER_KEY] = POISON_BYTES
        super().__setattr__(name, value)


class TestHighWaterLoadClampMemory:
    def test_construction_and_first_write_ignore_poisoned_high_water(self):
        # The poison is planted DURING construction (see the subclass seam),
        # i.e. before any construction-time load path could run.
        partition = PoisonedAtConstructionPartition(changelog_producer=None)
        assert partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] == POISON_BYTES

        # RED if an unclamped load is wired at the RocksDB-equivalent point
        # (end of construction / open): it would latch POISONED_HIGH_WATER_MS.
        assert partition._high_water_ms is None

        # First live ttl= write: the clock must come from the write's own
        # timestamp. ``advance_high_water`` keeps the maximum, so a poisoned
        # clock latched anywhere before this flush stays visible here.
        ttl = timedelta(days=7)
        with partition.begin() as tx:
            tx.set(key="k0", value="v0", prefix=b"pfx", timestamp=BASE_TS, ttl=ttl)
        assert partition.uses_ttl_stamps is True
        assert partition._high_water_ms == BASE_TS

        # Read path, with the poison re-planted (the flush above overwrote the
        # persisted entry with the legitimate clock): the finite-stamped
        # record must stay readable. No ``timestamp=`` here, so the read gates
        # purely on the partition's live clock.
        partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = POISON_BYTES
        assert partition.begin().get(key="k0", prefix=b"pfx") == "v0"
        partition.close()

    def test_recovery_completion_ignores_poisoned_high_water(self):
        # Step 1: one ttl= write; the record reads back and the live clock is
        # set.
        partition = MemoryStorePartition(changelog_producer=None)
        ttl = timedelta(days=7)
        with partition.begin() as tx:
            tx.set(key="k0", value="v0", prefix=b"pfx", timestamp=BASE_TS, ttl=ttl)
        assert partition._high_water_ms == BASE_TS

        # Step 2: poison the persisted metadata exactly the way ``write()``
        # persists it (mimicking an on-disk artifact from before the H1 guard
        # existed) — BEFORE the entry point under test runs.
        partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = POISON_BYTES

        # Step 3: recovery-completion is the post-replay finalize seam. With
        # no pending census this is the benign hygiene branch, so the ONLY
        # observable effect a re-added load could have here is on the clock.
        partition.complete_recovery()
        assert partition._high_water_ms == BASE_TS

        # Step 4: the finite-stamped record is still readable (not
        # mass-expired): an unclamped load feeding the read-expiry filter
        # would make ``stamp <= high_water`` true for every finite stamp.
        assert partition.begin().get(key="k0", prefix=b"pfx") == "v0"
        partition.close()
