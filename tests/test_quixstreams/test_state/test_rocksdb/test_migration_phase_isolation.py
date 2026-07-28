"""
Structural regression tests for PER-PHASE migration delivery accounting
(``quixstreams.state.base.migration_flush.MigrationDeliveryPhase``).

Every legacy-TTL migration produce phase — a live backfill, a
recovery-completion re-stamp pass, one done-marker produce — constructs its OWN
counter object, hands that object's bound ``on_delivery`` to each record it
produces, and hands its bound ``counters`` to the confirm helper. Two
user-visible properties follow structurally, and this file pins both:

1. **A partition is never left permanently uncorroborable after ONE swallowed
   done-marker delivery failure.** The earlier phase's unacked record is not in
   a later phase's counters at all, so a later marker whose own delivery acks
   confirms normally and corroboration completes. (With one shared counter pair
   this was a permanent wedge: the stale ``produced > acked`` skew pinned every
   later confirm — and with it the TTL sweep — off for the life of the
   instance.)
2. **A late ack from a finished phase can never confirm a later phase.** The ack
   lands on the *producing* phase's (by then otherwise unreferenced) object, so
   it can neither wedge the later phase nor falsely confirm it: the later
   phase's own unacked record still raises ``ChangelogFlushError`` and the local
   done-marker is never written ahead of the changelog.

Both properties are statements about the store's behavior, not about any
particular accounting implementation. This file is also the only coverage of the
reachable done-marker pair "``complete_recovery``'s empty-census best-effort
marker → ``corroborate_adoption``'s marker".
"""

from datetime import timedelta
from typing import Optional

import pytest

from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.metadata import METADATA_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import (
    TTL_ADOPT_PENDING_KEY,
    TTL_MIGRATION_DONE_KEY,
)
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000


class _FirstFlushFailsProducer:
    """A ``ChangelogProducer``-shaped double that records each ``produce()``'s
    ``on_delivery`` and serves the recorded callbacks from ``flush()`` **only**
    — never from ``produce()``, which is what a real confluent producer does
    (and what makes the confirm loop's per-slice ``counters()`` re-read
    observable; a stub that acks inside ``produce()`` resolves on slice 1 with
    the counters read once).

    The FIRST ``flush()`` serves its callbacks with a delivery ERROR; every
    later one serves them successfully. ``flush()`` always reports a drained
    (``0``) global queue: a failed delivery leaves the send queue too, which is
    exactly the ``DRAINED_UNACKED`` signature.
    """

    changelog_name = "cl"
    partition = 0

    def __init__(self) -> None:
        self._pending: list = []
        self.produced_keys: list[bytes] = []
        self.flush_count = 0

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers=None,
        migration: bool = False,
        on_delivery=None,
    ) -> None:
        self.produced_keys.append(key)
        self._pending.append(on_delivery)

    def flush(self, timeout: Optional[float] = None, migration: bool = False) -> int:
        self.flush_count += 1
        err: Optional[Exception] = None
        if self.flush_count == 1:
            err = RuntimeError("simulated delivery failure")
        pending, self._pending = self._pending, []
        for callback in pending:
            if callback is not None:
                callback(err, None)
        return 0


class _OneRoundTripBehindProducer:
    """A ``ChangelogProducer``-shaped double whose deliveries are always one
    ``flush()`` round-trip behind: each ``flush()`` serves (successfully) only
    the callbacks that were already queued when the PREVIOUS ``flush()`` ran,
    and reports a drained (``0``) global queue.

    So the record produced by the CURRENT phase is never acked by that phase's
    own confirming flush, while the PREVIOUS phase's still-in-flight record acks
    during it — the exact late-cross-phase-ack shape property 2 is about.
    """

    changelog_name = "cl"
    partition = 0

    def __init__(self) -> None:
        self.callbacks: list = []
        self.produced_keys: list[bytes] = []
        self._served = 0
        self._watermark = 0

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers=None,
        migration: bool = False,
        on_delivery=None,
    ) -> None:
        self.produced_keys.append(key)
        self.callbacks.append(on_delivery)

    def flush(self, timeout: Optional[float] = None, migration: bool = False) -> int:
        while self._served < self._watermark:
            callback = self.callbacks[self._served]
            self._served += 1
            if callback is not None:
                callback(None, None)
        self._watermark = len(self.callbacks)
        return 0


def _rocksdb_partition(tmp_path, name="db", changelog_producer=None):
    path = (tmp_path / name).as_posix()
    opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(
        path, options=opts, changelog_producer=changelog_producer
    )


def _v3240_msg(key_str, user_value, expiry_ms, prefix=b"pfx"):
    """One v3.24.0-style default-CF changelog message (already 8B-stamped)."""
    raw_key = prefix + b"|" + json_dumps(key_str)
    stamped = encode_ttl_value(expiry_ms, json_dumps(user_value))
    return (raw_key, stamped, False)


def _provision_partition(tmp_path, name, producer, n_keys=3):
    """Cold-provisionally-adopt a fresh partition via a 100%-stamped,
    not-all-past, full-coverage census replay (mirrors
    ``test_v3240_adoption_rollback_safety.py._provision_partition``). The
    adoption itself is changelog-silent, so ``producer`` sees no produce here.
    """
    expiry_ms = NOW_MS + 7 * DAY_MS
    msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry_ms) for i in range(n_keys)]
    partition = _rocksdb_partition(tmp_path, name=name, changelog_producer=producer)
    partition._now_ms = lambda: NOW_MS  # noqa: E731
    offset = 0
    for key, value, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1
    partition.complete_recovery()

    assert partition.uses_ttl_stamps is True, "precondition: flipped"
    assert partition._adopt_provisional is True, "precondition: provisional"
    metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
    assert (
        metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
    ), "precondition: pending marker on disk"
    return partition


def test_swallowed_marker_failure_does_not_block_later_corroboration(tmp_path):
    """One GENUINELY failed + swallowed done-marker delivery must not leave the
    provisionally cold-adopted store permanently uncorroborable.

    Drives the reachable production sequence end to end (no counter poking):

    1. cold-provisionally adopt a store, then close it, so the reopen loads
       ``__ttl_adopt_pending__`` from disk and re-arms ``_adopt_provisional``;
    2. reopen against a producer whose first delivery FAILS, and replay an
       unclean-shutdown changelog tail of only header-true stamped records —
       ``_recovery_saw_stamped`` latches and nothing is censused;
    3. ``complete_recovery()`` therefore takes Branch A with
       ``pending_count == 0`` and fires the BEST-EFFORT done marker
       (``rocksdb/partition.py``'s empty-census branch). Its delivery fails, the
       ``ChangelogFlushError`` is swallowed, and — because the raise happened
       BEFORE the local write — no local marker exists;
    4. a live ``tx.set(..., ttl=...)`` then corroborates through the REAL gate
       (``RocksDBPartitionTransaction._maybe_corroborate_adoption`` plus the
       deferred ``finalize_corroboration_teardown`` ordering), not via a direct
       ``corroborate_adoption()`` call.

    Step 3 deliberately fails the first marker's delivery, so no local marker is
    written and the separate pre-existing "unguarded provisional-store done
    marker" issue does not fire here; nothing below depends on its behavior.
    """
    provisioned = _provision_partition(tmp_path, "db", _FirstFlushFailsProducer())
    provisioned.close()

    producer = _FirstFlushFailsProducer()
    partition = _rocksdb_partition(tmp_path, name="db", changelog_producer=producer)
    try:
        assert partition.uses_ttl_stamps is True, "reopen: flipped from disk"
        assert partition._adopt_provisional is True, "reopen: provisional from disk"
        partition._now_ms = lambda: NOW_MS  # noqa: E731

        key, value, _ = _v3240_msg("k0", "v0", NOW_MS + 7 * DAY_MS)
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=10,
            ttl_stamped=True,
        )
        assert partition._recovery_saw_stamped is True
        assert partition._count_backfill_pending() == 0, "nothing censused"

        # Marker #1: produced, delivery FAILS, ChangelogFlushError swallowed.
        partition.complete_recovery()
        assert producer.produced_keys == [TTL_MIGRATION_DONE_KEY]
        assert producer.flush_count == 1
        assert partition._has_local_migration_done_marker() is False, (
            "the failed marker must have raised BEFORE the local write, so the "
            "swallow leaves the store unmarked"
        )
        assert partition._adopt_provisional is True, "still uncorroborated"

        # Marker #2, through the real live-write corroboration gate. Its own
        # delivery acks, and marker #1's stale unacked record is not in this
        # phase's counters at all — so this must NOT raise.
        with partition.begin() as tx:
            tx.set(
                key="klive",
                value="vlive",
                prefix=b"pfx",
                timestamp=NOW_MS,
                ttl=timedelta(days=1),
            )

        assert partition._adopt_provisional is False, (
            "corroboration must complete once a marker with its own successful "
            "delivery is produced, regardless of an earlier swallowed failure"
        )
        metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert (
            metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is None
        ), "the pending marker must be cleared once corroboration completes"
        assert partition._has_local_migration_done_marker() is True
    finally:
        # Release the RocksDB directory lock even on an assertion failure, so a
        # red run does not block ``tmp_path`` cleanup for the session (Windows).
        partition.close()


def test_late_ack_from_earlier_phase_cannot_confirm_a_later_phase(tmp_path):
    """The headline structural proof: an ack belonging to an EARLIER phase must
    never satisfy a LATER phase's confirm.

    Two done-marker phases run on ONE partition instance against a producer that
    is always one ``flush()`` round-trip behind. During marker #2's confirming
    flush, marker #1's record acks (late) while marker #2's own record does not,
    and the queue reports drained. Marker #2 must therefore raise
    ``DRAINED_UNACKED`` and leave the store unmarked: its own record is NOT on
    the changelog, so writing the local marker would break changelog-first.
    """
    producer = _OneRoundTripBehindProducer()
    partition = _rocksdb_partition(tmp_path, changelog_producer=producer)
    try:
        # Marker #1: nothing has acked yet, so its own confirm raises. Swallowed
        # here exactly as ``complete_recovery``'s empty-census branch does.
        with pytest.raises(ChangelogFlushError):
            partition._produce_migration_done_marker()
        assert partition._has_local_migration_done_marker() is False

        # Marker #2: the flush serves marker #1's callback (success) and leaves
        # marker #2's own record unacked, still reporting a drained queue.
        with pytest.raises(ChangelogFlushError):
            partition._produce_migration_done_marker()

        assert producer.produced_keys == [
            TTL_MIGRATION_DONE_KEY,
            TTL_MIGRATION_DONE_KEY,
        ]
        assert partition._has_local_migration_done_marker() is False, (
            "marker #2's own record never acked, so the local marker must not "
            "be written — an earlier phase's late ack is not a substitute"
        )

        # The load-bearing structural assertions. ``on_delivery`` is a bound
        # method, so ``__self__`` IS the phase object each produce site used.
        first, second = producer.callbacks
        assert first.__self__ is not second.__self__, (
            "each produce phase must own its counter object; sharing one across "
            "phases is the bug this design removes"
        )
        assert first.__self__.counters() == (1, 1), (
            "the late ack landed on the EARLIER (now otherwise unreferenced) "
            "phase object — that is the design, not a leak"
        )
        assert second.__self__.counters() == (1, 0), (
            "the later phase must see only its own produce, still unacked"
        )
    finally:
        partition.close()
