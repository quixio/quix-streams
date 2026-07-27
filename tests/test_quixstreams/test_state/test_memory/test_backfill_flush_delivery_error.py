"""
Regression tests for findings 1 and 2 (batch4 code review of commit
``56be260b``), memory-backend counterparts of
``test_rocksdb/test_backfill_flush_delivery_error.py``.

Finding 1 -- ``MemoryStorePartition.complete_recovery`` (~line 820): the
recovery-completion path produces the pending-census leftover re-stamps
through the migration producer (incrementing ``_backfill_produced``), then
issues a bare ``self._changelog_producer.flush(migration=True)`` whose return
value is **ignored**, before dropping the census and writing the done-marker.
It never calls ``_confirm_migration_delivery_or_raise``, so a re-stamp that
failed delivery (the delivery callback fired with ``err is not None``, so
``_backfill_produced > _backfill_acked``) is silently treated as
successfully migrated -- the local store then marks "done" ahead of the
changelog.

Finding 2 -- ``MemoryStorePartition._confirm_migration_delivery_or_raise``
(~line 858): raises only when ``unproduced > 0``. When the shared producer's
queue is fully drained (``unproduced == 0``) but this partition's own
``_backfill_produced > _backfill_acked`` (a failed delivery that was drained
without ever acking), it does **not** raise -- the exact drained-but-unacked
case ``RocksDBStorePartition._flush_backfill_changelog`` was hardened against
(finding M3, batch4 re-review).

The changelog-first invariant (documented on both methods) is that the local
commit / local "done" state must never proceed while an unacked/failed record
for this partition exists -- so both must raise ``ChangelogFlushError``, not
return cleanly.
"""

import pytest

from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

DAY_MS = 86_400_000
BASE_TS = 1_780_000_000_000


class _FailingRestampProducer:
    """A stub migration producer whose every ``produce()`` immediately fires
    its delivery callback with a delivery error (never acks), and whose
    ``flush()`` reports a 0 backlog -- e.g. because a sibling partition's
    record already drained the shared queue, even though THIS partition's
    own record failed delivery."""

    def __init__(self):
        self.produced_keys = []

    def produce(self, key, value, headers=None, migration=False, on_delivery=None):
        self.produced_keys.append(key)
        if on_delivery is not None:
            on_delivery(RuntimeError("simulated delivery failure"))

    def flush(self, timeout=None, migration=False):
        return 0


def _build_mixed_recovery_census(producer):
    """Replay a MIXED changelog (one header-true stamped survivor, one
    header-absent legacy leftover) so ``complete_recovery`` reaches the
    non-empty-pending completion branch (``_recovery_saw_stamped`` True,
    ``uses_ttl_stamps`` True, one un-stamped leftover censused)."""
    partition = MemoryStorePartition(changelog_producer=producer)
    partition._now_ms = lambda: BASE_TS  # noqa: E731

    survivor_expiry = BASE_TS + 30 * DAY_MS
    partition.recover_from_changelog_message(
        key=b"pfx|survivor",
        value=encode_ttl_value(survivor_expiry, b"stamped"),
        cf_name="default",
        offset=0,
        ttl_stamped=True,
    )
    partition.recover_from_changelog_message(
        key=b"pfx|leftover",
        value=b"legacy-value",
        cf_name="default",
        offset=1,
        ttl_stamped=False,
    )
    assert partition._recovery_saw_stamped is True
    assert partition.uses_ttl_stamps is True
    assert b"pfx|leftover" in partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
    return partition


class TestMemoryRecoveryCompletionIgnoresFlushResult:
    """Finding 1: the recovery-completion re-stamp flush's result is ignored."""

    def test_complete_recovery_raises_on_failed_restamp_delivery(self):
        producer = _FailingRestampProducer()
        partition = _build_mixed_recovery_census(producer)

        # Isolate finding 1 from finding 2: remove the done-marker step (a
        # separate call site, covered by its own test below) so only the
        # ignored bare flush right after the re-stamp loop is exercised.
        partition._produce_migration_done_marker = lambda: None

        # Desired: a failed re-stamp delivery must abort completion rather
        # than silently drop the census and (elsewhere) mark migration done.
        with pytest.raises(ChangelogFlushError):
            partition.complete_recovery()

        # The failed re-stamp really was produced through the counter-tracked
        # route (sanity: the bug is not "nothing was produced").
        assert partition._backfill_produced > partition._backfill_acked
        partition.close()


class TestMemoryConfirmMigrationDeliveryMissingGuard:
    """Finding 2: ``_confirm_migration_delivery_or_raise`` only checks
    ``unproduced > 0``, missing the drained-but-unacked (``produced > acked``
    while ``unproduced == 0``) guard."""

    def test_confirm_raises_when_queue_drained_but_partition_has_unacked_record(self):
        partition = MemoryStorePartition(changelog_producer=None)

        class _FlushZeroProducer:
            """A stub whose ``flush()`` reports a 0 GLOBAL backlog (queue
            drained), even though THIS partition produced one record that
            failed delivery (its callback fired with ``err is not None``,
            which never increments ``_backfill_acked``)."""

            def flush(self, timeout=None, migration=False):
                return 0

        # Simulate: this partition produced 1 backfill record whose delivery
        # callback fired with an error (never acked).
        partition._backfill_produced = 1
        partition._backfill_acked = 0

        with pytest.raises(ChangelogFlushError):
            partition._confirm_migration_delivery_or_raise(
                _FlushZeroProducer(), "test context"
            )

        partition.close()
