"""
Red-first coverage for the ``NO_TIMESTAMP`` flip crash.

``advance_high_water`` ignores any negative timestamp -- added so a Kafka
``NO_TIMESTAMP`` (``-1``) cannot set a negative high-water that the unsigned
stamp packer then fails to persist with a raw ``struct.error``.

But ``_compute_stamp`` only rejects a timestamp that is ``None``, or a
*non-positive expiry*. ``timestamp=-1`` with any ttl above 1 ms yields a
positive expiry and is accepted, so such a write marked the batch as
flip-triggering while leaving the high-water unset. When that write was also the
one flipping a POPULATED legacy store, the backfill found no high-water and
raised ``IncompatibleStateStoreError`` out of the flush -- and because the offset
is never committed, the same record is redelivered forever.

The fix withholds only the FLIP TRIGGER from a write that cannot anchor it. The
write still succeeds, which is required by the sibling guard
``TestNegativeEventTimeGuard::test_negative_first_timestamp_does_not_crash_high_water``
-- rejecting it outright was the obvious fix and breaks that contract.
"""

from datetime import timedelta

import pytest

from quixstreams.state.exceptions import IncompatibleStateStoreError
from quixstreams.state.rocksdb import RocksDBOptions

# Kafka's NO_TIMESTAMP. Messages produced without a timestamp arrive with this.
NO_TIMESTAMP = -1


def _seed_legacy_records(partition, records, prefix=b"pfx"):
    """Write plain (un-stamped) records, leaving the store in legacy mode."""
    with partition.begin() as tx:
        for key, value in records:
            tx.set(key=key, value=value, prefix=prefix)


class TestNoTimestampFlip:
    def test_no_timestamp_ttl_write_does_not_crash_the_migration(
        self, store_partition_factory
    ):
        """
        Migrating a populated legacy store must not die because one record
        carried no Kafka timestamp.

        ``IncompatibleStateStoreError`` escaping the flush is the failure: the
        offset is then never committed and the record is redelivered, so the
        crash repeats on every attempt and no operator action clears it.
        """
        partition = store_partition_factory(
            "db", options=RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        )
        try:
            _seed_legacy_records(partition, [("k1", "v1"), ("k2", "v2")])

            try:
                with partition.begin() as tx:
                    tx.set(
                        key="knew",
                        value="vnew",
                        prefix=b"pfx",
                        timestamp=NO_TIMESTAMP,
                        ttl=timedelta(seconds=5),
                    )
            except IncompatibleStateStoreError as exc:
                pytest.fail(
                    "a NO_TIMESTAMP ttl= write crash-looped the legacy migration: "
                    f"{exc}"
                )
        finally:
            partition.close()

    def test_no_timestamp_ttl_write_does_not_flip_the_store(
        self, store_partition_factory
    ):
        """
        The mechanism: such a write must not flip a legacy store.

        A flip anchors every pre-existing record's expiry on the high-water, so
        flipping without one either crashes or has to invent a wall-clock
        expiry. Staying legacy defers the migration to the first write that can
        actually anchor it.
        """
        partition = store_partition_factory(
            "db", options=RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        )
        try:
            _seed_legacy_records(partition, [("k1", "v1")])
            with partition.begin() as tx:
                tx.set(
                    key="knew",
                    value="vnew",
                    prefix=b"pfx",
                    timestamp=NO_TIMESTAMP,
                    ttl=timedelta(seconds=5),
                )
            assert not partition.uses_ttl_stamps, (
                "a write with no usable event-time flipped the store, so the "
                "backfill anchored the legacy records on a high-water it does "
                "not have"
            )
        finally:
            partition.close()

    def test_positive_timestamp_ttl_write_still_flips(self, store_partition_factory):
        """
        Guard against over-correcting: a normal ttl= write must still flip a
        populated legacy store, or the fix breaks the migration itself.
        """
        partition = store_partition_factory(
            "db", options=RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        )
        try:
            _seed_legacy_records(partition, [("k1", "v1")])
            with partition.begin() as tx:
                tx.set(
                    key="knew",
                    value="vnew",
                    prefix=b"pfx",
                    timestamp=1_700_000_000_000,
                    ttl=timedelta(days=1),
                )
            assert partition.uses_ttl_stamps, "the store should have flipped"
        finally:
            partition.close()

    def test_a_later_good_timestamp_in_the_same_batch_still_flips(
        self, store_partition_factory
    ):
        """
        Withholding the trigger must not disarm the whole batch: if any OTHER
        write carries a real timestamp, the store still flips and the
        no-timestamp record is re-stamped along with it.
        """
        partition = store_partition_factory(
            "db", options=RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        )
        try:
            _seed_legacy_records(partition, [("k1", "v1")])
            with partition.begin() as tx:
                tx.set(
                    key="kbad",
                    value="vbad",
                    prefix=b"pfx",
                    timestamp=NO_TIMESTAMP,
                    ttl=timedelta(seconds=5),
                )
                tx.set(
                    key="kgood",
                    value="vgood",
                    prefix=b"pfx",
                    timestamp=1_700_000_000_000,
                    ttl=timedelta(days=1),
                )
            assert (
                partition.uses_ttl_stamps
            ), "a batch containing a well-timestamped ttl= write should still flip"
        finally:
            partition.close()
