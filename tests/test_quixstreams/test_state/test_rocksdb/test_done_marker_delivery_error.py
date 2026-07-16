"""
Bug #2 (RocksDB): best-effort done-marker only catches ChangelogFlushError.

``complete_recovery``'s fully-migrated (empty pending census) branch wraps
``_produce_migration_done_marker()`` in ``except ChangelogFlushError`` and is
documented "must NOT fail recovery." However,
``_produce_migration_done_marker`` calls ``changelog_producer.produce()``,
which routes to ``InternalProducer.produce()`` -> ``_raise_for_error()`` at
the top. When a delivery error is latched (e.g. a sibling partition's failed
delivery on the shared migration producer), ``_raise_for_error()`` raises
``KafkaProducerDeliveryError`` -- which is NOT a ``ChangelogFlushError``, so
it escapes the ``except`` and crashes ``complete_recovery``.

**Trigger:** a fully-migrated partition (empty ``__ttl_backfill_pending__``)
whose changelog producer raises ``KafkaProducerDeliveryError`` from
``produce()``.

Validates spec: recovery best-effort done-marker -- a failed marker must NOT
fail recovery; it only forgoes the optimization (the next restart retries).
"""

from unittest.mock import MagicMock, PropertyMock

from quixstreams.kafka.exceptions import KafkaProducerDeliveryError
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

DAY_MS = 86_400_000


def _producer_mock():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _mock_kafka_error():
    """Create a mock KafkaError for constructing KafkaProducerDeliveryError."""
    err = MagicMock()
    err.code.return_value = -1
    err.str.return_value = "simulated delivery error from sibling partition"
    return err


def _replay_all_stamped(partition, n, stamp_expiry, now_ms):
    """Replay ``n`` stamped records (no legacy) so the partition flips and the
    pending census stays empty."""
    partition._now_ms = lambda: now_ms  # noqa: E731
    for i in range(n):
        partition.recover_from_changelog_message(
            key=f"pfx|s{i}".encode(),
            value=encode_ttl_value(stamp_expiry, f"stamped-{i}".encode()),
            cf_name="default",
            offset=i,
            ttl_stamped=True,
        )


class TestDoneMarkerDeliveryErrorRocksDB:
    def test_complete_recovery_does_not_raise_on_delivery_error(self, tmp_path):
        """complete_recovery on a fully-migrated store (empty pending census)
        must NOT raise when the changelog producer's produce() raises
        KafkaProducerDeliveryError.

        BUG: the except clause only catches ChangelogFlushError, so a latched
        delivery error on the shared migration producer escapes and crashes
        recovery."""
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        producer = _producer_mock()

        path = (tmp_path / "done-marker-err").as_posix()
        opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
        partition = RocksDBStorePartition(
            path, options=opts, changelog_producer=producer
        )

        # Replay only stamped records -> flipped, empty pending census.
        _replay_all_stamped(partition, n=3, stamp_expiry=stamp_expiry, now_ms=now_ms)
        assert partition.uses_ttl_stamps is True

        # Now make the producer raise KafkaProducerDeliveryError on produce().
        # This simulates a latched delivery error from a sibling partition's
        # failed delivery on the shared migration producer.
        producer.produce.side_effect = KafkaProducerDeliveryError(_mock_kafka_error())

        # complete_recovery must NOT raise -- the done-marker is best-effort.
        # On the buggy code this raises KafkaProducerDeliveryError.
        partition.complete_recovery()

        partition.close()

    def test_complete_recovery_does_not_raise_on_flush_delivery_error(self, tmp_path):
        """Same scenario but the delivery error comes from flush() instead of
        produce(). Both produce() and flush() can raise
        KafkaProducerDeliveryError; both must be caught."""
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        producer = _producer_mock()

        path = (tmp_path / "done-marker-flush-err").as_posix()
        opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
        partition = RocksDBStorePartition(
            path, options=opts, changelog_producer=producer
        )

        _replay_all_stamped(partition, n=3, stamp_expiry=stamp_expiry, now_ms=now_ms)
        assert partition.uses_ttl_stamps is True

        # produce() succeeds, but flush() raises KafkaProducerDeliveryError.
        producer.flush.side_effect = KafkaProducerDeliveryError(_mock_kafka_error())

        # Must NOT raise.
        partition.complete_recovery()

        partition.close()
