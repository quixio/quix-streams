import logging
from unittest.mock import MagicMock

from quixstreams.state.rocksdb import RocksDBStorePartition
from tests.utils import ConfluentKafkaMessageStub


class TestRecoveryPartition:
    def test_set_watermarks(self, recovery_partition_factory):
        recovery_partition = recovery_partition_factory()
        recovery_partition.set_watermarks(50, 100)
        assert recovery_partition.changelog_lowwater == 50
        assert recovery_partition.changelog_highwater == 100

    def test_needs_recovery(self, recovery_partition_factory):
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = 10

        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        recovery_partition.set_watermarks(0, 20)
        assert recovery_partition.needs_recovery

    def test_needs_recovery_caught_up(self, recovery_partition_factory):
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = 10
        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        recovery_partition.set_watermarks(0, 20)
        store_partition.get_changelog_offset.return_value = 20
        assert not recovery_partition.needs_recovery

    def test_needs_recovery_no_valid_offsets(self, recovery_partition_factory):
        # Create a RecoveryPartition with the offset ahead of the watermark
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = 101

        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        recovery_partition.set_watermarks(100, 100)
        assert not recovery_partition.needs_recovery
        assert recovery_partition.needs_offset_update

    def test_recover_from_changelog_message(self, recovery_partition_factory):
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = 10
        recovery_partition = recovery_partition_factory(
            store_partition=store_partition, committed_offset=1
        )
        recovery_partition.set_watermarks(10, 20)
        msg = ConfluentKafkaMessageStub()
        recovery_partition.recover_from_changelog_message(msg)

        store_partition.recover_from_changelog_message.assert_called_with(
            changelog_message=msg, committed_offset=1
        )

    def test_update_offset(self, recovery_partition_factory, caplog):
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = 10
        lowwater, highwater = 0, 9
        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        recovery_partition.set_watermarks(lowwater, highwater)
        recovery_partition.update_offset()

        store_partition.set_changelog_offset.assert_called_with(
            changelog_offset=highwater - 1
        )
        with caplog.at_level(level=logging.WARNING):
            recovery_partition.update_offset()
        assert caplog.text
