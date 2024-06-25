import logging
from unittest.mock import MagicMock

import pytest

from quixstreams.state.rocksdb import RocksDBStorePartition
from tests.utils import ConfluentKafkaMessageStub


class TestRecoveryPartition:
    @pytest.mark.parametrize(
        "offset, needs_check",
        ([21, False], [20, False], [19, False], [18, True], [17, True]),
    )
    def test_needs_recovery_check(
        self, recovery_partition_factory, offset, needs_check
    ):
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = offset

        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        recovery_partition.set_watermarks(0, 20)

        assert recovery_partition.needs_recovery_check == needs_check

    @pytest.mark.parametrize(
        "offset, needs_check, needs_update",
        (
            [21, False, True],
            [20, False, True],
            [19, False, False],
            [18, False, False],
            [17, False, False],
        ),
    )
    def test_needs_recovery_check_no_valid_offsets(
        self, recovery_partition_factory, offset, needs_check, needs_update
    ):
        # Create a RecoveryPartition with the offset ahead of the watermark
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = offset
        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        recovery_partition.set_watermarks(20, 20)

        assert recovery_partition.needs_recovery_check == needs_check
        assert recovery_partition.needs_offset_update == needs_update

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
