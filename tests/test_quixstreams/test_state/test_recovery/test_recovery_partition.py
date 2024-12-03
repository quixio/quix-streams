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

        recovery_partition = recovery_partition_factory(
            store_partition=store_partition, lowwater=0, highwater=20
        )

        assert recovery_partition.needs_recovery_check == needs_check

    @pytest.mark.parametrize(
        "offset, needs_check, invalid_offset",
        (
            [21, False, True],
            [20, False, True],
            [19, False, False],
            [18, False, False],
            [17, False, False],
        ),
    )
    def test_needs_recovery_check_no_valid_offsets(
        self, recovery_partition_factory, offset, needs_check, invalid_offset
    ):
        # Create a RecoveryPartition with the offset ahead of the watermark
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = offset

        recovery_partition = recovery_partition_factory(
            store_partition=store_partition, lowwater=20, highwater=20
        )

        assert recovery_partition.needs_recovery_check == needs_check
        assert recovery_partition.has_invalid_offset == invalid_offset

    def test_recover_from_changelog_message(self, recovery_partition_factory):
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = 10

        recovery_partition = recovery_partition_factory(
            store_partition=store_partition,
            committed_offset=1,
            lowwater=10,
            highwater=20,
        )

        msg = ConfluentKafkaMessageStub()
        recovery_partition.recover_from_changelog_message(msg)

        store_partition.recover_from_changelog_message.assert_called_with(
            changelog_message=msg, committed_offset=1
        )
