from unittest.mock import MagicMock

import pytest
from confluent_kafka import OFFSET_BEGINNING

from quixstreams.state.exceptions import ColumnFamilyHeaderMissing
from quixstreams.state.metadata import CHANGELOG_CF_MESSAGE_HEADER, SEPARATOR
from quixstreams.state.rocksdb import RocksDBStorePartition
from quixstreams.utils.json import dumps
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

    @pytest.mark.parametrize(
        "stored_offset, initial_offset",
        [
            (None, OFFSET_BEGINNING),
            (0, 0),
            (1, 1),
        ],
    )
    def test_initial_offset(
        self, stored_offset, initial_offset, recovery_partition_factory
    ):
        store_partition = MagicMock(RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = stored_offset

        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        assert recovery_partition.offset == initial_offset


class TestRecoverFromChangelogMessage:
    @pytest.mark.parametrize("store_value", [10, None])
    def test_recover_from_changelog_message_success(
        self, store_partition, store_value, recovery_partition_factory
    ):
        """
        Tests both a put (10) and delete (None)
        """
        recovery_partition = recovery_partition_factory(store_partition=store_partition)

        kafka_key = b"my_key"
        user_store_key = "count"
        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key + SEPARATOR + dumps(user_store_key),
            value=dumps(store_value),
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
            offset=50,
        )

        recovery_partition.recover_from_changelog_message(changelog_msg)

        assert store_partition.get_changelog_offset() == changelog_msg.offset()

    @pytest.mark.parametrize(
        ("headers", "error"),
        [
            ([], ColumnFamilyHeaderMissing),
        ],
    )
    def test_recover_from_changelog_message_missing_cf_headers(
        self, store_partition, headers, error, recovery_partition_factory
    ):
        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        changelog_msg = ConfluentKafkaMessageStub(
            key=b'my_key|"count"',
            value=b"10",
            headers=headers,
            offset=50,
        )
        with pytest.raises(error):
            recovery_partition.recover_from_changelog_message(changelog_msg)
        assert store_partition.get_changelog_offset() is None

    @pytest.mark.parametrize("key", ["key", None])
    def test_recover_from_changelog_message_invalid_key_type(
        self, store_partition, recovery_partition_factory, key
    ):
        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        changelog_msg = ConfluentKafkaMessageStub(
            key=key,
            value=b"10",
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
            offset=50,
        )
        with pytest.raises(TypeError, match="Invalid changelog key type"):
            recovery_partition.recover_from_changelog_message(
                changelog_message=changelog_msg
            )

    @pytest.mark.parametrize("value", ["value", 0])
    def test_recover_from_changelog_message_invalid_value_type(
        self, store_partition, recovery_partition_factory, value
    ):
        recovery_partition = recovery_partition_factory(store_partition=store_partition)
        changelog_msg = ConfluentKafkaMessageStub(
            key=b"key",
            value=value,
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
            offset=50,
        )
        with pytest.raises(TypeError, match="Invalid changelog value type"):
            recovery_partition.recover_from_changelog_message(
                changelog_message=changelog_msg
            )
