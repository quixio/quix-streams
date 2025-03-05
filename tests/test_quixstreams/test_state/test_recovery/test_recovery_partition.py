from unittest.mock import MagicMock

import pytest

from quixstreams.state.exceptions import ColumnFamilyHeaderMissing
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    SEPARATOR,
)
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


class TestRecoverFromChangelogMessage:
    @pytest.mark.parametrize("store_value", [10, None])
    def test_recover_from_changelog_message_no_processed_offset(
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

    def test_recover_from_changelog_message_with_processed_offset_behind_committed(
        self, store_partition, recovery_partition_factory
    ):
        """
        Test that changes from the changelog topic are applied if the
        source topic offset header is present and is smaller than the latest committed
        offset.
        """
        kafka_key = b"my_key"
        user_store_key = "count"

        # Processed offset is behind the committed offset - the changelog belongs
        # to an already committed message and should be applied
        processed_offsets = {"topic": 1}
        committed_offsets = {"topic": 2}

        recovery_partition = recovery_partition_factory(
            store_partition=store_partition, committed_offsets=committed_offsets
        )

        processed_offset_header = (
            CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
            dumps(processed_offsets),
        )

        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key + SEPARATOR + dumps(user_store_key),
            value=dumps(10),
            headers=[
                (CHANGELOG_CF_MESSAGE_HEADER, b"default"),
                processed_offset_header,
            ],
        )

        recovery_partition.recover_from_changelog_message(changelog_msg)

        with store_partition.begin() as tx:
            assert tx.get(user_store_key, prefix=kafka_key) == 10
        assert store_partition.get_changelog_offset() == changelog_msg.offset()

    def test_recover_from_changelog_message_with_processed_offset_ahead_committed(
        self, store_partition, recovery_partition_factory
    ):
        """
        Test that changes from the changelog topic are NOT applied if the
        source topic offset header is present but larger than the latest committed
        offset.
        It means that the changelog messages were produced during the checkpoint,
        but the topic offset was not committed.
        Possible reasons:
          - Producer couldn't verify the delivery of every changelog message
          - Consumer failed to commit the source topic offsets
        """
        kafka_key = b"my_key"
        user_store_key = "count"

        # Processed offset should be strictly lower than committed offset for
        # the change to be applied
        processed_offsets = {"topic": 2}
        committed_offsets = {"topic": 2}

        recovery_partition = recovery_partition_factory(
            store_partition=store_partition, committed_offsets=committed_offsets
        )

        # Generate the changelog message with processed offset ahead of the committed
        # one
        processed_offset_header = (
            CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
            dumps(processed_offsets),
        )
        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key + SEPARATOR + dumps(user_store_key),
            value=dumps(10),
            headers=[
                (CHANGELOG_CF_MESSAGE_HEADER, b"default"),
                processed_offset_header,
            ],
        )

        # Recover from the message
        recovery_partition.recover_from_changelog_message(changelog_msg)

        # Check that the changes have not been applied, but the changelog offset
        # increased
        with store_partition.begin() as tx:
            assert tx.get(user_store_key, prefix=kafka_key) is None

        assert store_partition.get_changelog_offset() == changelog_msg.offset()
