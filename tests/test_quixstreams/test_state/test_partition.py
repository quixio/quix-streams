import pytest

from quixstreams.state.exceptions import (
    ColumnFamilyHeaderMissing,
    ColumnFamilyDoesNotExist,
)
from quixstreams.state.manager import SUPPORTED_STORES

from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER,
    PREFIX_SEPARATOR,
)
from quixstreams.state.rocksdb import (
    ColumnFamilyAlreadyExists,
    RocksDBOptions,
    RocksDBStorePartition,
)
from quixstreams.utils.json import dumps
from tests.utils import ConfluentKafkaMessageStub


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestStorePartition:
    def test_open_db_close(self, store_partition_factory):
        with store_partition_factory():
            ...

    def test_get_db_closed_fails(self, store_partition_factory):
        storage = store_partition_factory()
        storage.close()
        with pytest.raises(Exception):
            storage.get(b"key")


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestStorePartitionChangelog:
    @pytest.mark.parametrize("store_value", [10, None])
    def test_recover_from_changelog_message_no_processed_offset(
        self, store_partition, store_value
    ):
        """
        Tests both a put (10) and delete (None)
        """
        kafka_key = b"my_key"
        user_store_key = "count"
        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key + PREFIX_SEPARATOR + dumps(user_store_key),
            value=dumps(store_value),
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
            offset=50,
        )

        store_partition.recover_from_changelog_message(
            changelog_msg, committed_offset=-1001
        )

        with store_partition.begin() as tx:
            assert tx.get(user_store_key, prefix=kafka_key) == store_value
        assert store_partition.get_changelog_offset() == changelog_msg.offset()

    @pytest.mark.parametrize(
        ("headers", "error"),
        [
            ([(CHANGELOG_CF_MESSAGE_HEADER, b"derp")], ColumnFamilyDoesNotExist),
            ([], ColumnFamilyHeaderMissing),
        ],
    )
    def test_recover_from_changelog_message_missing_cf_headers(
        self, store_partition, headers, error
    ):
        changelog_msg = ConfluentKafkaMessageStub(
            key=b'my_key|"count"',
            value=b"10",
            headers=headers,
            offset=50,
        )
        with pytest.raises(error):
            store_partition.recover_from_changelog_message(
                changelog_msg, committed_offset=-1001
            )
        assert store_partition.get_changelog_offset() is None

    def test_recover_from_changelog_message_with_processed_offset_behind_committed(
        self, store_partition
    ):
        """
        Test that changes from the changelog topic are applied if the
        source topic offset header is present and is smaller than the latest committed
        offset.
        """
        kafka_key = b"my_key"
        user_store_key = "count"

        processed_offset_header = (
            CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER,
            dumps(1),
        )
        committted_offset = 2
        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key + PREFIX_SEPARATOR + dumps(user_store_key),
            value=dumps(10),
            headers=[
                (CHANGELOG_CF_MESSAGE_HEADER, b"default"),
                processed_offset_header,
            ],
        )

        store_partition.recover_from_changelog_message(
            changelog_msg, committed_offset=committted_offset
        )

        with store_partition.begin() as tx:
            assert tx.get(user_store_key, prefix=kafka_key) == 10
        assert store_partition.get_changelog_offset() == changelog_msg.offset()

    def test_recover_from_changelog_message_with_processed_offset_ahead_committed(
        self, store_partition
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
        processed_offset = 2
        committed_offset = 2

        # Generate the changelog message with processed offset ahead of the committed
        # one
        processed_offset_header = (
            CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER,
            dumps(processed_offset),
        )
        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key + PREFIX_SEPARATOR + dumps(user_store_key),
            value=dumps(10),
            headers=[
                (CHANGELOG_CF_MESSAGE_HEADER, b"default"),
                processed_offset_header,
            ],
        )

        # Recover from the message
        store_partition.recover_from_changelog_message(
            changelog_msg, committed_offset=committed_offset
        )

        # Check that the changes have not been applied, but the changelog offset
        # increased
        with store_partition.begin() as tx:
            assert tx.get(user_store_key, prefix=kafka_key) is None
        assert store_partition.get_changelog_offset() == changelog_msg.offset()
