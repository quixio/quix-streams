import time
from pathlib import Path
from unittest.mock import patch

import pytest
from rocksdict import Rdict

from quixstreams.state.exceptions import ColumnFamilyHeaderMissing
from quixstreams.state.rocksdb import (
    RocksDBStorePartition,
    RocksDBOptions,
    ColumnFamilyAlreadyExists,
    ColumnFamilyDoesNotExist,
)
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    PREFIX_SEPARATOR,
    CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER,
)
from quixstreams.utils.json import dumps
from tests.utils import ConfluentKafkaMessageStub


class TestRocksDBStorePartition:
    def test_open_db_close(self, store_partition_factory):
        with store_partition_factory():
            ...

    def test_open_db_locked_retries(self, store_partition_factory, executor):
        db1 = store_partition_factory("db")

        def _close_db():
            time.sleep(3)
            db1.close()

        executor.submit(_close_db)

        store_partition_factory(
            "db", options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1)
        )

    def test_open_io_error_retries(self, store_partition_factory, executor):
        err = Exception("io error")
        patcher = patch.object(Rdict, "__init__", side_effect=err)
        patcher.start()

        def _stop_raising_on_db_open():
            time.sleep(3)
            patcher.stop()

        executor.submit(_stop_raising_on_db_open)

        store_partition_factory(
            "db", options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1)
        )

    def test_open_db_locked_no_retries_fails(self, store_partition_factory, executor):
        _ = store_partition_factory("db")

        with pytest.raises(Exception):
            store_partition_factory("db", options=RocksDBOptions(open_max_retries=0))

    def test_open_db_locked_retries_exhausted_fails(
        self, store_partition_factory, executor
    ):
        _ = store_partition_factory("db")

        with pytest.raises(Exception):
            store_partition_factory(
                "db", options=RocksDBOptions(open_max_retries=3, open_retry_backoff=1)
            )

    def test_open_arbitrary_exception_fails(self, store_partition_factory):
        err = Exception("some exception")
        with patch.object(Rdict, "__init__", side_effect=err):
            with pytest.raises(Exception) as raised:
                store_partition_factory()

        assert str(raised.value) == "some exception"

    def test_get_db_closed_fails(self, store_partition_factory):
        storage = store_partition_factory()
        storage.close()
        with pytest.raises(Exception):
            storage.get(b"key")

    @pytest.mark.parametrize("cf_name", ["default", "cf"])
    def test_get_key_doesnt_exist(self, cf_name, store_partition):
        try:
            store_partition.create_column_family(cf_name=cf_name)
        except ColumnFamilyAlreadyExists:
            pass

        assert store_partition.get(b"key", cf_name=cf_name) is None

    def test_destroy(self, store_partition_factory):
        with store_partition_factory() as storage:
            path = storage.path

        RocksDBStorePartition.destroy(path)

    def test_custom_options(self, store_partition_factory, tmp_path):
        """
        Pass custom "logs_dir" to Rdict and ensure it exists and has some files
        """

        logs_dir = Path(tmp_path / "db" / "logs")
        options = RocksDBOptions(db_log_dir=logs_dir.as_posix())
        with store_partition_factory(options=options):
            assert logs_dir.is_dir()
            assert len(list(logs_dir.rglob("*"))) == 1

    def test_create_and_get_column_family(self, store_partition):
        store_partition.create_column_family("cf")
        assert store_partition.get_column_family("cf")

    def test_create_column_family_already_exists(self, store_partition):
        store_partition.create_column_family("cf")
        with pytest.raises(ColumnFamilyAlreadyExists):
            store_partition.create_column_family("cf")

    def test_get_column_family_doesnt_exist(self, store_partition):
        with pytest.raises(ColumnFamilyDoesNotExist):
            store_partition.get_column_family("cf")

    def test_get_column_family_cached(self, store_partition):
        store_partition.create_column_family("cf")
        cf1 = store_partition.get_column_family("cf")
        cf2 = store_partition.get_column_family("cf")
        assert cf1 is cf2

    def test_create_and_drop_column_family(self, store_partition):
        store_partition.create_column_family("cf")
        store_partition.drop_column_family("cf")

        with pytest.raises(ColumnFamilyDoesNotExist):
            store_partition.get_column_family("cf")

    def test_drop_column_family_doesnt_exist(self, store_partition):
        with pytest.raises(ColumnFamilyDoesNotExist):
            store_partition.drop_column_family("cf")

    def test_list_column_families(self, store_partition):
        store_partition.create_column_family("cf1")
        store_partition.create_column_family("cf2")
        cfs = store_partition.list_column_families()
        assert "cf1" in cfs
        assert "cf2" in cfs

    def test_list_column_families_defaults(self, store_partition):
        cfs = store_partition.list_column_families()
        assert cfs == [
            # "default" CF is always present in RocksDB
            "default",
            # "__metadata__" CF is created by the RocksDBStorePartition
            "__metadata__",
        ]

    def test_ensure_metadata_cf(self, store_partition):
        assert store_partition.get_column_family("__metadata__")


class TestRocksDBStorePartitionChangelog:
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
