import contextlib
import secrets
from datetime import datetime
from unittest.mock import patch

import pytest
import rocksdict

from quixstreams.state.rocksdb import (
    StateSerializationError,
    StateTransactionError,
    RocksDBStorePartition,
    RocksDBOptions,
    RocksDBPartitionTransaction,
    InvalidChangelogOffset,
)
from quixstreams.state.rocksdb.metadata import CHANGELOG_CF_MESSAGE_HEADER
from quixstreams.state.rocksdb.serialization import serialize
from quixstreams.utils.json import dumps

TEST_KEYS = [
    "string",
    123,
    123.123,
    (123, 456),
]

TEST_VALUES = [
    None,
    "string",
    123,
    123.123,
    {"key": "value", "mapping": {"key": "value"}},
    [123, 456],
]

TEST_PREFIXES = [
    b"some_bytes",
    "string",
    123,
    123.123,
    (123, 456),
    [123, 456],
]


class TestRocksDBPartitionTransaction:
    def test_transaction_complete(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            ...

        assert tx.completed

    def test_transaction_doesnt_write_empty_batch(
        self, changelog_producer_mock, rocksdb_partition_factory
    ):
        """
        Test that transaction doesn't call "StateStore.write()" if the internal
        WriteBatch is empty (i.e. no keys were updated during the transaction).
        Writing empty batches costs more than doing
        """

        prefix = b"__key__"
        with rocksdb_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as partition:
            with patch.object(RocksDBStorePartition, "write") as mocked:
                with partition.begin() as tx:
                    tx.get("key", prefix=prefix)

                with partition.begin() as tx:
                    tx.get("key", prefix=prefix)

        assert not mocked.called
        assert not changelog_producer_mock.produce.called

    def test_delete_key_doesnt_exist(self, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.delete("key", prefix=prefix)

    @pytest.mark.parametrize(
        "key",
        TEST_KEYS,
    )
    @pytest.mark.parametrize(
        "value",
        TEST_VALUES,
    )
    def test_get_key_exists_cached(self, key, value, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.set(key, value, prefix=prefix)
            stored = tx.get(key, prefix=prefix)
            assert stored == value

    @pytest.mark.parametrize(
        "key",
        TEST_KEYS,
    )
    @pytest.mark.parametrize(
        "value",
        TEST_VALUES,
    )
    def test_get_key_exists_no_cache(self, key, value, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.set(key, value, prefix=prefix)

        with rocksdb_partition.begin() as tx:
            stored = tx.get(key, prefix=prefix)
            assert stored == value

    def test_get_key_doesnt_exist_default(self, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            value = tx.get("key", default=123, prefix=prefix)
            assert value == 123

    def test_delete_key_cached_no_flush(self, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)
            assert tx.get("key", prefix=prefix) == "value"
            tx.delete("key", prefix=prefix)
            assert tx.get("key", prefix=prefix) is None

    def test_delete_key_cached(self, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)

        with rocksdb_partition.begin() as tx:
            assert tx.get("key", prefix=prefix) == "value"
            tx.delete("key", prefix=prefix)
            assert tx.get("key", prefix=prefix) is None

    def test_delete_key_no_cache(self, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)
            assert tx.get("key", prefix=prefix) == "value"

        with rocksdb_partition.begin() as tx:
            tx.delete("key", prefix=prefix)

        with rocksdb_partition.begin() as tx:
            assert tx.get("key", prefix=prefix) is None

    def test_key_exists_cached(self, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)
            assert tx.exists("key", prefix=prefix)
            assert not tx.exists("key123", prefix=prefix)

    def test_key_exists_no_cache(self, rocksdb_partition):
        prefix = b"__key__"

        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)

        with rocksdb_partition.begin() as tx:
            assert tx.exists("key", prefix=prefix)
            assert not tx.exists("key123", prefix=prefix)

    def test_key_exists_deleted_in_cache(self, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)

        with rocksdb_partition.begin() as tx:
            assert tx.exists("key", prefix=prefix)
            tx.delete("key", prefix=prefix)
            assert not tx.exists("key", prefix=prefix)

    @pytest.mark.parametrize(
        "key, value",
        [
            ("string", object()),
            (object(), "string"),
            ("string", datetime.utcnow()),
            (datetime.utcnow(), "string"),
        ],
    )
    def test_set_serialization_error(self, key, value, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value, prefix=prefix)

    @pytest.mark.parametrize("key", [object(), b"somebytes", datetime.utcnow()])
    def test_delete_serialization_error(self, key, rocksdb_partition):
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.delete(key, prefix=prefix)

    def test_get_deserialization_error(self, rocksdb_partition):
        bytes_ = secrets.token_bytes(10)
        string_ = "string"

        batch = rocksdict.WriteBatch(raw_mode=True)
        # Set non-deserializable key and valid value
        batch.put(bytes_, serialize(string_, dumps=dumps))
        # Set valid key and non-deserializable value
        batch.put(serialize(string_, dumps=dumps), bytes_)
        rocksdb_partition.write(batch)

        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.get(string_, prefix=b"")
            with pytest.raises(StateSerializationError):
                tx.get(bytes_, prefix=b"")

    def test_set_key_different_prefixes(self, rocksdb_partition):
        prefix1, prefix2 = b"__key1__", b"__key2__"
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix1)
            assert tx.get("key", prefix=prefix1) == "value"
            assert tx.get("key", prefix=prefix2) is None

    def test_delete_key_different_prefixes_no_cache(self, rocksdb_partition):
        prefix1, prefix2 = b"__key1__", b"__key2__"
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix1)
            tx.set("key", "value", prefix=prefix2)
            assert tx.get("key", prefix=prefix1) == "value"
            assert tx.get("key", prefix=prefix2) == "value"
            tx.delete("key", prefix=prefix1)
            assert tx.get("key", prefix=prefix1) is None
            assert tx.get("key", prefix=prefix2) is not None

    @pytest.mark.parametrize(
        "operation",
        [
            lambda tx, prefix: tx.set("key", "value", prefix=prefix),
            lambda tx, prefix: tx.delete("key", prefix=prefix),
        ],
    )
    def test_update_key_failed_transaction_failed(self, operation, rocksdb_partition):
        """
        Test that if the update operation (set or delete) fails the transaction is
        marked as failed and cannot be re-used anymore.
        """

        prefix = b"__key__"
        with patch.object(
            RocksDBPartitionTransaction,
            "_serialize_key",
            side_effect=ValueError("test"),
        ):
            with rocksdb_partition.begin() as tx:
                with contextlib.suppress(ValueError):
                    operation(tx=tx, prefix=prefix)

                assert tx.failed

                # Ensure that Transaction cannot be used after it's failed
                with pytest.raises(StateTransactionError):
                    tx.set("key", "value", prefix=prefix)

                with pytest.raises(StateTransactionError):
                    tx.get("key", prefix=prefix)

                with pytest.raises(StateTransactionError):
                    tx.delete("key", prefix=prefix)

                with pytest.raises(StateTransactionError):
                    tx.exists("key", prefix=prefix)

                with pytest.raises(StateTransactionError):
                    tx.flush()

            assert not tx.completed

    def test_update_key_prepared_transaction_fails(self, rocksdb_partition):
        """
        Test that any update operation (set or delete) fails if the transaction is
        marked as prepared.
        """

        prefix = b"__key__"
        tx = rocksdb_partition.begin()

        tx.set(key="key", value="value", prefix=prefix)
        tx.prepare()
        assert tx.prepared

        with pytest.raises(StateTransactionError):
            tx.set("key", value="value", prefix=prefix)

        with pytest.raises(StateTransactionError):
            tx.delete("key", prefix=prefix)

    def test_transaction_not_flushed_on_error(self, rocksdb_partition):
        prefix = b"__key__"
        with contextlib.suppress(ValueError):
            with rocksdb_partition.begin() as tx:
                tx.set("key", "value", prefix=prefix)
                raise ValueError("test")

        with rocksdb_partition.begin() as tx:
            assert tx.get("key", prefix=prefix) is None

    def test_custom_dumps_loads(self, rocksdb_partition_factory):
        key = secrets.token_bytes(10)
        value = secrets.token_bytes(10)
        prefix = b"__key__"

        with rocksdb_partition_factory(
            options=RocksDBOptions(loads=lambda v: v, dumps=lambda v: v)
        ) as db:
            with db.begin() as tx:
                tx.set(key, value, prefix=prefix)

            with db.begin() as tx:
                assert tx.get(key, prefix=prefix) == value

    def test_set_dict_nonstr_keys_fails(self, rocksdb_partition):
        key = "key"
        value = {0: 1}
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value, prefix=prefix)

    def test_set_datetime_fails(self, rocksdb_partition):
        key = "key"
        value = datetime.utcnow()
        prefix = b"__key__"
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value, prefix=prefix)

    def test_set_get_with_column_family(self, rocksdb_partition):
        key = "key"
        value = "value"
        prefix = b"__key__"
        rocksdb_partition.create_column_family("cf")

        with rocksdb_partition.begin() as tx:
            tx.set(key, value, cf_name="cf", prefix=prefix)
            assert tx.get(key, cf_name="cf", prefix=prefix) == value

        with rocksdb_partition.begin() as tx:
            assert tx.get(key, cf_name="cf", prefix=prefix) == value

    def test_set_delete_get_with_column_family(self, rocksdb_partition):
        key = "key"
        value = "value"
        prefix = b"__key__"
        rocksdb_partition.create_column_family("cf")

        with rocksdb_partition.begin() as tx:
            tx.set(key, value, cf_name="cf", prefix=prefix)
            assert tx.get(key, cf_name="cf", prefix=prefix) == value
            tx.delete(key, cf_name="cf", prefix=prefix)
            assert tx.get(key, cf_name="cf", prefix=prefix) is None

        with rocksdb_partition.begin() as tx:
            assert tx.get(key, cf_name="cf", prefix=prefix) is None

    def test_set_exists_get_with_column_family(self, rocksdb_partition):
        key = "key"
        value = "value"
        rocksdb_partition.create_column_family("cf")
        prefix = b"__key__"

        with rocksdb_partition.begin() as tx:
            assert not tx.exists(key, cf_name="cf", prefix=prefix)
            tx.set(key, value, cf_name="cf", prefix=prefix)
            assert tx.exists(key, cf_name="cf", prefix=prefix)

        with rocksdb_partition.begin() as tx:
            assert tx.exists(key, cf_name="cf", prefix=prefix)

    def test_flush_failed_transaction_failed(self, rocksdb_partition):
        """
        Test that if the "flush()" fails the transaction is also marked
        as failed and cannot be re-used.
        """

        prefix = b"__key__"
        with patch.object(
            RocksDBStorePartition, "write", side_effect=ValueError("test")
        ):
            with rocksdb_partition.begin() as tx:
                tx.set("key", "value", prefix=prefix)

                with contextlib.suppress(ValueError):
                    tx.flush()

                assert tx.failed

                # Ensure that Transaction cannot be used after it's failed
                with pytest.raises(StateTransactionError):
                    tx.set("key", "value", prefix=prefix)

                with pytest.raises(StateTransactionError):
                    tx.get("key", prefix=prefix)

                with pytest.raises(StateTransactionError):
                    tx.delete("key", prefix=prefix)

                with pytest.raises(StateTransactionError):
                    tx.exists("key", prefix=prefix)

    @pytest.mark.parametrize(
        "processed_offset, changelog_offset", [(None, None), (1, 1)]
    )
    def test_flush_success(self, processed_offset, changelog_offset, rocksdb_partition):
        tx = rocksdb_partition.begin()

        # Set some key to probe the transaction
        tx.set(key="key", value="value", prefix=b"__key__")

        tx.flush(processed_offset=processed_offset, changelog_offset=changelog_offset)
        assert tx.completed

        assert rocksdb_partition.get_changelog_offset() == changelog_offset
        assert rocksdb_partition.get_processed_offset() == processed_offset

    def test_flush_invalid_changelog_offset(self, rocksdb_partition):
        tx1 = rocksdb_partition.begin()
        # Set some key to probe the transaction
        tx1.set(key="key", value="value", prefix=b"__key__")

        # Flush first transaction to update the changelog offset
        tx1.flush(changelog_offset=9999)
        assert tx1.completed

        tx2 = rocksdb_partition.begin()
        tx2.set(key="key", value="value", prefix=b"__key__")
        # Flush second transaction with a smaller changelog offset
        with pytest.raises(InvalidChangelogOffset):
            tx2.flush(changelog_offset=1)
        assert tx2.failed

    def test_set_and_prepare(self, rocksdb_partition_factory, changelog_producer_mock):
        data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ]
        cf = "default"
        prefix = b"__key__"

        with rocksdb_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as partition:
            tx = partition.begin()
            for key, value in data:
                tx.set(
                    key=key,
                    value=value,
                    cf_name=cf,
                    prefix=prefix,
                )
            tx.prepare()

            assert changelog_producer_mock.produce.call_count == len(data)
            for (key, value), call in zip(
                data, changelog_producer_mock.produce.call_args_list
            ):
                assert call.kwargs["key"] == tx._serialize_key(key=key, prefix=prefix)
                assert call.kwargs["value"] == tx._serialize_value(value=value)
                assert call.kwargs["headers"] == {CHANGELOG_CF_MESSAGE_HEADER: cf}

            assert tx.prepared

    def test_delete_and_prepare(
        self, rocksdb_partition_factory, changelog_producer_mock
    ):
        key, value = "key", "value"
        cf = "default"
        prefix = b"__key__"
        with rocksdb_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as partition:

            tx = partition.begin()
            tx.delete(key=key, cf_name=cf, prefix=prefix)

            tx.prepare()

            assert tx.prepared
            assert changelog_producer_mock.produce.call_count == 1

        delete_changelog = changelog_producer_mock.produce.call_args_list[0]
        assert delete_changelog.kwargs["key"] == tx._serialize_key(
            key=key, prefix=prefix
        )
        assert delete_changelog.kwargs["value"] is None
        assert delete_changelog.kwargs["headers"] == {CHANGELOG_CF_MESSAGE_HEADER: cf}

    def test_set_delete_and_prepare(
        self, rocksdb_partition_factory, changelog_producer_mock
    ):
        """
        Test that only "delete" changelog message is emited if the key is set
        and deleted in the same transaction.
        """
        key, value = "key", "value"
        cf = "default"
        prefix = b"__key__"

        with rocksdb_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as partition:
            tx = partition.begin()
            tx.set(key=key, value=value, cf_name=cf, prefix=prefix)
            tx.delete(key=key, cf_name=cf, prefix=prefix)

            tx.prepare()

            assert tx.prepared
            assert changelog_producer_mock.produce.call_count == 1
            delete_changelog = changelog_producer_mock.produce.call_args_list[0]
            assert delete_changelog.kwargs["key"] == tx._serialize_key(
                key=key, prefix=prefix
            )
            assert delete_changelog.kwargs["value"] is None
            assert delete_changelog.kwargs["headers"] == {
                CHANGELOG_CF_MESSAGE_HEADER: cf
            }
