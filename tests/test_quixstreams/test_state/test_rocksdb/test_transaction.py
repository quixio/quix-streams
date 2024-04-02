import contextlib
import secrets
from datetime import datetime
from unittest.mock import patch, call

import pytest
import rocksdict

from quixstreams.state.rocksdb import (
    StateSerializationError,
    StateTransactionError,
    RocksDBStorePartition,
    RocksDBOptions,
    RocksDBPartitionTransaction,
)
from quixstreams.state.rocksdb.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
)
from quixstreams.state.rocksdb.serialization import serialize
from quixstreams.utils.json import dumps
from .fixtures import TEST_KEYS, TEST_VALUES, TEST_PREFIXES


class TestRocksDBPartitionTransaction:
    def test_transaction_complete(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            ...

        assert tx.completed

    def test_transaction_with_changelog(self, rocksdb_partition):
        changelog_producer = rocksdb_partition._changelog_producer
        key_out = "my_key"
        value_out = "my_value"
        cf = "default"
        db_writes = 3
        assert rocksdb_partition.get_changelog_offset() is None

        with rocksdb_partition.begin() as tx:
            for i in range(db_writes):
                tx.set(key=f"{key_out}{i}", value=f"{value_out}{i}", cf_name=cf)

        changelog_producer.produce.assert_has_calls(
            [
                call(
                    key=tx._serialize_key(key=f"{key_out}{i}"),
                    value=tx._serialize_value(value=f"{value_out}{i}"),
                    headers={CHANGELOG_CF_MESSAGE_HEADER: cf},
                )
                for i in range(db_writes)
            ]
        )
        assert changelog_producer.produce.call_count == db_writes
        assert tx.completed
        assert rocksdb_partition.get_changelog_offset() == db_writes

    def test_transaction_with_changelog_delete(self, rocksdb_partition):
        changelog_producer = rocksdb_partition._changelog_producer
        key_out = "my_key"
        value_out = "my_value"
        cf = "default"
        assert rocksdb_partition.get_changelog_offset() is None

        with rocksdb_partition.begin() as tx:
            tx.set(key=key_out, value=value_out, cf_name=cf)

        with rocksdb_partition.begin() as tx:
            tx.delete(key=key_out, cf_name=cf)

        changelog_producer.produce.assert_has_calls(
            [
                call(
                    key=tx._serialize_key(key=key_out),
                    value=tx._serialize_value(value=value_out),
                    headers={CHANGELOG_CF_MESSAGE_HEADER: cf},
                ),
                call(
                    key=tx._serialize_key(key=key_out),
                    value=None,
                    headers={CHANGELOG_CF_MESSAGE_HEADER: cf},
                ),
            ]
        )
        assert changelog_producer.produce.call_count == 2
        assert tx.completed
        assert rocksdb_partition.get_changelog_offset() == 2

    def test_transaction_with_changelog_delete_cached(self, rocksdb_partition):
        changelog_producer = rocksdb_partition._changelog_producer
        key_out = "my_key"
        value_out = "my_value"
        cf = "default"
        db_writes = 3
        delete_index = 2
        assert rocksdb_partition.get_changelog_offset() is None

        with rocksdb_partition.begin() as tx:
            for i in range(db_writes):
                tx.set(key=f"{key_out}{i}", value=f"{value_out}{i}", cf_name=cf)
            tx.delete(key=f"{key_out}{delete_index}", cf_name=cf)

        changelog_producer.produce.assert_has_calls(
            [
                call(
                    key=tx._serialize_key(key=f"{key_out}{i}"),
                    value=tx._serialize_value(value=f"{value_out}{i}"),
                    headers={CHANGELOG_CF_MESSAGE_HEADER: cf},
                )
                for i in range(db_writes - 1)
            ]
            + [
                call(
                    key=tx._serialize_key(key=f"{key_out}{delete_index}"),
                    value=None,
                    headers={CHANGELOG_CF_MESSAGE_HEADER: cf},
                )
            ]
        )
        assert changelog_producer.produce.call_count == db_writes
        assert tx.completed
        assert rocksdb_partition.get_changelog_offset() == db_writes

    def test_transaction_with_changelog_delete_nonexisting_key(self, rocksdb_partition):
        changelog_producer = rocksdb_partition._changelog_producer
        key_out = "my_key"
        cf = "default"
        assert rocksdb_partition.get_changelog_offset() is None

        with rocksdb_partition.begin() as tx:
            tx.delete(key=key_out, cf_name=cf)

        changelog_producer.produce.assert_called_with(
            key=tx._serialize_key(key=key_out),
            value=None,
            headers={CHANGELOG_CF_MESSAGE_HEADER: cf},
        )

        assert tx.completed
        assert rocksdb_partition.get_changelog_offset() == 1

    def test_transaction_doesnt_write_empty_batch(self, rocksdb_partition):
        """
        Test that transaction doesn't call "StateStore.write()" if the internal
        WriteBatch is empty (i.e. no keys were updated during the transaction).
        Writing empty batches costs more than doing
        """
        changelog_producer = rocksdb_partition._changelog_producer
        with patch.object(RocksDBStorePartition, "write") as mocked:
            with rocksdb_partition.begin() as tx:
                tx.get("key")

            with rocksdb_partition.begin() as tx:
                tx.get("key")

        assert not mocked.called
        assert not changelog_producer.produce.called

    def test_delete_key_doesnt_exist(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.delete("key")

    @pytest.mark.parametrize(
        "key",
        TEST_KEYS,
    )
    @pytest.mark.parametrize(
        "value",
        TEST_VALUES,
    )
    def test_get_key_exists_cached(self, key, value, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set(key, value)
            stored = tx.get(key)
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
        with rocksdb_partition.begin() as tx:
            tx.set(key, value)
        with rocksdb_partition.begin() as tx:
            stored = tx.get(key, value)
            assert stored == value

    def test_get_key_doesnt_exist_default(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            value = tx.get("key", default=123)
            assert value == 123

    def test_delete_key_cached_no_flush(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value")
            assert tx.get("key") == "value"
            tx.delete("key")
            assert tx.get("key") is None

    def test_delete_key_cached(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value")

        with rocksdb_partition.begin() as tx:
            assert tx.get("key") == "value"
            tx.delete("key")
            assert tx.get("key") is None

    def test_delete_key_no_cache(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value")
            assert tx.get("key") == "value"

        with rocksdb_partition.begin() as tx:
            tx.delete("key")

        with rocksdb_partition.begin() as tx:
            assert tx.get("key") is None

    def test_key_exists_cached(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value")
            assert tx.exists("key")
            assert not tx.exists("key123")

    def test_key_exists_no_cache(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value")
        with rocksdb_partition.begin() as tx:
            assert tx.exists("key")
            assert not tx.exists("key123")

    def test_key_exists_deleted_in_cache(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value")

        with rocksdb_partition.begin() as tx:
            assert tx.exists("key")
            tx.delete("key")
            assert not tx.exists("key")

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
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value)

    @pytest.mark.parametrize("key", [object(), b"somebytes", datetime.utcnow()])
    def test_delete_serialization_error(self, key, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.delete(key)

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
                tx.get(string_)
            with pytest.raises(StateSerializationError):
                tx.get(bytes_)

    @pytest.mark.parametrize("prefix", TEST_PREFIXES)
    def test_set_key_with_prefix_no_cache(self, prefix, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            with tx.with_prefix(prefix):
                tx.set("key", "value")

        with rocksdb_partition.begin() as tx:
            with tx.with_prefix(prefix):
                assert tx.get("key") == "value"

        with rocksdb_partition.begin() as tx:
            assert tx.get("key") is None

    @pytest.mark.parametrize("prefix", TEST_PREFIXES)
    def test_delete_key_with_prefix_no_cache(self, prefix, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            with tx.with_prefix(prefix):
                tx.set("key", "value")

        with rocksdb_partition.begin() as tx:
            with tx.with_prefix(prefix):
                assert tx.get("key") == "value"

        with rocksdb_partition.begin() as tx:
            with tx.with_prefix(prefix):
                tx.delete("key")

        with rocksdb_partition.begin() as tx:
            with tx.with_prefix(prefix):
                assert tx.get("key") is None

    @pytest.mark.parametrize(
        "operation",
        [
            lambda tx: tx.set("key", "value"),
            lambda tx: tx.delete("key"),
        ],
    )
    def test_update_key_failed_transaction_failed(self, operation, rocksdb_partition):
        """
        Test that if the update operation (set or delete) fails the transaction is
        marked as failed and cannot be re-used anymore.
        """
        # TODO: Test fails because writebatch is not used anymore during updates
        # TODO: What's the point of this "failing?" - To not flush anything if one of transactions is incomplete on __exit__
        #   Since now each update translates
        with patch.object(
            RocksDBPartitionTransaction,
            "_serialize_key",
            side_effect=ValueError("test"),
        ):
            with rocksdb_partition.begin() as tx:
                with contextlib.suppress(ValueError):
                    operation(tx=tx)

                assert tx.failed

                # Ensure that Transaction cannot be used after it's failed
                with pytest.raises(StateTransactionError):
                    tx.set("key", "value")

                with pytest.raises(StateTransactionError):
                    tx.get("key")

                with pytest.raises(StateTransactionError):
                    tx.delete("key")

                with pytest.raises(StateTransactionError):
                    tx.exists("key")

                with pytest.raises(StateTransactionError):
                    tx.maybe_flush()

            assert not tx.completed

    def test_flush_failed_transaction_failed(self, rocksdb_partition):
        """
        Test that if the "maybe_flush()" fails the transaction is also marked
        as failed and cannot be re-used anymore.
        """

        with patch.object(
            RocksDBStorePartition, "write", side_effect=ValueError("test")
        ):
            with rocksdb_partition.begin() as tx:
                tx.set("key", "value")

                with contextlib.suppress(ValueError):
                    tx.maybe_flush()

                assert tx.failed

                # Ensure that Transaction cannot be used after it's failed
                with pytest.raises(StateTransactionError):
                    tx.set("key", "value")

                with pytest.raises(StateTransactionError):
                    tx.get("key")

                with pytest.raises(StateTransactionError):
                    tx.delete("key")

                with pytest.raises(StateTransactionError):
                    tx.exists("key")

            assert tx.completed

    def test_transaction_not_flushed_on_error(self, rocksdb_partition):
        with contextlib.suppress(ValueError):
            with rocksdb_partition.begin() as tx:
                tx.set("key", "value")
                raise ValueError("test")

        with rocksdb_partition.begin() as tx:
            assert tx.get("key") is None

    def test_custom_dumps_loads(self, rocksdb_partition_factory):
        key = secrets.token_bytes(10)
        value = secrets.token_bytes(10)

        with rocksdb_partition_factory(
            options=RocksDBOptions(loads=lambda v: v, dumps=lambda v: v)
        ) as db:
            with db.begin() as tx:
                tx.set(key, value)

            with db.begin() as tx:
                assert tx.get(key) == value

    def test_set_dict_nonstr_keys_fails(self, rocksdb_partition):
        key = "key"
        value = {0: 1}
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value)

    def test_set_datetime_fails(self, rocksdb_partition):
        key = "key"
        value = datetime.utcnow()
        with rocksdb_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value)

    def test_set_get_with_column_family(self, rocksdb_partition):
        key = "key"
        value = "value"
        rocksdb_partition.create_column_family("cf")

        with rocksdb_partition.begin() as tx:
            tx.set(key, value, cf_name="cf")
            assert tx.get(key, cf_name="cf") == value

        with rocksdb_partition.begin() as tx:
            assert tx.get(key, cf_name="cf") == value

    def test_set_delete_get_with_column_family(self, rocksdb_partition):
        key = "key"
        value = "value"
        rocksdb_partition.create_column_family("cf")

        with rocksdb_partition.begin() as tx:
            tx.set(key, value, cf_name="cf")
            assert tx.get(key, cf_name="cf") == value
            tx.delete(key, cf_name="cf")
            assert tx.get(key, cf_name="cf") is None

        with rocksdb_partition.begin() as tx:
            assert tx.get(key, cf_name="cf") is None

    def test_set_exists_get_with_column_family(self, rocksdb_partition):
        key = "key"
        value = "value"
        rocksdb_partition.create_column_family("cf")

        with rocksdb_partition.begin() as tx:
            assert not tx.exists(key, cf_name="cf")
            tx.set(key, value, cf_name="cf")
            assert tx.exists(key, cf_name="cf")

        with rocksdb_partition.begin() as tx:
            assert tx.exists(key, cf_name="cf")
