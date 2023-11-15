import contextlib
import secrets
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import rocksdict
from rocksdict import Rdict

from quixstreams.state.rocksdb import (
    StateSerializationError,
    StateTransactionError,
    RocksDBStorePartition,
    NestedPrefixError,
    RocksDBOptions,
)
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


class TestRocksDBStorePartition:
    def test_open_db_close(self, rocksdb_partition_factory):
        with rocksdb_partition_factory():
            ...

    def test_open_db_locked_retries(self, rocksdb_partition_factory, executor):
        db1 = rocksdb_partition_factory("db")

        def _close_db():
            time.sleep(3)
            db1.close()

        executor.submit(_close_db)

        rocksdb_partition_factory("db", open_max_retries=10, open_retry_backoff=1)

    def test_open_db_locked_no_retries_fails(self, rocksdb_partition_factory, executor):
        _ = rocksdb_partition_factory("db")

        with pytest.raises(Exception):
            rocksdb_partition_factory("db", open_max_retries=0)

    def test_open_db_locked_retries_exhausted_fails(
        self, rocksdb_partition_factory, executor
    ):
        _ = rocksdb_partition_factory("db")

        with pytest.raises(Exception):
            rocksdb_partition_factory("db", open_max_retries=3, open_retry_backoff=1)

    def test_open_arbitrary_exception_fails(self, rocksdb_partition_factory):
        err = Exception("some exception")
        with patch.object(Rdict, "__init__", side_effect=err):
            with pytest.raises(Exception) as raised:
                rocksdb_partition_factory()

        assert str(raised.value) == "some exception"

    def test_get_db_closed_fails(self, rocksdb_partition_factory):
        storage = rocksdb_partition_factory()
        storage.close()
        with pytest.raises(Exception):
            storage.get(b"key")

    def test_get_key_doesnt_exist(self, rocksdb_partition):
        assert rocksdb_partition.get(b"key") is None

    def test_destroy(self, rocksdb_partition_factory):
        with rocksdb_partition_factory() as storage:
            path = storage.path

        RocksDBStorePartition.destroy(path)

    def test_custom_options(self, rocksdb_partition_factory, tmp_path):
        """
        Pass custom "logs_dir" to Rdict and ensure it exists and has some files
        """

        logs_dir = Path(tmp_path / "db" / "logs")
        options = RocksDBOptions(db_log_dir=logs_dir.as_posix())
        with rocksdb_partition_factory(options=options):
            assert logs_dir.is_dir()
            assert len(list(logs_dir.rglob("*"))) == 1


class TestRocksDBPartitionTransaction:
    def test_transaction_complete(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            ...

        assert tx.completed

    def test_transaction_doesnt_write_empty_batch(self, rocksdb_partition):
        """
        Test that transaction doesn't call "StateStore.write()" if the internal
        WriteBatch is empty (i.e. no keys were updated during the transaction).
        Writing empty batches costs more than doing
        """

        with patch.object(RocksDBStorePartition, "write") as mocked:
            with rocksdb_partition.begin() as tx:
                tx.get("key")

            assert not mocked.called

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

    def test_delete_key_cached(self, rocksdb_partition):
        with rocksdb_partition.begin() as tx:
            tx.set("key", "value")
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
        with patch.object(
            rocksdict.WriteBatch, "put", side_effect=ValueError("test")
        ), patch.object(rocksdict.WriteBatch, "delete", side_effect=ValueError("test")):
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

    def test_nested_prefixes_fail(self, rocksdb_partition):
        tx = rocksdb_partition.begin()
        with pytest.raises(NestedPrefixError):
            with tx.with_prefix("prefix"):
                with tx.with_prefix("prefix"):
                    ...

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
