import contextlib
import secrets
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, call

import pytest
import rocksdict
from rocksdict import Rdict

from quixstreams.state.rocksdb import (
    StateSerializationError,
    StateTransactionError,
    RocksDBStorePartition,
    NestedPrefixError,
    RocksDBOptions,
    ColumnFamilyAlreadyExists,
    ColumnFamilyDoesNotExist,
    ColumnFamilyHeaderMissing,
)
from quixstreams.state.rocksdb.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    PREFIX_SEPARATOR,
)
from quixstreams.state.rocksdb.serialization import serialize
from quixstreams.utils.json import dumps
from ...utils import ConfluentKafkaMessageStub

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

        rocksdb_partition_factory(
            "db", options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1)
        )

    def test_open_io_error_retries(self, rocksdb_partition_factory, executor):
        err = Exception("io error")
        patcher = patch.object(Rdict, "__init__", side_effect=err)
        patcher.start()

        def _stop_raising_on_db_open():
            time.sleep(3)
            patcher.stop()

        executor.submit(_stop_raising_on_db_open)

        rocksdb_partition_factory(
            "db", options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1)
        )

    def test_open_db_locked_no_retries_fails(self, rocksdb_partition_factory, executor):
        _ = rocksdb_partition_factory("db")

        with pytest.raises(Exception):
            rocksdb_partition_factory("db", options=RocksDBOptions(open_max_retries=0))

    def test_open_db_locked_retries_exhausted_fails(
        self, rocksdb_partition_factory, executor
    ):
        _ = rocksdb_partition_factory("db")

        with pytest.raises(Exception):
            rocksdb_partition_factory(
                "db", options=RocksDBOptions(open_max_retries=3, open_retry_backoff=1)
            )

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

    @pytest.mark.parametrize("cf_name", ["default", "cf"])
    def test_get_key_doesnt_exist(self, cf_name, rocksdb_partition):
        try:
            rocksdb_partition.create_column_family(cf_name=cf_name)
        except ColumnFamilyAlreadyExists:
            pass

        assert rocksdb_partition.get(b"key", cf_name=cf_name) is None

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

    def test_create_and_get_column_family(self, rocksdb_partition):
        rocksdb_partition.create_column_family("cf")
        assert rocksdb_partition.get_column_family("cf")

    def test_create_column_family_already_exists(self, rocksdb_partition):
        rocksdb_partition.create_column_family("cf")
        with pytest.raises(ColumnFamilyAlreadyExists):
            rocksdb_partition.create_column_family("cf")

    def test_get_column_family_doesnt_exist(self, rocksdb_partition):
        with pytest.raises(ColumnFamilyDoesNotExist):
            rocksdb_partition.get_column_family("cf")

    def test_get_column_family_cached(self, rocksdb_partition):
        rocksdb_partition.create_column_family("cf")
        cf1 = rocksdb_partition.get_column_family("cf")
        cf2 = rocksdb_partition.get_column_family("cf")
        assert cf1 is cf2

    def test_create_and_drop_column_family(self, rocksdb_partition):
        rocksdb_partition.create_column_family("cf")
        rocksdb_partition.drop_column_family("cf")

        with pytest.raises(ColumnFamilyDoesNotExist):
            rocksdb_partition.get_column_family("cf")

    def test_drop_column_family_doesnt_exist(self, rocksdb_partition):
        with pytest.raises(ColumnFamilyDoesNotExist):
            rocksdb_partition.drop_column_family("cf")

    def test_list_column_families(self, rocksdb_partition):
        rocksdb_partition.create_column_family("cf1")
        rocksdb_partition.create_column_family("cf2")
        cfs = rocksdb_partition.list_column_families()
        assert "cf1" in cfs
        assert "cf2" in cfs

    def test_list_column_families_defaults(self, rocksdb_partition):
        cfs = rocksdb_partition.list_column_families()
        assert cfs == [
            # "default" CF is always present in RocksDB
            "default",
            # "__metadata__" CF is created by the RocksDBStorePartition
            "__metadata__",
        ]

    def test_ensure_metadata_cf(self, rocksdb_partition):
        assert rocksdb_partition.get_column_family("__metadata__")

    def test_recover(self, rocksdb_partition):
        """
        Perform a recovery from a changelog message.
        """
        key = "my_key"
        value = "my_value"
        changelog_msg = ConfluentKafkaMessageStub(
            offset=10,
            key=dumps(key),
            value=dumps(value),
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
        )

        assert rocksdb_partition.get_changelog_offset() is None
        rocksdb_partition.recover_from_changelog_message(
            changelog_message=changelog_msg
        )

        assert rocksdb_partition.get_changelog_offset() == changelog_msg.offset() + 1
        with rocksdb_partition.begin() as tx:
            assert tx.get(key) == value


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

    @pytest.mark.parametrize("store_value", [10, None])
    def test_recover_from_changelog_message(self, rocksdb_partition, store_value):
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

        rocksdb_partition.recover_from_changelog_message(changelog_msg)

        with rocksdb_partition.begin() as tx:
            with tx.with_prefix(kafka_key):
                assert tx.get(user_store_key) == store_value
        assert rocksdb_partition.get_changelog_offset() == changelog_msg.offset() + 1

    @pytest.mark.parametrize(
        ("headers", "error"),
        [
            ([(CHANGELOG_CF_MESSAGE_HEADER, b"derp")], ColumnFamilyDoesNotExist),
            ([], ColumnFamilyHeaderMissing),
        ],
    )
    def test_recover_from_changelog_message_cf_errors(
        self, rocksdb_partition, headers, error
    ):
        changelog_msg = ConfluentKafkaMessageStub(
            key=b'my_key|"count"',
            value=b"10",
            headers=headers,
            offset=50,
        )
        with pytest.raises(error):
            rocksdb_partition.recover_from_changelog_message(changelog_msg)
        assert rocksdb_partition.get_changelog_offset() is None
