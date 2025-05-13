import contextlib
import secrets
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from quixstreams.state.base import PartitionTransaction
from quixstreams.state.base.transaction import PartitionTransactionCache
from quixstreams.state.exceptions import (
    InvalidChangelogOffset,
    StateSerializationError,
    StateTransactionError,
)
from quixstreams.state.manager import SUPPORTED_STORES
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    Marker,
)
from quixstreams.state.serialization import serialize
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

TEST_BYTES_VALUES = [
    b"null",
    b'"string"',
    b"123",
    b"123.123",
    b'{"key":"value","mapping":{"key":"value"}}',
    b"[123, 456]",
]

TEST_PREFIXES = [
    b"some_bytes",
    "string",
    123,
    123.123,
    (123, 456),
    [123, 456],
]


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestPartitionTransaction:
    def test_transaction_complete(self, store_partition):
        with store_partition.begin() as tx:
            ...

        assert tx.completed

    def test_transaction_doesnt_write_empty_batch(
        self, changelog_producer_mock, store_partition_factory
    ):
        """
        Test that transaction doesn't call "StateStore.write()" if the internal
        WriteBatch is empty (i.e. no keys were updated during the transaction).
        Writing empty batches costs more than doing
        """

        prefix = b"__key__"
        with store_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as partition:
            with patch.object(partition, "write") as mocked:
                with partition.begin() as tx:
                    tx.get("key", prefix=prefix)

                with partition.begin() as tx:
                    tx.get("key", prefix=prefix)

        assert not mocked.called
        assert not changelog_producer_mock.produce.called

    def test_delete_key_doesnt_exist(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.delete("key", prefix=prefix)

    @pytest.mark.parametrize(
        "key,value,bytes_value",
        zip(TEST_KEYS, TEST_VALUES, TEST_BYTES_VALUES),
    )
    def test_get_key_exists_cached(self, key, value, bytes_value, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set(key, value, prefix=prefix)
            stored = tx.get(key, prefix=prefix)
            assert stored == value

            stored_bytes = tx.get_bytes(key, prefix=prefix)
            assert stored_bytes == bytes_value

    @pytest.mark.parametrize(
        "key,value,bytes_value",
        zip(TEST_KEYS, TEST_VALUES, TEST_BYTES_VALUES),
    )
    def test_bytes_get_key_exists_cached(
        self, key, value, bytes_value, store_partition
    ):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set_bytes(key, bytes_value, prefix=prefix)
            stored = tx.get(key, prefix=prefix)
            assert stored == value

            stored_bytes = tx.get_bytes(key, prefix=prefix)
            assert stored_bytes == bytes_value

    @pytest.mark.parametrize(
        "key,value,bytes_value",
        zip(TEST_KEYS, TEST_VALUES, TEST_BYTES_VALUES),
    )
    def test_get_key_exists_no_cache(self, key, value, bytes_value, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set(key, value, prefix=prefix)

        with store_partition.begin() as tx:
            stored = tx.get(key, prefix=prefix)
            assert stored == value

            stored_bytes = tx.get_bytes(key, prefix=prefix)
            assert stored_bytes == bytes_value

    @pytest.mark.parametrize(
        "key,value,bytes_value",
        zip(TEST_KEYS, TEST_VALUES, TEST_BYTES_VALUES),
    )
    def test_bytes_get_key_exists_no_cache(
        self, key, value, bytes_value, store_partition
    ):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set_bytes(key, bytes_value, prefix=prefix)

        with store_partition.begin() as tx:
            stored = tx.get(key, prefix=prefix)
            assert stored == value

            stored_bytes = tx.get_bytes(key, prefix=prefix)
            assert stored_bytes == bytes_value

    def test_get_key_doesnt_exist_default(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            value = tx.get("key", default=123, prefix=prefix)
            assert value == 123

    def test_delete_key_cached_no_flush(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)
            assert tx.get("key", prefix=prefix) == "value"
            tx.delete("key", prefix=prefix)
            assert tx.get("key", prefix=prefix) is None

    def test_delete_key_cached(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)

        with store_partition.begin() as tx:
            assert tx.get("key", prefix=prefix) == "value"
            tx.delete("key", prefix=prefix)
            assert tx.get("key", prefix=prefix) is None

    def test_delete_key_no_cache(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)
            assert tx.get("key", prefix=prefix) == "value"

        with store_partition.begin() as tx:
            tx.delete("key", prefix=prefix)

        with store_partition.begin() as tx:
            assert tx.get("key", prefix=prefix) is None

    def test_key_exists_cached(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)
            assert tx.exists("key", prefix=prefix)
            assert not tx.exists("key123", prefix=prefix)

    def test_key_exists_no_cache(self, store_partition):
        prefix = b"__key__"

        with store_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)

        with store_partition.begin() as tx:
            assert tx.exists("key", prefix=prefix)
            assert not tx.exists("key123", prefix=prefix)

    def test_key_exists_deleted_in_cache(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix)

        with store_partition.begin() as tx:
            assert tx.exists("key", prefix=prefix)
            tx.delete("key", prefix=prefix)
            assert not tx.exists("key", prefix=prefix)

    @pytest.mark.parametrize(
        "key, value",
        [
            ("string", object()),
            (object(), "string"),
            ("string", datetime.now(timezone.utc)),
            (datetime.now(timezone.utc), "string"),
        ],
    )
    def test_set_serialization_error(self, key, value, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value, prefix=prefix)

    def test_set_bytes_no_bytes(self, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set_bytes(key="key", value="value", prefix=prefix)

    @pytest.mark.parametrize(
        "key", [object(), b"somebytes", datetime.now(timezone.utc)]
    )
    def test_delete_serialization_error(self, key, store_partition):
        prefix = b"__key__"
        with store_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.delete(key, prefix=prefix)

    def test_get_deserialization_error(self, store_partition, cache):
        bytes_ = secrets.token_bytes(10)
        string_ = "string"

        cache.set(
            key=bytes_,
            value=serialize(string_, dumps=dumps),
            prefix=b"",
            cf_name="default",
        )
        cache.set(
            key=serialize(string_, dumps=dumps),
            value=bytes_,
            prefix=b"",
            cf_name="default",
        )

        store_partition.write(
            cache=cache,
            changelog_offset=None,
        )

        with store_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.get(string_, prefix=b"")
            with pytest.raises(StateSerializationError):
                tx.get(bytes_, prefix=b"")

    def test_set_key_different_prefixes(self, store_partition):
        prefix1, prefix2 = b"__key1__", b"__key2__"
        with store_partition.begin() as tx:
            tx.set("key", "value", prefix=prefix1)
            assert tx.get("key", prefix=prefix1) == "value"
            assert tx.get("key", prefix=prefix2) is None

    def test_delete_key_different_prefixes_no_cache(self, store_partition):
        prefix1, prefix2 = b"__key1__", b"__key2__"
        with store_partition.begin() as tx:
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
    def test_update_key_failed_transaction_failed(self, operation, store_partition):
        """
        Test that if the update operation (set or delete) fails the transaction is
        marked as failed and cannot be re-used anymore.
        """

        prefix = b"__key__"
        with patch.object(
            PartitionTransaction,
            "_serialize_key",
            side_effect=ValueError("test"),
        ):
            with store_partition.begin() as tx:
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

    def test_update_key_prepared_transaction_fails(self, store_partition):
        """
        Test that any update operation (set or delete) fails if the transaction is
        marked as prepared.
        """

        prefix = b"__key__"
        tx = store_partition.begin()

        tx.set(key="key", value="value", prefix=prefix)
        tx.prepare(processed_offsets={"topic": 1})
        assert tx.prepared

        with pytest.raises(StateTransactionError):
            tx.set("key", value="value", prefix=prefix)

        with pytest.raises(StateTransactionError):
            tx.delete("key", prefix=prefix)

    def test_transaction_not_flushed_on_error(self, store_partition):
        prefix = b"__key__"
        with contextlib.suppress(ValueError):
            with store_partition.begin() as tx:
                tx.set("key", "value", prefix=prefix)
                raise ValueError("test")

        with store_partition.begin() as tx:
            assert tx.get("key", prefix=prefix) is None

    def test_set_dict_nonstr_keys_fails(self, store_partition):
        key = "key"
        value = {0: 1}
        prefix = b"__key__"
        with store_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value, prefix=prefix)

    def test_set_datetime_fails(self, store_partition):
        key = "key"
        value = datetime.now(timezone.utc)
        prefix = b"__key__"
        with store_partition.begin() as tx:
            with pytest.raises(StateSerializationError):
                tx.set(key, value, prefix=prefix)

    def test_flush_failed_transaction_failed(self, store_partition):
        """
        Test that if the "flush()" fails the transaction is also marked
        as failed and cannot be re-used.
        """

        prefix = b"__key__"
        with patch.object(store_partition, "write", side_effect=ValueError("test")):
            with store_partition.begin() as tx:
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

    @pytest.mark.parametrize("changelog_offset", [None, 1])
    def test_flush_success(self, changelog_offset, store_partition):
        tx = store_partition.begin()

        # Set some key to probe the transaction
        tx.set(key="key", value="value", prefix=b"__key__")

        tx.flush(changelog_offset=changelog_offset)
        assert tx.completed

        assert store_partition.get_changelog_offset() == changelog_offset

    def test_flush_invalid_changelog_offset(self, store_partition):
        tx1 = store_partition.begin()
        # Set some key to probe the transaction
        tx1.set(key="key", value="value", prefix=b"__key__")

        # Flush first transaction to update the changelog offset
        tx1.flush(changelog_offset=9999)
        assert tx1.completed

        tx2 = store_partition.begin()
        tx2.set(key="key", value="value", prefix=b"__key__")
        # Flush second transaction with a smaller changelog offset
        with pytest.raises(InvalidChangelogOffset):
            tx2.flush(changelog_offset=1)
        assert tx2.failed

    def test_set_and_prepare(self, store_partition_factory, changelog_producer_mock):
        data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ]
        cf = "default"
        prefix = b"__key__"
        processed_offsets = {"topic": 1}

        with store_partition_factory(
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
            tx.prepare(processed_offsets=processed_offsets)

            assert changelog_producer_mock.produce.call_count == len(data)

            for (key, value), call in zip(
                data, changelog_producer_mock.produce.call_args_list
            ):
                assert call.kwargs["key"] == tx._serialize_key(key=key, prefix=prefix)
                assert call.kwargs["value"] == tx._serialize_value(value=value)
                assert call.kwargs["headers"] == {
                    CHANGELOG_CF_MESSAGE_HEADER: cf,
                    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: dumps(
                        processed_offsets
                    ),
                }

            assert tx.prepared

    def test_delete_and_prepare(self, store_partition_factory, changelog_producer_mock):
        key = "key"
        cf = "default"
        prefix = b"__key__"
        processed_offsets = {"topic": 1}

        with store_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as partition:
            tx = partition.begin()
            tx.delete(key=key, cf_name=cf, prefix=prefix)

            tx.prepare(processed_offsets=processed_offsets)

            assert tx.prepared
            assert changelog_producer_mock.produce.call_count == 1

        delete_changelog = changelog_producer_mock.produce.call_args_list[0]
        assert delete_changelog.kwargs["key"] == tx._serialize_key(
            key=key, prefix=prefix
        )
        assert delete_changelog.kwargs["value"] is None
        assert delete_changelog.kwargs["headers"] == {
            CHANGELOG_CF_MESSAGE_HEADER: cf,
            CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: dumps(processed_offsets),
        }

    def test_set_delete_and_prepare(
        self, store_partition_factory, changelog_producer_mock
    ):
        """
        Test that only "delete" changelog message is emited if the key is set
        and deleted in the same transaction.
        """
        key, value = "key", "value"
        cf = "default"
        prefix = b"__key__"
        processed_offsets = {"topic": 1}

        with store_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as partition:
            tx = partition.begin()
            tx.set(key=key, value=value, cf_name=cf, prefix=prefix)
            tx.delete(key=key, cf_name=cf, prefix=prefix)

            tx.prepare(processed_offsets=processed_offsets)

            assert tx.prepared
            assert changelog_producer_mock.produce.call_count == 1
            delete_changelog = changelog_producer_mock.produce.call_args_list[0]
            assert delete_changelog.kwargs["key"] == tx._serialize_key(
                key=key, prefix=prefix
            )
            assert delete_changelog.kwargs["value"] is None
            assert delete_changelog.kwargs["headers"] == {
                CHANGELOG_CF_MESSAGE_HEADER: cf,
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: dumps(processed_offsets),
            }


class TestPartitionTransactionCache:
    def test_set_get_key_present(self, cache: PartitionTransactionCache):
        cache.set(key=b"key", value=b"value", prefix=b"prefix", cf_name="cf_name")
        value = cache.get(key=b"key", prefix=b"prefix", cf_name="cf_name")
        assert value == b"value"

    def test_get_key_missing(self, cache: PartitionTransactionCache):
        value = cache.get(key=b"key", prefix=b"prefix", cf_name="cf_name")
        assert value is Marker.UNDEFINED

    def test_set_delete_get(self, cache: PartitionTransactionCache):
        cache.set(key=b"key", value=b"value", prefix=b"prefix", cf_name="cf_name")
        cache.delete(key=b"key", prefix=b"prefix", cf_name="cf_name")

        value = cache.get(key=b"key", prefix=b"prefix", cf_name="cf_name")
        assert value is Marker.DELETED

    def test_get_column_families_empty(self, cache: PartitionTransactionCache):
        assert not cache.get_column_families()

    def test_get_column_families_present(self, cache: PartitionTransactionCache):
        cache.set(key=b"key", value=b"value", prefix=b"prefix", cf_name="cf_name1")
        cache.delete(key=b"key", prefix=b"prefix", cf_name="cf_name2")
        assert cache.get_column_families() == {"cf_name1", "cf_name2"}

    def test_get_updates_empty(self, cache: PartitionTransactionCache):
        assert cache.get_updates(cf_name="cf_name") == {}

        # Delete an item and make sure it's not in "updates"
        cache.delete(key=b"key", prefix=b"prefix", cf_name="cf_name2")
        assert cache.get_updates(cf_name="cf_name") == {}

    def test_get_updates_present(self, cache: PartitionTransactionCache):
        cache.set(key=b"key", value=b"value", prefix=b"prefix", cf_name="cf_name")
        assert cache.get_updates(cf_name="cf_name") == {b"prefix": {b"key": b"value"}}

    def test_get_updates_after_delete(self, cache: PartitionTransactionCache):
        cache.set(key=b"key", value=b"value", prefix=b"prefix", cf_name="cf_name")
        cache.delete(key=b"key", prefix=b"prefix", cf_name="cf_name")
        assert cache.get_updates(cf_name="cf_name") == {b"prefix": {}}

    def test_get_deletes_empty(self, cache: PartitionTransactionCache):
        assert cache.get_deletes(cf_name="cf_name") == set()

        cache.set(key=b"key", value=b"value", prefix=b"prefix", cf_name="cf_name")
        assert cache.get_deletes(cf_name="cf_name") == set()

    def test_get_deletes_present(self, cache: PartitionTransactionCache):
        cache.delete(key=b"key1", prefix=b"prefix", cf_name="cf_name")
        cache.delete(key=b"key2", prefix=b"prefix", cf_name="cf_name")
        assert cache.get_deletes(cf_name="cf_name") == {b"key1", b"key2"}

    def test_get_deletes_after_set(self, cache: PartitionTransactionCache):
        cache.delete(key=b"key1", prefix=b"prefix", cf_name="cf_name")
        cache.set(key=b"key1", value=b"value", prefix=b"prefix", cf_name="cf_name")
        assert cache.get_deletes(cf_name="cf_name") == set()

    @pytest.mark.parametrize(
        "action, expected",
        [
            (lambda cache: None, True),
            (
                lambda cache: cache.set(key=b"key1", value=b"value", prefix=b"prefix"),
                False,
            ),
            (
                lambda cache: cache.delete(key=b"key1", prefix=b"prefix"),
                False,
            ),
        ],
    )
    def test_empty(self, action, expected, cache):
        action(cache)
        assert cache.is_empty() == expected
