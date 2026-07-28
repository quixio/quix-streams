from unittest.mock import patch

import pytest

from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
)
from quixstreams.state.rocksdb.transaction import RocksDBPartitionTransaction
from quixstreams.state.serialization import encode_integer_pair
from quixstreams.utils.json import dumps


class TestWindowedRocksDBPartitionTransaction:
    def test_update_window(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) == 1

        with store.start_partition_transaction(0) as tx:
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) == 1

    def test_get_window_doesnt_exist(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) is None

    def test_delete_window(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=1, prefix=prefix
            )
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) == 1
            tx.delete_window(start_ms=0, end_ms=10, prefix=prefix)

        with store.start_partition_transaction(0) as tx:
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) is None

    @pytest.mark.parametrize("delete", [True, False])
    def test_expire_windows_expired(self, windowed_rocksdb_store_factory, delete):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        duration_ms = 10

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )
            tx.update_window(
                start_ms=10, end_ms=20, value=2, timestamp_ms=10, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=20, end_ms=30, value=3, timestamp_ms=20, prefix=prefix
            )
            max_start_time = tx.get_latest_timestamp(prefix=prefix) - duration_ms
            expired = list(
                tx.expire_windows(
                    max_start_time=max_start_time, prefix=prefix, delete=delete
                )
            )
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not list(
                tx.expire_windows(
                    max_start_time=max_start_time, prefix=prefix, delete=delete
                )
            )

        assert len(expired) == 2
        assert expired == [
            ((0, 10), 1, [], prefix),
            ((10, 20), 2, [], prefix),
        ]

        with store.start_partition_transaction(0) as tx:
            assert (
                tx.get_window(start_ms=0, end_ms=10, prefix=prefix) == None
                if delete
                else 1
            )
            assert (
                tx.get_window(start_ms=10, end_ms=20, prefix=prefix) == None
                if delete
                else 2
            )
            assert tx.get_window(start_ms=20, end_ms=30, prefix=prefix) == 3

    @pytest.mark.parametrize("delete", [True, False])
    def test_expire_windows_cached(self, windowed_rocksdb_store_factory, delete):
        """
        Check that windows expire correctly even if they're not committed to the DB
        yet.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        duration_ms = 10

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )
            tx.update_window(
                start_ms=10, end_ms=20, value=2, timestamp_ms=10, prefix=prefix
            )
            tx.update_window(
                start_ms=20, end_ms=30, value=3, timestamp_ms=20, prefix=prefix
            )
            max_start_time = tx.get_latest_timestamp(prefix=prefix) - duration_ms
            expired = list(
                tx.expire_windows(
                    max_start_time=max_start_time, prefix=prefix, delete=delete
                )
            )
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not list(
                tx.expire_windows(
                    max_start_time=max_start_time, prefix=prefix, delete=delete
                )
            )
            assert len(expired) == 2
            assert expired == [
                ((0, 10), 1, [], prefix),
                ((10, 20), 2, [], prefix),
            ]
            assert (
                tx.get_window(start_ms=0, end_ms=10, prefix=prefix) == None
                if delete
                else 1
            )
            assert (
                tx.get_window(start_ms=10, end_ms=20, prefix=prefix) == None
                if delete
                else 2
            )
            assert tx.get_window(start_ms=20, end_ms=30, prefix=prefix) == 3

    def test_expire_windows_empty(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        duration_ms = 10

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=3, end_ms=13, value=1, timestamp_ms=3, prefix=prefix
            )
            max_start_time = tx.get_latest_timestamp(prefix=prefix) - duration_ms
            assert not list(
                tx.expire_windows(max_start_time=max_start_time, prefix=prefix)
            )

    def test_expire_windows_with_grace_expired(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        duration_ms = 10
        grace_ms = 5

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=15, end_ms=25, value=1, timestamp_ms=15, prefix=prefix
            )
            max_start_time = (
                tx.get_latest_timestamp(prefix=prefix) - duration_ms - grace_ms
            )
            expired = list(
                tx.expire_windows(max_start_time=max_start_time, prefix=prefix)
            )

        assert len(expired) == 1
        assert expired == [((0, 10), 1, [], prefix)]

    def test_expire_windows_with_grace_empty(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        duration_ms = 10
        grace_ms = 5

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=13, end_ms=23, value=1, timestamp_ms=13, prefix=prefix
            )
            max_start_time = (
                tx.get_latest_timestamp(prefix=prefix) - duration_ms - grace_ms
            )
            expired = list(
                tx.expire_windows(max_start_time=max_start_time, prefix=prefix)
            )

        assert not expired

    @pytest.mark.parametrize("start_ms, end_ms", [(1, 0), (0, 0)])
    def test_get_window_invalid_duration(
        self, windowed_rocksdb_store_factory, start_ms, end_ms
    ):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.get_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)

    @pytest.mark.parametrize("start_ms, end_ms", [(1, 0), (0, 0)])
    def test_update_window_invalid_duration(
        self, windowed_rocksdb_store_factory, start_ms, end_ms
    ):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.update_window(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    value=1,
                    timestamp_ms=1,
                    prefix=prefix,
                )

    @pytest.mark.parametrize("start_ms, end_ms", [(1, 0), (0, 0)])
    def test_delete_window_invalid_duration(
        self, windowed_rocksdb_store_factory, start_ms, end_ms
    ):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.delete_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)

    def test_expire_windows_no_expired(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        duration_ms = 10

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=1, end_ms=11, value=1, timestamp_ms=9, prefix=prefix
            )
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            max_start_time = tx.get_latest_timestamp(prefix=prefix) - duration_ms
            assert not list(
                tx.expire_windows(max_start_time=max_start_time, prefix=prefix)
            )

    def test_expire_windows_multiple_windows(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        duration_ms = 10

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )
            tx.update_window(
                start_ms=10, end_ms=20, value=1, timestamp_ms=11, prefix=prefix
            )
            tx.update_window(
                start_ms=20, end_ms=30, value=1, timestamp_ms=21, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=30, end_ms=40, value=1, timestamp_ms=31, prefix=prefix
            )
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            max_start_time = tx.get_latest_timestamp(prefix=prefix) - duration_ms
            expired = list(
                tx.expire_windows(max_start_time=max_start_time, prefix=prefix)
            )

        assert len(expired) == 3
        assert expired[0] == ((0, 10), 1, [], prefix)
        assert expired[1] == ((10, 20), 1, [], prefix)
        assert expired[2] == ((20, 30), 1, [], prefix)

    def test_get_latest_timestamp_update(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 123
        prefix = b"__key__"
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp, prefix=prefix)

        with partition.begin() as tx:
            assert tx.get_latest_timestamp(prefix=prefix) == timestamp

    def test_get_latest_timestamp_cannot_go_backwards(
        self, windowed_rocksdb_store_factory
    ):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 9
        prefix = b"__key__"
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp, prefix=prefix)
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp - 1, prefix=prefix)
            assert tx.get_latest_timestamp(prefix=prefix) == timestamp

        with partition.begin() as tx:
            assert tx.get_latest_timestamp(prefix=prefix) == timestamp

    def test_update_window_and_prepare(
        self, windowed_rocksdb_partition_factory, changelog_producer_mock
    ):
        prefix = b"__key__"
        start_ms = 0
        end_ms = 10
        value = 1
        processed_offsets = {"topic": 1}

        with windowed_rocksdb_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as store_partition:
            tx = store_partition.begin()
            tx.update_window(
                start_ms=start_ms,
                end_ms=end_ms,
                value=value,
                timestamp_ms=2,
                prefix=prefix,
            )
            tx.prepare(processed_offsets=processed_offsets)
            assert tx.prepared

        # The transaction is expected to produce 2 keys for each updated one:
        # One for the window itself, and another for the latest timestamp
        assert changelog_producer_mock.produce.call_count == 2
        expected_produced_key = tx._serialize_key(
            encode_integer_pair(start_ms, end_ms), prefix=prefix
        )
        expected_produced_value = tx._serialize_value(value)
        changelog_producer_mock.produce.assert_any_call(
            key=expected_produced_key,
            value=expected_produced_value,
            headers={
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: dumps(processed_offsets),
            },
        )

    def test_delete_window_and_prepare(
        self, windowed_rocksdb_partition_factory, changelog_producer_mock
    ):
        prefix = b"__key__"
        start_ms = 0
        end_ms = 10
        processed_offsets = {"topic": 1}

        with windowed_rocksdb_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as store_partition:
            tx = store_partition.begin()
            tx.delete_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)
            tx.prepare(processed_offsets=processed_offsets)
            assert tx.prepared

        assert changelog_producer_mock.produce.call_count == 1
        expected_produced_key = tx._serialize_key(
            encode_integer_pair(start_ms, end_ms), prefix=prefix
        )
        changelog_producer_mock.produce.assert_called_with(
            key=expected_produced_key,
            value=None,
            headers={
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: dumps(processed_offsets),
            },
        )


class TestIterWindows:
    """
    Validates spec §7.4: `iter_windows` is the lazy, inclusive-lower-bound
    counterpart of `get_windows` (B1/B5/B7).
    """

    def test_inclusive_lower_bound(self, windowed_rocksdb_store_factory):
        """
        Validates spec §7.4 (B5 fix): a window starting exactly at
        `start_from_ms` is included, unlike `get_windows`' exclusive bound.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            result = list(tx.iter_windows(prefix=prefix, start_from_ms=0))

        assert result == [((0, 1), 1, prefix)]

    def test_unbounded_upper_bound(self, windowed_rocksdb_store_factory):
        """Validates spec §7.4: `start_to_ms=None` means unbounded above."""
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=prefix
            )
            tx.update_window(
                start_ms=1000, end_ms=1001, value=2, timestamp_ms=1000, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            result = list(
                tx.iter_windows(prefix=prefix, start_from_ms=0, start_to_ms=None)
            )

        assert result == [((0, 1), 1, prefix), ((1000, 1001), 2, prefix)]

    def test_backwards_returns_greatest_start_first(
        self, windowed_rocksdb_store_factory
    ):
        """Validates spec §7.4: `backwards=True` yields greatest-start-first."""
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=prefix
            )
            tx.update_window(
                start_ms=1000, end_ms=1001, value=2, timestamp_ms=1000, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            result = list(
                tx.iter_windows(prefix=prefix, start_to_ms=1000, backwards=True)
            )

        assert result == [((1000, 1001), 2, prefix), ((0, 1), 1, prefix)]

    def test_uncommitted_update_cache_visible(self, windowed_rocksdb_store_factory):
        """
        Validates spec §7.4: uncommitted writes made earlier in the same
        transaction are visible to `iter_windows` before any flush.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=prefix
            )
            result = list(tx.iter_windows(prefix=prefix))

        assert result == [((0, 1), 1, prefix)]

    def test_deleted_windows_not_returned(self, windowed_rocksdb_store_factory):
        """Validates spec §7.4: a window deleted in this transaction's cache
        must not be yielded, even though it is still on disk."""
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.delete_window(start_ms=0, end_ms=1, prefix=prefix)
            result = list(tx.iter_windows(prefix=prefix))

        assert result == []

    def test_is_lazy_does_not_use_materializing_get_items(
        self, windowed_rocksdb_store_factory
    ):
        """
        Validates spec §7.4 (B7 fix): `iter_windows` must reach its first
        element without falling back to `_get_items`, the materializing
        primitive `get_windows` uses. A prefix holding 10k windows would be
        fully built into a list on every message if this regressed.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            for i in range(10_000):
                tx.update_window(
                    start_ms=i, end_ms=i + 1, value=i, timestamp_ms=i, prefix=prefix
                )

        with store.start_partition_transaction(0) as tx:
            with patch.object(RocksDBPartitionTransaction, "_get_items") as spy:
                first = next(tx.iter_windows(prefix=prefix))

        assert first == ((0, 1), 0, prefix)
        spy.assert_not_called()

    def test_prefix_leak_for_separator_extended_key(
        self, windowed_rocksdb_store_factory
    ):
        """
        Review finding F1: `_PREFIX_UPPER_BOUND` is meant to bound exactly one
        prefix's windows, but a message key that is a SEPARATOR-extension of
        another key (`b"user|123"` extends `b"user"`) serializes to a window
        key that sorts *inside* `b"user"`'s bound (`b"user"` + SEPARATOR +
        `<8 bytes>` + SEPARATOR + `<8 bytes>` sorts below `b"user"` +
        `_PREFIX_UPPER_BOUND` because any real byte after the separator is far
        below `0xff`). `iter_windows(prefix=b"user", ...)` must not leak
        `b"user|123"`'s windows.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"user"
        extended_prefix = b"user|123"

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=prefix
            )
            tx.update_window(
                start_ms=1000,
                end_ms=1001,
                value=2,
                timestamp_ms=1000,
                prefix=extended_prefix,
            )

        with store.start_partition_transaction(0) as tx:
            result = list(
                tx.iter_windows(prefix=prefix, start_from_ms=0, start_to_ms=None)
            )

        assert result == [((0, 1), 1, prefix)]


class TestIterPrefixes:
    """Validates spec §7.5: `iter_prefixes` (B6 fix)."""

    def test_iter_prefixes_deterministic_order(self, windowed_rocksdb_store_factory):
        """
        Three message keys x two windows each, one key present only in the
        uncommitted cache: exactly three distinct prefixes, in deterministic
        (byte-sorted) order.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        key_a, key_b, key_c = b"key_a", b"key_b", b"key_c"

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=key_a
            )
            tx.update_window(
                start_ms=1, end_ms=2, value=1, timestamp_ms=1, prefix=key_a
            )
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=key_c
            )
            tx.update_window(
                start_ms=1, end_ms=2, value=1, timestamp_ms=1, prefix=key_c
            )

        with store.start_partition_transaction(0) as tx:
            # key_b exists only in this transaction's uncommitted update cache.
            tx.update_window(
                start_ms=0, end_ms=1, value=1, timestamp_ms=0, prefix=key_b
            )
            tx.update_window(
                start_ms=1, end_ms=2, value=1, timestamp_ms=1, prefix=key_b
            )
            prefixes = list(tx.iter_prefixes())

        assert prefixes == [key_a, key_b, key_c]

    def test_iter_prefixes_skips_separator_extended_key(
        self, windowed_rocksdb_store_factory
    ):
        """
        Review finding F2: `_iter_db_prefixes` seeks to
        `prefix + _PREFIX_UPPER_BOUND` after yielding a prefix, assuming that
        skips exactly that prefix's keys and nothing else. For a message key
        that is a SEPARATOR-extension of another key (`b"user|123"` extends
        `b"user"`), that seek target sorts *above* the extended key's window
        keys too (see the companion `test_prefix_leak_for_separator_extended_
        key` in `TestIterWindows`), so the extended key's prefix is skipped
        and never yielded - both prefixes must be committed to the DB, not
        left in the uncommitted cache, or the cache-merge side of
        `iter_prefixes` would rescue it and the test would prove nothing.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"user"
        extended_prefix = b"user|123"

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=1000, end_ms=1001, value=1, timestamp_ms=1000, prefix=prefix
            )
            tx.update_window(
                start_ms=1000,
                end_ms=1001,
                value=2,
                timestamp_ms=1000,
                prefix=extended_prefix,
            )

        with store.start_partition_transaction(0) as tx:
            prefixes = list(tx.iter_prefixes())

        assert prefixes == [prefix, extended_prefix]
