import pytest

from quixstreams.state.metadata import CHANGELOG_CF_MESSAGE_HEADER
from quixstreams.state.serialization import encode_integer_pair


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
    def test_expire_all_windows_expired(self, windowed_rocksdb_store_factory, delete):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix1 = b"__key__1"
        prefix2 = b"__key__2"

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix1
            )
            tx.update_window(
                start_ms=10, end_ms=20, value=2, timestamp_ms=10, prefix=prefix2
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=20, end_ms=30, value=3, timestamp_ms=20, prefix=prefix1
            )
            expired = list(tx.expire_all_windows(max_end_time=20, delete=delete))
            assert not list(tx.expire_all_windows(max_end_time=20, delete=delete))

        assert len(expired) == 2
        assert expired == [
            ((0, 10), 1, [], prefix1),
            ((10, 20), 2, [], prefix2),
        ]

        with store.start_partition_transaction(0) as tx:
            assert (
                tx.get_window(start_ms=0, end_ms=10, prefix=prefix1) is None
                if delete
                else 1
            )
            assert (
                tx.get_window(start_ms=10, end_ms=20, prefix=prefix2) is None
                if delete
                else 2
            )
            assert tx.get_window(start_ms=20, end_ms=30, prefix=prefix1) == 3

    @pytest.mark.parametrize("delete", [True, False])
    def test_expire_all_windows_cached(self, windowed_rocksdb_store_factory, delete):
        """
        Check that windows expire correctly even if they're not committed to the DB
        yet.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"

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
            expired = list(tx.expire_all_windows(max_end_time=20, delete=delete))
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not list(tx.expire_all_windows(max_end_time=20, delete=delete))
            assert len(expired) == 2
            assert expired == [
                ((0, 10), 1, [], prefix),
                ((10, 20), 2, [], prefix),
            ]
            assert (
                tx.get_window(start_ms=0, end_ms=10, prefix=prefix) is None
                if delete
                else 1
            )
            assert (
                tx.get_window(start_ms=10, end_ms=20, prefix=prefix) is None
                if delete
                else 2
            )
            assert tx.get_window(start_ms=20, end_ms=30, prefix=prefix) == 3

    def test_expire_all_windows_empty(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"

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
            assert not list(tx.expire_all_windows(max_end_time=3))

    @pytest.mark.parametrize("end_inclusive", [True, False])
    def test_expire_all_windows_with_collect(
        self, windowed_rocksdb_store_factory, end_inclusive
    ):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"

        with store.start_partition_transaction(0) as tx:
            # Different window types store values differently:
            # - Tumbling/hopping windows use None as placeholder values
            # - Sliding windows use [int, None] format where int is the max timestamp
            # Note: In production, these different value types would not be mixed
            # within the same state.
            tx.update_window(
                start_ms=0, end_ms=10, value=None, timestamp_ms=2, prefix=prefix
            )
            tx.update_window(
                start_ms=10,
                end_ms=20,
                value=[777, None],
                timestamp_ms=10,
                prefix=prefix,
            )

            tx.add_to_collection(value="a", id=0, prefix=prefix)
            tx.add_to_collection(value="b", id=10, prefix=prefix)
            tx.add_to_collection(value="c", id=20, prefix=prefix)

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=20, end_ms=30, value=None, timestamp_ms=20, prefix=prefix
            )
            expired = list(
                tx.expire_all_windows(
                    max_end_time=20,
                    collect=True,
                    end_inclusive=end_inclusive,
                )
            )

        window_1_value = ["a", "b"] if end_inclusive else ["a"]
        window_2_value = ["b", "c"] if end_inclusive else ["b"]
        assert expired == [
            ((0, 10), None, window_1_value, b"__key__"),
            ((10, 20), [777, None], window_2_value, b"__key__"),
        ]

    def test_expire_all_windows_same_keys_in_db_and_update_cache(
        self, windowed_rocksdb_store_factory
    ):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            # The same window already exists in the db
            tx.update_window(
                start_ms=0, end_ms=10, value=3, timestamp_ms=8, prefix=prefix
            )
            tx.update_window(
                start_ms=10, end_ms=20, value=2, timestamp_ms=10, prefix=prefix
            )
            expired = list(tx.expire_all_windows(max_end_time=10))

            # Value from the cache takes precedence over the value in the db
            assert expired == [((0, 10), 3, [], b"__key__")]

    @pytest.mark.parametrize(
        "start_ms, end_ms",
        [
            (0, 0),
            (1, 0),
        ],
    )
    def test_get_window_invalid_duration(
        self, start_ms, end_ms, windowed_rocksdb_store_factory
    ):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.get_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)

    @pytest.mark.parametrize(
        "start_ms, end_ms",
        [
            (0, 0),
            (1, 0),
        ],
    )
    def test_update_window_invalid_duration(
        self, start_ms, end_ms, windowed_rocksdb_store_factory
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

    @pytest.mark.parametrize(
        "start_ms, end_ms",
        [
            (0, 0),
            (1, 0),
        ],
    )
    def test_delete_window_invalid_duration(
        self, start_ms, end_ms, windowed_rocksdb_store_factory
    ):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.delete_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)

    def test_update_window_and_prepare(
        self, windowed_rocksdb_partition_factory, changelog_producer_mock
    ):
        prefix = b"__key__"
        start_ms = 0
        end_ms = 10
        value = 1

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
            tx.prepare()
            assert tx.prepared

        assert changelog_producer_mock.produce.call_count == 1
        expected_produced_key = tx._serialize_key(
            encode_integer_pair(start_ms, end_ms), prefix=prefix
        )
        expected_produced_value = tx._serialize_value(value)
        changelog_producer_mock.produce.assert_any_call(
            key=expected_produced_key,
            value=expected_produced_value,
            headers={CHANGELOG_CF_MESSAGE_HEADER: "default"},
        )

    def test_delete_window_and_prepare(
        self, windowed_rocksdb_partition_factory, changelog_producer_mock
    ):
        prefix = b"__key__"
        start_ms = 0
        end_ms = 10

        with windowed_rocksdb_partition_factory(
            changelog_producer=changelog_producer_mock
        ) as store_partition:
            tx = store_partition.begin()
            tx.delete_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)
            tx.prepare()
            assert tx.prepared

        assert changelog_producer_mock.produce.call_count == 1
        expected_produced_key = tx._serialize_key(
            encode_integer_pair(start_ms, end_ms), prefix=prefix
        )
        changelog_producer_mock.produce.assert_called_with(
            key=expected_produced_key,
            value=None,
            headers={CHANGELOG_CF_MESSAGE_HEADER: "default"},
        )
