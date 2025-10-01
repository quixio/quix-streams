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
