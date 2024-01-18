import pytest


class TestWindowedRocksDBPartitionTransaction:
    def test_update_window(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
                assert tx.get_window(start_ms=0, end_ms=10) == 1

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                assert tx.get_window(start_ms=0, end_ms=10) == 1

    def test_get_window_doesnt_exist(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                assert tx.get_window(start_ms=0, end_ms=10) is None

    def test_delete_window(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=1)
                assert tx.get_window(start_ms=0, end_ms=10) == 1
                tx.delete_window(start_ms=0, end_ms=10)

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                assert tx.get_window(start_ms=0, end_ms=10) is None

    def test_expire_windows_expired(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
                tx.update_window(start_ms=10, end_ms=20, value=2, timestamp_ms=10)

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=20, end_ms=30, value=3, timestamp_ms=20)
                expired = tx.expire_windows(duration_ms=10)
                # "expire_windows" must update the expiration index so that the same
                # windows are not expired twice
                assert not tx.expire_windows(duration_ms=10)

        assert len(expired) == 2
        assert expired == [
            ((0, 10), 1),
            ((10, 20), 2),
        ]

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                assert tx.get_window(start_ms=0, end_ms=10) is None
                assert tx.get_window(start_ms=10, end_ms=20) is None
                assert tx.get_window(start_ms=20, end_ms=30) == 3

    def test_expire_windows_cached(self, windowed_rocksdb_store_factory):
        """
        Check that windows expire correctly even if they're not committed to the DB
        yet.
        """
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
                tx.update_window(start_ms=10, end_ms=20, value=2, timestamp_ms=10)
                tx.update_window(start_ms=20, end_ms=30, value=3, timestamp_ms=20)
                expired = tx.expire_windows(duration_ms=10)
                # "expire_windows" must update the expiration index so that the same
                # windows are not expired twice
                assert not tx.expire_windows(duration_ms=10)

        assert len(expired) == 2
        assert expired == [
            ((0, 10), 1),
            ((10, 20), 2),
        ]

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                assert tx.get_window(start_ms=0, end_ms=10) is None
                assert tx.get_window(start_ms=10, end_ms=20) is None
                assert tx.get_window(start_ms=20, end_ms=30) == 3

    def test_expire_windows_empty(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=3, end_ms=13, value=1, timestamp_ms=3)
                assert not tx.expire_windows(duration_ms=10)

    def test_expire_windows_with_grace_expired(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=15, end_ms=25, value=1, timestamp_ms=15)
                expired = tx.expire_windows(duration_ms=10, grace_ms=5)

        assert len(expired) == 1
        assert expired == [((0, 10), 1)]

    def test_expire_windows_with_grace_empty(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=13, end_ms=23, value=1, timestamp_ms=13)
                expired = tx.expire_windows(duration_ms=10, grace_ms=5)

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
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.get_window(start_ms=start_ms, end_ms=end_ms)

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
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.update_window(
                    start_ms=start_ms, end_ms=end_ms, value=1, timestamp_ms=1
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
        with store.start_partition_transaction(0) as tx:
            with pytest.raises(ValueError, match="Invalid window duration"):
                tx.delete_window(start_ms=start_ms, end_ms=end_ms)

    def test_expire_windows_no_expired(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=1, end_ms=11, value=1, timestamp_ms=9)
                # "expire_windows" must update the expiration index so that the same
                # windows are not expired twice
                assert not tx.expire_windows(duration_ms=10)

    def test_expire_windows_multiple_windows(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
                tx.update_window(start_ms=10, end_ms=20, value=1, timestamp_ms=11)
                tx.update_window(start_ms=20, end_ms=30, value=1, timestamp_ms=21)

        with store.start_partition_transaction(0) as tx:
            with tx.with_prefix(b"__key__"):
                tx.update_window(start_ms=30, end_ms=40, value=1, timestamp_ms=31)
                # "expire_windows" must update the expiration index so that the same
                # windows are not expired twice
                expired = tx.expire_windows(duration_ms=10)

        assert len(expired) == 3
        assert expired[0] == ((0, 10), 1)
        assert expired[1] == ((10, 20), 1)
        assert expired[2] == ((20, 30), 1)

    def test_set_latest_timestamp_transaction(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 123
        partition.set_latest_timestamp(timestamp)
        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == timestamp

    def test_get_latest_timestamp_zero_on_init(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == 0

    def test_get_latest_timestamp_update(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 123
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp)

        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == timestamp

    def test_get_latest_timestamp_loaded_from_db(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 123
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp)
        store.revoke_partition(0)

        partition = store.assign_partition(0)
        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == timestamp

    def test_get_latest_timestamp_cannot_go_backwards(
        self, windowed_rocksdb_store_factory
    ):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 9
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp)
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp - 1)
            assert tx.get_latest_timestamp() == timestamp

        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == timestamp
