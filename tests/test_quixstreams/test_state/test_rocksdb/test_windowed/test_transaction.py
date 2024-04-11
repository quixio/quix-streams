from unittest.mock import call

import pytest

from quixstreams.state.rocksdb.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    PREFIX_SEPARATOR,
)
from quixstreams.state.rocksdb.windowed.metadata import (
    LATEST_EXPIRED_WINDOW_CF_NAME,
    LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
)
from quixstreams.state.rocksdb.windowed.serialization import encode_window_key


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

    def test_expire_windows_expired(self, windowed_rocksdb_store_factory):
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

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=20, end_ms=30, value=3, timestamp_ms=20, prefix=prefix
            )
            expired = tx.expire_windows(duration_ms=10, prefix=prefix)
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not tx.expire_windows(duration_ms=10, prefix=prefix)

        assert len(expired) == 2
        assert expired == [
            ((0, 10), 1),
            ((10, 20), 2),
        ]

        with store.start_partition_transaction(0) as tx:
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) is None
            assert tx.get_window(start_ms=10, end_ms=20, prefix=prefix) is None
            assert tx.get_window(start_ms=20, end_ms=30, prefix=prefix) == 3

    def test_expire_windows_cached(self, windowed_rocksdb_store_factory):
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
            expired = tx.expire_windows(duration_ms=10, prefix=prefix)
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not tx.expire_windows(duration_ms=10, prefix=prefix)
            assert len(expired) == 2
            assert expired == [
                ((0, 10), 1),
                ((10, 20), 2),
            ]
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) is None
            assert tx.get_window(start_ms=10, end_ms=20, prefix=prefix) is None
            assert tx.get_window(start_ms=20, end_ms=30, prefix=prefix) == 3

    def test_expire_windows_empty(self, windowed_rocksdb_store_factory):
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
            assert not tx.expire_windows(duration_ms=10, prefix=prefix)

    def test_expire_windows_with_grace_expired(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=15, end_ms=25, value=1, timestamp_ms=15, prefix=prefix
            )
            expired = tx.expire_windows(duration_ms=10, grace_ms=5, prefix=prefix)

        assert len(expired) == 1
        assert expired == [((0, 10), 1)]

    def test_expire_windows_with_grace_empty(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=0, end_ms=10, value=1, timestamp_ms=2, prefix=prefix
            )

        with store.start_partition_transaction(0) as tx:
            tx.update_window(
                start_ms=13, end_ms=23, value=1, timestamp_ms=13, prefix=prefix
            )
            expired = tx.expire_windows(duration_ms=10, grace_ms=5, prefix=prefix)

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
            assert not tx.expire_windows(duration_ms=10, prefix=prefix)

    def test_expire_windows_multiple_windows(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        prefix = b"__key__"
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
            expired = tx.expire_windows(duration_ms=10, prefix=prefix)

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
        prefix = b"__key__"
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp, prefix=prefix)

        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == timestamp

    def test_get_latest_timestamp_loaded_from_db(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 123
        prefix = b"__key__"
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp, prefix=prefix)
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
        prefix = b"__key__"
        with partition.begin() as tx:
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp, prefix=prefix)
            tx.update_window(0, 10, value=1, timestamp_ms=timestamp - 1, prefix=prefix)
            assert tx.get_latest_timestamp() == timestamp

        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == timestamp


class TestWindowedRocksDBPartitionTransactionChangelog:
    def test_update_window(self, windowed_rocksdb_store_factory_changelog):
        store = windowed_rocksdb_store_factory_changelog()
        partition_num = 0
        store_partition = store.assign_partition(partition_num)
        producer = store_partition._changelog_producer._producer
        prefix = b"__key__"
        start_ms = 0
        end_ms = 10
        value = 1

        with store.start_partition_transaction(partition_num) as tx:
            expected_produced_key = tx._serialize_key(
                encode_window_key(start_ms, end_ms), prefix=prefix
            )
            expected_produced_value = tx._serialize_value(value)
            tx.update_window(
                start_ms=start_ms,
                end_ms=end_ms,
                value=value,
                timestamp_ms=2,
                prefix=prefix,
            )
            assert (
                tx.get_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix) == value
            )

        with store.start_partition_transaction(partition_num) as tx:
            assert (
                tx.get_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix) == value
            )

        assert (
            store_partition.get_changelog_offset() == producer.produce.call_count == 1
        )
        producer.produce.assert_called_with(
            key=expected_produced_key,
            value=expected_produced_value,
            headers={CHANGELOG_CF_MESSAGE_HEADER: "default"},
            topic=store_partition._changelog_producer._changelog_name,
            partition=store_partition._changelog_producer._partition_num,
        )

    def test_delete_window(self, windowed_rocksdb_store_factory_changelog):
        store = windowed_rocksdb_store_factory_changelog()
        partition_num = 0
        store_partition = store.assign_partition(partition_num)
        producer = store_partition._changelog_producer._producer
        prefix = b"__key__"
        expected_produced_value = None
        start_ms = 0
        end_ms = 10

        with store.start_partition_transaction(partition_num) as tx:
            expected_produced_key = tx._serialize_key(
                encode_window_key(start_ms, end_ms), prefix=prefix
            )
            tx.update_window(
                start_ms=start_ms, end_ms=end_ms, value=1, timestamp_ms=1, prefix=prefix
            )
            assert tx.get_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix) == 1
            tx.delete_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)

        with store.start_partition_transaction(partition_num) as tx:
            assert (
                tx.get_window(start_ms=start_ms, end_ms=end_ms, prefix=prefix)
                is expected_produced_value
            )

        assert (
            store_partition.get_changelog_offset() == producer.produce.call_count == 1
        )
        producer.produce.assert_called_with(
            key=expected_produced_key,
            value=expected_produced_value,
            headers={CHANGELOG_CF_MESSAGE_HEADER: "default"},
            topic=store_partition._changelog_producer._changelog_name,
            partition=store_partition._changelog_producer._partition_num,
        )

    def test_expire_windows_expired(self, windowed_rocksdb_store_factory_changelog):
        store = windowed_rocksdb_store_factory_changelog()
        partition_num = 0
        store_partition = store.assign_partition(partition_num)
        producer = store_partition._changelog_producer._producer
        prefix = b"__key__"
        expected_update_produce_keys = []
        expected_update_produce_values = []
        expected_expired_window_keys = []
        expected_expired_windows = [
            dict(start_ms=0, end_ms=10, value=1, timestamp_ms=2),
            dict(start_ms=10, end_ms=20, value=2, timestamp_ms=10),
        ]

        # update windows, which will become expired later
        with store.start_partition_transaction(partition_num) as tx:
            for kwargs in expected_expired_windows:
                serialized_key = tx._serialize_key(
                    encode_window_key(kwargs["start_ms"], kwargs["end_ms"]),
                    prefix=prefix,
                )
                expected_update_produce_keys.append(serialized_key)
                expected_expired_window_keys.append(serialized_key)
                expected_update_produce_values.append(
                    tx._serialize_value(kwargs["value"])
                )
                tx.update_window(**kwargs, prefix=prefix)

        # add new window update, which expires previous windows
        with store.start_partition_transaction(partition_num) as tx:
            kwargs = dict(start_ms=20, end_ms=30, value=3, timestamp_ms=20)
            expected_update_produce_keys.append(
                tx._serialize_key(
                    encode_window_key(kwargs["start_ms"], kwargs["end_ms"]),
                    prefix=prefix,
                )
            )
            expected_update_produce_values.append(tx._serialize_value(kwargs["value"]))
            tx.update_window(**kwargs, prefix=prefix)
            expired = tx.expire_windows(duration_ms=10, prefix=prefix)
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not tx.expire_windows(duration_ms=10, prefix=prefix)

        assert expired == [
            ((w["start_ms"], w["end_ms"]), w["value"]) for w in expected_expired_windows
        ]

        produce_calls = [
            call(
                key=k,
                value=v,
                headers={CHANGELOG_CF_MESSAGE_HEADER: "default"},
                topic=store_partition._changelog_producer._changelog_name,
                partition=store_partition._changelog_producer._partition_num,
            )
            for k, v in zip(
                expected_update_produce_keys, expected_update_produce_values
            )
        ]

        produce_calls.extend(
            [
                call(
                    key=k,
                    value=None,
                    headers={CHANGELOG_CF_MESSAGE_HEADER: "default"},
                    topic=store_partition._changelog_producer._changelog_name,
                    partition=store_partition._changelog_producer._partition_num,
                )
                for k in expected_expired_window_keys
            ]
        )

        produce_calls.append(
            call(
                key=prefix + PREFIX_SEPARATOR + LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
                value=str(expected_expired_windows[-1]["start_ms"]).encode(),
                headers={CHANGELOG_CF_MESSAGE_HEADER: LATEST_EXPIRED_WINDOW_CF_NAME},
                topic=store_partition._changelog_producer._changelog_name,
                partition=store_partition._changelog_producer._partition_num,
            )
        )

        producer.produce.assert_has_calls(produce_calls)
        assert producer.produce.call_count == len(produce_calls)

        with store.start_partition_transaction(0) as tx:
            prefix = b"__key__"
            assert tx.get_window(start_ms=0, end_ms=10, prefix=prefix) is None
            assert tx.get_window(start_ms=10, end_ms=20, prefix=prefix) is None
            assert tx.get_window(start_ms=20, end_ms=30, prefix=prefix) == 3

    def test_expire_windows_cached(self, windowed_rocksdb_store_factory_changelog):
        """
        Check that windows expire correctly even if they're not committed to the DB
        yet.

        Consequently, only the end result of a window should be produced to the
        changelog topic, not every update.
        """
        store = windowed_rocksdb_store_factory_changelog()
        partition_num = 0
        store_partition = store.assign_partition(partition_num)
        producer = store_partition._changelog_producer._producer
        key = b"__key__"

        expected_update_produce_keys = []
        expected_update_produce_values = []
        update_windows = [
            dict(start_ms=0, end_ms=10, value=1, timestamp_ms=2),
            dict(start_ms=10, end_ms=20, value=2, timestamp_ms=10),
            dict(start_ms=20, end_ms=30, value=3, timestamp_ms=20),
        ]
        expected_expired_windows = update_windows[:2]

        with store.start_partition_transaction(0) as tx:

            for kwargs in update_windows:
                serialized_key = tx._serialize_key(
                    encode_window_key(kwargs["start_ms"], kwargs["end_ms"]),
                    prefix=key,
                )
                tx.update_window(**kwargs, prefix=key)
                expected_update_produce_keys.append(serialized_key)
                if kwargs in expected_expired_windows:
                    expected_update_produce_values.append(None)
                else:
                    expected_update_produce_values.append(
                        tx._serialize_value(kwargs["value"])
                    )

            expired = tx.expire_windows(duration_ms=10, prefix=key)
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not tx.expire_windows(duration_ms=10, prefix=key)

        assert expired == [
            ((w["start_ms"], w["end_ms"]), w["value"]) for w in expected_expired_windows
        ]

        produce_calls = [
            call(
                key=k,
                value=v,
                headers={CHANGELOG_CF_MESSAGE_HEADER: "default"},
                topic=store_partition._changelog_producer._changelog_name,
                partition=store_partition._changelog_producer._partition_num,
            )
            for k, v in zip(
                expected_update_produce_keys, expected_update_produce_values
            )
        ]

        produce_calls.append(
            call(
                key=key + PREFIX_SEPARATOR + LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
                value=str(expected_expired_windows[-1]["start_ms"]).encode(),
                headers={CHANGELOG_CF_MESSAGE_HEADER: LATEST_EXPIRED_WINDOW_CF_NAME},
                topic=store_partition._changelog_producer._changelog_name,
                partition=store_partition._changelog_producer._partition_num,
            )
        )

        producer.produce.assert_has_calls(produce_calls)
        assert producer.produce.call_count == len(produce_calls)

        with store.start_partition_transaction(0) as tx:
            assert tx.get_window(start_ms=0, end_ms=10, prefix=key) is None
            assert tx.get_window(start_ms=10, end_ms=20, prefix=key) is None
            assert tx.get_window(start_ms=20, end_ms=30, prefix=key) == 3
