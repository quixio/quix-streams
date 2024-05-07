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
from quixstreams.utils.json import dumps
from tests.utils import ConfluentKafkaMessageStub


class TestWindowedRocksDBPartitionTransactionChangelog:
    @pytest.mark.parametrize("store_value", [10, None])
    def test_recover_window_from_changelog_message(
        self,
        windowed_rocksdb_store_factory_changelog,
        store_value,
    ):
        """
        Tests both a put (10) and delete (None)
        """
        store = windowed_rocksdb_store_factory_changelog()
        store_partition = store.assign_partition(0)

        kafka_key = b"my_key"
        window = dict(start_ms=0, end_ms=10, value=store_value, timestamp_ms=2)
        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key
            + PREFIX_SEPARATOR
            + encode_window_key(window["start_ms"], window["end_ms"]),
            value=dumps(store_value),
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
            offset=50,
        )

        store_partition.recover_from_changelog_message(
            changelog_msg, committed_offset=-1001
        )
        with store_partition.begin() as tx:
            assert (
                tx.get_window(window["start_ms"], window["end_ms"], prefix=kafka_key)
                == store_value
            )
        assert store_partition.get_changelog_offset() == changelog_msg.offset() + 1

    def test_recover_latest_expire_from_changelog_message(
        self,
        windowed_rocksdb_store_factory_changelog,
    ):
        """
        Tests both a put (10) and delete (None)
        """
        store = windowed_rocksdb_store_factory_changelog()
        store_partition = store.assign_partition(0)

        kafka_key = b"my_key"
        store_value = 10
        changelog_msg = ConfluentKafkaMessageStub(
            key=kafka_key + PREFIX_SEPARATOR + LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
            value=dumps(store_value),
            headers=[
                (CHANGELOG_CF_MESSAGE_HEADER, LATEST_EXPIRED_WINDOW_CF_NAME.encode())
            ],
            offset=50,
        )

        store_partition.recover_from_changelog_message(
            changelog_msg, committed_offset=-1001
        )

        with store_partition.begin() as tx:
            assert (
                tx.get(
                    LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
                    cf_name=LATEST_EXPIRED_WINDOW_CF_NAME,
                    prefix=kafka_key,
                )
                == store_value
            )
        assert store_partition.get_changelog_offset() == changelog_msg.offset() + 1
