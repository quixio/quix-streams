import secrets
import pytest

from quixstreams.state import StoreTypes
from quixstreams.state.rocksdb import RocksDBOptions


@pytest.mark.parametrize("store_type", [StoreTypes.ROCKSDB], indirect=True)
class TestRocksDBPartitionTransaction:
    def test_set_get_with_column_family(self, store_partition):
        key = "key"
        value = "value"
        prefix = b"__key__"
        store_partition.create_column_family("cf")

        with store_partition.begin() as tx:
            tx.set(key, value, cf_name="cf", prefix=prefix)
            assert tx.get(key, cf_name="cf", prefix=prefix) == value

        with store_partition.begin() as tx:
            assert tx.get(key, cf_name="cf", prefix=prefix) == value

    def test_set_delete_get_with_column_family(self, store_partition):
        key = "key"
        value = "value"
        prefix = b"__key__"
        store_partition.create_column_family("cf")

        with store_partition.begin() as tx:
            tx.set(key, value, cf_name="cf", prefix=prefix)
            assert tx.get(key, cf_name="cf", prefix=prefix) == value
            tx.delete(key, cf_name="cf", prefix=prefix)
            assert tx.get(key, cf_name="cf", prefix=prefix) is None

        with store_partition.begin() as tx:
            assert tx.get(key, cf_name="cf", prefix=prefix) is None

    def test_set_exists_get_with_column_family(self, store_partition):
        key = "key"
        value = "value"
        store_partition.create_column_family("cf")
        prefix = b"__key__"

        with store_partition.begin() as tx:
            assert not tx.exists(key, cf_name="cf", prefix=prefix)
            tx.set(key, value, cf_name="cf", prefix=prefix)
            assert tx.exists(key, cf_name="cf", prefix=prefix)

        with store_partition.begin() as tx:
            assert tx.exists(key, cf_name="cf", prefix=prefix)

    def test_custom_dumps_loads(self, store_partition_factory):
        key = secrets.token_bytes(10)
        value = secrets.token_bytes(10)
        prefix = b"__key__"

        with store_partition_factory(
            options=RocksDBOptions(loads=lambda v: v, dumps=lambda v: v)
        ) as db:
            with db.begin() as tx:
                tx.set(key, value, prefix=prefix)

            with db.begin() as tx:
                assert tx.get(key, prefix=prefix) == value
