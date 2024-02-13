import pytest

from quixstreams.state.exceptions import PartitionNotAssignedError


class TestRocksDBStore:
    def test_open_close(self, rocksdb_store_factory):
        with rocksdb_store_factory():
            pass

    def test_assign_revoke_partition(self, rocksdb_store):
        # Assign a partition to the store
        rocksdb_store.assign_partition(0)
        assert rocksdb_store.partitions[0]
        # Revoke partition
        rocksdb_store.revoke_partition(0)
        assert 0 not in rocksdb_store.partitions
        # Assign partition again
        rocksdb_store.assign_partition(0)

    def test_assign_partition_twice(self, rocksdb_store):
        rocksdb_store.assign_partition(0)
        rocksdb_store.assign_partition(0)

    def test_revoke_partition_not_assigned(self, rocksdb_store):
        rocksdb_store.revoke_partition(0)

    def test_create_transaction(self, rocksdb_store):
        rocksdb_store.assign_partition(0)
        with rocksdb_store.start_partition_transaction(0) as tx:
            tx.set("key", "value")
        rocksdb_store.revoke_partition(0)

        # Assign partition again and check the value
        rocksdb_store.assign_partition(0)
        with rocksdb_store.start_partition_transaction(0) as tx:
            assert tx.get("key") == "value"
        assert rocksdb_store._changelog_producer_factory is None

    def test_get_transaction_partition_not_assigned(self, rocksdb_store):
        with pytest.raises(PartitionNotAssignedError):
            rocksdb_store.start_partition_transaction(0)

        rocksdb_store.assign_partition(0)
        rocksdb_store.revoke_partition(0)
        with pytest.raises(PartitionNotAssignedError):
            rocksdb_store.start_partition_transaction(0)
