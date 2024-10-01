import pytest

from quixstreams.state.exceptions import PartitionNotAssignedError
from quixstreams.state.manager import SUPPORTED_STORES


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestStore:
    def test_open_close(self, store_factory):
        with store_factory():
            pass

    def test_assign_revoke_partition(self, store):
        # Assign a partition to the store
        store.assign_partition(0)
        assert store.partitions[0]
        # Revoke partition
        store.revoke_partition(0)
        assert 0 not in store.partitions
        # Assign partition again
        store.assign_partition(0)

    def test_assign_partition_twice(self, store):
        store.assign_partition(0)
        store.assign_partition(0)

    def test_revoke_partition_not_assigned(self, store):
        store.revoke_partition(0)

    def test_create_transaction(self, store):
        prefix = b"__key__"
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            tx.set("key", "value", prefix=prefix)

        with store.start_partition_transaction(0) as tx:
            assert tx.get("key", prefix=prefix) == "value"
        assert store._changelog_producer_factory is None

    def test_get_transaction_partition_not_assigned(self, store):
        with pytest.raises(PartitionNotAssignedError):
            store.start_partition_transaction(0)

        store.assign_partition(0)
        store.revoke_partition(0)
        with pytest.raises(PartitionNotAssignedError):
            store.start_partition_transaction(0)
