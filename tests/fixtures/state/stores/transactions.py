import pytest

from quixstreams.state.base.transaction import PartitionTransactionCache


@pytest.fixture()
def cache() -> PartitionTransactionCache:
    return PartitionTransactionCache()
