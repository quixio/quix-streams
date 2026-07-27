"""
Bug #4 parity (Memory twin): the memory backend did not validate
``max_evictions_per_flush``.

Round-4's #4 added a ``max_evictions_per_flush <= 0`` reject to
``RocksDBOptions.__post_init__`` (see
``test_rocksdb/test_max_evictions_validation.py``), but the memory backend
accepted the value directly with no validation, so
``MemoryStorePartition(max_evictions_per_flush=0)`` silently disabled the
per-flush TTL sweep (``_run_sweep`` / ``sweep_expired_into_cache`` early-return
on ``budget <= 0``) and the tombstone reclamation that rides on it -- expired
records accumulate unbounded with no error.

The fix rejects a non-positive cap at construction on BOTH memory chokepoints:
``MemoryStorePartition.__init__`` (every partition funnels the value here) and
``MemoryStore.__init__`` (fail-fast parity with ``RocksDBOptions`` -- a store
constructed with an invalid cap that never creates a partition would otherwise
hold the bad value silently). Message parity with the rocksdb guard
("strictly positive").

RED before the fix: both constructors accepted 0 / -1 fine.
"""

import pytest

from quixstreams.state.memory import MemoryStore, MemoryStorePartition


class TestMemoryPartitionMaxEvictionsValidation:
    def test_zero_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            MemoryStorePartition(changelog_producer=None, max_evictions_per_flush=0)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            MemoryStorePartition(changelog_producer=None, max_evictions_per_flush=-1)

    def test_positive_accepted(self):
        partition = MemoryStorePartition(
            changelog_producer=None, max_evictions_per_flush=5
        )
        assert partition.max_evictions_per_flush == 5

    def test_default_is_positive(self):
        partition = MemoryStorePartition(changelog_producer=None)
        assert partition.max_evictions_per_flush == 10_000


class TestMemoryStoreMaxEvictionsValidation:
    def test_zero_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            MemoryStore(name="s", stream_id="t", max_evictions_per_flush=0)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            MemoryStore(name="s", stream_id="t", max_evictions_per_flush=-1)

    def test_positive_forwarded_to_partition(self):
        store = MemoryStore(name="s", stream_id="t", max_evictions_per_flush=5)
        assert store.create_new_partition(0).max_evictions_per_flush == 5

    def test_default_is_positive(self):
        store = MemoryStore(name="s", stream_id="t")
        assert store.create_new_partition(0).max_evictions_per_flush == 10_000
