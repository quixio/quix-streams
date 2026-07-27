"""
Bug #4 (review batch 4): RocksDBOptions did not validate max_evictions_per_flush.

``RocksDBOptions.__post_init__`` validated ``legacy_records_ttl`` and
``legacy_backfill_chunk_size`` but not ``max_evictions_per_flush``. A 0 or negative
cap silently disables the per-flush TTL sweep AND the tombstone reclamation that
rides on it, so expired records accumulate unbounded with no error. The fix rejects
a non-positive cap at construction, mirroring the other bound checks.

RED before the fix: ``RocksDBOptions(max_evictions_per_flush=0)`` constructed fine.
"""

import pytest

from quixstreams.state.rocksdb import RocksDBOptions


class TestMaxEvictionsPerFlushValidation:
    def test_zero_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            RocksDBOptions(max_evictions_per_flush=0)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            RocksDBOptions(max_evictions_per_flush=-1)

    def test_positive_accepted(self):
        assert RocksDBOptions(max_evictions_per_flush=5).max_evictions_per_flush == 5

    def test_default_is_positive(self):
        assert RocksDBOptions().max_evictions_per_flush == 10_000
