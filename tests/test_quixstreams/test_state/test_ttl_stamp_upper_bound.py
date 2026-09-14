"""
Regression tests for review re-review finding #3 (Fix 3): symmetric
plausible-stamp upper bound.

Before the fix, an expiry ``>= _MAX_PLAUSIBLE_STAMP_MS (10**15)`` (e.g.
``ttl=timedelta.max``, or a nanosecond-scale timestamp fed to a stamped write)
encoded fine (``< 2**64-1``) but was refused by the strict read validator on
every read -> a permanent per-key ``StateSerializationError`` plus a
sweep-stranded index entry. The fix rejects such an expiry symmetrically at
write time with a ``ValueError`` on both backends, and rejects a too-large
``legacy_records_ttl`` at ``RocksDBOptions`` construction.
"""

from datetime import timedelta

import pytest

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.transaction import _MAX_PLAUSIBLE_STAMP_MS, _ttl_to_ms

BASE_TS = 1_752_000_000_000  # ~mid-2025, epoch ms
PREFIX = b"pfx"


@pytest.fixture()
def rocksdb_partition(tmp_path):
    p = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=None,
    )
    yield p
    p.close()


def _flip_rocksdb(partition):
    with partition.begin() as tx:
        tx.set(
            key="seed",
            value="seed",
            prefix=b"seed",
            timestamp=BASE_TS,
            ttl=timedelta(days=1),
        )
    assert partition.uses_ttl_stamps is True


def _flip_memory(partition):
    with partition.begin() as tx:
        tx.set(
            key="seed",
            value="seed",
            prefix=b"seed",
            timestamp=BASE_TS,
            ttl=timedelta(days=1),
        )
    assert partition.uses_ttl_stamps is True


# -------------------------------------------------------------------
# The value that motivates the bound: encodable but implausible.
# -------------------------------------------------------------------
def test_timedelta_max_is_encodable_but_exceeds_plausible_bound():
    ms = _ttl_to_ms(timedelta.max)
    stamp = BASE_TS + ms
    assert stamp >= _MAX_PLAUSIBLE_STAMP_MS  # would be read-refused
    assert stamp < 2**64 - 1  # encodable, non-sentinel -> the trap


# -------------------------------------------------------------------
# RocksDB: reject at write, both the flipped and the unflipped path.
# -------------------------------------------------------------------
def test_rocksdb_timedelta_max_rejected_at_write_flipped(rocksdb_partition):
    _flip_rocksdb(rocksdb_partition)
    tx = rocksdb_partition.begin()
    with pytest.raises(ValueError, match="implausible expiry"):
        tx.set(key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta.max)


def test_rocksdb_timedelta_max_rejected_at_write_unflipped(rocksdb_partition):
    # First ttl= write on a fresh (unflipped) partition also validates the stamp
    # before staging, so it rejects too (and nothing is stranded / stored).
    tx = rocksdb_partition.begin()
    with pytest.raises(ValueError, match="implausible expiry"):
        tx.set(key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta.max)


def test_rocksdb_ns_timestamp_mistake_rejected_at_write(rocksdb_partition):
    _flip_rocksdb(rocksdb_partition)
    ns_ts = BASE_TS * 1_000_000  # ns instead of ms: ~1.75e18
    tx = rocksdb_partition.begin()
    with pytest.raises(ValueError, match="implausible expiry"):
        tx.set(
            key="k2", value="v2", prefix=PREFIX, timestamp=ns_ts, ttl=timedelta(days=1)
        )


def test_rocksdb_reasonable_large_ttl_still_accepted(rocksdb_partition):
    # A 1000-year ttl is still well below the bound and must NOT be rejected.
    _flip_rocksdb(rocksdb_partition)
    with rocksdb_partition.begin() as tx:
        tx.set(
            key="k",
            value="v",
            prefix=PREFIX,
            timestamp=BASE_TS,
            ttl=timedelta(days=365_000),
        )
    tx = rocksdb_partition.begin()
    assert tx.get(key="k", prefix=PREFIX, timestamp=BASE_TS + 1) == "v"


# -------------------------------------------------------------------
# Memory backend: same write-time reject.
# -------------------------------------------------------------------
def test_memory_timedelta_max_rejected_at_write():
    partition = MemoryStorePartition(changelog_producer=None)
    _flip_memory(partition)
    tx = partition.begin()
    with pytest.raises(ValueError, match="implausible expiry"):
        tx.set(key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta.max)


def test_memory_ns_timestamp_mistake_rejected_at_write():
    partition = MemoryStorePartition(changelog_producer=None)
    _flip_memory(partition)
    ns_ts = BASE_TS * 1_000_000
    tx = partition.begin()
    with pytest.raises(ValueError, match="implausible expiry"):
        tx.set(
            key="k2", value="v2", prefix=PREFIX, timestamp=ns_ts, ttl=timedelta(days=1)
        )


# -------------------------------------------------------------------
# Options: reject a too-large legacy_records_ttl at construction.
# -------------------------------------------------------------------
def test_options_reject_timedelta_max_legacy_ttl():
    with pytest.raises(ValueError, match="implausibly large"):
        RocksDBOptions(legacy_records_ttl=timedelta.max)


def test_options_accept_reasonable_legacy_ttl():
    # A 1000-year legacy_records_ttl is below the bound and must be accepted.
    opts = RocksDBOptions(legacy_records_ttl=timedelta(days=365_000))
    assert opts.legacy_records_ttl == timedelta(days=365_000)
