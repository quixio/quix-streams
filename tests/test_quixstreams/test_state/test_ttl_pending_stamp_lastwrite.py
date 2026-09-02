"""
Regression tests for review re-review finding #2 (Fix 2): pending-stamp
last-write-wins on the unflipped path.

On an UNFLIPPED partition, ``set(k, v1, ttl=1h)`` records a pending stamp; a
subsequent plain ``set(k, v2)`` (ttl=None) — or ``set(ttl)``→``delete``→
``set(no-ttl)`` — of the SAME key in the SAME transaction must clear that
pending stamp. Otherwise the flip stamps the final never-expires value with the
stale finite expiry and the next sweep deletes a record whose last write asked
for "never expires".

Covers both backends, plus the already-flipped inline control (which was always
correct — this guards it stays correct).
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import SENTINEL_NEVER, decode_ttl_value

TS = 1_000_000_000_000  # event-time base, ms
HOUR_MS = 3_600_000
TTL = timedelta(hours=1)
PREFIX = b"pfx"


def _rocksdb_partition(tmp_path, name="db"):
    path = (tmp_path / name).as_posix()
    opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(path, options=opts, changelog_producer=None)


def _read(partition, key, timestamp):
    tx = partition.begin()
    return tx.get(key=key, prefix=PREFIX, cf_name="default", timestamp=timestamp)


def _decode_rocksdb(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: decode_ttl_value(value) for key, value in cf.items()}


def _decode_memory(partition):
    return {
        key: decode_ttl_value(value)
        for key, value in partition._state.get("default", {}).items()
    }


def _single_user_entry(stamps):
    matching = {k: v for k, v in stamps.items() if k.startswith(PREFIX)}
    assert len(matching) == 1, f"expected 1 user key, got {stamps}"
    return next(iter(matching.items()))


# -------------------------------------------------------------------
# 1. RocksDB: ttl set then plain set, same key, same (flip) transaction
# -------------------------------------------------------------------
def test_rocksdb_flip_ttl_then_plain_set_same_key(tmp_path):
    partition = _rocksdb_partition(tmp_path)
    with partition.begin() as tx:
        tx.set(key="k", value="v1", prefix=PREFIX, timestamp=TS, ttl=TTL)
        tx.set(key="k", value="v2", prefix=PREFIX, timestamp=TS)  # no ttl

    _raw_key, (stamp, _payload) = _single_user_entry(_decode_rocksdb(partition))
    # Last write had NO ttl -> stamp must be the never-expires sentinel.
    assert stamp == SENTINEL_NEVER
    # Value must still be readable past the (cleared) stale expiry.
    assert _read(partition, "k", timestamp=TS + HOUR_MS + 1) == "v2"
    partition.close()


# -------------------------------------------------------------------
# 2. RocksDB: ttl set, delete, plain set (delete must clear the stamp)
# -------------------------------------------------------------------
def test_rocksdb_flip_ttl_set_delete_set(tmp_path):
    partition = _rocksdb_partition(tmp_path)
    with partition.begin() as tx:
        tx.set(key="k", value="v1", prefix=PREFIX, timestamp=TS, ttl=TTL)
        tx.delete(key="k", prefix=PREFIX)
        tx.set(key="k", value="v2", prefix=PREFIX, timestamp=TS)  # no ttl

    _raw_key, (stamp, _payload) = _single_user_entry(_decode_rocksdb(partition))
    assert stamp == SENTINEL_NEVER
    assert _read(partition, "k", timestamp=TS + HOUR_MS + 1) == "v2"
    partition.close()


# -------------------------------------------------------------------
# 3. CONTROL — already-flipped partition, same sequence stays correct
# -------------------------------------------------------------------
def test_rocksdb_flipped_inline_ttl_then_plain_set_is_correct(tmp_path):
    partition = _rocksdb_partition(tmp_path)
    # Flip the partition first with an unrelated key.
    with partition.begin() as tx:
        tx.set(key="flipper", value="x", prefix=PREFIX, timestamp=TS, ttl=TTL)
    assert partition.uses_ttl_stamps, "partition should be flipped now"

    # Same-key sequence in a NEW transaction on the flipped store.
    with partition.begin() as tx:
        tx.set(key="k", value="v1", prefix=PREFIX, timestamp=TS, ttl=TTL)
        tx.set(key="k", value="v2", prefix=PREFIX, timestamp=TS)  # no ttl

    stamps = _decode_rocksdb(partition)
    k_entries = {
        k: v for k, v in stamps.items() if k.startswith(PREFIX) and b"flipper" not in k
    }
    assert len(k_entries) == 1, f"expected 1 'k' key, got {stamps}"
    _raw_key, (stamp, _payload) = next(iter(k_entries.items()))
    assert stamp == SENTINEL_NEVER
    assert _read(partition, "k", timestamp=TS + HOUR_MS + 1) == "v2"
    partition.close()


# -------------------------------------------------------------------
# 4. Memory backend: same flip-day sequence
# -------------------------------------------------------------------
def test_memory_flip_ttl_then_plain_set_same_key():
    partition = MemoryStorePartition(changelog_producer=None)
    with partition.begin() as tx:
        tx.set(key="k", value="v1", prefix=PREFIX, timestamp=TS, ttl=TTL)
        tx.set(key="k", value="v2", prefix=PREFIX, timestamp=TS)  # no ttl

    _raw_key, (stamp, _payload) = _single_user_entry(_decode_memory(partition))
    assert stamp == SENTINEL_NEVER
    assert _read(partition, "k", timestamp=TS + HOUR_MS + 1) == "v2"


def test_memory_flip_ttl_set_delete_set():
    partition = MemoryStorePartition(changelog_producer=None)
    with partition.begin() as tx:
        tx.set(key="k", value="v1", prefix=PREFIX, timestamp=TS, ttl=TTL)
        tx.delete(key="k", prefix=PREFIX)
        tx.set(key="k", value="v2", prefix=PREFIX, timestamp=TS)  # no ttl

    _raw_key, (stamp, _payload) = _single_user_entry(_decode_memory(partition))
    assert stamp == SENTINEL_NEVER
    assert _read(partition, "k", timestamp=TS + HOUR_MS + 1) == "v2"
