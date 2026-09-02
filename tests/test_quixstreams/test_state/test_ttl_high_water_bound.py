"""
Regression tests for finding H1 (batch4 re-review): unbounded event-time
high-water poisons a whole TTL partition.

``RocksDBStorePartition.advance_high_water`` / ``MemoryStorePartition.
advance_high_water`` bound a NEGATIVE timestamp (review batch 3, #12) but have
NO upper bound. A single mis-scaled event-time timestamp (e.g. nanoseconds or
microseconds fed in where milliseconds are expected, or seconds vs ms) on ANY
TTL-aware read or write -- even for an unrelated key, even with no ``ttl=`` at
all -- can set the partition's high-water clock to an absurd value. Every
OTHER finite-stamped record in the partition then reads back as
``Marker.UNDEFINED`` (the read filter: ``stamp <= high_water_ms``) and gets
physically swept / tombstoned on the very next flush, because ``now_ms =
self._high_water_ms`` in the sweep has no upper bound either.

See ``quixstreams/state/rocksdb/partition.py::advance_high_water`` (~line 452)
and ``quixstreams/state/memory/partition.py::advance_high_water`` (~line 277).
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition

BASE_TS = 1_752_000_000_000  # ~mid-2025, epoch ms
PREFIX = b"pfx"
DAY_MS = 86_400_000
MIS_SCALED_TS = 10**18  # e.g. a nanosecond-scale timestamp fed in as-is


def _read(partition, key, timestamp):
    tx = partition.begin()
    return tx.get(key=key, prefix=PREFIX, cf_name="default", timestamp=timestamp)


# ---------------------------------------------------------------------------
# RocksDB
# ---------------------------------------------------------------------------
def test_rocksdb_misscaled_timestamp_does_not_poison_other_keys(tmp_path):
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=None,
    )
    # A genuine, still-valid finite-ttl record (1 day ttl, read shortly after).
    with part.begin() as tx:
        tx.set(
            key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta(days=1)
        )
    assert part.uses_ttl_stamps is True
    # Sanity: readable well before its real expiry.
    assert _read(part, "k", timestamp=BASE_TS + 2000) == "v"

    # An UNRELATED write carries a mis-scaled (e.g. ns instead of ms) timestamp
    # and NO ttl=. This must not be able to poison the partition's high-water.
    with part.begin() as tx2:
        tx2.set(key="mis", value="junk", prefix=PREFIX, timestamp=MIS_SCALED_TS)

    # Desired: "k" is still readable well before its real expiry.
    assert _read(part, "k", timestamp=BASE_TS + 3000) == "v", (
        "H1: a mis-scaled timestamp on an unrelated key must not hide/evict "
        "an unrelated still-valid finite-ttl record"
    )

    # Desired: the persisted high-water stays plausible, never an absurd value.
    hw = part.high_water_ms
    assert (
        hw is not None and hw < 10**15
    ), f"H1: high-water must stay bounded to a plausible epoch-ms value, got {hw}"
    part.close()


def test_rocksdb_misscaled_timestamp_does_not_sweep_valid_record(tmp_path):
    """Same as above, but asserts the record is not PHYSICALLY evicted from the
    default CF / TTL index (not just hidden by the read filter)."""
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=None,
    )
    with part.begin() as tx:
        tx.set(
            key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta(days=1)
        )

    with part.begin() as tx2:
        tx2.set(key="mis", value="junk", prefix=PREFIX, timestamp=MIS_SCALED_TS)

    default_cf = part.get_or_create_column_family("default")
    remaining_keys = {k for k in default_cf.keys() if PREFIX in k and b"mis" not in k}
    assert remaining_keys, (
        "H1: the finite-ttl record must not be physically evicted from the "
        "default CF as a side effect of an unrelated mis-scaled timestamp"
    )
    part.close()


def test_rocksdb_misscaled_get_timestamp_does_not_poison(tmp_path):
    """The read path (``_get_bytes``) advances high-water too; a plain ``get``
    with a mis-scaled timestamp must not poison the clock either."""
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=None,
    )
    with part.begin() as tx:
        tx.set(
            key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta(days=1)
        )

    tx2 = part.begin()
    tx2.get(key="nonexistent", prefix=PREFIX, timestamp=MIS_SCALED_TS)

    hw = part.high_water_ms
    assert hw is not None and hw < 10**15, (
        f"H1: a mis-scaled read timestamp must not establish an absurd "
        f"high-water, got {hw}"
    )
    part.close()


# ---------------------------------------------------------------------------
# Memory (same unbounded ``advance_high_water`` + sweep pattern)
# ---------------------------------------------------------------------------
def test_memory_misscaled_timestamp_does_not_poison_other_keys():
    part = MemoryStorePartition(changelog_producer=None)
    with part.begin() as tx:
        tx.set(
            key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta(days=1)
        )
    assert part.uses_ttl_stamps is True
    assert _read(part, "k", timestamp=BASE_TS + 2000) == "v"

    with part.begin() as tx2:
        tx2.set(key="mis", value="junk", prefix=PREFIX, timestamp=MIS_SCALED_TS)

    assert _read(part, "k", timestamp=BASE_TS + 3000) == "v", (
        "H1 (memory): a mis-scaled timestamp on an unrelated key must not "
        "hide/evict an unrelated still-valid finite-ttl record"
    )
    hw = part.high_water_ms
    assert (
        hw is not None and hw < 10**15
    ), f"H1 (memory): high-water must stay bounded, got {hw}"


def test_memory_misscaled_timestamp_does_not_sweep_valid_record():
    part = MemoryStorePartition(changelog_producer=None)
    with part.begin() as tx:
        tx.set(
            key="k", value="v", prefix=PREFIX, timestamp=BASE_TS, ttl=timedelta(days=1)
        )

    with part.begin() as tx2:
        tx2.set(key="mis", value="junk", prefix=PREFIX, timestamp=MIS_SCALED_TS)

    main = part._state.get("default", {})
    remaining_keys = {k for k in main if PREFIX in k and b"mis" not in k}
    assert remaining_keys, (
        "H1 (memory): the finite-ttl record must not be physically evicted "
        "as a side effect of an unrelated mis-scaled timestamp"
    )
