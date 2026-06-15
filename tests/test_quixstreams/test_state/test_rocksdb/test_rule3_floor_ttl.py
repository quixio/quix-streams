"""
Unit tests for Rule 3 — "no never-expires data once active" (shortcut 73191).

Spec §0 Rule 3 + §6.7 + §11 case 10/11.

Once a partition is flipped into TTL mode AND ``legacy_records_ttl`` is set, a
``state.set(k, v)`` with no explicit ``ttl=`` is floored to
``event_time + legacy_records_ttl`` instead of ``SENTINEL_NEVER`` — so nothing
ever-expires while the feature is active in that partition.

Gating (Rule 1 activation gate):
- a NON-flipped partition (no ``ttl=`` ever) with ``legacy_records_ttl`` set
  writes raw / un-stamped — no floor;
- a flipped partition with ``legacy_records_ttl`` UNSET keeps no-``ttl=`` writes
  at ``SENTINEL_NEVER``;
- windowed / timestamped partitions never floor (class-level opt-out).

See ``dev-planning/state-ttl-legacy-backfill/spec.md``.
"""

from datetime import timedelta

from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.timestamped import TimestampedStorePartition
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
)

DAY_MS = 86_400_000


def _decode_default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: decode_ttl_value(value) for key, value in cf.items()}


def _decode_index_cf(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _flip_partition(partition, ttl, timestamp):
    """Flip an empty partition into TTL mode via a real ``ttl=`` write."""
    with partition.begin() as tx:
        tx.set(
            key="kflip",
            value="vflip",
            prefix=b"pfx",
            timestamp=timestamp,
            ttl=ttl,
        )
    assert partition.uses_ttl_stamps is True


class TestRule3FloorTTL:
    # Case 10 (positive) — floored to event_time + legacy_records_ttl.
    def test_no_ttl_write_is_floored_when_active(self, store_partition_factory):
        legacy_ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        _flip_partition(partition, ttl=timedelta(days=1), timestamp=1_000_000_000_000)

        event_time = 1_000_000_500_000
        with partition.begin() as tx:
            # No ttl= — under Rule 3 this must be floored, not SENTINEL_NEVER.
            tx.set(key="kfloor", value="vfloor", prefix=b"pfx", timestamp=event_time)

        decoded = _decode_default_cf(partition)
        floor_key = next(k for k in decoded if b"kfloor" in k)
        expires_at, payload = decoded[floor_key]
        assert expires_at == event_time + 7 * DAY_MS
        assert expires_at != SENTINEL_NEVER
        assert payload == b'"vfloor"'

        # Indexed at the floored expiry.
        index = _decode_index_cf(partition)
        assert index[floor_key] == event_time + 7 * DAY_MS
        partition.close()

    # Case 10 — floored key expires via the normal read filter / sweep.
    def test_floored_key_expires_via_read_filter(self, store_partition_factory):
        legacy_ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        _flip_partition(partition, ttl=timedelta(days=1), timestamp=1_000)

        event_time = 2_000
        with partition.begin() as tx:
            tx.set(key="kfloor", value="vfloor", prefix=b"pfx", timestamp=event_time)

        floor_expiry = event_time + 7 * DAY_MS
        # Read before expiry (high-water below the floor) -> visible.
        with partition.begin() as tx:
            assert (
                tx.get(key="kfloor", prefix=b"pfx", timestamp=event_time + 1)
                == "vfloor"
            )
        # Read at/after expiry (high-water advanced past the floor) -> filtered.
        with partition.begin() as tx:
            assert (
                tx.get(key="kfloor", prefix=b"pfx", timestamp=floor_expiry + 1)
                is None
            )
        partition.close()

    # Case 10 (negative) — flipped store, legacy_records_ttl UNSET -> sentinel.
    def test_no_floor_when_legacy_ttl_unset(self, store_partition_factory):
        # Flip via a ttl= write but DO NOT configure legacy_records_ttl.
        partition = store_partition_factory(name="db", options=RocksDBOptions())
        _flip_partition(partition, ttl=timedelta(days=1), timestamp=1_000)

        with partition.begin() as tx:
            tx.set(key="kperm", value="vperm", prefix=b"pfx", timestamp=2_000)

        decoded = _decode_default_cf(partition)
        perm_key = next(k for k in decoded if b"kperm" in k)
        expires_at, _ = decoded[perm_key]
        assert expires_at == SENTINEL_NEVER

        # Sentinel entries are not indexed.
        index = _decode_index_cf(partition)
        assert perm_key not in index
        partition.close()

    # Case 11 (Rule 1 gating) — NON-flipped store, legacy_records_ttl set, only
    # no-ttl= writes -> never flips, values un-stamped (byte-identical legacy).
    def test_non_flipped_partition_does_not_floor(self, store_partition_factory):
        legacy_ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        # Only plain writes, never a ttl= write -> the partition must stay legacy.
        with partition.begin() as tx:
            tx.set(key="k1", value="v1", prefix=b"pfx", timestamp=1_000)
            tx.set(key="k2", value="v2", prefix=b"pfx", timestamp=2_000)

        assert partition.uses_ttl_stamps is False
        # No TTL index CF should have been created (Rule 1: config inert).
        assert TTL_INDEX_CF_NAME not in partition.list_column_families()

        # Values are stored raw (un-stamped legacy layout). A raw JSON value
        # is not a stamped blob: decoding would either fail or not equal the
        # floored stamp. Assert the stored bytes are the plain serialized value.
        cf = partition.get_or_create_column_family("default")
        raw = {k: v for k, v in cf.items()}
        assert any(v == b'"v1"' for v in raw.values())
        partition.close()

    # Timestamped partition never floors (class-level opt-out).
    def test_timestamped_partition_never_floors(self, tmp_path):
        path = (tmp_path / "ts").as_posix()
        partition = TimestampedStorePartition(
            path,
            grace_ms=0,
            keep_duplicates=False,
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
        )
        assert type(partition).uses_ttl_stamps is False
        with partition.begin() as tx:
            tx.set_for_timestamp(timestamp=1_000, value="v1", prefix=b"pfx")
        assert partition.uses_ttl_stamps is False
        assert TTL_INDEX_CF_NAME not in partition.list_column_families()
        partition.close()

    # Timestamp-None fallback returns SENTINEL_NEVER (cannot floor without an
    # event-time). Exercised directly against the transaction's _compute_stamp,
    # which is the unit under test for the fallback branch.
    def test_timestamp_none_floor_fallback_is_sentinel(self, store_partition_factory):
        legacy_ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        _flip_partition(partition, ttl=timedelta(days=1), timestamp=1_000)
        with partition.begin() as tx:
            stamp = tx._compute_stamp(ttl=None, timestamp=None)
        assert stamp == SENTINEL_NEVER
        partition.close()
