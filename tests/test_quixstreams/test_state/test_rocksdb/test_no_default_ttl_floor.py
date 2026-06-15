"""
Unit tests for the per-write-only TTL contract (shortcut 73191).

Rule 3 — the "no never-expires data once active" floor that forced a no-``ttl=``
write to ``event_time + legacy_records_ttl`` on a flipped partition — was REMOVED
by design. TTL is strictly per-write: only ``state.set(..., ttl=...)`` ever sets
an expiry. ``legacy_records_ttl`` is ONLY a one-time migration knob for
pre-existing legacy records (applied via backfill); it must NOT impose a
store-wide default on steady-state writes.

These tests pin that contract:
- a no-``ttl=`` write on a flipped partition with ``legacy_records_ttl`` set is
  ``SENTINEL_NEVER`` (never-expires), NOT floored;
- backfill still stamps pre-existing legacy records, and a subsequent no-``ttl=``
  write is never-expires.

See ``dev-planning/state-ttl-legacy-backfill/spec.md`` §0 (Rule 3 REMOVED).
"""

from datetime import timedelta

from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
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


def _seed_legacy_records(partition, records, prefix=b"pfx"):
    with partition.begin() as tx:
        for key, value in records:
            tx.set(key=key, value=value, prefix=prefix, timestamp=1_000)


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


class TestNoDefaultTTLFloor:
    # A no-ttl= write on a flipped, legacy_records_ttl-set partition is
    # never-expires — NOT floored (Rule 3 removed).
    def test_no_ttl_write_is_never_expires_when_active(self, store_partition_factory):
        legacy_ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        _flip_partition(partition, ttl=timedelta(days=1), timestamp=1_000_000_000_000)

        event_time = 1_000_000_500_000
        with partition.begin() as tx:
            # No ttl= — must be SENTINEL_NEVER, not event_time + legacy_records_ttl.
            tx.set(key="kperm", value="vperm", prefix=b"pfx", timestamp=event_time)

        decoded = _decode_default_cf(partition)
        perm_key = next(k for k in decoded if b"kperm" in k)
        expires_at, payload = decoded[perm_key]
        assert expires_at == SENTINEL_NEVER
        assert payload == b'"vperm"'

        # Sentinel entries are never indexed.
        index = _decode_index_cf(partition)
        assert perm_key not in index
        partition.close()

    # _compute_stamp(ttl=None) is SENTINEL_NEVER regardless of flip/config state.
    def test_compute_stamp_no_ttl_is_sentinel(self, store_partition_factory):
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        )
        _flip_partition(partition, ttl=timedelta(days=1), timestamp=1_000)
        with partition.begin() as tx:
            assert tx._compute_stamp(ttl=None, timestamp=2_000) == SENTINEL_NEVER
            assert tx._compute_stamp(ttl=None, timestamp=None) == SENTINEL_NEVER
        partition.close()

    # Backfill still stamps pre-existing legacy records; a SUBSEQUENT no-ttl=
    # write is never-expires (migration knob does not leak into steady state).
    def test_backfill_stamps_legacy_but_new_write_is_never_expires(
        self, store_partition_factory
    ):
        legacy_ttl = timedelta(days=7)
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("kold1", "vold1"), ("kold2", "vold2")])
        partition.close()

        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        flip_ts = 1_000_000_000_000
        # ttl= write triggers the flip + one-time backfill of the pre-existing
        # legacy records.
        with partition.begin() as tx:
            tx.set(
                key="kflip",
                value="vflip",
                prefix=b"pfx",
                timestamp=flip_ts,
                ttl=timedelta(days=1),
            )
        assert partition.uses_ttl_stamps is True

        decoded = _decode_default_cf(partition)
        legacy_expiry = flip_ts + 7 * DAY_MS
        # Pre-existing legacy records were backfilled to the uniform expiry.
        for key in (k for k in decoded if b"kold" in k):
            assert decoded[key][0] == legacy_expiry

        # A new no-ttl= write AFTER the flip is never-expires, not floored.
        new_ts = flip_ts + 5_000
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=new_ts)

        decoded = _decode_default_cf(partition)
        new_key = next(k for k in decoded if b"knew" in k)
        assert decoded[new_key][0] == SENTINEL_NEVER
        index = _decode_index_cf(partition)
        assert new_key not in index
        partition.close()
