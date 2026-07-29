"""
Memory-backend twin of the RocksDB tombstone stale-census-entry bug (see
``test_rocksdb/test_v3240_tombstone_census_leak.py`` for the full narrative).

``MemoryStorePartition.recover_from_changelog_message`` censuses a
header-absent default-CF record like this::

    if ttl_stamped:
        pending.pop(key, None)      # header-true supersedes
    elif value is not None:
        pending[key] = b""          # header-absent PUT -> census

There is no branch for a header-absent TOMBSTONE (``value is None``): the key
is removed from the in-RAM ``default`` dict, but its earlier pending-census
entry is never popped. ``_all_pending_values_are_stamped`` then point-gets a
now-missing key -> ``None`` -> short-circuits ``False``, defeating auto-adopt
for the whole partition.
"""

import struct
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000
PREFIX = b"pfx"


def _make_producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _raw_key(key_str, prefix=PREFIX):
    return prefix + b"|" + json_dumps(key_str)


def _dedup_value(epoch_ms, suffix):
    return struct.pack(">Q", epoch_ms) + suffix


def _replay(partition, ops, now_ms=NOW_MS):
    """Replay header-absent ``(raw_key, value)`` default-CF messages (writes
    AND tombstones, ``value=None``) through the normal recovery path."""
    partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value in ops:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=False,
        )
        offset += 1


def _read_bytes_via_tx(partition, key_str, prefix=PREFIX, timestamp=None):
    tx = partition.begin()
    return tx.get_bytes(
        key=key_str, prefix=prefix, cf_name="default", timestamp=timestamp
    )


class TestTombstoneLeavesStaleCensusEntryMemory:
    """Finding A (memory twin): a header-absent tombstone must un-census its
    key so a complete, live-key-only census can still auto-adopt.

    RED on HEAD: the stale ``k0`` census entry poisons
    ``_all_pending_values_are_stamped`` for the whole partition even though
    k1..k3 are a full, 100%-stamped, not-all-past census of every LIVE
    default-CF key.
    """

    def test_tombstone_uncensuses_key_and_full_census_still_adopts(self):
        producer = _make_producer()
        partition = MemoryStorePartition(changelog_producer=producer)

        k0_raw = _raw_key("k0")
        k1_raw = _raw_key("k1")
        k2_raw = _raw_key("k2")
        k3_raw = _raw_key("k3")

        # k1..k3 are all FUTURE-dated relative to the recovery clock, so
        # adopting their v3.24.0 stamps does not also expire them out from
        # under the read-back assertions below (a past-dated stamp
        # correctly reads back as expired once the store is in TTL mode --
        # that is desired behavior, not this test's point).
        v1 = _dedup_value(NOW_MS + 30 * DAY_MS, b"-c1")
        v2 = _dedup_value(NOW_MS + 20 * DAY_MS, b"-c2")
        v3 = _dedup_value(NOW_MS + 10 * DAY_MS, b"-c3")

        ops = [
            (k0_raw, _dedup_value(NOW_MS - 5 * DAY_MS, b"-c0")),
            (k1_raw, v1),
            (k2_raw, v2),
            (k3_raw, v3),
            (k0_raw, None),  # legacy tombstone -- must un-census k0
        ]
        _replay(partition, ops, now_ms=NOW_MS)

        partition.complete_recovery()

        assert partition.uses_ttl_stamps is True, (
            "a tombstoned key must not leave a stale census entry that "
            "poisons the quorum check for the whole in-memory partition -- "
            "k1..k3 are a complete, 100%-stamped, not-all-past census of "
            "every LIVE default-CF key and should auto-adopt"
        )

        for key_str, raw_value in (("k1", v1), ("k2", v2), ("k3", v3)):
            expected_payload = raw_value[8:]
            assert (
                _read_bytes_via_tx(partition, key_str, timestamp=NOW_MS)
                == expected_payload
            )

        assert k0_raw not in partition._state.get("default", {})

        pending = partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        assert k0_raw not in pending, (
            "a tombstoned key must be un-censused, not left as a stale "
            "__ttl_backfill_pending__ entry"
        )

        partition.close()
