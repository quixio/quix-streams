"""
Red-first test for PR #1134 round-2 review Finding A: a header-absent
TOMBSTONE (legacy delete) replayed during a MIXED-changelog recovery leaves a
STALE ``__ttl_backfill_pending__`` census entry behind, defeating the
automatic v3.24.0-stamp cold adopt (spec ``dev-planning/state-ttl-v3240-auto-
adopt/spec.md`` §5.2/§5.7) for an otherwise fully-censused, genuine v3.24.0
store.

``RocksDBStorePartition.recover_from_changelog_message`` censuses a
header-absent default-CF record like this::

    if ttl_stamped:
        batch.delete(key, pending_handle)      # header-true supersedes
    elif value is not None:
        batch.put(key, b"", pending_handle)     # header-absent PUT -> census

There is NO branch for a header-absent TOMBSTONE (``value is None``): the key
is correctly deleted from the ``default`` CF (the tombstone lands verbatim
below), but the earlier PUT into the pending census is never undone.

On ``complete_recovery`` Branch B (cold auto-adopt),
``_all_pending_values_are_stamped`` point-gets every censused key's CURRENT
default-CF value and short-circuits ``False`` on the first ``None`` (see
``rocksdb/partition.py::_all_pending_values_are_stamped``). The stale,
now-deleted key therefore poisons the quorum check for the WHOLE partition,
even though every key that is actually still alive in the default CF is
100%-stamped and the live-key census is complete.

Desired fix: a header-absent tombstone must un-census any earlier legacy
entry for the same key (mirroring the header-true delete branch), so a
genuine v3.24.0 store whose census tracks exactly its LIVE keys still
auto-adopts.
"""

import struct
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME, Marker
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000
PREFIX = b"pfx"


def _make_producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _rocksdb_partition(tmp_path, name="db", options=None, changelog_producer=None):
    path = (tmp_path / name).as_posix()
    opts = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(
        path, options=opts, changelog_producer=changelog_producer
    )


def _raw_key(key_str, prefix=PREFIX):
    return prefix + b"|" + json_dumps(key_str)


def _dedup_value(epoch_ms, suffix):
    """An 8-byte big-endian-int-prefixed value -- the shape a genuine
    v3.24.0-stamped record (or a legacy dedup ``set_bytes`` leftover) has."""
    return struct.pack(">Q", epoch_ms) + suffix


def _replay(partition, ops, now_ms=NOW_MS):
    """Replay header-absent ``(raw_key, value)`` default-CF messages (writes
    AND tombstones, ``value=None``) through the normal recovery path -- this
    is what censuses / un-censuses a key in ``__ttl_backfill_pending__``."""
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


def _pending_census_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


class TestTombstoneLeavesStaleCensusEntry:
    """Finding A: a header-absent tombstone must un-census its key so a
    complete, live-key-only census can still auto-adopt.

    RED on HEAD: the stale ``k0`` census entry (deleted from ``default`` but
    never un-censused) makes ``_all_pending_values_are_stamped`` return
    ``False`` for the whole partition, so ``complete_recovery`` refuses to
    adopt even though k1..k3 are a full, 100%-stamped, not-all-past census of
    every LIVE default-CF key.
    """

    def test_tombstone_uncensuses_key_and_full_census_still_adopts(self, tmp_path):
        producer = _make_producer()
        partition = _rocksdb_partition(
            tmp_path, name="tombstone_census", changelog_producer=producer
        )

        k0_raw = _raw_key("k0")
        k1_raw = _raw_key("k1")
        k2_raw = _raw_key("k2")
        k3_raw = _raw_key("k3")

        # k1..k3 are the surviving census -- all FUTURE-dated relative to the
        # recovery clock, so adopting their v3.24.0 stamps does not also
        # expire them out from under the read-back assertions below (a
        # past-dated stamp correctly reads back as expired once the store
        # is in TTL mode -- that is desired behavior, not this test's point).
        ops = [
            # k0: a header-absent leftover record replayed and censused ...
            (k0_raw, _dedup_value(NOW_MS - 5 * DAY_MS, b"-c0")),
            (k1_raw, _dedup_value(NOW_MS + 30 * DAY_MS, b"-c1")),
            (k2_raw, _dedup_value(NOW_MS + 20 * DAY_MS, b"-c2")),
            (k3_raw, _dedup_value(NOW_MS + 10 * DAY_MS, b"-c3")),
            # ... then k0 is deleted (a legacy tombstone) -- header-absent,
            # value=None. Must un-census k0.
            (k0_raw, None),
        ]
        _replay(partition, ops, now_ms=NOW_MS)

        partition.complete_recovery()

        # Desired (post-fix): k1..k3 are a full, live-key census -> auto-adopt.
        assert partition.uses_ttl_stamps is True, (
            "a tombstoned key must not leave a stale census entry that "
            "poisons the quorum check for the whole partition -- k1..k3 are "
            "a complete, 100%-stamped, not-all-past census of every LIVE "
            "default-CF key and should auto-adopt"
        )

        # k1..k3 read back correctly stripped to their payload -- all
        # future-dated, so adoption does not expire them.
        for key_str, raw_key, raw_value in (
            ("k1", k1_raw, ops[1][1]),
            ("k2", k2_raw, ops[2][1]),
            ("k3", k3_raw, ops[3][1]),
        ):
            expected_payload = raw_value[8:]
            assert (
                _read_bytes_via_tx(partition, key_str, timestamp=NOW_MS)
                == expected_payload
            )

        # k0 is gone from the default CF (the tombstone landed) ...
        assert partition.get(k0_raw, cf_name="default") is Marker.UNDEFINED

        # ... AND must not linger in the pending census either.
        assert k0_raw not in _pending_census_keys(partition), (
            "a tombstoned key must be un-censused, not left as a stale "
            "__ttl_backfill_pending__ entry"
        )

        partition.close()
