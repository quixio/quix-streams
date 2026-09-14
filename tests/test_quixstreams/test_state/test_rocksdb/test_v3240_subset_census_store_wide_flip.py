"""
Red-first test for a data-corruption bug in the AUTOMATIC v3.24.0-stamp
adoption path (``RocksDBStorePartition.complete_recovery`` Branch B,
``_adopt_v3240_stamps``).

Automatic adoption itself is INTENTIONAL and stays automatic (spec
``dev-planning/state-ttl-v3240-auto-adopt/spec.md`` §5.2/§5.7) — a full-census,
100%-stamped, not-all-past cold store is *supposed* to auto-adopt. That is
covered by the positive regression guard below and must stay GREEN.

The bug is narrower: :meth:`RocksDBStorePartition._all_pending_values_are_stamped`
and :meth:`RocksDBStorePartition._pending_all_stamps_in_past` only walk the
``__ttl_backfill_pending__`` CENSUS, never the full default-CF population. On a
warm restart of a legacy store BEHIND its changelog, recovery replays only the
TAIL of the changelog, so the census can be a strict SUBSET of the on-disk
default-CF keys. ``_adopt_v3240_stamps`` nonetheless flips
``partition.uses_ttl_stamps`` STORE-WIDE. Every subsequent read
(``RocksDBPartitionTransaction._get_bytes``) strips the leading 8 bytes off
*any* default-CF value once the partition is flipped — including keys that were
never censused, never backed up, and never proven to look like a stamp at all.
A non-censused legacy key whose value happens to be >= 8 bytes therefore reads
back 8 bytes SHORT after such a restart, with no ``__ttl_adopt_backup__`` entry
to recover it from.

Desired post-fix behavior (per the correction): adopt only when the census
covers the WHOLE store (i.e. the census population is known-complete). A
partial/subset census must not trigger a store-wide flip.
"""

import struct
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.metadata import TTL_ADOPT_BACKUP_CF_NAME
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
    """An 8-byte big-endian-int-prefixed value — the shape a genuine legacy
    dedup / set_bytes store (or a v3.24.0-stamped store) writes."""
    return struct.pack(">Q", epoch_ms) + suffix


def _replay_default(partition, msgs, now_ms=NOW_MS):
    """Replay header-absent ``(raw_key, value)`` default-CF messages through
    the normal recovery path — this is what censuses a key into
    ``__ttl_backfill_pending__``."""
    partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=False,
        )
        offset += 1


def _write_preexisting(partition, msgs):
    """Write default-CF values DIRECTLY (bypassing
    ``recover_from_changelog_message``) to simulate keys already durably on
    disk from a prior session — never censused this session."""
    default_cf = partition.get_or_create_column_family("default")
    for key, value in msgs:
        default_cf[key] = value


def _raw_default_get(partition, raw_key):
    return partition.get(raw_key, cf_name="default")


def _read_bytes_via_tx(partition, key_str, prefix=PREFIX, timestamp=None):
    tx = partition.begin()
    return tx.get_bytes(
        key=key_str, prefix=prefix, cf_name="default", timestamp=timestamp
    )


def _backup_keys(partition):
    cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
    return set(cf.keys())


class TestSubsetCensusStoreWideFlipCorruption:
    """Finding 1: a SUBSET pending census still triggers a STORE-WIDE flip,
    corrupting non-censused keys on read.

    RED on HEAD: ``complete_recovery`` auto-adopts off the subset census,
    flipping ``uses_ttl_stamps`` for the whole partition. Reads of the
    non-censused key ``k0`` then strip 8 bytes it never had stripped from it,
    and ``k0`` has no backup entry to recover from.
    """

    def test_subset_census_flip_corrupts_noncensused_key(self, tmp_path):
        producer = _make_producer()
        partition = _rocksdb_partition(
            tmp_path, name="subset_census", changelog_producer=producer
        )

        # k0..k2: already on disk from a prior session (e.g. a warm restart
        # behind its changelog) — NOT censused this session.
        preexisting = {
            f"k{i}": _dedup_value(NOW_MS - (10 - i) * DAY_MS, f"-c{i}".encode())
            for i in range(3)
        }
        _write_preexisting(
            partition,
            [(_raw_key(k), v) for k, v in preexisting.items()],
        )

        # k3..k5: replayed THIS session (the tail of the changelog) — these
        # get censused into __ttl_backfill_pending__. k4 is future-dated so
        # the census is not-all-past (the auto-adopt trigger condition).
        replayed = {
            "k3": _dedup_value(NOW_MS - 5 * DAY_MS, b"-c3"),
            "k4": _dedup_value(NOW_MS + 30 * DAY_MS, b"-c4"),
            "k5": _dedup_value(NOW_MS - 3 * DAY_MS, b"-c5"),
        }
        _replay_default(
            partition,
            [(_raw_key(k), v) for k, v in replayed.items()],
            now_ms=NOW_MS,
        )

        partition.complete_recovery()

        # Desired (post-fix): a subset census must not trigger a store-wide
        # flip. RED on HEAD: the subset census is (wrongly) treated as
        # sufficient and the store flips.
        assert partition.uses_ttl_stamps is False, (
            "a SUBSET pending census (k3..k5) must not auto-adopt the whole "
            "store — k0..k2 were never censused and would be corrupted by "
            "the store-wide flip"
        )

        # Behavioral proof: k0 (non-censused) must read back byte-identical
        # via the normal transaction read path, not stripped by 8 bytes.
        k0_raw = preexisting["k0"]
        k0_read = _read_bytes_via_tx(partition, "k0", timestamp=NOW_MS)
        assert k0_read == k0_raw, (
            f"non-censused key k0 must read back byte-identical "
            f"({k0_raw!r}); got {k0_read!r} (looks 8 bytes short -- the "
            "store-wide flip stripped a stamp k0 never had censused/backed up)"
        )

        # On-disk bytes are untouched either way (adoption never re-wraps),
        # but k0 must also have no backup entry — proof no per-key adoption
        # bookkeeping ever covered it.
        assert _raw_default_get(partition, _raw_key("k0")) == k0_raw
        assert _raw_key("k0") not in _backup_keys(partition), (
            "k0 was never censused, so it must never appear in "
            "__ttl_adopt_backup__ either"
        )

        partition.close()


class TestFullCensusAdoptRegressionGuard:
    """Positive regression guard: automatic adoption is INTENTIONAL when the
    census covers the WHOLE store (the cold-restore / fresh-volume case).

    Must stay GREEN before and after the subset-census fix — proves the fix
    does not disable the intended automatic-adoption behavior.
    """

    def test_full_census_cold_restore_still_adopts(self, tmp_path):
        producer = _make_producer()
        partition = _rocksdb_partition(
            tmp_path, name="full_census", changelog_producer=producer
        )

        # Every default-CF key replayed (and therefore censused) this
        # session -- a genuine cold restore / fresh-volume v3.24.0 store.
        # All future-dated (still not-all-past, and none expired at read time).
        records = {
            "k0": _dedup_value(NOW_MS + 2 * DAY_MS, b"-c0"),
            "k1": _dedup_value(NOW_MS + 7 * DAY_MS, b"-c1"),
            "k2": _dedup_value(NOW_MS + 1 * DAY_MS, b"-c2"),
        }
        _replay_default(
            partition,
            [(_raw_key(k), v) for k, v in records.items()],
            now_ms=NOW_MS,
        )

        partition.complete_recovery()

        assert partition.uses_ttl_stamps is True, (
            "a FULL census (every default-CF key censused) must still "
            "auto-adopt -- automatic adoption is intentional, not the bug"
        )

        # Every key was backed up (full coverage).
        backups = _backup_keys(partition)
        for k in records:
            assert _raw_key(k) in backups, f"{k} must be backed up on full adopt"

        # Values still read back correctly stripped to the payload.
        for k, raw in records.items():
            expected_payload = raw[8:]
            assert (
                _read_bytes_via_tx(partition, k, timestamp=NOW_MS) == expected_payload
            )

        partition.close()
