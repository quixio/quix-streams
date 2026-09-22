"""
Memory-backend twin of the RocksDB subset-census store-wide-flip bug (see
``test_rocksdb/test_v3240_subset_census_store_wide_flip.py`` for the full
narrative).

Automatic v3.24.0-stamp adoption is INTENTIONAL and stays automatic — a
full-census, 100%-stamped, not-all-past store is *supposed* to auto-adopt
(spec ``dev-planning/state-ttl-v3240-auto-adopt/spec.md`` §5.2/§5.7). That is
covered by the positive regression guard below and must stay GREEN.

The bug: ``MemoryStorePartition._all_pending_values_are_stamped`` /
``_pending_all_stamps_in_past`` only walk the ``__ttl_backfill_pending__``
census dict, never the full ``default`` population. ``_adopt_v3240_stamps``
flips ``uses_ttl_stamps`` STORE-WIDE off a census that can be a strict SUBSET
of the in-RAM default population (constructed here directly, bypassing
``recover_from_changelog_message``, to isolate the mechanism). Every
subsequent read (``MemoryPartitionTransaction._get_bytes``) then strips the
leading 8 bytes off *any* default-CF value whose partition is flipped,
corrupting keys that were never censused.
"""

import struct
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.memory import MemoryStorePartition
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


def _replay_default(partition, msgs, now_ms=NOW_MS):
    """Replay header-absent ``(raw_key, value)`` default-CF messages through
    the normal recovery path -- this censuses the key into
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
    """Write default-CF values DIRECTLY into the in-RAM dict (bypassing
    ``recover_from_changelog_message``) -- never censused this session."""
    default = partition._state.setdefault("default", {})
    for key, value in msgs:
        default[key] = value


def _read_bytes_via_tx(partition, key_str, prefix=PREFIX, timestamp=None):
    tx = partition.begin()
    return tx.get_bytes(
        key=key_str, prefix=prefix, cf_name="default", timestamp=timestamp
    )


class TestSubsetCensusStoreWideFlipCorruptionMemory:
    """Finding 1 (memory twin): a SUBSET pending census still triggers a
    STORE-WIDE flip, corrupting non-censused keys on read.

    RED on HEAD: ``complete_recovery`` auto-adopts off the subset census,
    flipping ``uses_ttl_stamps`` for the whole partition; the non-censused
    key ``k0`` then reads back 8 bytes short.

    Note: the memory backend always does a FULL changelog replay on every
    open (no persisted offset, no ledger) -- a real warm-restart-behind-
    changelog scenario as described for RocksDB cannot arise naturally on
    memory. This test constructs the subset-census PRECONDITION directly
    (bypassing ``recover_from_changelog_message`` for k0..k2) to exercise the
    same store-wide-flip mechanism in ``complete_recovery`` /
    ``_adopt_v3240_stamps``, which is shared code-shape with RocksDB.
    """

    def test_subset_census_flip_corrupts_noncensused_key(self):
        producer = _make_producer()
        partition = MemoryStorePartition(changelog_producer=producer)

        # k0..k2: present in the in-RAM default dict but NOT censused this
        # session (the subset-census precondition).
        preexisting = {
            f"k{i}": _dedup_value(NOW_MS - (10 - i) * DAY_MS, f"-c{i}".encode())
            for i in range(3)
        }
        _write_preexisting(
            partition,
            [(_raw_key(k), v) for k, v in preexisting.items()],
        )

        # k3..k5: replayed THIS session -- censused. k4 is future-dated so
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
            "in-memory store -- k0..k2 were never censused and would be "
            "corrupted by the store-wide flip"
        )

        k0_raw = preexisting["k0"]
        k0_read = _read_bytes_via_tx(partition, "k0", timestamp=NOW_MS)
        assert k0_read == k0_raw, (
            f"non-censused key k0 must read back byte-identical "
            f"({k0_raw!r}); got {k0_read!r} (looks 8 bytes short -- the "
            "store-wide flip stripped a stamp k0 never had censused)"
        )

        partition.close()


class TestFullCensusAdoptRegressionGuardMemory:
    """Positive regression guard (memory twin): automatic adoption is
    INTENTIONAL when the census covers the WHOLE store. Must stay GREEN
    before and after the subset-census fix.
    """

    def test_full_census_cold_restore_still_adopts(self):
        producer = _make_producer()
        partition = MemoryStorePartition(changelog_producer=producer)

        # Every default-CF key replayed (and therefore censused) this
        # session. All future-dated (not-all-past, none expired at read time).
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

        for k, raw in records.items():
            expected_payload = raw[8:]
            assert (
                _read_bytes_via_tx(partition, k, timestamp=NOW_MS) == expected_payload
            )

        partition.close()
