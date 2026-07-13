"""
Regression tests for review re-review finding #5 (Fix 5): clamp the
survivor-derived completion / resume fallback against the recovery clock.

Both survivor-derived fallbacks used to stamp leftovers with the max on-disk
index stamp even when it was already in the PAST after downtime — the next sweep
then deleted every leftover (violating the never-mass-delete policy), while the
same changelog cold-restored kept them forever (SENTINEL_NEVER) — a
nondeterministic opposite outcome. The fix clamps: a derived expiry ``<= now`` is
replaced by SENTINEL_NEVER (kept, never deleted), so warm and cold restores of
one changelog converge.
"""

import logging
from unittest.mock import MagicMock, PropertyMock

import pytest
from rocksdict import WriteBatch

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000
T0 = 1_780_000_000_000  # enable-time wallclock
STAMP_EXPIRY = T0 + 30 * DAY_MS  # survivors stamped ~enable + 30d
T_RESTART = T0 + 60 * DAY_MS  # restart after 60d downtime (> the 30d window)


@pytest.fixture()
def changelog_producer_mock():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


@pytest.fixture()
def partition_factory(tmp_path, changelog_producer_mock):
    def factory(name="dst", options=None, changelog_producer=changelog_producer_mock):
        return RocksDBStorePartition(
            (tmp_path / name).as_posix(),
            changelog_producer=changelog_producer,
            options=options or RocksDBOptions(open_max_retries=0),
        )

    return factory


def _replay(partition, msgs, now_ms):
    partition._now_ms = lambda: now_ms  # noqa: E731
    for offset, (key, value, ttl_stamped) in enumerate(msgs):
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )


def _mixed_changelog(n_stamped, n_legacy):
    msgs, legacy = [], {}
    for i in range(n_stamped):
        msgs.append(
            (
                f"pfx|s{i}".encode(),
                encode_ttl_value(STAMP_EXPIRY, f"stamped-{i}".encode()),
                True,
            )
        )
    for i in range(n_legacy):
        key, raw = f"pfx|l{i}".encode(), f"legacy-payload-{i}".encode()
        legacy[key] = raw
        msgs.append((key, raw, False))
    return msgs, legacy


def _default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {bytes(k): bytes(v) for k, v in cf.items()}


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return {bytes(k) for k in cf.keys()}


# -------------------------------------------------------------------
# Completion path: warm restart after downtime clamps to SENTINEL_NEVER.
# -------------------------------------------------------------------
def test_warm_restart_after_downtime_clamps_to_sentinel_never(
    partition_factory, caplog
):
    msgs, legacy = _mixed_changelog(n_stamped=2, n_legacy=4)

    # Run 1: MIXED replay at T0, crash before complete_recovery.
    run1 = partition_factory()
    _replay(run1, msgs, now_ms=T0)
    assert run1.uses_ttl_stamps is True
    assert _pending_keys(run1) == set(legacy)
    run1.close()

    # Run 2: warm restart on the SAME volume after 60 days of downtime.
    # Offset caught up -> no replay -> _recovery_max_survivor_expiry_ms unset, and
    # the only derivable expiry (max on-disk index stamp) is now in the PAST.
    run2 = partition_factory()
    run2._now_ms = lambda: T_RESTART  # noqa: E731
    assert run2._max_index_stamp_ms() == STAMP_EXPIRY
    assert STAMP_EXPIRY < T_RESTART

    with caplog.at_level(logging.WARNING):
        run2.complete_recovery()

    decoded = {k: decode_ttl_value(v) for k, v in _default_cf(run2).items()}
    for key, raw in legacy.items():
        exp, payload = decoded[key]
        assert payload == raw
        assert exp == SENTINEL_NEVER, "past survivor expiry must be clamped"
    assert _pending_keys(run2) == set()
    # The clamp WARN must NOT dangle a legacy_records_ttl redeploy fix.
    assert not any("redeploy" in r.getMessage() for r in caplog.records)

    # First sweep at current event-time: the SENTINEL leftovers must survive
    # (the pre-fix past-expiry stamp would have deleted all of them).
    run2.advance_high_water(T_RESTART)
    batch = WriteBatch(raw_mode=True)
    run2._run_sweep(batch)
    run2._write(batch)
    survivors = _default_cf(run2)
    assert set(legacy) <= set(survivors), "clamped leftovers must NOT be swept"
    run2.close()


# -------------------------------------------------------------------
# Determinism: the same changelog cold-restored also yields SENTINEL_NEVER.
# -------------------------------------------------------------------
def test_cold_restore_same_changelog_matches_warm_restart(partition_factory):
    msgs, legacy = _mixed_changelog(n_stamped=2, n_legacy=4)

    cold = partition_factory(name="fresh")
    _replay(cold, msgs, now_ms=T_RESTART)  # replay AT restart time
    assert cold.uses_ttl_stamps is True
    assert cold._recovery_max_survivor_expiry_ms is None
    assert cold._max_index_stamp_ms() is None  # every survivor expired-dropped

    cold.complete_recovery()

    decoded = {k: decode_ttl_value(v) for k, v in _default_cf(cold).items()}
    for key, raw in legacy.items():
        assert decoded[key] == (SENTINEL_NEVER, raw)  # same outcome as warm
    cold.close()


# -------------------------------------------------------------------
# Resume path: a past _max_index_stamp_ms() fallback also clamps.
# -------------------------------------------------------------------
def test_resume_path_past_index_stamp_clamps_to_sentinel(partition_factory):
    # Construct an interrupted-live-backfill signature directly: seed 4 legacy
    # records, backfill only k0/k1 (stamped at STAMP_EXPIRY, ledgered + indexed),
    # leave k2/k3 legacy, flip on disk WITHOUT a done-marker.
    setup = partition_factory(name="resume")
    with setup.begin() as tx:
        for i in range(4):
            tx.set_bytes(
                key=f"k{i}".encode(), value=f"legacy-{i}".encode(), prefix=b"pfx"
            )
    setup.backfill_legacy_records(
        expires_at_ms=STAMP_EXPIRY,
        changelog_producer=None,
        processed_offsets=None,
        staged_default_keys={b"pfx|k2", b"pfx|k3"},  # skipped -> stay legacy
        chunk_size=10,
    )
    setup._stamp_flip_metadata()  # durable flip, no done-marker
    setup.close()

    # Warm restart, offset caught up (no replay). The max on-disk index stamp
    # (STAMP_EXPIRY) is now in the past relative to the restart clock.
    resume = partition_factory(name="resume")
    resume._now_ms = lambda: T_RESTART  # noqa: E731
    assert resume._max_index_stamp_ms() == STAMP_EXPIRY

    resume.complete_recovery()  # routes to _resume_interrupted_live_backfill

    decoded = {k: decode_ttl_value(v) for k, v in _default_cf(resume).items()}
    # k2/k3 (the resumed complement) clamped to never-expire, not a past expiry.
    assert decoded[b"pfx|k2"] == (SENTINEL_NEVER, b"legacy-2")
    assert decoded[b"pfx|k3"] == (SENTINEL_NEVER, b"legacy-3")
    # k0/k1 keep their original (future-at-enable) cohort stamp, untouched.
    assert decoded[b"pfx|k0"][0] == STAMP_EXPIRY
    assert decoded[b"pfx|k1"][0] == STAMP_EXPIRY

    # Sweep at current event-time: the clamped complement survives.
    resume.advance_high_water(T_RESTART)
    batch = WriteBatch(raw_mode=True)
    resume._run_sweep(batch)
    resume._write(batch)
    survivors = _default_cf(resume)
    assert b"pfx|k2" in survivors and b"pfx|k3" in survivors
    resume.close()


# -------------------------------------------------------------------
# Memory defensive sibling: an all-expired cohort folds into the clamp.
# -------------------------------------------------------------------
def test_memory_all_expired_survivor_clamps_to_sentinel():
    partition = MemoryStorePartition(changelog_producer=None)
    msgs, legacy = _mixed_changelog(n_stamped=2, n_legacy=4)
    # Replay AT restart time so every stamped survivor is expired-dropped.
    partition._now_ms = lambda: T_RESTART  # noqa: E731
    for offset, (key, value, ttl_stamped) in enumerate(msgs):
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
    assert partition.uses_ttl_stamps is True
    assert partition._recovery_max_survivor_expiry_ms is None
    assert partition._max_index_stamp_ms() is None

    partition.complete_recovery()

    default = partition._state.get("default", {})
    for key, raw in legacy.items():
        assert decode_ttl_value(default[key]) == (SENTINEL_NEVER, raw)
