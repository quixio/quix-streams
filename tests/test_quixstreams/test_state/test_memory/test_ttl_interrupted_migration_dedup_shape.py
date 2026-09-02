"""
Memory-backend sibling of
``test_rocksdb/test_ttl_interrupted_migration_dedup_shape.py`` (review re-review
finding #4, Fix 4). Memory has no persisted flip flag and no ledger, so
``_recovery_saw_stamped`` alone is the this-branch discriminator; the branch
semantics are otherwise identical.

RECONCILED to the automatic + reversible v3.24.0-stamp adoption (spec
``dev-planning/state-ttl-v3240-auto-adopt/spec.md``): the ``adopt_v3240_stamps``
flag is removed; adoption is automatic (provisional, with an in-RAM backup and
sweep-guard) and the CRITICAL/HALT dead-ends become WARN + auto-adopt.
"""

import logging
import struct
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.ttl_codec import decode_ttl_value, encode_ttl_value

NOW_MS = 1_780_000_000_000
DAY_MS = 86_400_000
STAMP_EXPIRY = NOW_MS + 30 * DAY_MS

PAST_DEDUP = {
    b"pfx|l0": struct.pack(">Q", NOW_MS - 5 * DAY_MS),
    b"pfx|l1": struct.pack(">Q", NOW_MS - 4 * DAY_MS),
}


def _producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _recover(partition, msgs, now_ms=NOW_MS):
    partition._now_ms = lambda: now_ms  # noqa: E731
    for offset, (key, value, ttl_stamped) in enumerate(msgs):
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )


def _pending_keys(partition):
    return set(partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {}).keys())


def _stored(partition, raw_key):
    return partition._state.get("default", {}).get(raw_key)


def _mixed_with_survivors(leftovers):
    msgs = [
        (f"pfx|s{i}".encode(), encode_ttl_value(STAMP_EXPIRY, f"v{i}".encode()), True)
        for i in range(2)
    ]
    msgs += [(k, v, False) for k, v in leftovers.items()]
    return msgs


def test_scenario_a_all_past_dedup_leftovers_complete():
    partition = MemoryStorePartition(changelog_producer=_producer())
    _recover(partition, _mixed_with_survivors(PAST_DEDUP))
    assert partition.uses_ttl_stamps is True
    assert _pending_keys(partition) == set(PAST_DEDUP)

    partition.complete_recovery()

    for raw_key, original in PAST_DEDUP.items():
        stamp, payload = decode_ttl_value(_stored(partition, raw_key))
        assert stamp == STAMP_EXPIRY  # future -> no mass-delete
        assert payload == original  # byte-preserved
    assert _pending_keys(partition) == set()


def test_branch_b_unflipped_all_stamped_auto_adopts(caplog):
    # NEW (spec §5.2 / §5.7): all header-absent FUTURE-stamped, no survivors ->
    # provisional auto-adopt (flip + in-RAM backup), WARN (not CRITICAL).
    v3240 = {
        b"pfx|k0": encode_ttl_value(STAMP_EXPIRY, b"a"),
        b"pfx|k1": encode_ttl_value(STAMP_EXPIRY, b"b"),
    }
    partition = MemoryStorePartition(changelog_producer=_producer())
    with caplog.at_level(logging.WARNING):
        _recover(partition, [(k, v, False) for k, v in v3240.items()])
        assert partition.uses_ttl_stamps is False
        partition.complete_recovery()

    # Provisionally adopted: flipped, census drained, no dead in-RAM backup dict.
    assert partition.uses_ttl_stamps is True
    assert partition._adopt_provisional is True
    # finding #5: memory rollback is replay-suppression, not restore, so no
    # duplicate in-RAM copy of the values is retained.
    assert not hasattr(partition, "_adopt_backup")
    assert _pending_keys(partition) == set()
    # Values kept verbatim (each carries its own v3.24.0 stamp).
    for raw_key, verbatim in v3240.items():
        assert _stored(partition, raw_key) == verbatim
    # WARN (not CRITICAL); no mention of the removed flag.
    assert not [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert any("auto-adopted" in r.getMessage().lower() for r in caplog.records)


def test_all_past_quarantined(caplog):
    # NEW: all-PAST census -> QUARANTINE (WARN, downgraded from CRITICAL).
    partition = MemoryStorePartition(changelog_producer=_producer())
    with caplog.at_level(logging.WARNING):
        _recover(partition, [(k, v, False) for k, v in PAST_DEDUP.items()])
        partition.complete_recovery()

    assert partition.uses_ttl_stamps is False
    assert _pending_keys(partition) == set(PAST_DEDUP)  # preserved
    assert not [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    for raw_key, verbatim in PAST_DEDUP.items():
        assert _stored(partition, raw_key) == verbatim


def test_ambiguous_future_this_branch_auto_adopts():
    # NEW (spec §5.2 Branch-A reconciliation): keep verbatim via provisional adopt
    # instead of HALTing.
    future_leftovers = {
        b"pfx|l0": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p0"),
        b"pfx|l1": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p1"),
    }
    partition = MemoryStorePartition(changelog_producer=_producer())
    _recover(partition, _mixed_with_survivors(future_leftovers))

    partition.complete_recovery()  # must NOT raise

    assert partition.uses_ttl_stamps is True
    assert _pending_keys(partition) == set()  # census drained
    # Leftovers kept VERBATIM (adopted, not re-wrapped).
    for raw_key, verbatim in future_leftovers.items():
        assert _stored(partition, raw_key) == verbatim
