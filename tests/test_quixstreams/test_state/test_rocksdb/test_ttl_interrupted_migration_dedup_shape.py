"""
Regression tests for review re-review finding #4 (Fix 4): the all-stamped
recovery gate must discriminate an interrupted THIS-BRANCH dedup migration from
v3.24.0 adoption, and never corrupt / mass-delete either scenario.

RECONCILED to the automatic + reversible v3.24.0-stamp adoption
(``dev-planning/state-ttl-v3240-auto-adopt/spec.md``): the ``adopt_v3240_stamps``
flag is removed; adoption is automatic (cold-heuristic provisional adopt with
backup + sweep-guard) and the CRITICAL/HALT dead-ends become WARN + auto-adopt.

Key shapes (all leftover values are 8-byte-prefixed, so per-record byte routing
is impossible — Fix 4 makes ONE store-level all-or-nothing decision):

- Scenario A (this-branch mixed, all-PAST dedup leftovers): header-true survivors
  flip the store; header-absent 8-byte epoch-ms leftovers are all in the past. The
  default auto-COMPLETES (wrap once at the survivor expiry, payload preserved, no
  mass-delete). UNCHANGED.
- Branch B, all-stamped FUTURE: an unflipped store byte-identical to a stock
  v3.24.0 cold restore. NEW: provisional AUTO-ADOPT (flip + backup + index), WARN.
- All-PAST census: QUARANTINE (WARN, downgraded from CRITICAL) — stay legacy,
  census preserved, byte-identical (would mass-delete if adopted).
- Ambiguous FUTURE-stamped this-branch census (no legacy_records_ttl): NEW —
  provisional auto-adopt (keep verbatim), no HALT.
- Ambiguous future + legacy_records_ttl: completes (wrap once). UNCHANGED.
"""

import logging
import struct
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.metadata import (
    TTL_ADOPT_BACKUP_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import decode_ttl_value, encode_ttl_value

NOW_MS = 1_780_000_000_000
DAY_MS = 86_400_000


def _producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _partition(tmp_path, name="db", options=None, changelog_producer=None):
    opts = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(
        (tmp_path / name).as_posix(),
        options=opts,
        changelog_producer=changelog_producer or _producer(),
    )


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
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return {bytes(k) for k in cf.keys()}


def _index_count(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    return sum(1 for _ in cf.keys())


def _read_back(partition, raw_key):
    """User read-path value (strips the stamp iff the partition is flipped)."""
    tx = partition.begin()
    bare = raw_key.split(b"|", 1)[1]
    return tx.get_bytes(bare, prefix=b"pfx", default=None)


def _critical_records(caplog):
    return [r for r in caplog.records if r.levelno >= logging.CRITICAL]


# 8-byte big-endian epoch-ms values, dedup-style (all PAST relative to NOW_MS).
PAST_DEDUP = {
    b"pfx|l0": struct.pack(">Q", NOW_MS - 5 * DAY_MS),
    b"pfx|l1": struct.pack(">Q", NOW_MS - 4 * DAY_MS),
    b"pfx|l2": struct.pack(">Q", NOW_MS - 3 * DAY_MS),
}
STAMP_EXPIRY = NOW_MS + 30 * DAY_MS  # future survivor stamp


def _mixed_with_survivors(leftovers):
    """3 header-true future survivors + the given header-absent leftovers."""
    msgs = [
        (f"pfx|s{i}".encode(), encode_ttl_value(STAMP_EXPIRY, f"v{i}".encode()), True)
        for i in range(3)
    ]
    msgs += [(k, v, False) for k, v in leftovers.items()]
    return msgs


# -------------------------------------------------------------------
# Scenario A — this-branch mixed, all-PAST dedup leftovers → COMPLETE.
# -------------------------------------------------------------------
def test_scenario_a_all_past_dedup_leftovers_complete(tmp_path):
    partition = _partition(tmp_path)
    _recover(partition, _mixed_with_survivors(PAST_DEDUP))
    assert partition.uses_ttl_stamps is True
    assert partition._recovery_saw_stamped is True
    assert _pending_keys(partition) == set(PAST_DEDUP)

    partition.complete_recovery()

    # Leftovers COMPLETED: wrapped once at the survivor expiry, payload preserved.
    for raw_key, original in PAST_DEDUP.items():
        stored = partition.get(raw_key, cf_name="default")
        stamp, payload = decode_ttl_value(bytes(stored))
        assert stamp == STAMP_EXPIRY  # future -> no mass-delete
        assert payload == original  # byte-preserved dedup value
        assert _read_back(partition, raw_key) == original
    assert _pending_keys(partition) == set()
    partition.close()


# -------------------------------------------------------------------
# Branch B — unflipped all-stamped FUTURE → provisional AUTO-ADOPT (WARN).
# -------------------------------------------------------------------
def test_branch_b_unflipped_all_stamped_auto_adopts(tmp_path, caplog):
    # Pure stock-v3.24.0 shape: all header-absent FUTURE-stamped, no survivors.
    # NEW (spec §5.2): auto-adopt PROVISIONALLY (flip + backup + index), WARN.
    v3240 = {
        b"pfx|k0": encode_ttl_value(STAMP_EXPIRY, b"a"),
        b"pfx|k1": encode_ttl_value(STAMP_EXPIRY, b"b"),
    }
    partition = _partition(tmp_path)
    with caplog.at_level(logging.WARNING):
        _recover(partition, [(k, v, False) for k, v in v3240.items()])
        assert partition.uses_ttl_stamps is False  # not flipped until adopt
        partition.complete_recovery()

    # Provisionally adopted: flipped, index rebuilt, census drained, backup made.
    assert partition.uses_ttl_stamps is True
    assert _pending_keys(partition) == set()
    assert _index_count(partition) == 2
    assert TTL_ADOPT_BACKUP_CF_NAME in partition.list_column_families()
    # Values kept verbatim on disk (each carries its own v3.24.0 stamp).
    for raw_key, verbatim in v3240.items():
        assert bytes(partition.get(raw_key, cf_name="default")) == verbatim
    # WARN (not CRITICAL); no mention of the removed flag.
    assert not _critical_records(caplog)
    assert any("auto-adopted" in r.getMessage().lower() for r in caplog.records)
    partition.close()


# -------------------------------------------------------------------
# All-PAST census → QUARANTINE (WARN, downgraded from CRITICAL).
# -------------------------------------------------------------------
def test_all_past_quarantined(tmp_path, caplog):
    partition = _partition(tmp_path)
    with caplog.at_level(logging.WARNING):
        _recover(partition, [(k, v, False) for k, v in PAST_DEDUP.items()])
        assert partition.uses_ttl_stamps is False
        partition.complete_recovery()

    # QUARANTINE: unflipped, census preserved, no index, values byte-identical.
    assert partition.uses_ttl_stamps is False
    assert _pending_keys(partition) == set(PAST_DEDUP)
    assert _index_count(partition) == 0
    assert not _critical_records(caplog)  # downgraded to WARN
    assert any(
        ("refused" in r.getMessage().lower() or "past" in r.getMessage().lower())
        and r.levelno == logging.WARNING
        for r in caplog.records
    )
    for raw_key, verbatim in PAST_DEDUP.items():
        assert bytes(partition.get(raw_key, cf_name="default")) == verbatim
    partition.close()


# -------------------------------------------------------------------
# Ambiguous FUTURE-stamped this-branch census (no ttl) → provisional adopt.
# -------------------------------------------------------------------
def test_ambiguous_future_this_branch_auto_adopts(tmp_path):
    # NEW (spec §5.2 Branch-A reconciliation): keep verbatim via provisional adopt
    # instead of HALTing. ``legacy_records_ttl`` stays the explicit wrap-once
    # override (see test_ambiguous_future_legacy_ttl_completes).
    future_leftovers = {
        b"pfx|l0": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p0"),
        b"pfx|l1": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p1"),
    }
    partition = _partition(tmp_path)
    _recover(partition, _mixed_with_survivors(future_leftovers))
    assert partition.uses_ttl_stamps is True

    partition.complete_recovery()  # must NOT raise

    assert partition.uses_ttl_stamps is True
    # Leftovers kept VERBATIM (adopted, not re-wrapped); census drained; backup made.
    for raw_key, verbatim in future_leftovers.items():
        assert bytes(partition.get(raw_key, cf_name="default")) == verbatim
    assert _pending_keys(partition) == set()
    assert TTL_ADOPT_BACKUP_CF_NAME in partition.list_column_families()
    partition.close()


# -------------------------------------------------------------------
# Ambiguous future + legacy_records_ttl → completes (wrap once).
# -------------------------------------------------------------------
def test_ambiguous_future_legacy_ttl_completes(tmp_path):
    from datetime import timedelta

    future_leftovers = {
        b"pfx|l0": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p0"),
        b"pfx|l1": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p1"),
    }
    partition = _partition(
        tmp_path,
        options=RocksDBOptions(
            open_max_retries=0, legacy_records_ttl=timedelta(days=7)
        ),
    )
    _recover(partition, _mixed_with_survivors(future_leftovers))

    partition.complete_recovery()  # must NOT raise (operator asserted legacy intent)

    expected = NOW_MS + 7 * DAY_MS
    for raw_key, verbatim in future_leftovers.items():
        stamp, payload = decode_ttl_value(
            bytes(partition.get(raw_key, cf_name="default"))
        )
        assert stamp == expected  # wrapped once at wallclock + ttl
        assert payload == verbatim  # the WHOLE original value is the payload
    assert _pending_keys(partition) == set()
    partition.close()


# -------------------------------------------------------------------
# Provisional adopt is REVERSIBLE: marker set + sweep suppressed until corroborated.
# -------------------------------------------------------------------
def test_provisional_adopt_marks_reversible_and_suppresses_sweep(tmp_path):
    # Branch B future-stamped census -> provisional adopt. The provisional marker
    # is set and the sweep is suppressed (a past-stamped adopted record survives a
    # flush with an advanced high-water until a live ttl= write corroborates).
    v3240 = {
        b"pfx|k_past": encode_ttl_value(NOW_MS - DAY_MS, b"pastval"),
        b"pfx|k_future": encode_ttl_value(STAMP_EXPIRY, b"futureval"),
    }
    partition = _partition(tmp_path)
    _recover(partition, [(k, v, False) for k, v in v3240.items()])
    partition.complete_recovery()

    assert partition.uses_ttl_stamps is True
    assert partition._adopt_provisional is True

    # Advance high-water past the expired key and flush a plain (non-ttl) write:
    # the sweep must be suppressed, so the past-stamped adopted key survives.
    partition._high_water_ms = NOW_MS + 1
    tx = partition.begin()
    tx.set(key="klive", value="vlive", prefix=b"pfx", timestamp=NOW_MS)
    tx.prepare(processed_offsets={"topic": 1})
    tx.flush(changelog_offset=100)

    assert partition.get(b"pfx|k_past", cf_name="default") is not None
    partition.close()
