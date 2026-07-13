"""
Regression tests for review re-review finding #4 (Fix 4): the all-stamped
recovery gate must discriminate an interrupted THIS-BRANCH dedup migration from
opt-in v3.24.0 adoption, and never corrupt / mass-delete either scenario by
default.

Key shapes (all leftover values are 8-byte-prefixed, so per-record byte routing
is impossible — Fix 4 makes ONE store-level all-or-nothing decision):

- Scenario A (this-branch mixed, all-PAST dedup leftovers): header-true survivors
  flip the store; header-absent 8-byte epoch-ms leftovers are all in the past. The
  default auto-COMPLETES (wrap once at the survivor expiry, payload preserved, no
  mass-delete).
- Branch B, no flag, all-stamped: an unflipped store that is byte-identical to a
  stock v3.24.0 cold restore. CRITICAL + QUARANTINE (census preserved), unflipped,
  byte-identical.
- adopt=True + all-PAST census: REFUSED (would mass-delete), census preserved,
  unflipped.
- Ambiguous FUTURE-stamped this-branch census (no flag): HALT with
  IncompatibleStateStoreError, census preserved.
- Ambiguous future + legacy_records_ttl: completes (wrap once).
- Ambiguous future + adopt flag: adopts (keep verbatim).
"""

import logging
import struct
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.exceptions import IncompatibleStateStoreError
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
# Branch B — unflipped all-stamped, no flag → CRITICAL + QUARANTINE.
# -------------------------------------------------------------------
def test_branch_b_unflipped_all_stamped_no_flag_quarantines(tmp_path, caplog):
    # Pure stock-v3.24.0 shape: all header-absent stamped, no survivors.
    v3240 = {
        b"pfx|k0": encode_ttl_value(STAMP_EXPIRY, b"a"),
        b"pfx|k1": encode_ttl_value(STAMP_EXPIRY, b"b"),
    }
    partition = _partition(tmp_path)
    with caplog.at_level(logging.CRITICAL):
        _recover(partition, [(k, v, False) for k, v in v3240.items()])
        assert partition.uses_ttl_stamps is False  # never flipped
        partition.complete_recovery()

    assert partition.uses_ttl_stamps is False
    assert any(
        "adopt_v3240_stamps" in r.getMessage() for r in _critical_records(caplog)
    )
    # QUARANTINE: census preserved (repair vector), values byte-identical.
    assert _pending_keys(partition) == set(v3240)
    for raw_key, verbatim in v3240.items():
        assert bytes(partition.get(raw_key, cf_name="default")) == verbatim
    partition.close()


# -------------------------------------------------------------------
# adopt=True + all-PAST census → REFUSED (would mass-delete).
# -------------------------------------------------------------------
def test_adopt_all_past_refused(tmp_path, caplog):
    partition = _partition(
        tmp_path, options=RocksDBOptions(open_max_retries=0, adopt_v3240_stamps=True)
    )
    with caplog.at_level(logging.CRITICAL):
        _recover(partition, [(k, v, False) for k, v in PAST_DEDUP.items()])
        assert partition.uses_ttl_stamps is False
        partition.complete_recovery()

    # Adoption REFUSED: unflipped, census preserved, no index built.
    assert partition.uses_ttl_stamps is False
    assert _pending_keys(partition) == set(PAST_DEDUP)
    assert _index_count(partition) == 0
    assert any(
        "REFUSED" in r.getMessage() or "past" in r.getMessage()
        for r in _critical_records(caplog)
    )
    for raw_key, verbatim in PAST_DEDUP.items():
        assert bytes(partition.get(raw_key, cf_name="default")) == verbatim
    partition.close()


# -------------------------------------------------------------------
# Ambiguous FUTURE-stamped this-branch census, no flag → HALT.
# -------------------------------------------------------------------
def test_ambiguous_future_this_branch_halts(tmp_path):
    future_leftovers = {
        b"pfx|l0": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p0"),
        b"pfx|l1": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p1"),
    }
    partition = _partition(tmp_path)
    _recover(partition, _mixed_with_survivors(future_leftovers))
    assert partition.uses_ttl_stamps is True

    with pytest.raises(IncompatibleStateStoreError, match="Ambiguous"):
        partition.complete_recovery()

    # Census preserved (quarantined) so the operator can pick a flag and retry.
    assert _pending_keys(partition) == set(future_leftovers)
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
# Ambiguous future + adopt flag → adopts (keep verbatim).
# -------------------------------------------------------------------
def test_ambiguous_future_adopt_adopts(tmp_path):
    future_leftovers = {
        b"pfx|l0": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p0"),
        b"pfx|l1": encode_ttl_value(NOW_MS + 10 * DAY_MS, b"p1"),
    }
    partition = _partition(
        tmp_path, options=RocksDBOptions(open_max_retries=0, adopt_v3240_stamps=True)
    )
    _recover(partition, _mixed_with_survivors(future_leftovers))

    partition.complete_recovery()

    assert partition.uses_ttl_stamps is True
    # Adopted verbatim: the stored value is UNCHANGED (no re-wrap).
    for raw_key, verbatim in future_leftovers.items():
        assert bytes(partition.get(raw_key, cf_name="default")) == verbatim
    assert _pending_keys(partition) == set()
    partition.close()
