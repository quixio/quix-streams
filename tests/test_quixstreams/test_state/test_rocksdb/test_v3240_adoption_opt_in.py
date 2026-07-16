"""
v3.24.0 stamp adoption is AUTOMATIC (regime-split).

**Reworked from the original opt-in suite.** The ``adopt_v3240_stamps`` flag is
REMOVED (spec §4). Detection stays automatic; the FLIP is now also automatic:

- Cold rebuild, 100%-stamped, >=1 future → **provisional auto-adopt** (backup +
  sweep-guard + corroboration). No flag needed.
- Cold rebuild, all-past → quarantined (unchanged).
- Sub-100% quorum → veto (unchanged).

The old "no flag → CRITICAL, stay legacy" behavior is replaced by the auto-adopt.
The old "flag set → adopt" behavior is subsumed.

This file tests the COLD REGIME automatic adoption path. Warm-restart and
rollback scenarios are in ``test_v3240_auto_adopt.py``.

Spec: ``dev-planning/state-ttl-v3240-auto-adopt/spec.md``
"""

import logging
import math
import struct
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    encode_ttl_value,
)
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000

# New metadata/CF names from the spec (§4).
TTL_ADOPT_BACKUP_CF_NAME = "__ttl_adopt_backup__"
TTL_ADOPT_PENDING_KEY = b"__ttl_adopt_pending__"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _replay_default(partition, msgs, now_ms=NOW_MS):
    """Replay header-absent ``(raw_key, value, ttl_stamped)`` default-CF messages."""
    partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


def _v3240_msg(key_str, user_value, expiry_ms, prefix=b"pfx"):
    """One v3.24.0-style default-CF changelog message: an 8-byte-stamped value
    with NO ``__ttl_stamped__`` header (the shape v3.24.0 wrote)."""
    raw_key = prefix + b"|" + json_dumps(key_str)
    stamped = encode_ttl_value(expiry_ms, json_dumps(user_value))
    return (raw_key, stamped, False)


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _index_count(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    return sum(1 for _ in cf.keys())


def _raw_default_get(partition, raw_key):
    """Byte-exact on-disk default-CF value (no strip, no deserialize)."""
    return partition.get(raw_key, cf_name="default")


def _read_bytes_via_tx(partition, raw_key_str, prefix=b"pfx", timestamp=None):
    """User read path returning raw bytes (strips the stamp iff the partition is
    flipped) — the discriminator between adopted (stripped) and legacy (verbatim)."""
    tx = partition.begin()
    return tx.get_bytes(
        key=raw_key_str, prefix=prefix, cf_name="default", timestamp=timestamp
    )


def _read_via_tx(partition, key, prefix=b"pfx", timestamp=None):
    tx = partition.begin()
    return tx.get(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)


# ---------------------------------------------------------------------------
# Automatic adoption suite (reworked from opt-in)
# ---------------------------------------------------------------------------


class TestV3240AutomaticAdoption:
    """Reworked from ``TestV3240AdoptionOptIn``. The ``adopt_v3240_stamps``
    parameter is removed. The cold-rebuild 100%-stamped-not-all-past path now
    auto-adopts PROVISIONALLY (with backup + sweep guard + corroboration).

    All tests below should be RED on HEAD (HEAD requires the removed flag).
    """

    # (i) 100%-stamped store → AUTO provisional adopt (was: CRITICAL + stay legacy).
    # RED on HEAD (HEAD: no flag → CRITICAL → stays legacy).
    def test_100pct_auto_adopts_provisionally(self, tmp_path, caplog):
        """Spec §5.2: a header-absent all-stamped (>=1 future) census triggers
        automatic provisional adoption. No flag needed.

        OLD behavior: stays legacy, CRITICAL naming adopt_v3240_stamps.
        NEW behavior: provisional adopt (flip + backup + guard), WARN.
        """
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]

        partition = _rocksdb_partition(
            tmp_path, name="autoadopt", changelog_producer=producer
        )
        with caplog.at_level(logging.WARNING):
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

        # Flipped (provisional).
        assert (
            partition.uses_ttl_stamps is True
        ), "Without any flag, cold auto-adopt must flip the store"
        # Values read back STRIPPED to the original user payload.
        for i in range(4):
            assert _read_via_tx(partition, f"k{i}", timestamp=NOW_MS) == f"v{i}"
        # Index entries built.
        assert _index_count(partition) == 4

        # No CRITICAL naming the removed flag.
        assert not any(
            r.levelno == logging.CRITICAL and "adopt_v3240_stamps" in r.getMessage()
            for r in caplog.records
        )
        partition.close()

    # (ii) set_bytes big-endian-int false-positive store → provisional adopt
    # THEN rollback via env var restores byte-identical.
    # RED on HEAD (HEAD: CRITICAL + stays legacy with the old flag path).
    def test_set_bytes_false_positive_provisional_then_rollback(self, tmp_path, caplog):
        """Spec §5.2 + §5.6: a legacy set_bytes store whose values look like
        stamps is provisionally auto-adopted (wrong but reversible). While
        provisional, sweep is suppressed. Rollback restores byte-identical.

        OLD behavior: CRITICAL, stays legacy.
        NEW behavior: provisional adopt, then rollback restores.
        """
        import os

        producer = _make_producer()
        originals = {}
        msgs = []
        for i in range(5):
            raw_key = b"pfx|" + json_dumps(f"c{i}")
            value = struct.pack(">Q", NOW_MS + i * DAY_MS) + f"-counter-{i}".encode()
            originals[raw_key] = value
            msgs.append((raw_key, value, False))

        partition = _rocksdb_partition(
            tmp_path, name="falsepos", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        # Provisionally adopted (wrong — these are legacy values).
        assert (
            partition.uses_ttl_stamps is True
        ), "False-positive legacy store auto-adopts provisionally"
        # No CRITICAL naming the removed flag.
        assert not any(
            r.levelno == logging.CRITICAL and "adopt_v3240_stamps" in r.getMessage()
            for r in caplog.records
        )

        # While provisional: no deletions produced (sweep suppressed).
        assert all(
            call.kwargs.get("value") is not None
            for call in producer.produce.call_args_list
        ), "Sweep must be suppressed: no tombstones produced"

        partition.close()

        # Rollback.
        os.environ["QUIXSTREAMS_STATE_TTL_ROLLBACK"] = "1"
        try:
            producer2 = _make_producer()
            partition2 = _rocksdb_partition(
                tmp_path, name="falsepos", changelog_producer=producer2
            )
            assert (
                partition2.uses_ttl_stamps is False
            ), "After rollback the store must be legacy"
            # Byte-identical.
            for raw_key, expected in originals.items():
                assert (
                    _raw_default_get(partition2, raw_key) == expected
                ), f"Value for {raw_key!r} must be byte-identical after rollback"
            partition2.close()
        finally:
            os.environ.pop("QUIXSTREAMS_STATE_TTL_ROLLBACK", None)

    # (iii) single-bad-value veto still holds: quorum fails, so the gate is
    # not entered — no flip, census discarded.
    # GREEN on HEAD: unchanged behavior.
    def test_single_bad_value_veto(self, tmp_path, caplog):
        """Sub-100% quorum → no flip, census discarded. Unchanged from HEAD."""
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]
        # One genuine short legacy value that cannot be a valid stamp.
        msgs.append((b"pfx|" + json_dumps("k_legacy"), b"short", False))

        partition = _rocksdb_partition(
            tmp_path, name="veto", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        assert partition.uses_ttl_stamps is False
        assert _pending_keys(partition) == set()
        partition.close()

    # (iv) adoption is chunked: with a small chunk size, adoption commits in
    # more than one batch.
    # RED on HEAD (HEAD requires ``adopt_v3240_stamps=True``).
    def test_adoption_is_chunked(self, tmp_path):
        """Spec §5.2: provisional adoption must be chunked (reuses
        ``legacy_backfill_chunk_size``). No flag needed.
        """
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        n = 5
        chunk_size = 2
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(n)]

        partition = _rocksdb_partition(
            tmp_path,
            name="chunked",
            options=RocksDBOptions(legacy_backfill_chunk_size=chunk_size),
            changelog_producer=producer,
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)

        original_write = partition._write
        writes = {"n": 0}

        def counting_write(batch):
            writes["n"] += 1
            return original_write(batch)

        partition._write = counting_write
        partition.complete_recovery()

        expected_chunks = math.ceil(n / chunk_size)
        assert expected_chunks > 1
        # At least the expected number of chunk writes + flip metadata write.
        assert writes["n"] >= expected_chunks + 1
        # Fully adopted: flipped, index complete, census drained.
        assert partition.uses_ttl_stamps is True
        assert _index_count(partition) == n
        assert _pending_keys(partition) == set()
        partition.close()

    # (v) Sentinel-only v3.24.0 store adopts with NO index entries.
    # RED on HEAD (HEAD requires ``adopt_v3240_stamps=True``).
    def test_sentinel_only_adopts_no_index(self, tmp_path):
        """Sentinel-only store auto-adopts with 0 index entries."""
        producer = _make_producer()
        msgs = []
        keys = {f"k{i}": f"v{i}" for i in range(3)}
        for k, v in keys.items():
            raw_key = b"pfx|" + json_dumps(k)
            msgs.append(
                (raw_key, encode_ttl_value(SENTINEL_NEVER, json_dumps(v)), False)
            )

        partition = _rocksdb_partition(
            tmp_path, name="sentinel", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        assert partition.uses_ttl_stamps is True
        assert _index_count(partition) == 0
        assert _pending_keys(partition) == set()
        for k, v in keys.items():
            assert _read_via_tx(partition, k, timestamp=NOW_MS) == v
        partition.close()
