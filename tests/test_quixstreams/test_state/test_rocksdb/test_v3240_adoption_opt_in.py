"""
v3.24.0 stamp adoption is opt-in.

On a 100%-quorum stamp detection ``complete_recovery`` used to AUTOMATICALLY flip
a store into TTL mode and rewrite its ``__ttl_index__``. That is unsafe: a genuine
v3.24.0 store is byte-for-byte indistinguishable from a legacy ``set_bytes()``
store whose values happen to begin with 8 plausible big-endian bytes (epoch-ms,
counters). Auto-flipping the latter turns the first 8 bytes of every value into an
expiry stamp; the sweep + changelog tombstones then delete the data.

DETECTION stays automatic but the FLIP is opt-in via a new
``RocksDBOptions(adopt_v3240_stamps=...)`` boolean (default ``False``). Without the
flag a 100%-quorum detection logs a CRITICAL naming the flag and then follows the
pure-legacy disposition (stay legacy, discard the census, values byte-identical).
With the flag, adoption proceeds as before but its ``WriteBatch`` is chunked.
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


def _critical_names_flag(caplog):
    return any(
        r.levelno == logging.CRITICAL and "adopt_v3240_stamps" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Opt-in adoption suite
# ---------------------------------------------------------------------------


class TestV3240AdoptionOptIn:
    # (i) 100%-stamped store, NO flag → no flip, CRITICAL, raw-readable.
    # RED on HEAD (HEAD auto-flips → uses_ttl_stamps True, no CRITICAL).
    def test_100pct_no_flag_stays_legacy_and_criticals(self, tmp_path, caplog):
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]

        partition = _rocksdb_partition(
            tmp_path, name="noflag", changelog_producer=producer
        )
        with caplog.at_level(logging.CRITICAL):
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

        # No flip: the store stays legacy.
        assert partition.uses_ttl_stamps is False
        # A CRITICAL naming the opt-in flag was logged.
        assert _critical_names_flag(caplog)
        # Every value reads back byte-identical (still carries the 8-byte prefix).
        for i in range(4):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            expected = encode_ttl_value(expiry, json_dumps(f"v{i}"))
            assert _raw_default_get(partition, raw_key) == expected
        # No index built. Fix 4 (review re-review #4): the census is now
        # QUARANTINED (preserved as the repair vector for a later opt-in
        # adoption), NOT discarded, on the unflipped all-stamped Branch-B path.
        assert _index_count(partition) == 0
        assert _pending_keys(partition) == {
            b"pfx|" + json_dumps(f"k{i}") for i in range(4)
        }
        partition.close()

    # (ii) same store, flag SET → adopts (flip + verbatim values + index +
    # discard + one INFO). Cannot run on HEAD (the flag does not exist there).
    def test_100pct_with_flag_adopts(self, tmp_path, caplog):
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        keys = {f"k{i}": f"v{i}" for i in range(4)}
        msgs = [_v3240_msg(k, v, expiry) for k, v in keys.items()]

        partition = _rocksdb_partition(
            tmp_path,
            name="withflag",
            options=RocksDBOptions(adopt_v3240_stamps=True),
            changelog_producer=producer,
        )
        with caplog.at_level(logging.INFO):
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

        assert partition.uses_ttl_stamps is True
        # Values read back STRIPPED to the original user payload.
        for k, v in keys.items():
            assert _read_via_tx(partition, k, timestamp=NOW_MS) == v
        # One index entry per non-sentinel stamp; census discarded.
        assert _index_count(partition) == 4
        assert _pending_keys(partition) == set()
        # An INFO adoption log was emitted.
        assert any(
            r.levelno >= logging.INFO and "adopted" in r.getMessage().lower()
            for r in caplog.records
        )
        partition.close()

    # (iii) set_bytes big-endian-int false-positive store, NO flag → untouched
    # byte-identical (the previously-missing regression). RED on HEAD.
    def test_set_bytes_false_positive_no_flag_untouched(self, tmp_path, caplog):
        producer = _make_producer()
        # Legacy values written via set_bytes(): a leading big-endian uint64 in
        # the plausible range so each passes _safe_decode_stamp → 100% quorum.
        originals = {}
        msgs = []
        for i in range(5):
            raw_key = b"pfx|" + json_dumps(f"c{i}")
            value = struct.pack(">Q", NOW_MS - i) + f"-counter-{i}".encode()
            originals[raw_key] = value
            msgs.append((raw_key, value, False))

        partition = _rocksdb_partition(
            tmp_path, name="falsepos", changelog_producer=producer
        )
        with caplog.at_level(logging.CRITICAL):
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

        # No flip, CRITICAL logged.
        assert partition.uses_ttl_stamps is False
        assert _critical_names_flag(caplog)
        # Every value reads back byte-identical through the user path — no prefix
        # is stripped because the store never flipped.
        for i in range(5):
            val = _read_bytes_via_tx(partition, f"c{i}", timestamp=NOW_MS)
            assert val == originals[b"pfx|" + json_dumps(f"c{i}")]
        # No __ttl_index__ writes and no changelog tombstone (value=None) produced.
        assert _index_count(partition) == 0
        assert all(
            call.kwargs.get("value") is not None
            for call in producer.produce.call_args_list
        )
        partition.close()

    # (iv) single-bad-value veto still holds WITH the flag set: quorum fails, so
    # the gate is not entered — no flip, no CRITICAL, census discarded.
    def test_single_bad_value_veto_with_flag(self, tmp_path, caplog):
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]
        # One genuine short legacy value that cannot be a valid stamp.
        msgs.append((b"pfx|" + json_dumps("k_legacy"), b"short", False))

        partition = _rocksdb_partition(
            tmp_path,
            name="veto",
            options=RocksDBOptions(adopt_v3240_stamps=True),
            changelog_producer=producer,
        )
        with caplog.at_level(logging.CRITICAL):
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

        assert partition.uses_ttl_stamps is False
        # The gate `if` was never entered, so no CRITICAL fired.
        assert not _critical_names_flag(caplog)
        # Pure-legacy disposition: census discarded.
        assert _pending_keys(partition) == set()
        partition.close()

    # (v) adoption is chunked: with the flag set and a small chunk size, adoption
    # commits in more than one batch.
    def test_adoption_is_chunked(self, tmp_path):
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        n = 5
        chunk_size = 2
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(n)]

        partition = _rocksdb_partition(
            tmp_path,
            name="chunked",
            options=RocksDBOptions(
                adopt_v3240_stamps=True, legacy_backfill_chunk_size=chunk_size
            ),
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
        # One durable flip-metadata write + one commit per adoption chunk.
        assert writes["n"] == expected_chunks + 1
        # Fully adopted: flipped, index complete, census drained.
        assert partition.uses_ttl_stamps is True
        assert _index_count(partition) == n
        assert _pending_keys(partition) == set()
        partition.close()

    # Sanity: a sentinel-only v3.24.0 store with the flag adopts with NO index
    # entries (sentinel records skip the index) — guards the chunk-skip branch.
    def test_sentinel_only_with_flag_adopts_no_index(self, tmp_path):
        producer = _make_producer()
        msgs = []
        keys = {f"k{i}": f"v{i}" for i in range(3)}
        for k, v in keys.items():
            raw_key = b"pfx|" + json_dumps(k)
            msgs.append(
                (raw_key, encode_ttl_value(SENTINEL_NEVER, json_dumps(v)), False)
            )

        partition = _rocksdb_partition(
            tmp_path,
            name="sentinel",
            options=RocksDBOptions(adopt_v3240_stamps=True),
            changelog_producer=producer,
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        assert partition.uses_ttl_stamps is True
        assert _index_count(partition) == 0
        assert _pending_keys(partition) == set()
        for k, v in keys.items():
            assert _read_via_tx(partition, k, timestamp=NOW_MS) == v
        partition.close()
