"""
Unit tests for v3.24.0-upgrade-and-recovery-clock scenarios not already covered by
the black-box contract suite or existing unit tests.

Covers:
- Sentinel-only v3.24.0 store adoption.
- Quorum-fail (one non-validating value blocks adoption).
- Done-flag idempotency (second restore no-ops).
- Done-flag back-compat (marker-absent falls back to header/pending).
- Memory parity mirrors.
"""

import logging
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_ttl_value,
    encode_ttl_value,
)
from quixstreams.state.serialization import int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_producer_mock():
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


def _replay(partition, msgs, now_ms=None):
    """Replay ``(key, value, cf_name, ttl_stamped)`` into a partition."""
    if now_ms is not None:
        partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, cf_name, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name=cf_name,
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


def _replay_default(partition, msgs, now_ms=None):
    """Replay ``(key, value, ttl_stamped)`` default-CF messages."""
    full = [(k, v, "default", s) for k, v, s in msgs]
    _replay(partition, full, now_ms=now_ms)


def _v3240_msg(key_str, user_value, expiry_ms, prefix=b"pfx"):
    """Build one v3.24.0-style default-CF changelog message (no header)."""
    raw_key = prefix + b"|" + json_dumps(key_str)
    stamped = encode_ttl_value(expiry_ms, json_dumps(user_value))
    return (raw_key, stamped, False)


def _done_flag_msg():
    """Build one done-flag marker message for the __ttl_system__ CF."""
    from quixstreams.state.rocksdb.metadata import STATE_FORMAT_VERSION

    return (
        TTL_MIGRATION_DONE_KEY,
        int_to_bytes(STATE_FORMAT_VERSION),
        TTL_SYSTEM_CF_NAME,
        False,
    )


def _decode_default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: decode_ttl_value(value) for key, value in cf.items()}


def _read_via_tx(partition, key, prefix=b"pfx", timestamp=None):
    tx = partition.begin()
    return tx.get(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


# ===========================================================================
# Sentinel-only v3.24.0 store adoption
# ===========================================================================


class TestSentinelOnlyAdoption:
    def test_sentinel_only_store_adopted_as_never_expire(self, tmp_path, caplog):
        """A v3.24.0 store where every value is SENTINEL|value adopts
        correctly as never-expire. No index entries are created (sentinel
        records skip the index). Reads return the original payloads."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        msgs = []
        keys = {}
        for i in range(4):
            key = f"k{i}"
            val = f"v{i}"
            keys[key] = val
            raw_key = b"pfx|" + json_dumps(key)
            stamped = encode_ttl_value(SENTINEL_NEVER, json_dumps(val))
            msgs.append((raw_key, stamped, False))

        partition = _rocksdb_partition(
            tmp_path,
            name="sentinel",
            # v3.24.0 adoption is opt-in; assert the adopt behavior with the flag
            # set (the no-flag path is covered in test_v3240_adoption_opt_in).
            options=RocksDBOptions(adopt_v3240_stamps=True),
            changelog_producer=producer,
        )
        with caplog.at_level(logging.INFO):
            _replay_default(partition, msgs, now_ms=now_ms)
            partition.complete_recovery()

        assert partition.uses_ttl_stamps is True, "Partition must flip after adoption"

        # Every value readable as original payload (never-expire).
        for key, expected in keys.items():
            val = _read_via_tx(partition, key, timestamp=now_ms)
            assert val == expected, f"Sentinel key {key!r} should read {expected!r}"

        # No index entries (sentinel records skip the index).
        index_cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        index_count = sum(1 for _ in index_cf.keys())
        assert index_count == 0, "Sentinel-only store should have 0 index entries"

        # INFO adoption log emitted.
        assert any(
            "adopted" in r.message.lower() and r.levelno >= logging.INFO
            for r in caplog.records
        ), "Adoption INFO log must be emitted for sentinel-only store"
        partition.close()


# ===========================================================================
# Quorum-fail: one non-validating value blocks adoption
# ===========================================================================


class TestQuorumFail:
    def test_one_non_validating_value_blocks_adoption(self, tmp_path):
        """When one pending value fails strict stamp validation, the
        whole store stays verbatim (no adoption, no flip). The v3.24.0 subset
        remains prefix-corrupt (a documented residual)."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        expiry = now_ms + 7 * DAY_MS

        msgs = []
        # 4 valid v3.24.0-stamped records.
        for i in range(4):
            msgs.append(_v3240_msg(f"k{i}", f"v{i}", expiry))
        # 1 genuine legacy record (short value, cannot be a valid stamp).
        legacy_key = b"pfx|" + json_dumps("k_legacy")
        msgs.append((legacy_key, b"short", False))

        partition = _rocksdb_partition(
            tmp_path, name="qfail", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        # Partition must NOT have flipped (quorum failed).
        assert (
            partition.uses_ttl_stamps is False
        ), "Quorum-fail: partition must stay legacy when any value fails validation"
        # Pending census discarded.
        assert (
            _pending_keys(partition) == set()
        ), "Pending census must be discarded after quorum-fail"
        partition.close()


# ===========================================================================
# Done-flag idempotency: second restore no-ops
# ===========================================================================


class TestDoneFlagIdempotency:
    def test_second_restore_after_done_flag_noop(self, tmp_path, caplog):
        """A second cold restore of a changelog that contains the
        done-flag marker must flip the partition via the marker path and run
        NO backfill/completion (no-op). No adoption, no pending census."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        expiry = now_ms + 30 * DAY_MS

        # Build a changelog that a completed migration would produce:
        # all stamped + header-true default-CF records, plus the done-flag.
        msgs = []
        for i in range(3):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            stamped = encode_ttl_value(expiry, json_dumps(f"v{i}"))
            msgs.append((raw_key, stamped, "default", True))
        msgs.append(_done_flag_msg())

        # First restore.
        p1 = _rocksdb_partition(tmp_path, name="r1", changelog_producer=producer)
        _replay(p1, msgs, now_ms=now_ms)
        p1.complete_recovery()
        assert p1.uses_ttl_stamps is True
        assert _pending_keys(p1) == set()
        p1.close()

        # Second restore into a fresh partition.
        caplog.clear()
        p2 = _rocksdb_partition(tmp_path, name="r2", changelog_producer=producer)
        with caplog.at_level(logging.INFO):
            _replay(p2, msgs, now_ms=now_ms)
            p2.complete_recovery()

        assert p2.uses_ttl_stamps is True, "Second restore must flip via done-flag"
        assert _pending_keys(p2) == set(), "No pending census after done-flag path"

        # No adoption or completion logs (only the done-flag path).
        backfill_logs = [
            r
            for r in caplog.records
            if ("backfill" in r.message.lower() or "adopted" in r.message.lower())
            and r.levelno >= logging.INFO
        ]
        assert len(backfill_logs) == 0, (
            f"Second restore via done-flag must not run any backfill/adoption, "
            f"but got: {[r.message for r in backfill_logs]}"
        )

        # Values still intact via transaction reads.
        for i in range(3):
            val = _read_via_tx(p2, f"k{i}", timestamp=now_ms)
            assert val == f"v{i}", f"Key k{i} must be readable after second restore"
        p2.close()

    def test_done_marker_discard_logs_info(self, tmp_path, caplog):
        """The done-marker short-circuit is the only census discard path that was
        silent. Replay a done-marker plus a header-absent orphan (re-censused into
        __ttl_backfill_pending__); complete_recovery must log ONE INFO naming the
        discarded count.

        RED (HEAD): no discard INFO on the done-marker path.
        GREEN: one INFO with the discarded count; store flipped + census empty."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000

        # A header-absent orphan legacy record (which the census gate PUTs into the
        # pending CF) followed by the done-marker. Flag-last ordering is realistic
        # (the marker is always produced last) AND required after #9 (review batch
        # 3): the census short-circuit skips any record replayed AFTER the marker,
        # so the orphan must precede it to be censused (then discarded by
        # complete_recovery on the done-marker branch).
        orphan_key = b"pfx|" + json_dumps("orphan")
        msgs = [
            (orphan_key, b"orphan-legacy-value", "default", False),
            _done_flag_msg(),
        ]

        partition = _rocksdb_partition(
            tmp_path, name="donediscard", changelog_producer=producer
        )
        _replay(partition, msgs, now_ms=now_ms)
        assert _pending_keys(partition) == {orphan_key}, "orphan must be censused"

        with caplog.at_level(logging.INFO):
            partition.complete_recovery()

        discard_logs = [
            r
            for r in caplog.records
            if "done-marker present" in r.message and r.levelno == logging.INFO
        ]
        assert len(discard_logs) == 1, (
            f"exactly one done-marker discard INFO expected, got "
            f"{[r.message for r in discard_logs]}"
        )
        assert "discarding 1 orphan" in discard_logs[0].getMessage()
        # Store flipped via the marker, census drained, no spurious backfill.
        assert partition.uses_ttl_stamps is True
        assert _pending_keys(partition) == set()
        assert not any("RESUME STARTED" in r.message for r in caplog.records)
        partition.close()


# ===========================================================================
# Done-flag back-compat: marker-absent changelog
# ===========================================================================


class TestDoneFlagBackCompat:
    def test_marker_absent_falls_back_to_header_pending_logic(self, tmp_path):
        """A pre-marker this-branch changelog (stamped + header-true
        default-CF records, NO done-flag marker) recovers via the existing
        header/pending logic unchanged. The partition flips on the first
        header-true record, completes recovery normally."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        expiry = now_ms + 30 * DAY_MS

        # All stamped + header-true, NO done-flag.
        msgs = []
        for i in range(3):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            stamped = encode_ttl_value(expiry, json_dumps(f"v{i}"))
            msgs.append((raw_key, stamped, "default", True))

        partition = _rocksdb_partition(
            tmp_path, name="backcompat", changelog_producer=producer
        )
        _replay(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        # Flipped via header path (not done-flag path).
        assert partition.uses_ttl_stamps is True
        assert (
            partition._recovery_saw_migration_done is False
        ), "Marker-absent changelog must NOT set _recovery_saw_migration_done"
        # Values intact.
        for i in range(3):
            val = _read_via_tx(partition, f"k{i}", timestamp=now_ms)
            assert (
                val == f"v{i}"
            ), f"Key k{i} must be readable after header-based recovery"
        partition.close()


# ===========================================================================
# Memory parity mirrors
# ===========================================================================


def _replay_memory(partition, msgs, now_ms=None):
    """Replay ``(key, value, cf_name, ttl_stamped)`` into a memory partition."""
    if now_ms is not None:
        partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, cf_name, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name=cf_name,
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


def _read_memory_tx(partition, key, prefix=b"pfx", timestamp=None):
    tx = partition.begin()
    return tx.get(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)


class TestMemoryV3240Adoption:
    """Memory v3.24.0 adoption is OPT-IN, mirroring the RocksDB
    ``adopt_v3240_stamps`` contract. Split from the former single
    auto-adopt assertion into (a) no-flag detection-only, (b) flag adopts, and
    (c) a ``set_bytes`` false-positive that must stay untouched."""

    def _all_stamped_msgs(self, now_ms):
        expiry = now_ms + 7 * DAY_MS
        msgs = []
        raw_by_key = {}
        payload = {}
        for i in range(4):
            key = f"k{i}"
            val = f"v{i}"
            raw_key = b"pfx|" + json_dumps(key)
            stamped = encode_ttl_value(expiry, json_dumps(val))
            msgs.append((raw_key, stamped, "default", False))
            raw_by_key[raw_key] = stamped
            payload[key] = val
        return msgs, raw_by_key, payload

    def test_no_flag_stays_legacy_and_criticals(self, caplog):
        """(a) A default-constructed memory partition replaying an all-stamped
        header-absent changelog must NOT flip.

        RED (HEAD): the memory backend auto-adopts (``uses_ttl_stamps`` True) and
        an ``adopted`` INFO is logged.
        GREEN: stays legacy, a CRITICAL naming ``adopt_v3240_stamps`` is logged,
        every value is byte-identical, and the census is discarded."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        msgs, raw_by_key, _ = self._all_stamped_msgs(now_ms)

        partition = MemoryStorePartition(changelog_producer=producer)
        with caplog.at_level(logging.INFO):
            _replay_memory(partition, msgs, now_ms=now_ms)
            partition.complete_recovery()

        assert partition.uses_ttl_stamps is False, "no-flag memory must stay legacy"
        assert any(
            r.levelno >= logging.CRITICAL and "adopt_v3240_stamps" in r.message
            for r in caplog.records
        ), "a CRITICAL naming adopt_v3240_stamps must be logged"
        assert not any(
            "adopted" in r.message.lower() and r.levelno >= logging.INFO
            for r in caplog.records
        ), "no adoption may happen without the flag"
        # Byte-identical: every value untouched in the store.
        default = partition._state.get("default", {})
        for raw_key, stamped in raw_by_key.items():
            assert default.get(raw_key) == stamped, "values must be byte-identical"
        # Census discarded.
        assert not partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        partition.close()

    def test_flag_adopts(self, caplog):
        """(b) The ported happy path: ``adopt_v3240_stamps=True`` adopts — values
        read back stripped, partition flipped, INFO adoption log emitted."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        msgs, _, payload = self._all_stamped_msgs(now_ms)

        partition = MemoryStorePartition(
            changelog_producer=producer, adopt_v3240_stamps=True
        )
        with caplog.at_level(logging.INFO):
            _replay_memory(partition, msgs, now_ms=now_ms)
            partition.complete_recovery()

        assert partition.uses_ttl_stamps is True, "flag must adopt (flip)"
        for key, expected in payload.items():
            val = _read_memory_tx(partition, key, timestamp=now_ms)
            assert val == expected, f"adopted record {key!r}: expected {expected!r}"
        assert any(
            "adopted" in r.message.lower() and r.levelno >= logging.INFO
            for r in caplog.records
        ), "adoption INFO log must be emitted with the flag set"
        # Read-only property mirrors the RocksDB surface.
        assert partition.adopt_v3240_stamps is True
        partition.close()

    def test_set_bytes_false_positive_no_flag_untouched(self, caplog):
        """(c) A legacy ``set_bytes`` store whose values begin with plausible
        big-endian 8-byte prefixes (``struct.pack(">Q", n) + bytes``) replays.

        RED (HEAD): memory auto-flips → the store enters TTL mode.
        GREEN: no flip, a CRITICAL is logged, every value is byte-identical, and
        NO changelog tombstone (``value=None``) is ever produced."""
        import struct

        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000

        msgs = []
        raw_by_key = {}
        for i in range(4):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            # A counter/epoch-ms value written via set_bytes() — its first 8 bytes
            # decode as a plausible stamp, but it is genuine user data.
            set_bytes_value = struct.pack(">Q", now_ms + i) + f"user-data-{i}".encode()
            msgs.append((raw_key, set_bytes_value, "default", False))
            raw_by_key[raw_key] = set_bytes_value

        partition = MemoryStorePartition(changelog_producer=producer)
        with caplog.at_level(logging.INFO):
            _replay_memory(partition, msgs, now_ms=now_ms)
            partition.complete_recovery()

        assert partition.uses_ttl_stamps is False, "set_bytes store must NOT flip"
        assert any(
            r.levelno >= logging.CRITICAL and "adopt_v3240_stamps" in r.message
            for r in caplog.records
        )
        default = partition._state.get("default", {})
        for raw_key, value in raw_by_key.items():
            assert default.get(raw_key) == value, "set_bytes values byte-identical"
        # No changelog tombstone was produced (data is not deleted).
        produced_none = [
            call
            for call in producer.produce.call_args_list
            if call.kwargs.get("value") is None
        ]
        assert produced_none == [], "no tombstone may be produced for a legacy store"
        partition.close()


class TestMemoryQuorumFail:
    """Memory parity mirror for quorum-fail blocking adoption."""

    def test_memory_quorum_fail_stays_legacy(self):
        """Memory mirror: one non-validating value blocks adoption in
        the memory backend. Store stays legacy, pending discarded."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        expiry = now_ms + 7 * DAY_MS

        msgs = []
        for i in range(3):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            stamped = encode_ttl_value(expiry, json_dumps(f"v{i}"))
            msgs.append((raw_key, stamped, "default", False))
        # One genuine legacy record (too short for a valid stamp).
        legacy_key = b"pfx|" + json_dumps("k_legacy")
        msgs.append((legacy_key, b"tiny", "default", False))

        partition = MemoryStorePartition(changelog_producer=producer)
        _replay_memory(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        assert (
            partition.uses_ttl_stamps is False
        ), "Memory quorum-fail: partition must stay legacy"
        pending = partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        assert len(pending) == 0, "Pending census must be discarded after quorum-fail"
        partition.close()


# ===========================================================================
# Marker-production crash window regression (changelog-first ordering)
# ===========================================================================


class TestMarkerFlushFailure:
    """Regression: if the changelog flush during done-flag marker production
    fails, the completion must fail loudly (exception surfaces) and the local
    store must NOT be marked migration-done, so a subsequent retry succeeds."""

    def _build_mixed_changelog(self, now_ms):
        """Build a MIXED changelog (stamped + legacy leftovers) that triggers
        recovery completion."""
        expiry = now_ms + 30 * DAY_MS
        msgs = []
        legacy = {}
        for i in range(2):
            raw_key = b"pfx|" + json_dumps(f"s{i}")
            stamped = encode_ttl_value(expiry, json_dumps(f"sv{i}"))
            msgs.append((raw_key, stamped, "default", True))
        for i in range(2):
            raw_key = b"pfx|" + json_dumps(f"l{i}")
            raw_val = json_dumps(f"lv{i}")
            legacy[f"l{i}"] = f"lv{i}"
            msgs.append((raw_key, raw_val, "default", False))
        return msgs, legacy

    def test_rocksdb_flush_failure_surfaces_and_no_local_done(self, tmp_path):
        """Changelog flush failure during marker production must raise and
        must NOT leave the local store marked migration-done, so a retry
        with a working producer succeeds and marks done."""
        import pytest

        now_ms = 1_780_000_000_000
        msgs, legacy = self._build_mixed_changelog(now_ms)

        # Producer whose flush raises on the marker production attempt.
        producer = _make_producer_mock()
        flush_count = {"n": 0}
        real_flush = producer.flush

        def failing_flush(*a, **kw):
            flush_count["n"] += 1
            # The completion backfill flushes chunks first; fail only on the
            # marker flush (the last one in complete_recovery).
            if flush_count["n"] >= 2:
                raise RuntimeError("simulated changelog flush failure")
            return real_flush(*a, **kw)

        producer.flush = failing_flush

        partition = _rocksdb_partition(
            tmp_path,
            name="flush_fail",
            options=RocksDBOptions(legacy_records_ttl=None),
            changelog_producer=producer,
        )
        _replay(partition, msgs, now_ms=now_ms)

        # (a) Completion must fail loudly.
        with pytest.raises(RuntimeError, match="simulated changelog flush failure"):
            partition.complete_recovery()

        # (b) Local store must NOT be marked migration-done. Read and
        # release the CF reference before close (Windows RocksDB lock).
        system_cf = partition.get_or_create_column_family(TTL_SYSTEM_CF_NAME)
        local_marker = system_cf.get(TTL_MIGRATION_DONE_KEY, default=None)
        del system_cf
        assert local_marker is None, (
            "After a failed changelog flush the local store must NOT have the "
            "migration-done marker; the once-only guarantee requires changelog "
            "confirmation before local commit."
        )
        # Must close before reopening (RocksDB file lock).
        partition.close()

        # Retry with a working producer succeeds.
        producer2 = _make_producer_mock()
        partition2 = _rocksdb_partition(
            tmp_path,
            name="flush_fail",
            options=RocksDBOptions(legacy_records_ttl=None),
            changelog_producer=producer2,
        )
        _replay(partition2, msgs, now_ms=now_ms)
        partition2.complete_recovery()  # must not raise

        system_cf2 = partition2.get_or_create_column_family(TTL_SYSTEM_CF_NAME)
        local_marker2 = system_cf2.get(TTL_MIGRATION_DONE_KEY, default=None)
        assert (
            local_marker2 is not None
        ), "Retry with a working producer must successfully mark migration-done"
        partition2.close()

    def test_memory_flush_failure_surfaces_and_no_local_done(self):
        """Memory mirror: changelog flush failure during marker production
        must raise and must NOT leave the in-RAM store marked done."""
        import pytest

        now_ms = 1_780_000_000_000
        msgs, _ = self._build_mixed_changelog(now_ms)

        producer = _make_producer_mock()
        flush_count = {"n": 0}

        def failing_flush(*a, **kw):
            flush_count["n"] += 1
            if flush_count["n"] >= 2:
                raise RuntimeError("simulated changelog flush failure")

        producer.flush = failing_flush

        partition = MemoryStorePartition(changelog_producer=producer)
        _replay_memory(partition, msgs, now_ms=now_ms)

        with pytest.raises(RuntimeError, match="simulated changelog flush failure"):
            partition.complete_recovery()

        # Local RAM must NOT have the marker.
        system = partition._state.get(TTL_SYSTEM_CF_NAME, {})
        assert TTL_MIGRATION_DONE_KEY not in system, (
            "After a failed changelog flush the in-RAM store must NOT have the "
            "migration-done marker."
        )
        partition.close()


class TestMemoryDoneFlag:
    """Memory parity mirror for the done-flag being consumed on recovery."""

    def test_memory_done_flag_flips_and_noop(self):
        """Memory mirror: replaying a changelog with the done-flag
        marker flips the memory partition and complete_recovery is a no-op
        (no backfill/completion runs)."""
        producer = _make_producer_mock()
        now_ms = 1_780_000_000_000
        expiry = now_ms + 30 * DAY_MS
        from quixstreams.state.rocksdb.metadata import STATE_FORMAT_VERSION

        msgs = []
        for i in range(3):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            stamped = encode_ttl_value(expiry, json_dumps(f"v{i}"))
            msgs.append((raw_key, stamped, "default", True))
        # Done-flag marker.
        msgs.append(
            (
                TTL_MIGRATION_DONE_KEY,
                int_to_bytes(STATE_FORMAT_VERSION),
                TTL_SYSTEM_CF_NAME,
                False,
            )
        )

        partition = MemoryStorePartition(changelog_producer=producer)
        _replay_memory(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        assert (
            partition.uses_ttl_stamps is True
        ), "Memory partition must flip via done-flag"
        assert partition._recovery_saw_migration_done is True
        pending = partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        assert len(pending) == 0, "No pending census after done-flag path"

        for i in range(3):
            val = _read_memory_tx(partition, f"k{i}", timestamp=now_ms)
            assert (
                val == f"v{i}"
            ), f"Memory key k{i} must be readable after done-flag restore"
        partition.close()
