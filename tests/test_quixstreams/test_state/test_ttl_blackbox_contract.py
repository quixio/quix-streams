"""
Black-box acceptance suite for the state-TTL / migration feature.

Two classes partition the tests by current expectation:

- ``TestContractHolds`` -- must be GREEN today. These are the regression gate:
  passing on every PR is a hard requirement.
- ``TestContractTargets`` -- expected RED today. Each test demonstrates a real
  defect whose failure message describes the corruption / mis-behavior clearly.
  These become green as ArchDev ships the fixes.

Every test operates through the public partition / transaction surface (values
read back, expiry outcomes, exceptions, WARNING logs via ``caplog``). The only
internal peek is scenario H (no-residue: pending CF empty/absent).
"""

import logging
from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
    TTL_BACKFILL_PENDING_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import (
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_changelog_producer_mock():
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


def _seed_legacy(partition, records, prefix=b"pfx"):
    """Write plain (un-stamped) records to a legacy partition."""
    with partition.begin() as tx:
        for key, value in records:
            tx.set(key=key, value=value, prefix=prefix)


def _read_via_tx(partition, key, prefix=b"pfx", timestamp=None):
    """Read a value through a fresh transaction (the public read path)."""
    tx = partition.begin()
    result = tx.get(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)
    return result


def _read_via_tx_exists(partition, key, prefix=b"pfx", timestamp=None):
    """Check existence through a fresh transaction."""
    tx = partition.begin()
    return tx.exists(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)


def _replay_msgs(partition, msgs, now_ms=None):
    """Replay ``(key, value, ttl_stamped)`` into a partition."""
    if now_ms is not None:
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


def _mixed_changelog(n_stamped, n_legacy, stamp_expiry):
    """Build a MIXED changelog with disjoint keys."""
    msgs = []
    legacy_values = {}
    for i in range(n_stamped):
        key = f"pfx|s{i}".encode()
        value = encode_ttl_value(stamp_expiry, f"stamped-{i}".encode())
        msgs.append((key, value, True))
    for i in range(n_legacy):
        key = f"pfx|l{i}".encode()
        raw = f"legacy-payload-{i}".encode()
        legacy_values[key] = raw
        msgs.append((key, raw, False))
    return msgs, legacy_values


def _decode_default_cf_rocksdb(partition):
    """Return ``{raw_key: (expires_at, payload)}`` for the default CF."""
    cf = partition.get_or_create_column_family("default")
    return {key: decode_ttl_value(value) for key, value in cf.items()}


def _decode_default_cf_memory(partition):
    """Return ``{raw_key: (expires_at, payload)}`` for default CF in memory."""
    out = {}
    for key, value in partition._state.get("default", {}).items():
        try:
            out[key] = decode_ttl_value(value)
        except ValueError:
            out[key] = (None, value)
    return out


def _pending_keys_rocksdb(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _pending_keys_memory(partition):
    return set(partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {}).keys())


# ===================================================================
# TestContractHolds -- must all be GREEN today
# ===================================================================


class TestContractHolds:
    """Scenarios that must pass on the current branch. Failing here = regression."""

    # ---------------------------------------------------------------
    # A: Fresh store TTL basics
    # ---------------------------------------------------------------

    def test_a1_ttl_expires_after_event_time(self, tmp_path):
        """A.1: A key written with ttl= on a fresh store expires when the
        partition's event-time high-water passes the expiry."""
        partition = _rocksdb_partition(tmp_path)
        ts = 1_000_000_000_000
        ttl = timedelta(days=1)
        with partition.begin() as tx:
            tx.set(key="k1", value="v1", prefix=b"pfx", timestamp=ts, ttl=ttl)

        # Readable immediately at the same timestamp (not expired).
        val = _read_via_tx(partition, "k1", timestamp=ts)
        assert val == "v1", "Value should be readable before expiry"

        # Readable just before expiry.
        val = _read_via_tx(partition, "k1", timestamp=ts + DAY_MS - 1)
        assert val == "v1", "Value should be readable just before expiry"

        # Expired at exactly expiry time (stamp <= now).
        val = _read_via_tx(partition, "k1", timestamp=ts + DAY_MS)
        assert val is None, "Value should be expired at ts + ttl"
        partition.close()

    def test_a2_plain_set_never_expires(self, tmp_path):
        """A.2: A key written with plain set() (no ttl=) never expires,
        even when the high-water moves far into the future."""
        partition = _rocksdb_partition(tmp_path)
        ts = 1_000_000_000_000
        # Flip the store into TTL mode with a TTL write first.
        with partition.begin() as tx:
            tx.set(
                key="trigger",
                value="x",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )
            tx.set(key="permanent", value="stays", prefix=b"pfx", timestamp=ts)

        # Far future: the ttl key is gone, the plain key survives.
        far_future = ts + 365 * DAY_MS
        val = _read_via_tx(partition, "permanent", timestamp=far_future)
        assert val == "stays", "Plain set() should never expire"
        val = _read_via_tx(partition, "trigger", timestamp=far_future)
        assert val is None, "TTL key should have expired by now"
        partition.close()

    def test_a3_refresh_before_expiry_extends(self, tmp_path):
        """A.3: Re-writing a key with a new ttl= before expiry extends the
        effective lifetime; the old expiry does not apply."""
        partition = _rocksdb_partition(tmp_path)
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="k1", value="v1", prefix=b"pfx", timestamp=ts, ttl=timedelta(days=1)
            )

        # Re-write at ts+0.5 day with a 2-day ttl -> new expiry = ts + 0.5d + 2d.
        ts2 = ts + DAY_MS // 2
        with partition.begin() as tx:
            tx.set(
                key="k1",
                value="v1-refreshed",
                prefix=b"pfx",
                timestamp=ts2,
                ttl=timedelta(days=2),
            )

        # At old expiry time (ts + 1 day), the refreshed key must still exist.
        val = _read_via_tx(partition, "k1", timestamp=ts + DAY_MS)
        assert val == "v1-refreshed", "Refreshed key must survive past old expiry"

        # At new expiry time it should be gone.
        new_expiry = ts2 + 2 * DAY_MS
        val = _read_via_tx(partition, "k1", timestamp=new_expiry)
        assert val is None, "Refreshed key should expire at new expiry"
        partition.close()

    # ---------------------------------------------------------------
    # B: Legacy store + legacy_records_ttl configured
    # ---------------------------------------------------------------

    def test_b1_legacy_values_readable_immediately_after_migration(self, tmp_path):
        """B.1: After seeding a legacy store and then writing with ttl= and
        legacy_records_ttl configured, all old values are readable immediately."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(partition, [("old1", "val1"), ("old2", "val2")])
        partition.close()

        partition = _rocksdb_partition(
            tmp_path, options=RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        )
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="new1",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        # Old values readable.
        val1 = _read_via_tx(partition, "old1", timestamp=ts)
        val2 = _read_via_tx(partition, "old2", timestamp=ts)
        assert val1 == "val1", "Legacy record old1 must be intact after migration"
        assert val2 == "val2", "Legacy record old2 must be intact after migration"
        partition.close()

    def test_b2_legacy_values_expire_after_configured_ttl(self, tmp_path):
        """B.2: Legacy values expire after the configured legacy_records_ttl
        window relative to enable_time (event-time high-water)."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(partition, [("old1", "val1")])
        partition.close()

        legacy_ttl = timedelta(days=7)
        partition = _rocksdb_partition(
            tmp_path, options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="new1",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=30),
            )

        # Before legacy expiry: readable.
        val = _read_via_tx(partition, "old1", timestamp=ts + 6 * DAY_MS)
        assert val == "val1", "Legacy record should survive before its ttl"

        # After legacy expiry: gone.
        val = _read_via_tx(partition, "old1", timestamp=ts + 7 * DAY_MS)
        assert val is None, "Legacy record should expire after legacy_records_ttl"
        partition.close()

    def test_b3_new_writes_follow_per_write_ttl(self, tmp_path):
        """B.3: New writes on a migrated store follow their own per-write ttl,
        not the legacy_records_ttl."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(partition, [("old1", "val1")])
        partition.close()

        partition = _rocksdb_partition(
            tmp_path, options=RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        )
        ts = 1_000_000_000_000
        new_ttl = timedelta(days=2)
        with partition.begin() as tx:
            tx.set(key="new1", value="vnew", prefix=b"pfx", timestamp=ts, ttl=new_ttl)

        # New record has its own 2-day expiry, not the 7-day legacy window.
        val = _read_via_tx(partition, "new1", timestamp=ts + 2 * DAY_MS - 1)
        assert val == "vnew", "New write should survive before its own ttl"
        val = _read_via_tx(partition, "new1", timestamp=ts + 2 * DAY_MS)
        assert val is None, "New write should expire at its own ttl"
        partition.close()

    # ---------------------------------------------------------------
    # C: Legacy store + NO legacy_records_ttl
    # ---------------------------------------------------------------

    def test_c1_migration_completes_without_exception(self, tmp_path):
        """C.1: A populated legacy store + ttl= write WITHOUT legacy_records_ttl
        completes the migration (no exception). The old records get stamped."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(partition, [("old1", "val1")])
        partition.close()

        partition = _rocksdb_partition(tmp_path, options=RocksDBOptions())
        ts = 1_000_000_000_000
        # Must NOT raise.
        with partition.begin() as tx:
            tx.set(
                key="new1",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )
        assert partition.uses_ttl_stamps is True
        partition.close()

    def test_c2_warning_emitted_no_config(self, tmp_path, caplog):
        """C.2: When legacy_records_ttl is absent, a WARNING is emitted during
        the auto-backfill to inform the operator of the implicit window."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(partition, [("old1", "val1")])
        partition.close()

        partition = _rocksdb_partition(tmp_path, options=RocksDBOptions())
        ts = 1_000_000_000_000
        with caplog.at_level(
            logging.WARNING, logger="quixstreams.state.rocksdb.transaction"
        ):
            with partition.begin() as tx:
                tx.set(
                    key="new1",
                    value="vnew",
                    prefix=b"pfx",
                    timestamp=ts,
                    ttl=timedelta(days=1),
                )
        assert any(
            "legacy_records_ttl" in rec.message and rec.levelno == logging.WARNING
            for rec in caplog.records
        ), "A WARNING mentioning legacy_records_ttl should be emitted"
        partition.close()

    def test_c3_old_records_expire_after_batch_max_ttl(self, tmp_path):
        """C.3: Without legacy_records_ttl, old records expire after the
        triggering batch's max ttl (high_water + max(ttl=))."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(partition, [("old1", "val1")])
        partition.close()

        partition = _rocksdb_partition(tmp_path, options=RocksDBOptions())
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="new1",
                value="v1",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=3),
            )
            tx.set(
                key="new2",
                value="v2",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=5),
            )

        # The max ttl in the batch is 5 days. Legacy expiry = ts + 5 days.
        val = _read_via_tx(partition, "old1", timestamp=ts + 5 * DAY_MS - 1)
        assert val == "val1", "Legacy record should survive before max batch ttl"
        val = _read_via_tx(partition, "old1", timestamp=ts + 5 * DAY_MS)
        assert val is None, "Legacy record should expire at high_water + max(ttl=)"
        partition.close()

    # ---------------------------------------------------------------
    # D: Crash mid-migration + interleaved writes + delete + retry
    # ---------------------------------------------------------------

    def test_d_crash_midmigration_retry_converges(self, tmp_path):
        """D: Crash mid-migration (simulated), interleave a plain write AND a
        delete, then retry via a fresh ttl= write. Every surviving key must be
        readable and intact, the interleaved key gets stamped, no double-wrap."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(
            partition,
            [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5")],
        )
        partition.close()

        ttl = timedelta(days=7)
        chunk_size = 2
        partition = _rocksdb_partition(
            tmp_path,
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        # Inject crash after the first chunk write.
        crash = {"n": 0}
        real_write = partition._write

        def crashing_write(batch):
            crash["n"] += 1
            if crash["n"] > 1:
                raise RuntimeError("simulated crash mid-backfill")
            return real_write(batch)

        partition._write = crashing_write
        ts = 1_000_000_000_000
        with pytest.raises(RuntimeError, match="simulated crash"):
            with partition.begin() as tx:
                tx.set(key="kttl", value="vttl", prefix=b"pfx", timestamp=ts, ttl=ttl)
        partition._write = real_write
        partition.close()

        # Interleave: reopen, add a new key and delete one.
        partition = _rocksdb_partition(
            tmp_path,
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        with partition.begin() as tx:
            tx.set(key="k_interleaved", value="v_inter", prefix=b"pfx")
            tx.delete(key="k3", prefix=b"pfx")

        # Retry: a fresh ttl= write should re-run the backfill and converge.
        ts2 = ts + 1_000
        with partition.begin() as tx:
            tx.set(key="kttl", value="vttl2", prefix=b"pfx", timestamp=ts2, ttl=ttl)

        assert partition.uses_ttl_stamps is True

        # Surviving original keys: k1, k2, k4, k5 (k3 deleted).
        for key in ["k1", "k2", "k4", "k5"]:
            val = _read_via_tx(partition, key, timestamp=ts2)
            assert val is not None, f"Surviving key {key} must be readable after retry"

        # Deleted key is gone.
        val = _read_via_tx(partition, "k3", timestamp=ts2)
        assert val is None, "Deleted key k3 must stay deleted"

        # Interleaved key is readable.
        val = _read_via_tx(partition, "k_interleaved", timestamp=ts2)
        assert val == "v_inter", "Interleaved key must be stamped and readable"
        partition.close()

    # ---------------------------------------------------------------
    # E: Changelog replay of a half-migrated (MIXED) store
    # ---------------------------------------------------------------

    def test_e1_mixed_replay_with_config_completes(self, tmp_path):
        """E.1: Changelog replay of a MIXED store with legacy_records_ttl
        configured completes without error; leftovers expire per config."""
        producer = _make_changelog_producer_mock()
        now_ms = 1_780_000_000_000
        legacy_ttl = timedelta(days=7)
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(3, 4, stamp_expiry)

        partition = _rocksdb_partition(
            tmp_path,
            name="e1",
            options=RocksDBOptions(legacy_records_ttl=legacy_ttl),
            changelog_producer=producer,
        )
        _replay_msgs(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        expected_expiry = now_ms + 7 * DAY_MS
        decoded = _decode_default_cf_rocksdb(partition)
        for key, raw in legacy_values.items():
            exp, payload = decoded[key]
            assert payload == raw, f"Legacy record {key!r} payload must be intact"
            assert (
                exp == expected_expiry
            ), f"Legacy record {key!r} should expire at {expected_expiry}"
        partition.close()

    def test_e2_mixed_replay_without_config_completes(self, tmp_path):
        """E.2: Changelog replay of a MIXED store WITHOUT legacy_records_ttl
        completes without error. Leftovers get a derived expiry following
        the default-value chain: legacy_records_ttl (absent here) -> derived
        from the service's own usage (survivor-derived = max surviving future
        stamp) -> never-expires as true last resort only."""
        producer = _make_changelog_producer_mock()
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(2, 3, stamp_expiry)

        partition = _rocksdb_partition(tmp_path, name="e2", changelog_producer=producer)
        _replay_msgs(partition, msgs, now_ms=now_ms)
        # Must NOT raise.
        partition.complete_recovery()

        decoded = _decode_default_cf_rocksdb(partition)
        for key, raw in legacy_values.items():
            exp, payload = decoded[key]
            assert payload == raw, f"Legacy record {key!r} payload must be intact"
            assert (
                exp == stamp_expiry
            ), f"Legacy record {key!r} should inherit survivor-derived expiry {stamp_expiry}"
        partition.close()

    # ---------------------------------------------------------------
    # H: No-TTL store -- byte-identical round-trip, no residue
    # ---------------------------------------------------------------

    def test_h1_no_ttl_byte_identical_roundtrip(self, tmp_path):
        """H.1: A store that never uses ttl: values are byte-identical on
        round-trip via set/get."""
        partition = _rocksdb_partition(tmp_path)
        with partition.begin() as tx:
            tx.set(key="k1", value={"nested": [1, 2, 3]}, prefix=b"pfx")
            tx.set(key="k2", value="simple-string", prefix=b"pfx")

        val1 = _read_via_tx(partition, "k1")
        val2 = _read_via_tx(partition, "k2")
        assert val1 == {"nested": [1, 2, 3]}, "Structured value must round-trip exactly"
        assert val2 == "simple-string", "String value must round-trip exactly"
        partition.close()

    def test_h2_no_ttl_no_residue_after_changelog_rebuild(self, tmp_path):
        """H.2: After a changelog rebuild of a no-TTL store, no TTL
        bookkeeping residue remains (pending CF empty/absent). This is the one
        allowed internals peek."""
        producer = _make_changelog_producer_mock()
        partition = _rocksdb_partition(tmp_path, name="h2", changelog_producer=producer)
        # Replay only legacy (un-stamped) records.
        msgs = [(f"pfx|k{i}".encode(), f"value-{i}".encode(), False) for i in range(5)]
        _replay_msgs(partition, msgs, now_ms=1_780_000_000_000)
        partition.complete_recovery()

        assert (
            partition.uses_ttl_stamps is False
        ), "A pure-legacy store must not flip into TTL mode"
        # Pending CF should be empty/absent after complete_recovery.
        pending = _pending_keys_rocksdb(partition)
        assert pending == set(), (
            "After complete_recovery on a no-TTL store, __ttl_backfill_pending__ "
            "must be empty (no residue)"
        )
        partition.close()

    # ---------------------------------------------------------------
    # I-holds: Memory backend mirrors B/E outcomes
    # ---------------------------------------------------------------

    def test_i1_memory_mixed_replay_with_config(self):
        """I.1: MemoryStorePartition MIXED replay + complete_recovery with
        legacy_records_ttl configured stamps leftovers correctly."""
        producer = _make_changelog_producer_mock()
        now_ms = 1_780_000_000_000
        legacy_ttl = timedelta(days=7)
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(3, 4, stamp_expiry)

        partition = MemoryStorePartition(
            changelog_producer=producer,
            legacy_records_ttl=legacy_ttl,
        )
        _replay_msgs(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        expected_expiry = now_ms + 7 * DAY_MS
        decoded = _decode_default_cf_memory(partition)
        for key, raw in legacy_values.items():
            exp, payload = decoded[key]
            assert (
                payload == raw
            ), f"Memory legacy record {key!r} payload must be intact"
            assert (
                exp == expected_expiry
            ), f"Memory legacy record {key!r} should expire at {expected_expiry}"
        partition.close()

    def test_i2_memory_mixed_replay_without_config(self):
        """I.2: MemoryStorePartition MIXED replay + complete_recovery WITHOUT
        legacy_records_ttl: auto-finishes (no exception), stamps at derived
        expiry following the default-value chain: legacy_records_ttl (absent)
        -> survivor-derived (max surviving future stamp) -> never-expires as
        true last resort only."""
        producer = _make_changelog_producer_mock()
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(2, 3, stamp_expiry)

        partition = MemoryStorePartition(changelog_producer=producer)
        _replay_msgs(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        decoded = _decode_default_cf_memory(partition)
        for key, raw in legacy_values.items():
            exp, payload = decoded[key]
            assert (
                payload == raw
            ), f"Memory legacy record {key!r} payload must be intact"
            assert (
                exp == stamp_expiry
            ), f"Memory legacy record {key!r} should inherit survivor-derived expiry"
        partition.close()

    # ---------------------------------------------------------------
    # K: Backfill happens exactly once (durable done-flag)
    # ---------------------------------------------------------------

    def test_k_backfill_once_only_after_changelog_rebuild(self, tmp_path, caplog):
        """K: After a completed migration, a full changelog rebuild must NOT
        redo the backfill. The rebuilt store must come back TTL-enabled with
        the same expiry behavior (values intact, TTL still works), and no
        re-migration side-effects such as re-stamping WARNING logs.

        Observable: after replaying an all-stamped changelog produced by a
        completed migration, complete_recovery is a no-op (no backfill), the
        partition is TTL-enabled, and values read back correctly with working
        TTL expiry.
        """
        producer = _make_changelog_producer_mock()

        # Step 1: perform a full migration (seed legacy + ttl= write).
        partition = _rocksdb_partition(
            tmp_path,
            name="k-src",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=producer,
        )
        _seed_legacy(partition, [("old1", "val1"), ("old2", "val2")])
        producer.produce.reset_mock()

        ts = 1_000_000_000_000
        tx = partition.begin()
        tx.set(
            key="new1",
            value="vnew",
            prefix=b"pfx",
            timestamp=ts,
            ttl=timedelta(days=30),
        )
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)

        # Capture the changelog messages produced by the migration.
        changelog_msgs = []
        for call in producer.produce.call_args_list:
            headers = call.kwargs.get("headers", {})
            cf_name = headers.get(CHANGELOG_CF_MESSAGE_HEADER, "default")
            if cf_name != "default":
                continue
            ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))
            changelog_msgs.append(
                (call.kwargs["key"], call.kwargs["value"], ttl_stamped)
            )
        partition.close()

        # All produced default-CF records should carry __ttl_stamped__=True
        # (the backfill produced header-bearing stamped records).
        assert all(
            s for _, _, s in changelog_msgs
        ), "All changelog records from a completed migration must be stamped"

        # Step 2: full changelog rebuild into a fresh partition.
        producer2 = _make_changelog_producer_mock()
        rebuilt = _rocksdb_partition(
            tmp_path, name="k-dst", changelog_producer=producer2
        )
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            _replay_msgs(rebuilt, changelog_msgs, now_ms=ts - DAY_MS)
            rebuilt.complete_recovery()

        # No backfill-related WARNINGs (no re-migration side-effects).
        backfill_warnings = [
            rec
            for rec in caplog.records
            if rec.levelno == logging.WARNING and "backfill" in rec.message.lower()
        ]
        assert len(backfill_warnings) == 0, (
            f"Changelog rebuild should NOT emit backfill warnings, but got: "
            f"{[r.message for r in backfill_warnings]}"
        )

        # The rebuilt store is TTL-enabled.
        assert (
            rebuilt.uses_ttl_stamps is True
        ), "Rebuilt store must come back TTL-enabled"

        # Values intact and TTL-functional: old1/old2 readable,
        # new1 readable before its expiry.
        decoded = _decode_default_cf_rocksdb(rebuilt)
        old1_key = next(k for k in decoded if b"old1" in k)
        old2_key = next(k for k in decoded if b"old2" in k)
        new1_key = next(k for k in decoded if b"new1" in k)

        _, old1_payload = decoded[old1_key]
        _, old2_payload = decoded[old2_key]
        assert old1_payload == b'"val1"', "old1 payload must be intact after rebuild"
        assert old2_payload == b'"val2"', "old2 payload must be intact after rebuild"

        new1_exp, new1_payload = decoded[new1_key]
        assert new1_payload == b'"vnew"', "new1 payload must be intact after rebuild"
        assert new1_exp == ts + 30 * DAY_MS, "new1 must keep its original expiry"

        # No pending backfill residue.
        pending = _pending_keys_rocksdb(rebuilt)
        assert pending == set(), "Rebuilt store must have no pending backfill keys"
        rebuilt.close()

    # ---------------------------------------------------------------
    # L: Ordering -- backfill completes before new messages
    # ---------------------------------------------------------------

    def test_l_backfill_completes_before_new_messages(self, tmp_path):
        """L: On live enable, the backfill completes BEFORE new topic messages
        are processed. Observable: a new transaction opened right after the
        enabling flush sees the store fully migrated -- all legacy keys are
        readable with correct TTL behavior, uses_ttl_stamps is True."""
        partition = _rocksdb_partition(tmp_path)
        _seed_legacy(
            partition,
            [("old1", "val1"), ("old2", "val2"), ("old3", "val3")],
        )
        partition.close()

        legacy_ttl = timedelta(days=7)
        partition = _rocksdb_partition(
            tmp_path, options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        ts = 1_000_000_000_000
        # This flush enables TTL and runs the backfill atomically.
        with partition.begin() as tx:
            tx.set(
                key="new1",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        # Immediately after the flush: the store is fully migrated.
        assert (
            partition.uses_ttl_stamps is True
        ), "uses_ttl_stamps must be True immediately after the enabling flush"

        # A "new message" arriving right after enable: it sees the fully
        # migrated store. All old keys readable at current event-time.
        for key in ["old1", "old2", "old3"]:
            val = _read_via_tx(partition, key, timestamp=ts)
            assert val is not None, (
                f"Legacy key {key!r} must be readable immediately after enable "
                f"(backfill must complete before new messages are processed)"
            )

        # New writes go through the stamped path (not legacy).
        ts2 = ts + 1_000
        with partition.begin() as tx:
            tx.set(
                key="post_enable",
                value="pval",
                prefix=b"pfx",
                timestamp=ts2,
                ttl=timedelta(days=2),
            )

        val = _read_via_tx(partition, "post_enable", timestamp=ts2)
        assert val == "pval", "Post-enable write must be readable"

        # Verify it expires at its own ttl, proving it went through stamped path.
        val = _read_via_tx(partition, "post_enable", timestamp=ts2 + 2 * DAY_MS)
        assert val is None, "Post-enable write must expire at its own ttl"
        partition.close()

    # ---------------------------------------------------------------
    # G: Old event-time writes after changelog rebuild
    # ---------------------------------------------------------------

    def test_g_old_event_time_after_changelog_rebuild(self, tmp_path):
        """G: After a changelog rebuild, live writes with OLD event-time
        timestamps (hours/days behind wallclock) + their own ttl must be
        readable immediately after writing. The recovery wallclock is used
        ONLY for the Rule 4 replay drop filter and is NOT seeded into the
        live high_water_ms clock (spec Finding 3, section 7.4). Post-recovery
        high_water is the loaded persisted value or None; only live event-time
        writes advance it."""
        producer = _make_changelog_producer_mock()
        now_ms = 1_780_000_000_000

        # Build a changelog with a few stamped records.
        expiry_far_future = now_ms + 30 * DAY_MS
        msgs = []
        for i in range(3):
            key = f"pfx|k{i}".encode()
            stamped = encode_ttl_value(expiry_far_future, f'"existing-{i}"'.encode())
            msgs.append((key, stamped, True))

        partition = _rocksdb_partition(tmp_path, name="g", changelog_producer=producer)
        _replay_msgs(partition, msgs, now_ms=now_ms)
        partition.complete_recovery()

        # Post-recovery: high_water is NOT wallclock-seeded (Finding 3 fix).
        # A live write with old event-time (2 days behind wallclock) + 1-day
        # ttl produces an absolute expiry = old_ts + 1d = now_ms - 1d. Since
        # the high_water is NOT seeded to now_ms, this does NOT instantly
        # expire. The first live event-time write sets the high_water.
        old_ts = now_ms - 2 * DAY_MS
        write_ttl = timedelta(days=1)
        with partition.begin() as tx:
            tx.set(
                key="late",
                value="late-val",
                prefix=b"pfx",
                timestamp=old_ts,
                ttl=write_ttl,
            )

        # The value was just written. It must be readable at its own timestamp.
        val = _read_via_tx(partition, "late", timestamp=old_ts)
        assert val == "late-val", (
            f"A value just written with timestamp={old_ts} and ttl=1d should be "
            f"readable at the same timestamp. Got {val!r} instead."
        )
        partition.close()

    # ---------------------------------------------------------------
    # I-target: Memory backend WARNING on populated legacy store
    # ---------------------------------------------------------------

    def test_i_holds_memory_populated_legacy_warning(self, caplog):
        """I-holds: A live ttl= write on a populated legacy MEMORY store emits
        a WARNING (spec section 7.3 row I-target). The warning tells the
        operator that the ttl= write landed as legacy (un-stamped) this session
        and that migration is owned by the next changelog rebuild."""
        producer = _make_changelog_producer_mock()
        partition = MemoryStorePartition(changelog_producer=producer)
        # Populate with legacy data (direct replay, no TTL header).
        msgs = [
            (f"pfx|k{i}".encode(), f'"legacy-{i}"'.encode(), False) for i in range(3)
        ]
        _replay_msgs(partition, msgs)

        ts = 1_000_000_000_000
        with caplog.at_level(logging.WARNING):
            with partition.begin() as tx:
                tx.set(
                    key="new1",
                    value="vnew",
                    prefix=b"pfx",
                    timestamp=ts,
                    ttl=timedelta(days=1),
                )

        warnings = [
            rec
            for rec in caplog.records
            if rec.levelno == logging.WARNING
            and ("legacy" in rec.message.lower() or "populated" in rec.message.lower())
        ]
        assert len(warnings) > 0, (
            "A WARNING should be emitted when a ttl= write hits a populated "
            "legacy memory store, but no relevant WARNING was logged."
        )
        partition.close()

    # ---------------------------------------------------------------
    # J: Expire + re-write in same flush (sweep same-flush guarantee)
    # ---------------------------------------------------------------

    def test_j_expire_rewrite_same_flush(self, tmp_path):
        """J: A key written with ttl, expired (high-water advanced past expiry),
        then re-written with a fresh ttl in a later flush must read back the new
        value. The sweep's staged_default_keys guard prevents eviction of keys
        re-written in the same flush."""
        partition = _rocksdb_partition(tmp_path)
        ts = 1_000_000_000_000
        ttl_short = timedelta(days=1)

        # Write key with short ttl.
        with partition.begin() as tx:
            tx.set(
                key="dedup", value="first", prefix=b"pfx", timestamp=ts, ttl=ttl_short
            )

        # Advance high-water past expiry.
        ts_expired = ts + DAY_MS + 1
        val = _read_via_tx(partition, "dedup", timestamp=ts_expired)
        assert val is None, "Key should be expired after ttl"

        # Re-write with a fresh ttl in a new flush.
        ts_rewrite = ts_expired + 1_000
        ttl_long = timedelta(days=7)
        with partition.begin() as tx:
            tx.set(
                key="dedup",
                value="second",
                prefix=b"pfx",
                timestamp=ts_rewrite,
                ttl=ttl_long,
            )

        # The new value must be readable.
        val = _read_via_tx(partition, "dedup", timestamp=ts_rewrite)
        assert val == "second", (
            f"After re-writing an expired key with a fresh ttl, the new value "
            f"must be readable. Got {val!r} instead."
        )
        partition.close()


# ===================================================================
# TestContractTargets -- expected RED today
# ===================================================================


class TestContractTargets:
    """Scenarios that started RED as the contract loop's work queue and went
    green as fixes landed (rounds 1-3). Kept in their own class for history;
    all are now regression gates like TestContractHolds."""

    # ---------------------------------------------------------------
    # F: Changelog replay of a stock v3.24.0 store (self-heal)
    # ---------------------------------------------------------------

    def test_f_v3240_changelog_replay_values_intact(self, tmp_path, caplog):
        """F: Replaying a stock v3.24.0 changelog (stamped values, no
        __ttl_stamped__ header, no __ttl_index__ records) must: (1) complete
        recovery without crash; (2) return every value as the ORIGINAL user
        payload via the transaction read surface; (3) preserve TTL semantics
        afterward (a record whose v3.24.0 stamp is in the future survives, new
        writes with ttl= expire correctly); (4) emit an INFO adoption log.

        The design adopts the existing v3.24.0 stamps verbatim (no byte
        changes to stored values) and the TTL-mode flip makes transaction reads
        strip the 8-byte prefix transparently. Customers keep their original
        v3.24.0 TTLs. The __ttl_migration_done__ flag closes this class for
        every store migrated on this branch onward.

        Expected RED until ArchDev round 2 self-heal lands.
        """
        from quixstreams.utils.json import dumps as json_dumps

        producer = _make_changelog_producer_mock()
        now_ms = 1_780_000_000_000
        expiry = now_ms + 7 * DAY_MS

        # Craft v3.24.0-style changelog messages: 3 keys with future expiry
        # (should survive), 2 keys with past expiry (should be expired).
        # Keys must use the same serialization as the transaction layer:
        # prefix + "|" + json_dumps(key_string).
        future_keys = {}
        msgs = []
        past_expiry = now_ms - DAY_MS
        for i in range(3):
            key = f"k_future_{i}"
            user_value = f"value-{i}"
            future_keys[key] = user_value
            raw_key = b"pfx|" + json_dumps(key)
            stamped_value = encode_ttl_value(expiry, json_dumps(user_value))
            msgs.append((raw_key, stamped_value, False))
        for i in range(2):
            key = f"k_expired_{i}"
            raw_key = b"pfx|" + json_dumps(key)
            stamped_value = encode_ttl_value(past_expiry, json_dumps(f"old-{i}"))
            msgs.append((raw_key, stamped_value, False))

        partition = _rocksdb_partition(tmp_path, name="f", changelog_producer=producer)

        caplog.clear()
        with caplog.at_level(logging.INFO):
            _replay_msgs(partition, msgs, now_ms=now_ms)
            # (1) Recovery must complete without crash.
            partition.complete_recovery()

        # (2) Every future-stamped value must read back as the ORIGINAL payload
        # through the transaction surface (the user API, not raw bytes).
        for key, expected_value in future_keys.items():
            val = _read_via_tx(partition, key, timestamp=now_ms)
            assert val == expected_value, (
                f"v3.24.0 record {key!r}: expected original payload "
                f"{expected_value!r} via tx.get(), got {val!r}. The self-heal "
                f"has not adopted the existing v3.24.0 stamps."
            )

        # (3a) New writes with ttl= work correctly on the adopted store.
        # Must run BEFORE the expiry checks because those advance the
        # high_water past the future records' stamps and the sweep on the
        # next flush would evict them.
        ts_new = now_ms + 1_000
        new_ttl = timedelta(days=2)
        with partition.begin() as tx:
            tx.set(
                key="post_adopt",
                value="new-val",
                prefix=b"pfx",
                timestamp=ts_new,
                ttl=new_ttl,
            )

        val = _read_via_tx(partition, "post_adopt", timestamp=ts_new)
        assert val == "new-val", "Post-adoption write must be readable"
        val = _read_via_tx(partition, "post_adopt", timestamp=ts_new + 2 * DAY_MS)
        assert val is None, "Post-adoption write must expire at its own ttl"

        # (3b) TTL semantics: future-stamped records expire at their stamp.
        for key in future_keys:
            val = _read_via_tx(partition, key, timestamp=expiry)
            assert (
                val is None
            ), f"v3.24.0 record {key!r} should be expired at stamp={expiry}"

        # (4) An INFO adoption log must be emitted during recovery.
        adoption_logs = [
            rec
            for rec in caplog.records
            if rec.levelno >= logging.INFO
            and ("v3.24.0" in rec.message.lower() or "adopt" in rec.message.lower())
        ]
        assert len(adoption_logs) > 0, (
            "An INFO-level adoption log should be emitted when a v3.24.0 "
            "changelog is detected and adopted."
        )
        partition.close()
