"""
Unit tests for the State TTL legacy-store backfill feature (shortcut 73191).

Covers spec §11 unit cases 1-9:

1. ``legacy_records_ttl <= 0`` raises ``ValueError`` at ``RocksDBOptions``.
2. Populated legacy store + opt-in set → first ``ttl=`` write backfills with
   ``expires_at == high_water + ttl_ms``; index + flip metadata set.
3. Populated legacy store + opt-in unset → still raises
   ``IncompatibleStateStoreError`` with the new operator-callable message.
4. Empty store + opt-in set → behaves like the empty-store flip.
5. Idempotency: backfill once, reopen, write again → no second backfill.
6. enable_time = event-time high-water, not wall-clock.
7. Windowed / timestamped partition + opt-in set → opt-in ignored.
8. Recovery: backfill produces stamps to the changelog so a fresh partition
   rebuilds identically.
9. Crash-before-flag: stamps without the flag → reopen is legacy, backfill
   re-runs and converges.

See ``dev-planning/state-ttl-legacy-backfill/spec.md``.
"""

from datetime import timedelta

import pytest

from quixstreams.state.metadata import METADATA_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.exceptions import IncompatibleStateStoreError
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ENABLED_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.timestamped import TimestampedStorePartition
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
)
from quixstreams.state.serialization import int_from_bytes

DAY_MS = 86_400_000


def _seed_legacy_records(partition, records, prefix=b"pfx"):
    """Write plain (un-stamped) records to a legacy partition."""
    with partition.begin() as tx:
        for key, value in records:
            tx.set(key=key, value=value, prefix=prefix)


def _decode_default_cf(partition):
    """Return ``{raw_key: (expires_at, payload)}`` for the default CF."""
    cf = partition.get_or_create_column_family("default")
    out = {}
    for key, value in cf.items():
        out[key] = decode_ttl_value(value)
    return out


def _decode_index_cf(partition):
    """Return ``{user_key: expires_at}`` for the ``__ttl_index__`` CF."""
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _capture_default_cf_changelog(changelog_producer_mock):
    """Return the list of ``(key, value, ttl_stamped)`` default-CF changelog
    messages, carrying the ``__ttl_stamped__`` header bit (spec §8.7)."""
    from quixstreams.state.metadata import (
        CHANGELOG_CF_MESSAGE_HEADER,
        CHANGELOG_TTL_STAMPED_HEADER,
    )

    msgs = []
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs["headers"]
        if headers[CHANGELOG_CF_MESSAGE_HEADER] == "default":
            ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))
            msgs.append((call.kwargs["key"], call.kwargs["value"], ttl_stamped))
    return msgs


def _replay_default(recovered, msgs, now_ms):
    """
    Replay default-CF ``msgs`` into ``recovered`` with an injected wallclock
    ``now_ms`` (test clock seam). Threads the captured ``__ttl_stamped__`` header
    so recovery routes on the header (spec §8.7), as the live recovery manager
    does. Returns nothing; inspect ``recovered`` after.
    """
    recovered._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, ttl_stamped in msgs:
        recovered.recover_from_changelog_message(
            key=key, value=value, cf_name="default", offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


# ---------------------------------------------------------------------------
# Case 1 — validation
# ---------------------------------------------------------------------------


class TestLegacyRecordsTTLValidation:
    def test_zero_ttl_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            RocksDBOptions(legacy_records_ttl=timedelta(0))

    def test_negative_ttl_raises(self):
        with pytest.raises(ValueError, match="strictly positive"):
            RocksDBOptions(legacy_records_ttl=timedelta(seconds=-1))

    def test_none_is_default(self):
        assert RocksDBOptions().legacy_records_ttl is None

    def test_positive_ttl_accepted(self):
        opts = RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        assert opts.legacy_records_ttl == timedelta(days=7)


# ---------------------------------------------------------------------------
# Cases 2-9 — behavior
# ---------------------------------------------------------------------------


class TestLegacyBackfill:
    # Case 2 — populated legacy store + opt-in set → backfill
    def test_backfill_restamps_existing_records(self, store_partition_factory):
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("k1", "v1"), ("k2", "v2")])
        partition.close()

        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=ttl)
        )
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="knew",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        assert partition.uses_ttl_stamps is True
        assert partition.high_water_ms == ts

        decoded = _decode_default_cf(partition)
        expected_legacy_expiry = ts + 7 * DAY_MS
        # Pre-existing records carry the uniform legacy expiry, not sentinel.
        legacy_keys = [k for k in decoded if b"knew" not in k]
        assert len(legacy_keys) == 2
        for key in legacy_keys:
            expires_at, _ = decoded[key]
            assert expires_at == expected_legacy_expiry

        # The new record keeps its true event-time expiry.
        new_key = next(k for k in decoded if b"knew" in k)
        assert decoded[new_key][0] == ts + DAY_MS

        # Index has one entry per pre-existing key at the legacy expiry.
        index = _decode_index_cf(partition)
        for key in legacy_keys:
            assert index[key] == expected_legacy_expiry

        # Flip metadata is set.
        meta = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert meta.get(TTL_ENABLED_KEY) == b"\x01"
        assert int_from_bytes(meta.get(STATE_FORMAT_VERSION_KEY)) == (
            STATE_FORMAT_VERSION
        )
        partition.close()

    # Case 3 — populated legacy store + opt-in unset → reject (new message)
    def test_reject_when_opt_in_unset(self, store_partition_factory):
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("k1", "v1")])
        partition.close()

        partition = store_partition_factory(name="db", options=RocksDBOptions())
        with pytest.raises(IncompatibleStateStoreError) as exc_info:
            with partition.begin() as tx:
                tx.set(
                    key="knew",
                    value="vnew",
                    prefix=b"pfx",
                    timestamp=1_000,
                    ttl=timedelta(days=1),
                )
        msg = str(exc_info.value)
        assert "legacy_records_ttl" in msg
        assert "delete the state directory" not in msg.lower()
        assert partition.uses_ttl_stamps is False
        partition.close()

    # Case 4 — empty store + opt-in set → identical to empty-store flip
    def test_empty_store_with_opt_in_behaves_like_flip(self, store_partition_factory):
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
        )
        ts = 5_000
        with partition.begin() as tx:
            tx.set(
                key="k1",
                value="v1",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        assert partition.uses_ttl_stamps is True
        decoded = _decode_default_cf(partition)
        # Only the in-batch key is stamped — nothing was backfilled.
        assert len(decoded) == 1
        (expires_at, _), = decoded.values()
        assert expires_at == ts + DAY_MS
        partition.close()

    # Case 5 — idempotency across reopen
    def test_backfill_runs_exactly_once(self, store_partition_factory):
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("k1", "v1")])
        partition.close()

        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=ttl)
        )
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)
        first_decoded = _decode_default_cf(partition)
        legacy_key = next(k for k in first_decoded if b"k1" in k)
        original_legacy_expiry = first_decoded[legacy_key][0]
        partition.close()

        # Reopen with a *different* legacy_records_ttl and write again. The
        # store is already flipped, so backfill must NOT run again and the
        # pre-existing record must keep its original expiry.
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=99)),
        )
        assert partition.uses_ttl_stamps is True
        with partition.begin() as tx:
            tx.set(
                key="k3",
                value="v3",
                prefix=b"pfx",
                timestamp=ts + 10,
                ttl=ttl,
            )
        decoded = _decode_default_cf(partition)
        legacy_key = next(k for k in decoded if b"k1" in k)
        assert decoded[legacy_key][0] == original_legacy_expiry
        partition.close()

    # Case 6 — enable_time is event-time high-water, not wall-clock
    def test_enable_time_is_event_time_high_water(self, store_partition_factory):
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("k1", "v1")])
        partition.close()

        ttl = timedelta(days=2)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=ttl)
        )
        # An event-time far from wall-clock (year 2001-ish epoch ms).
        event_time = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="knew",
                value="vnew",
                prefix=b"pfx",
                timestamp=event_time,
                ttl=timedelta(days=1),
            )
        decoded = _decode_default_cf(partition)
        legacy_key = next(k for k in decoded if b"k1" in k)
        assert decoded[legacy_key][0] == event_time + 2 * DAY_MS
        partition.close()

    # Case 7 — windowed / timestamped partition ignores the opt-in
    def test_timestamped_partition_ignores_opt_in(self, tmp_path):
        # Timestamped stores opt out of the TTL stamp machinery at the class
        # level, so the opt-in must be inert: no flip, no TTL index CF, even
        # with ``legacy_records_ttl`` set. We drive a real write through the
        # timestamped store's own API (it has a different key/timestamp model)
        # and assert the partition never enters TTL mode.
        path = (tmp_path / "ts").as_posix()
        partition = TimestampedStorePartition(
            path,
            grace_ms=0,
            keep_duplicates=False,
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
        )
        # Class-level opt-out: the machinery is permanently off.
        assert type(partition).uses_ttl_stamps is False
        assert partition.uses_ttl_stamps is False
        # The opt-in is stored but never consulted for these partitions.
        assert partition.legacy_records_ttl == timedelta(days=7)

        with partition.begin() as tx:
            tx.set_for_timestamp(timestamp=1_000, value="v1", prefix=b"pfx")

        # No flip, no TTL index CF created.
        assert partition.uses_ttl_stamps is False
        assert TTL_INDEX_CF_NAME not in partition.list_column_families()
        partition.close()

    # Case 8 — recovery rebuilds identically from the changelog
    def test_recovery_rebuilds_identical_store(
        self, store_partition_factory, changelog_producer_mock
    ):
        # Open a legacy store WITH the opt-in and a changelog producer
        # attached. Seed un-stamped records in a first (legacy) transaction,
        # then a second transaction does a ttl= write that triggers the
        # backfill. We keep a single open partition (no reopen) so the test
        # is not subject to Windows lock-release timing.
        #
        # Recovery now judges expiry against wallclock-at-recovery (OP-1 fix,
        # spec-recovery-wallclock.md), NOT against a stamp-ratcheted clock. So
        # this test seeds a MULTI-record legacy store with a uniform backfill
        # expiry and rebuilds with an injected wallclock strictly BEFORE every
        # stamp — all records survive, including the ones sharing the uniform
        # backfill expiry that the old ratchet would have mutually dropped.
        legacy_ttl = timedelta(days=7)
        new_ttl = timedelta(days=30)
        partition = store_partition_factory(
            name="src",
            options=RocksDBOptions(legacy_records_ttl=legacy_ttl),
            changelog_producer=changelog_producer_mock,
        )
        _seed_legacy_records(partition, [("k1", "v1"), ("k2", "v2"), ("k3", "v3")])

        changelog_producer_mock.produce.reset_mock()
        ts = 1_000_000_000_000
        offset = 0
        tx = partition.begin()
        tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=new_ttl)
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=offset)

        source_default = _decode_default_cf(partition)

        # Collect the default-CF changelog messages produced at flip.
        from quixstreams.state.metadata import (
            CHANGELOG_CF_MESSAGE_HEADER,
            CHANGELOG_TTL_STAMPED_HEADER,
        )

        changelog_msgs = []
        for call in changelog_producer_mock.produce.call_args_list:
            headers = call.kwargs["headers"]
            cf_name = headers[CHANGELOG_CF_MESSAGE_HEADER]
            ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))
            changelog_msgs.append(
                (call.kwargs["key"], call.kwargs["value"], cf_name, ttl_stamped)
            )
        partition.close()

        # The index CF is local-only and must NOT appear on the changelog.
        assert all(cf != TTL_INDEX_CF_NAME for _, _, cf, _ in changelog_msgs)
        # The pre-existing keys must be present on the changelog (option a):
        # this is the core recovery-wiring guarantee the backfill controls.
        default_changelog_keys = {
            key for key, _, cf, _ in changelog_msgs if cf == "default"
        }
        assert default_changelog_keys >= set(source_default.keys())

        # Replay the changelog into a fresh partition with an injected wallclock
        # strictly BEFORE every stamp, so nothing is expired against "now".
        recovered = store_partition_factory(name="dst")
        recovered._now_ms = lambda: ts - DAY_MS  # noqa: E731
        default_msgs = [
            (k, v, ts_h) for (k, v, cf, ts_h) in changelog_msgs if cf == "default"
        ]
        replay_offset = 0
        for key, value, ttl_stamped in default_msgs:
            recovered.recover_from_changelog_message(
                key=key, value=value, cf_name="default", offset=replay_offset,
                ttl_stamped=ttl_stamped,
            )
            replay_offset += 1

        recovered_default = _decode_default_cf(recovered)
        # The recovered store holds identical stamped values for every key,
        # including all the uniform-expiry backfilled ones (was: collapse).
        assert recovered_default == source_default
        # And the index is rebuilt locally for the non-sentinel entries.
        recovered_index = _decode_index_cf(recovered)
        for key, (expires_at, _) in source_default.items():
            if expires_at != SENTINEL_NEVER:
                assert recovered_index.get(key) == expires_at
        recovered.close()

    # Case 9 — crash before flag → reopen legacy, backfill re-runs cleanly
    def test_crash_before_flag_reruns_backfill(self, store_partition_factory):
        # Seed a pure legacy store and close it. Because the flip metadata
        # (``__ttl_enabled__``) is written LAST in the same atomic batch as
        # the re-stamped values, a crash before that batch commits leaves the
        # store exactly in this state: legacy data, no flag. This is
        # byte-identical to "crashed mid-backfill before the flag write".
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("k1", "v1")])
        # Sanity: no flip metadata was written on a pure legacy store. Drop the
        # CF reference before closing so RocksDB releases the file lock.
        meta = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert meta.get(TTL_ENABLED_KEY) is None
        del meta
        partition.close()

        # Reopen with the opt-in: the store is still legacy, so the backfill
        # re-runs cleanly and converges without double-stamping.
        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=ttl)
        )
        assert partition.uses_ttl_stamps is False
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)
        assert partition.uses_ttl_stamps is True
        decoded = _decode_default_cf(partition)
        legacy_key = next(k for k in decoded if b"k1" in k)
        # No double-stamp: payload decodes back to the original serialized
        # value, not a nested stamped blob.
        expires_at, payload = decoded[legacy_key]
        assert expires_at == ts + 7 * DAY_MS
        assert payload == b'"v1"'
        partition.close()


# ---------------------------------------------------------------------------
# OP-1 fix — wallclock-at-recovery TTL filtering on changelog replay.
# See dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md §8.
# ---------------------------------------------------------------------------


class TestRecoveryWallclock:
    """
    Recovery judges TTL expiry against a wallclock captured ONCE per recovery
    session (spec-recovery-wallclock.md), not against a stamp-ratcheted clock.

    The old recovery filter read ``recovery_now = self._high_water_ms`` per
    message and advanced that high-water by each entry's expiry stamp. With a
    uniform-expiry backfill (every record stamped with the same ``E``), the
    first replay ratcheted the clock to ``E`` and every later record was then
    dropped as ``E <= E`` — collapsing the store to ~1 survivor (OP-1). The new
    filter drops iff ``stamp != SENTINEL_NEVER and stamp <= wallclock_now``,
    which is order-independent and retains all entries rebuilt within their TTL
    window.
    """

    def _backfilled_changelog(
        self, store_partition_factory, changelog_producer_mock, n, legacy_ttl, ts
    ):
        """
        Build a backfilled store of ``n`` uniform-expiry legacy records (+ one
        new ttl= write that triggers the flip) and return
        ``(default_changelog_msgs, source_default, legacy_expiry)``.
        """
        partition = store_partition_factory(
            name="src",
            options=RocksDBOptions(legacy_records_ttl=legacy_ttl),
            changelog_producer=changelog_producer_mock,
        )
        _seed_legacy_records(
            partition, [(f"k{i}", f"v{i}") for i in range(n)]
        )
        changelog_producer_mock.produce.reset_mock()
        tx = partition.begin()
        tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=legacy_ttl)
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)
        source_default = _decode_default_cf(partition)
        legacy_expiry = ts + 7 * DAY_MS
        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        partition.close()
        return msgs, source_default, legacy_expiry

    # Spec §8 case 1 — backfilled store rebuilt WITHIN TTL window: all survive.
    def test_op1_all_records_survive_within_window(
        self, store_partition_factory, changelog_producer_mock
    ):
        ts = 1_000_000_000_000
        legacy_ttl = timedelta(days=7)
        n = 5
        msgs, source_default, legacy_expiry = self._backfilled_changelog(
            store_partition_factory, changelog_producer_mock, n, legacy_ttl, ts
        )
        # Sanity: the n legacy records really do share the same uniform expiry.
        legacy_expiries = {
            exp for k, (exp, _) in source_default.items() if b"knew" not in k
        }
        assert legacy_expiries == {legacy_expiry}

        recovered = store_partition_factory(name="dst")
        # Rebuild strictly BEFORE the uniform expiry -> nothing is expired.
        _replay_default(recovered, msgs, now_ms=legacy_expiry - 1)

        recovered_default = _decode_default_cf(recovered)
        # OP-1: every backfilled record (was: collapse to ~1) is retained.
        assert recovered_default == source_default
        # __ttl_index__ has exactly one entry per surviving non-sentinel key.
        recovered_index = _decode_index_cf(recovered)
        assert len(recovered_index) == len(source_default)
        for key, (exp, _) in source_default.items():
            assert recovered_index[key] == exp
        recovered.close()

    # Spec §8 case 2 — backfilled store rebuilt AFTER window: none survive.
    def test_op1_none_survive_after_window(
        self, store_partition_factory, changelog_producer_mock
    ):
        ts = 1_000_000_000_000
        legacy_ttl = timedelta(days=7)
        msgs, source_default, legacy_expiry = self._backfilled_changelog(
            store_partition_factory, changelog_producer_mock, 5, legacy_ttl, ts
        )
        # The new record (knew) uses the same ttl here, so its expiry equals
        # the legacy expiry; rebuilding strictly after it drops everything.
        max_expiry = max(exp for exp, _ in source_default.values())
        recovered = store_partition_factory(name="dst")
        _replay_default(recovered, msgs, now_ms=max_expiry + 1)

        recovered_default = _decode_default_cf(recovered)
        assert recovered_default == {}
        recovered_index = _decode_index_cf(recovered)
        assert recovered_index == {}
        recovered.close()

    # Spec §8 case 3 — mixed expiries: exactly stamp > now survive,
    # order-independent.
    def test_mixed_expiries_order_independent(
        self, store_partition_factory, changelog_producer_mock
    ):
        # Drive distinct expiries via distinct event-times on a fresh TTL store
        # (each set() carries its own timestamp+ttl, so stamps differ).
        partition = store_partition_factory(
            name="src", changelog_producer=changelog_producer_mock
        )
        base = 1_000_000_000_000
        # expiries: k0 -> base+1*DAY, k1 -> base+2*DAY, k2 -> base+3*DAY
        with partition.begin() as tx:
            for i in range(3):
                tx.set(
                    key=f"k{i}",
                    value=f"v{i}",
                    prefix=b"pfx",
                    timestamp=base,
                    ttl=timedelta(days=i + 1),
                )
        source_default = _decode_default_cf(partition)
        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        partition.close()

        # Rebuild at now strictly between k1's and k2's expiry: only the entry
        # with the largest stamp (stamp > now) survives. Reverse the replay
        # order to prove order-independence vs the old stamp ratchet.
        cutoff = base + 2 * DAY_MS + 1
        recovered = store_partition_factory(name="dst")
        _replay_default(recovered, list(reversed(msgs)), now_ms=cutoff)

        recovered_default = _decode_default_cf(recovered)
        expected = {
            k: payload
            for k, payload in source_default.items()
            if payload[0] > cutoff
        }
        assert recovered_default == expected
        assert len(recovered_default) == 1
        recovered.close()

    # Spec §8 case 4 — SENTINEL_NEVER entries always survive recovery.
    def test_sentinel_never_always_survives(
        self, store_partition_factory, changelog_producer_mock
    ):
        # A plain (no-ttl) set() on a TTL-flipped store stamps SENTINEL_NEVER.
        partition = store_partition_factory(
            name="src", changelog_producer=changelog_producer_mock
        )
        base = 1_000_000_000_000
        with partition.begin() as tx:
            # one expiring entry to flip into TTL mode + one sentinel entry
            tx.set(
                key="kexp", value="vexp", prefix=b"pfx",
                timestamp=base, ttl=timedelta(days=1),
            )
            tx.set(key="kperm", value="vperm", prefix=b"pfx", timestamp=base)
        source_default = _decode_default_cf(partition)
        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        partition.close()

        # Rebuild in the far future: the expiring entry is dropped, the
        # sentinel entry survives unconditionally.
        recovered = store_partition_factory(name="dst")
        _replay_default(recovered, msgs, now_ms=base + 10_000 * DAY_MS)

        recovered_default = _decode_default_cf(recovered)
        sentinel_keys = {
            k for k, (exp, _) in source_default.items() if exp == SENTINEL_NEVER
        }
        assert sentinel_keys, "fixture must produce at least one sentinel entry"
        for k in sentinel_keys:
            assert recovered_default[k] == source_default[k]
        # The non-sentinel expiring entry was dropped.
        assert set(recovered_default) == sentinel_keys
        recovered.close()

    # Spec §8 case 5 — post-recovery high-water seeding + monotonicity edge.
    def test_high_water_seeded_to_wallclock_and_monotonic(
        self, store_partition_factory, changelog_producer_mock
    ):
        ts = 1_000_000_000_000
        legacy_ttl = timedelta(days=7)
        msgs, source_default, legacy_expiry = self._backfilled_changelog(
            store_partition_factory, changelog_producer_mock, 3, legacy_ttl, ts
        )
        now_ms = legacy_expiry - 1
        recovered = store_partition_factory(name="dst")
        _replay_default(recovered, msgs, now_ms=now_ms)

        # High-water seeded to the session wallclock (§3.3).
        assert recovered.high_water_ms == now_ms
        # A live event-time below the seed does NOT roll high-water back.
        recovered.advance_high_water(now_ms - DAY_MS)
        assert recovered.high_water_ms == now_ms
        # A live event-time above the seed advances it.
        recovered.advance_high_water(now_ms + DAY_MS)
        assert recovered.high_water_ms == now_ms + DAY_MS
        recovered.close()

    # Spec §8 case 6 — determinism within a single rebuild session.
    def test_single_wallclock_capture_is_deterministic(
        self, store_partition_factory, changelog_producer_mock
    ):
        ts = 1_000_000_000_000
        legacy_ttl = timedelta(days=7)
        msgs, source_default, legacy_expiry = self._backfilled_changelog(
            store_partition_factory, changelog_producer_mock, 4, legacy_ttl, ts
        )
        now_ms = legacy_expiry - 1

        # Two independent rebuilds with the SAME injected clock produce
        # identical survivor sets and identical index contents.
        rec_a = store_partition_factory(name="dst_a")
        _replay_default(rec_a, msgs, now_ms=now_ms)
        default_a = _decode_default_cf(rec_a)
        index_a = _decode_index_cf(rec_a)
        rec_a.close()

        rec_b = store_partition_factory(name="dst_b")
        _replay_default(rec_b, msgs, now_ms=now_ms)
        default_b = _decode_default_cf(rec_b)
        index_b = _decode_index_cf(rec_b)
        rec_b.close()

        assert default_a == default_b == source_default
        assert index_a == index_b

    # Wallclock is captured exactly ONCE per session: a clock that "ticks"
    # between messages must not change the survivor set.
    def test_wallclock_captured_once_per_session(
        self, store_partition_factory, changelog_producer_mock
    ):
        ts = 1_000_000_000_000
        legacy_ttl = timedelta(days=7)
        msgs, source_default, legacy_expiry = self._backfilled_changelog(
            store_partition_factory, changelog_producer_mock, 4, legacy_ttl, ts
        )
        recovered = store_partition_factory(name="dst")
        # A clock that jumps PAST the uniform expiry after the first capture.
        # If _now_ms were read per-message, later records would be dropped.
        ticks = iter([legacy_expiry - 1] + [legacy_expiry + DAY_MS] * 100)
        recovered._now_ms = lambda: next(ticks)  # noqa: E731
        offset = 0
        for key, value, ttl_stamped in msgs:
            recovered.recover_from_changelog_message(
                key=key, value=value, cf_name="default", offset=offset,
                ttl_stamped=ttl_stamped,
            )
            offset += 1

        # All records survive: the single captured clock (legacy_expiry - 1)
        # governs the whole session, not the later ticks.
        assert _decode_default_cf(recovered) == source_default
        assert recovered.high_water_ms == legacy_expiry - 1
        recovered.close()
