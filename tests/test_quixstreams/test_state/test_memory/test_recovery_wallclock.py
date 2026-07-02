"""
Recovery wallclock-at-rebuild tests for the in-memory backend (OP-2,
shortcut 73191).

Mirrors the RocksDB recovery tests in
``test_state/test_rocksdb/test_legacy_backfill.py::TestRecoveryWallclock``.

The old memory recovery filter read ``recovery_now = self._high_water_ms`` per
message and advanced that high-water by each entry's stamp
(``advance_high_water(stamp)``). With a uniform-expiry store (every record
stamped with the same ``E``), the first replay ratcheted the clock to ``E`` and
every later record was dropped as ``E <= E`` — collapsing the store to ~1
survivor. The new filter drops iff
``stamp != SENTINEL_NEVER and stamp <= wallclock_now`` where ``wallclock_now`` is
captured ONCE per rebuild session, which is order-independent and retains all
entries rebuilt within their TTL window.

See ``dev-planning/state-ttl-legacy-backfill/spec-recovery-wallclock.md``.
"""

import struct
from datetime import timedelta
from unittest.mock import patch

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    TTL_BACKFILL_PENDING_CF_NAME,
)
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000


def _decode_default(partition):
    return {
        key: decode_ttl_value(value)
        for key, value in partition._state.get("default", {}).items()
    }


def _decode_index(partition):
    out = {}
    for key in partition._state.get(TTL_INDEX_CF_NAME, {}):
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _capture_default_changelog(changelog_producer_mock):
    msgs = []
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs["headers"]
        if headers[CHANGELOG_CF_MESSAGE_HEADER] == "default":
            msgs.append((call.kwargs["key"], call.kwargs["value"]))
    return msgs


def _replay_default(recovered, msgs, now_ms, ttl_stamped=True):
    recovered._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value in msgs:
        recovered.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


def _uniform_expiry_changelog(changelog_producer_mock, n, ts, ttl):
    """
    Build a memory store of ``n`` records all sharing a single uniform expiry
    (same timestamp + same ttl) and return ``(default_msgs, source_default)``.

    This reproduces the uniform-expiry collapse scenario the OP-2 fix targets:
    the memory backend has no ``legacy_records_ttl`` config, so we synthesize the
    same on-changelog shape (N identical stamps) via explicit ``ttl=`` writes.
    """
    partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
    tx = partition.begin()
    for i in range(n):
        tx.set(key=f"k{i}", value=f"v{i}", prefix=b"pfx", timestamp=ts, ttl=ttl)
    tx.prepare(processed_offsets={"topic": 1})
    tx.flush(changelog_offset=0)
    source_default = _decode_default(partition)
    msgs = _capture_default_changelog(changelog_producer_mock)
    partition.close()
    return msgs, source_default


class TestMemoryRecoveryWallclock:
    # All uniform-expiry records rebuilt WITHIN the window survive (no collapse).
    def test_all_records_survive_within_window(self, changelog_producer_mock):
        ts = 1_000_000_000_000
        ttl = timedelta(days=7)
        uniform_expiry = ts + 7 * DAY_MS
        msgs, source_default = _uniform_expiry_changelog(
            changelog_producer_mock, n=5, ts=ts, ttl=ttl
        )
        # Sanity: all 5 records really share the same uniform expiry.
        assert {exp for exp, _ in source_default.values()} == {uniform_expiry}

        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(recovered, msgs, now_ms=uniform_expiry - 1)

        recovered_default = _decode_default(recovered)
        # OP-2: every record survives (the old ratchet collapsed this to ~1).
        assert recovered_default == source_default
        assert len(recovered_default) == 5
        # Index rebuilt one entry per non-sentinel survivor.
        recovered_index = _decode_index(recovered)
        assert len(recovered_index) == 5
        for key, (exp, _) in source_default.items():
            assert recovered_index[key] == exp
        recovered.close()

    # Uniform-expiry records rebuilt AFTER the window: none survive.
    def test_none_survive_after_window(self, changelog_producer_mock):
        ts = 1_000_000_000_000
        ttl = timedelta(days=7)
        uniform_expiry = ts + 7 * DAY_MS
        msgs, _ = _uniform_expiry_changelog(
            changelog_producer_mock, n=5, ts=ts, ttl=ttl
        )
        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(recovered, msgs, now_ms=uniform_expiry + 1)

        assert _decode_default(recovered) == {}
        assert _decode_index(recovered) == {}
        recovered.close()

    # Mixed expiries: exactly stamp > now survive, order-independent.
    def test_mixed_expiries_order_independent(self, changelog_producer_mock):
        partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        base = 1_000_000_000_000
        with partition.begin() as tx:
            for i in range(3):
                tx.set(
                    key=f"k{i}",
                    value=f"v{i}",
                    prefix=b"pfx",
                    timestamp=base,
                    ttl=timedelta(days=i + 1),
                )
        source_default = _decode_default(partition)
        msgs = _capture_default_changelog(changelog_producer_mock)
        partition.close()

        cutoff = base + 2 * DAY_MS + 1
        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        # Reverse the replay order to prove order-independence vs the old ratchet.
        _replay_default(recovered, list(reversed(msgs)), now_ms=cutoff)

        recovered_default = _decode_default(recovered)
        expected = {
            k: payload for k, payload in source_default.items() if payload[0] > cutoff
        }
        assert recovered_default == expected
        assert len(recovered_default) == 1
        recovered.close()

    # SENTINEL_NEVER entries always survive recovery.
    def test_sentinel_never_always_survives(self, changelog_producer_mock):
        partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        base = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="kexp",
                value="vexp",
                prefix=b"pfx",
                timestamp=base,
                ttl=timedelta(days=1),
            )
            tx.set(key="kperm", value="vperm", prefix=b"pfx", timestamp=base)
        source_default = _decode_default(partition)
        msgs = _capture_default_changelog(changelog_producer_mock)
        partition.close()

        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(recovered, msgs, now_ms=base + 10_000 * DAY_MS)

        recovered_default = _decode_default(recovered)
        sentinel_keys = {
            k for k, (exp, _) in source_default.items() if exp == SENTINEL_NEVER
        }
        assert sentinel_keys, "fixture must produce at least one sentinel entry"
        for k in sentinel_keys:
            assert recovered_default[k] == source_default[k]
        assert set(recovered_default) == sentinel_keys
        recovered.close()

    # Spec §10 case 11 — post-recovery high-water is the loaded persisted
    # value or None, never wallclock-seeded (Finding 3, spec §7.4). The
    # recovery wallclock is used ONLY for the Rule 4 drop filter.
    def test_high_water_not_wallclock_seeded_after_recovery(
        self, changelog_producer_mock
    ):
        ts = 1_000_000_000_000
        ttl = timedelta(days=7)
        uniform_expiry = ts + 7 * DAY_MS
        msgs, _ = _uniform_expiry_changelog(
            changelog_producer_mock, n=3, ts=ts, ttl=ttl
        )
        now_ms = uniform_expiry - 1
        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(recovered, msgs, now_ms=now_ms)

        # Post-recovery high-water is NOT seeded to the session wallclock.
        # Memory partitions start fresh (no persisted high-water), so it
        # remains None after recovery (spec Finding 3, §7.4).
        assert recovered.high_water_ms is None, (
            "Post-recovery high_water_ms must be None (not wallclock-seeded); "
            "recovery wallclock is Rule 4 only (spec Finding 3, §7.4)"
        )
        # A live event-time write now sets the high-water.
        recovered.advance_high_water(ts)
        assert recovered.high_water_ms == ts
        # Monotonicity still holds.
        recovered.advance_high_water(ts - DAY_MS)
        assert recovered.high_water_ms == ts
        recovered.advance_high_water(ts + DAY_MS)
        assert recovered.high_water_ms == ts + DAY_MS
        recovered.close()

    # Wallclock is captured exactly ONCE per session: a clock that "ticks"
    # between messages must not change the survivor set. Finding 3 (§7.4):
    # the wallclock is NOT seeded into the live high_water_ms.
    def test_wallclock_captured_once_per_session(self, changelog_producer_mock):
        ts = 1_000_000_000_000
        ttl = timedelta(days=7)
        uniform_expiry = ts + 7 * DAY_MS
        msgs, source_default = _uniform_expiry_changelog(
            changelog_producer_mock, n=4, ts=ts, ttl=ttl
        )
        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        # A clock that jumps PAST the uniform expiry after the first capture.
        ticks = iter([uniform_expiry - 1] + [uniform_expiry + DAY_MS] * 100)
        recovered._now_ms = lambda: next(ticks)  # noqa: E731
        offset = 0
        for key, value in msgs:
            recovered.recover_from_changelog_message(
                key=key,
                value=value,
                cf_name="default",
                offset=offset,
                ttl_stamped=True,
            )
            offset += 1

        # All records survive: the single captured clock governs the session.
        assert _decode_default(recovered) == source_default
        # Finding 3 (§7.4): high_water is NOT seeded to the wallclock.
        assert recovered.high_water_ms is None, (
            "Post-recovery high_water_ms must be None (not wallclock-seeded); "
            "recovery wallclock is Rule 4 only (spec Finding 3, §7.4)"
        )
        recovered.close()

    # Determinism: two rebuilds with the same injected clock are identical.
    def test_single_wallclock_capture_is_deterministic(self, changelog_producer_mock):
        ts = 1_000_000_000_000
        ttl = timedelta(days=7)
        uniform_expiry = ts + 7 * DAY_MS
        msgs, source_default = _uniform_expiry_changelog(
            changelog_producer_mock, n=4, ts=ts, ttl=ttl
        )
        now_ms = uniform_expiry - 1

        rec_a = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(rec_a, msgs, now_ms=now_ms)
        default_a = _decode_default(rec_a)
        index_a = _decode_index(rec_a)
        rec_a.close()

        rec_b = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(rec_b, msgs, now_ms=now_ms)
        default_b = _decode_default(rec_b)
        index_b = _decode_index(rec_b)
        rec_b.close()

        assert default_a == default_b == source_default
        assert index_a == index_b


class TestMemoryRecoveryRoutesOnHeaderOnly:
    """§8.7 / spec-memory-backend-recovery §6.1 — recovery routes purely on the
    ``__ttl_stamped__`` header, never on value content; the
    ``_looks_like_stamped_value`` heuristic is not consulted."""

    def test_header_absent_stamp_shaped_value_stays_legacy(
        self, changelog_producer_mock
    ):
        # The OP-2/OP-3 false-positive shape: legacy dedup values that are 8-byte
        # BE epoch-ms timestamps, with NO __ttl_stamped__ header. Recovery must
        # stay legacy, replay every value verbatim (no 8B strip, none dropped),
        # and must NOT consult the value-content heuristic.
        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        recovered._now_ms = lambda: 1_780_000_000_000  # noqa: E731
        with patch.object(recovered, "_looks_like_stamped_value") as looks_spy:
            offset = 0
            for i in range(10):
                value = struct.pack(">q", 1_700_000_000_000 + i)
                recovered.recover_from_changelog_message(
                    key=f"pfx|k{i}".encode(),
                    value=value,
                    cf_name="default",
                    offset=offset,
                    ttl_stamped=False,
                )
                offset += 1
            looks_spy.assert_not_called()

        assert recovered.uses_ttl_stamps is False
        stored = recovered._state.get("default", {})
        assert len(stored) == 10
        # Verbatim: the stored bytes equal the raw 8-byte value (no strip).
        assert stored[b"pfx|k0"] == struct.pack(">q", 1_700_000_000_000)
        # Legacy stores never build a TTL index.
        assert not recovered._state.get(TTL_INDEX_CF_NAME)
        recovered.close()

    def test_header_true_flips_and_filters(self, changelog_producer_mock):
        # header-true stamped value -> partition flips, stamped branch + Rule 4.
        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        base = 1_000_000_000_000
        recovered._now_ms = lambda: base  # noqa: E731
        with patch.object(recovered, "_looks_like_stamped_value") as looks_spy:
            value = encode_ttl_value(base + 365 * DAY_MS, b"v")
            recovered.recover_from_changelog_message(
                key=b"pfx|k0",
                value=value,
                cf_name="default",
                offset=0,
                ttl_stamped=True,
            )
            looks_spy.assert_not_called()

        assert recovered.uses_ttl_stamps is True
        index = _decode_index(recovered)
        assert index[b"pfx|k0"] == base + 365 * DAY_MS
        recovered.close()

    def test_first_header_true_latches_for_session(self, changelog_producer_mock):
        # Once flipped on the first header-true record, the partition stays in TTL
        # mode (the ``uses_ttl_stamps`` FLAG latches for the session). Per-record
        # ROUTING, however, is on the header: a later HEADER-ABSENT default-CF
        # record is a MIXED-changelog leftover — it lands VERBATIM (no stamp
        # strip, no index write) and is censused into ``__ttl_backfill_pending__``
        # for completion, mirroring RocksDB (§15.4 item 3; this is the fix for the
        # memory 8-byte-strip corruption). Was: the memory latch wrongly routed
        # header-absent records through the stamped branch and indexed them.
        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        base = 1_000_000_000_000
        recovered._now_ms = lambda: base  # noqa: E731
        v0 = encode_ttl_value(base + 365 * DAY_MS, b"v0")
        recovered.recover_from_changelog_message(
            key=b"pfx|k0", value=v0, cf_name="default", offset=0, ttl_stamped=True
        )
        assert recovered.uses_ttl_stamps is True

        # Subsequent HEADER-ABSENT record: the flag latch holds (still TTL mode),
        # but the record is a leftover — verbatim landing + census, not stamped.
        v1 = encode_ttl_value(base + 365 * DAY_MS, b"v1")
        recovered.recover_from_changelog_message(
            key=b"pfx|k1", value=v1, cf_name="default", offset=1, ttl_stamped=False
        )
        assert recovered.uses_ttl_stamps is True  # flag still latched
        index = _decode_index(recovered)
        assert b"pfx|k0" in index  # header-true → indexed
        assert b"pfx|k1" not in index  # header-absent leftover → NOT indexed
        # k1 landed verbatim (raw bytes preserved, no 8-byte strip) and was
        # censused as a leftover for :meth:`complete_recovery`.
        assert recovered._state["default"][b"pfx|k1"] == v1
        assert b"pfx|k1" in recovered._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        recovered.close()
        recovered.close()
