"""
Red-first test for suspected finding #3 (memory backend): cold-restore
wallclock drop diverges from the live event-time filter.

Memory-backend mirror of the RocksDB test at
``test_state/test_rocksdb/test_recovery_wallclock_lagging.py``.

Both backends share the identical drop logic:
    ``expired = stamp <= recovery_now_ms``
where ``recovery_now_ms = self._now_ms()`` (wallclock). The live filter uses
``self._partition.high_water_ms`` (event-time). The divergence is identical.

If the test is RED (records dropped), finding #3 is CONFIRMED for memory.
If the test is GREEN (records survive), finding #3 is REFUTED for memory.
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
)
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
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


class TestMemoryRecoveryWallclockLagging:
    """
    Memory-backend parity: event-time-lagging cold restore drops records via
    wallclock that the live event-time filter would keep.
    """

    def test_lagging_store_records_survive_cold_restore(self, changelog_producer_mock):
        """
        Scenario: dedup store at event-time T = 1_000_000_000_000, TTL = 7d,
        stamps = T + 7d.  Cold restore with wallclock W = T + 365d.

        Live filter: high_water_ms = None after cold restore → stamp <= None
        is always False → records NOT expired.

        Recovery filter (current): stamp <= W → EXPIRED → DROPPED.

        Expected: records SURVIVE.
        Actual (suspected): records DROPPED.
        """
        event_time = 1_000_000_000_000
        ttl = timedelta(days=7)
        expiry = event_time + 7 * DAY_MS

        partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        with partition.begin() as tx:
            for i in range(5):
                tx.set(
                    key=f"dedup_key_{i}",
                    value=f"seen_{i}",
                    prefix=b"pfx",
                    timestamp=event_time,
                    ttl=ttl,
                )
        source_default = _decode_default(partition)
        msgs = _capture_default_changelog(changelog_producer_mock)
        partition.close()

        # Sanity: all 5 records share the same expiry.
        expiries = {exp for exp, _ in source_default.values()}
        assert expiries == {expiry}

        # Cold restore with wallclock FAR in the future.
        wallclock_now = event_time + 365 * DAY_MS

        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(recovered, msgs, now_ms=wallclock_now)

        recovered_default = _decode_default(recovered)

        assert (
            recovered.high_water_ms is None
        ), "Post-recovery high_water_ms must be None on a cold restore"

        # CORE ASSERTION: records must survive.
        assert len(recovered_default) == 5, (
            f"Expected 5 records to survive cold restore (event-time-lagging "
            f"store: stamps expired by wallclock {wallclock_now} but NOT by "
            f"event-time frontier high_water_ms=None). "
            f"Got {len(recovered_default)} survivors. "
            f"Confirms finding #3 for the memory backend."
        )
        assert recovered_default == source_default

        recovered_index = _decode_index(recovered)
        assert len(recovered_index) == 5
        for key, (exp, _) in source_default.items():
            if exp != SENTINEL_NEVER:
                assert recovered_index[key] == exp
        recovered.close()

    def test_mixed_lagging_only_truly_expired_dropped(self, changelog_producer_mock):
        """
        Mixed TTLs.  Wallclock between the two stamps.

        Under event-time (high_water=None): both survive.
        Under wallclock: short_ttl dropped, long_ttl survives.
        """
        base_event_time = 1_000_000_000_000

        partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        with partition.begin() as tx:
            tx.set(
                key="short_ttl",
                value="v_short",
                prefix=b"pfx",
                timestamp=base_event_time,
                ttl=timedelta(days=1),
            )
            tx.set(
                key="long_ttl",
                value="v_long",
                prefix=b"pfx",
                timestamp=base_event_time,
                ttl=timedelta(days=30),
            )
        source_default = _decode_default(partition)
        msgs = _capture_default_changelog(changelog_producer_mock)
        partition.close()

        wallclock_now = base_event_time + 10 * DAY_MS

        recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _replay_default(recovered, msgs, now_ms=wallclock_now)

        recovered_default = _decode_default(recovered)
        assert recovered.high_water_ms is None

        assert len(recovered_default) == 2, (
            f"Expected 2 records (event-time high_water=None means nothing is "
            f"expired). Got {len(recovered_default)}. "
            f"Wallclock dropped records the live filter would keep."
        )
        assert recovered_default == source_default
        recovered.close()


def _wide_range_changelog(changelog_producer_mock, base, day_spans):
    """Build a memory store whose stamps span a WIDE event-time range (one record
    per entry in ``day_spans``) and return ``(msgs, source_default)``."""
    partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
    with partition.begin() as tx:
        for i, days in enumerate(day_spans):
            tx.set(
                key=f"k{i}",
                value=f"v{i}",
                prefix=b"pfx",
                timestamp=base,
                ttl=timedelta(days=days),
            )
    source_default = _decode_default(partition)
    msgs = _capture_default_changelog(changelog_producer_mock)
    partition.close()
    return msgs, source_default


class TestMemoryNoRatchetFrontier:
    """Memory parity: the event-time replay-drop frontier is a single FIXED
    pre-replay snapshot and never ratchets up toward the max replayed stamp.
    Proven across a WIDE stamp range in both ascending and descending replay
    order, cold and warm.
    """

    DAY_SPANS = [1, 100, 200, 400]

    def test_wide_range_no_ratchet_cold_keeps_all(self, changelog_producer_mock):
        base = 1_000_000_000_000
        msgs, source_default = _wide_range_changelog(
            changelog_producer_mock, base, self.DAY_SPANS
        )
        # A far-future wallclock: the OLD wallclock drop deleted every record; the
        # memory frontier is always None (no persisted high-water), so nothing
        # drops in EITHER replay order.
        wallclock_now = base + 10_000 * DAY_MS
        for order in ("ascending", "descending"):
            replay = msgs if order == "ascending" else list(reversed(msgs))
            recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
            _replay_default(recovered, replay, now_ms=wallclock_now)
            assert recovered.high_water_ms is None
            recovered_default = _decode_default(recovered)
            assert recovered_default == source_default, (
                f"cold restore ({order}) must keep ALL {len(source_default)} "
                f"wide-range records (frontier None); got {len(recovered_default)}"
            )
            recovered.close()

    def test_wide_range_no_ratchet_warm_drops_only_past_frontier(
        self, changelog_producer_mock
    ):
        base = 1_000_000_000_000
        msgs, source_default = _wide_range_changelog(
            changelog_producer_mock, base, self.DAY_SPANS
        )
        # Fixed frontier BETWEEN the low (base+1d) and high (base+400d) stamps.
        frontier = base + 150 * DAY_MS
        expected = {
            k: payload for k, payload in source_default.items() if payload[0] > frontier
        }
        # Sanity: the frontier really splits the cohort (some drop, some survive).
        assert 0 < len(expected) < len(source_default)
        for order in ("ascending", "descending"):
            replay = msgs if order == "ascending" else list(reversed(msgs))
            recovered = MemoryStorePartition(changelog_producer=changelog_producer_mock)
            # WARM restore: seed the event-time high-water. A RATCHET would advance
            # it toward the max replayed stamp (worst in descending order, where
            # the max stamp replays first) and collateral-drop the mid-range
            # records; a FIXED frontier drops exactly stamp <= frontier,
            # identically in both orders.
            recovered.advance_high_water(frontier)
            _replay_default(recovered, replay, now_ms=frontier)
            recovered_default = _decode_default(recovered)
            assert recovered_default == expected, (
                f"warm restore ({order}) must drop exactly stamps <= frontier "
                f"(no ratchet); expected {sorted(expected)} got "
                f"{sorted(recovered_default)}"
            )
            recovered.close()
