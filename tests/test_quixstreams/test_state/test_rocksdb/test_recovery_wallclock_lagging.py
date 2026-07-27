"""
Red-first test for suspected finding #3: cold-restore wallclock drop diverges
from the live event-time filter.

During cold restore, ``recover_from_changelog_message`` drops a stamped record
when ``stamp <= self._recovery_now_ms`` where ``_recovery_now_ms`` is pure
wallclock (``time.time() * 1000``). But the live read filter in
``PartitionTransaction._deserialize_value`` (transaction.py ~line 579)
expires against ``self._partition.high_water_ms`` which is **event-time**.

For an event-time-lagging store (downtime longer than the TTL window, or
historical backlog replay), these two clocks diverge: a record's stamp may be
``<= wallclock`` (expired by recovery) yet ``> high_water_ms`` (NOT expired by
the live filter). Recovery deletes records that live processing would keep.

This violates the "never mass-delete on rebuild" guarantee: e.g. a dedup set
gets wiped on cold restore, causing duplicate emissions.

If the test is RED (records dropped), finding #3 is CONFIRMED.
If the test is GREEN (records survive), finding #3 is REFUTED.
"""

from datetime import timedelta

from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
)
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
)

DAY_MS = 86_400_000


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
    """Return ``[(key, value, ttl_stamped)]`` for default-CF changelog msgs."""
    msgs = []
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs["headers"]
        if headers[CHANGELOG_CF_MESSAGE_HEADER] == "default":
            ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))
            msgs.append((call.kwargs["key"], call.kwargs["value"], ttl_stamped))
    return msgs


def _replay_default(recovered, msgs, now_ms):
    """Replay default-CF msgs with an injected wallclock ``now_ms``."""
    recovered._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, ttl_stamped in msgs:
        recovered.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


class TestRecoveryWallclockLagging:
    """
    Event-time-lagging cold restore: stamps are expired by wallclock but NOT by
    the event-time frontier (high_water_ms).  The live filter would KEEP these
    records; recovery should not drop them.
    """

    def test_lagging_store_records_survive_cold_restore(
        self, store_partition_factory, changelog_producer_mock
    ):
        """
        Scenario: a dedup application processes historical events at event-time
        T = 1_000_000_000_000 (Sep 2001).  TTL = 7 days, so stamps (expiry) =
        T + 7d.  A cold restore happens with wallclock W = T + 365d (Sep 2002),
        far past every stamp.

        Live filter: high_water_ms after recovery is None (cold restore, no
        persisted high-water).  The first live event at event-time T would set
        high_water_ms = T.  stamp = T + 7d > T → NOT expired.

        Recovery filter (current code): stamp = T + 7d <= W (T + 365d) →
        EXPIRED → DROPPED.

        Expected (spec): records SURVIVE (event-time semantics).
        Actual (suspected): records DROPPED (wallclock semantics).
        """
        # -- Source: create a TTL-stamped store with 5 records --
        event_time = 1_000_000_000_000  # ~Sep 2001 epoch ms
        ttl = timedelta(days=7)
        expiry = event_time + 7 * DAY_MS  # T + 7 days

        partition = store_partition_factory(
            name="src", changelog_producer=changelog_producer_mock
        )
        with partition.begin() as tx:
            for i in range(5):
                tx.set(
                    key=f"dedup_key_{i}",
                    value=f"seen_{i}",
                    prefix=b"pfx",
                    timestamp=event_time,
                    ttl=ttl,
                )

        source_default = _decode_default_cf(partition)
        # Sanity: all 5 records share the same expiry = event_time + 7d.
        expiries = {exp for exp, _ in source_default.values()}
        assert expiries == {expiry}, f"Expected uniform expiry {expiry}, got {expiries}"

        changelog_producer_mock.produce.reset_mock()
        # Force a flush to capture changelog messages (the context-manager
        # begin() above auto-flushes, but produce mock was not reset yet).
        # Re-write to get the changelog capture.
        partition.close()
        partition = store_partition_factory(
            name="src2", changelog_producer=changelog_producer_mock
        )
        with partition.begin() as tx:
            for i in range(5):
                tx.set(
                    key=f"dedup_key_{i}",
                    value=f"seen_{i}",
                    prefix=b"pfx",
                    timestamp=event_time,
                    ttl=ttl,
                )
        source_default = _decode_default_cf(partition)
        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        partition.close()

        assert len(msgs) >= 5, f"Expected >= 5 changelog msgs, got {len(msgs)}"

        # -- Recovery: cold restore with wallclock FAR in the future --
        # Wallclock = event_time + 365 days (one year after event time).
        # Every stamp (event_time + 7d) is deeply in the past relative to
        # wallclock, but NOT relative to event-time frontier.
        wallclock_now = event_time + 365 * DAY_MS  # ~Sep 2002

        recovered = store_partition_factory(name="dst")
        _replay_default(recovered, msgs, now_ms=wallclock_now)

        # -- Assertions --
        recovered_default = _decode_default_cf(recovered)

        # The post-recovery high_water_ms is None (cold restore, no persisted
        # high-water). This means the live filter would NEVER expire any record
        # (stamp <= None is always False). All 5 records should survive.
        assert (
            recovered.high_water_ms is None
        ), "Post-recovery high_water_ms must be None on a cold restore"

        # CORE ASSERTION: records must survive recovery.
        # If finding #3 is real, this fails because the wallclock drop
        # deleted all 5 records (stamp <= wallclock_now).
        assert len(recovered_default) == 5, (
            f"Expected 5 records to survive cold restore (event-time-lagging "
            f"store: stamps are expired by wallclock {wallclock_now} but NOT "
            f"by event-time frontier high_water_ms=None). "
            f"Got {len(recovered_default)} survivors. "
            f"This confirms finding #3: recovery wallclock drop diverges from "
            f"the live event-time filter."
        )
        assert recovered_default == source_default

        # Index should also be rebuilt for every survivor.
        recovered_index = _decode_index_cf(recovered)
        assert len(recovered_index) == 5
        for key, (exp, _) in source_default.items():
            if exp != SENTINEL_NEVER:
                assert recovered_index[key] == exp
        recovered.close()

    def test_lagging_store_live_filter_would_keep_records(
        self, store_partition_factory, changelog_producer_mock
    ):
        """
        Supplementary: prove the live filter uses event-time (high_water_ms),
        not wallclock — so a record with stamp > high_water_ms is NOT expired
        even if stamp < wallclock.

        This test validates the DIVERGENCE between the two clocks by reading
        a record through the live transaction path after manually setting
        high_water_ms to a value below the stamp.
        """
        event_time = 1_000_000_000_000
        ttl = timedelta(days=7)
        expiry = event_time + 7 * DAY_MS

        partition = store_partition_factory(
            name="live_filter", changelog_producer=changelog_producer_mock
        )
        # Write a record with TTL.
        with partition.begin() as tx:
            tx.set(
                key="dedup_key",
                value="seen",
                prefix=b"pfx",
                timestamp=event_time,
                ttl=ttl,
            )

        # high_water_ms = event_time after the write.
        assert partition.high_water_ms == event_time

        # Now read the record back through the live path.
        # stamp = event_time + 7d > event_time (high_water) → NOT expired.
        tx = partition.begin()
        result = tx.get(key="dedup_key", prefix=b"pfx")
        assert result == "seen", (
            f"Live filter should KEEP record (stamp {expiry} > "
            f"high_water {event_time}), got {result!r}"
        )
        partition.close()

    def test_mixed_lagging_only_truly_expired_dropped(
        self, store_partition_factory, changelog_producer_mock
    ):
        """
        Mixed scenario: some records have stamps above event-time frontier
        (should survive) and some have stamps below (genuinely expired in
        event-time too).

        Under event-time semantics, the two groups are cleanly separable.
        Under wallclock semantics, ALL are dropped if wallclock is far enough.
        """
        base_event_time = 1_000_000_000_000

        partition = store_partition_factory(
            name="src_mixed", changelog_producer=changelog_producer_mock
        )
        with partition.begin() as tx:
            # Record at event_time with TTL = 1 day → stamp = base + 1d
            tx.set(
                key="short_ttl",
                value="v_short",
                prefix=b"pfx",
                timestamp=base_event_time,
                ttl=timedelta(days=1),
            )
            # Record at event_time with TTL = 30 days → stamp = base + 30d
            tx.set(
                key="long_ttl",
                value="v_long",
                prefix=b"pfx",
                timestamp=base_event_time,
                ttl=timedelta(days=30),
            )

        source_default = _decode_default_cf(partition)
        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        partition.close()

        # Wallclock = base + 10 days.
        # short_ttl stamp = base + 1d < base + 10d → expired by BOTH clocks
        # long_ttl stamp = base + 30d > base + 10d → expired by NEITHER clock
        # Under event-time (high_water=None on cold restore): BOTH survive
        #   because stamp <= None is always False.
        # Under wallclock: short_ttl dropped, long_ttl survives.
        #
        # The correct event-time behavior: both survive (high_water is None).
        wallclock_now = base_event_time + 10 * DAY_MS

        recovered = store_partition_factory(name="dst_mixed")
        _replay_default(recovered, msgs, now_ms=wallclock_now)

        recovered_default = _decode_default_cf(recovered)
        assert recovered.high_water_ms is None

        # Event-time semantics: both should survive (high_water is None).
        # If finding #3 is real: short_ttl is dropped (stamp <= wallclock),
        # only long_ttl survives → len == 1 instead of 2.
        assert len(recovered_default) == 2, (
            f"Expected 2 records (event-time high_water=None means nothing is "
            f"expired). Got {len(recovered_default)}. "
            f"Wallclock at {wallclock_now} dropped records that the live "
            f"event-time filter would keep."
        )
        assert recovered_default == source_default
        recovered.close()


def _wide_range_changelog(
    store_partition_factory, changelog_producer_mock, base, day_spans, name
):
    """Build a store whose stamps span a WIDE event-time range (one record per
    entry in ``day_spans``, each expiring at ``base + days``) and return
    ``(msgs, source_default)``."""
    partition = store_partition_factory(
        name=name, changelog_producer=changelog_producer_mock
    )
    with partition.begin() as tx:
        for i, days in enumerate(day_spans):
            tx.set(
                key=f"k{i}",
                value=f"v{i}",
                prefix=b"pfx",
                timestamp=base,
                ttl=timedelta(days=days),
            )
    source_default = _decode_default_cf(partition)
    msgs = _capture_default_cf_changelog(changelog_producer_mock)
    partition.close()
    return msgs, source_default


class TestNoRatchetFrontier:
    """The event-time replay-drop frontier is a single FIXED pre-replay snapshot;
    it never ratchets up toward the max replayed stamp (the collapse the old
    per-record event-time clock caused). Proven across a WIDE stamp range in both
    ascending and descending replay order, cold and warm.
    """

    DAY_SPANS = [1, 100, 200, 400]

    def test_wide_range_no_ratchet_cold_keeps_all(
        self, store_partition_factory, changelog_producer_mock
    ):
        base = 1_000_000_000_000
        msgs, source_default = _wide_range_changelog(
            store_partition_factory,
            changelog_producer_mock,
            base,
            self.DAY_SPANS,
            "src_wide_cold",
        )
        # A far-future wallclock: the OLD wallclock drop deleted every record; the
        # fix's COLD frontier is None, so nothing drops in EITHER replay order.
        wallclock_now = base + 10_000 * DAY_MS
        for order in ("ascending", "descending"):
            replay = msgs if order == "ascending" else list(reversed(msgs))
            recovered = store_partition_factory(name=f"dst_wide_cold_{order}")
            _replay_default(recovered, replay, now_ms=wallclock_now)
            assert recovered.high_water_ms is None
            recovered_default = _decode_default_cf(recovered)
            assert recovered_default == source_default, (
                f"cold restore ({order}) must keep ALL {len(source_default)} "
                f"wide-range records (frontier None); got {len(recovered_default)}"
            )
            recovered.close()

    def test_wide_range_no_ratchet_warm_drops_only_past_frontier(
        self, store_partition_factory, changelog_producer_mock
    ):
        base = 1_000_000_000_000
        msgs, source_default = _wide_range_changelog(
            store_partition_factory,
            changelog_producer_mock,
            base,
            self.DAY_SPANS,
            "src_wide_warm",
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
            recovered = store_partition_factory(name=f"dst_wide_warm_{order}")
            # WARM restore: seed the persisted event-time high-water. A RATCHET
            # would advance it toward the max replayed stamp and collateral-drop
            # the mid-range records (worst in descending order, where the max stamp
            # replays first); a FIXED frontier drops exactly stamp <= frontier,
            # identically in both orders.
            recovered.advance_high_water(frontier)
            _replay_default(recovered, replay, now_ms=frontier)
            recovered_default = _decode_default_cf(recovered)
            assert recovered_default == expected, (
                f"warm restore ({order}) must drop exactly stamps <= frontier "
                f"(no ratchet); expected {sorted(expected)} got "
                f"{sorted(recovered_default)}"
            )
            recovered.close()
