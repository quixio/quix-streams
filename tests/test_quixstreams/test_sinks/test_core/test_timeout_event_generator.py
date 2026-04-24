"""
Tests for StreamTimeoutTracker (quixstreams/sinks/core/timeout_event_generator.py).

Migrated from the sink test file; these exercise the tracker directly
without any sink construction. All behavioural coverage for the v6
per-key silence detector lives here.
"""

import logging
from unittest.mock import MagicMock

import pytest

from quixstreams.sinks.core.timeout_event_generator import StreamTimeoutTracker


# =============================================================================
# Helpers
# =============================================================================


def _freeze_now(tracker: StreamTimeoutTracker, value: int) -> None:
    """Monkeypatch the tracker's clock to return ``value`` ms."""
    tracker._now_ms = lambda: value


def _tracker(
    *,
    stream_timeout_ms=None,
    on_stream_timeout=None,
    check_interval_ms=None,
    idle_exit_cycles=3,
    logger=None,
) -> StreamTimeoutTracker:
    """Construct a tracker and disable the background thread by replacing
    ``_ensure_timer_thread_alive`` with a no-op. Tests that need the
    thread manipulate it explicitly.
    """
    t = StreamTimeoutTracker(
        stream_timeout_ms=stream_timeout_ms,
        on_stream_timeout=on_stream_timeout,
        check_interval_ms=check_interval_ms,
        idle_exit_cycles=idle_exit_cycles,
        logger=logger,
    )
    # Prevent the timer thread from starting during unit tests.
    t._ensure_timer_thread_alive = lambda: None
    return t


# =============================================================================
# 1. Disabled-feature tests (migrated from TestStreamTimeoutDisabled)
# =============================================================================


class TestTrackerDisabled:
    """When the pair is (None, None) the tracker is inert; mismatched
    pairs raise at construction time.
    """

    def test_both_none_disables_tracker(self):
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=None, on_stream_timeout=None
        )
        assert tracker.enabled is False
        # Disabled path: no per-stream dict is allocated.
        assert not hasattr(tracker, "_last_seen_by_stream")

    def test_disabled_touch_is_noop(self):
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=None, on_stream_timeout=None
        )
        # Does not raise, does not allocate, does not track.
        tracker.touch("s1")
        tracker.touch(b"bytes-key")
        tracker.touch(None)
        assert not hasattr(tracker, "_last_seen_by_stream")

    def test_disabled_check_now_is_noop(self):
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=None, on_stream_timeout=None
        )
        # Does not raise — no dict, no threshold, no callback.
        tracker.check_now()

    def test_disabled_start_stop_are_noops(self):
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=None, on_stream_timeout=None
        )
        tracker.start()  # no thread spawned
        tracker.stop()  # no exception
        assert not hasattr(tracker, "_timer_thread")

    def test_mismatched_pair_raises_threshold_only(self):
        with pytest.raises(ValueError, match="must both be provided"):
            StreamTimeoutTracker(
                stream_timeout_ms=6000, on_stream_timeout=None
            )

    def test_mismatched_pair_raises_callback_only(self):
        with pytest.raises(ValueError, match="must both be provided"):
            StreamTimeoutTracker(
                stream_timeout_ms=None, on_stream_timeout=MagicMock()
            )

    def test_non_positive_threshold_raises(self):
        with pytest.raises(ValueError, match="positive int"):
            StreamTimeoutTracker(
                stream_timeout_ms=0, on_stream_timeout=lambda s: None
            )
        with pytest.raises(ValueError, match="positive int"):
            StreamTimeoutTracker(
                stream_timeout_ms=-10, on_stream_timeout=lambda s: None
            )

    def test_bool_threshold_rejected(self):
        # bool is a subclass of int in Python; the validator rejects it.
        with pytest.raises(ValueError, match="positive int"):
            StreamTimeoutTracker(
                stream_timeout_ms=True, on_stream_timeout=lambda s: None
            )

    def test_non_callable_callback_raises(self):
        with pytest.raises(ValueError, match="callable"):
            StreamTimeoutTracker(
                stream_timeout_ms=6000, on_stream_timeout="not-callable"
            )


# =============================================================================
# 2. Construction defaults
# =============================================================================


class TestTrackerConstruction:
    """Default interval and overrides."""

    def test_default_check_interval_computed(self):
        tracker = _tracker(stream_timeout_ms=6000, on_stream_timeout=lambda s: None)
        # 6000 // 5 = 1200 -> clamped to [100, 1000] -> 1000
        assert tracker._check_interval_ms == 1000

    def test_default_check_interval_floor(self):
        tracker = _tracker(stream_timeout_ms=200, on_stream_timeout=lambda s: None)
        # 200 // 5 = 40 -> clamped up to 100
        assert tracker._check_interval_ms == 100

    def test_default_check_interval_midrange(self):
        tracker = _tracker(stream_timeout_ms=2500, on_stream_timeout=lambda s: None)
        # 2500 // 5 = 500, inside [100, 1000]
        assert tracker._check_interval_ms == 500

    def test_override_check_interval(self):
        tracker = _tracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=250,
        )
        assert tracker._check_interval_ms == 250

    def test_override_check_interval_rejected_for_non_positive(self):
        tracker = _tracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=0,
        )
        # Falls back to computed default.
        assert tracker._check_interval_ms == 1000

    def test_override_check_interval_rejected_for_bool(self):
        tracker = _tracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=True,
        )
        assert tracker._check_interval_ms == 1000

    def test_idle_exit_cycles_default(self):
        tracker = _tracker(stream_timeout_ms=6000, on_stream_timeout=lambda s: None)
        assert tracker._idle_exit_cycles == 3

    def test_idle_exit_cycles_override(self):
        tracker = _tracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            idle_exit_cycles=5,
        )
        assert tracker._idle_exit_cycles == 5


# =============================================================================
# 3. Simultaneous-silence tests (migrated from TestStreamTimeoutSimultaneous)
# =============================================================================


class TestTrackerSimultaneous:
    """4 keys go silent simultaneously; callback invoked once per key."""

    def test_four_keys_simultaneous_fires_four_callbacks(self):
        callback = MagicMock()
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=callback
        )

        _freeze_now(tracker, 10_000)
        for key in ["s1", "s2", "s3", "s4"]:
            tracker.touch(key)

        assert len(tracker._last_seen_by_stream) == 4
        assert set(tracker._last_seen_by_stream.keys()) == {"s1", "s2", "s3", "s4"}

        # Advance to t=16001 ms — 6001 ms of silence, past 6000 ms threshold
        _freeze_now(tracker, 16_001)
        tracker.check_now()

        assert callback.call_count == 4
        fired_keys = {call.args[0] for call in callback.call_args_list}
        assert fired_keys == {"s1", "s2", "s3", "s4"}
        assert len(tracker._last_seen_by_stream) == 0

    def test_four_keys_no_fire_before_threshold(self):
        callback = MagicMock()
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=callback
        )

        _freeze_now(tracker, 10_000)
        for key in ["s1", "s2", "s3", "s4"]:
            tracker.touch(key)

        _freeze_now(tracker, 15_999)  # 5999 ms of silence, < threshold
        tracker.check_now()

        callback.assert_not_called()
        assert len(tracker._last_seen_by_stream) == 4


# =============================================================================
# 4. Serial-silence tests (migrated from TestStreamTimeoutSerial)
# =============================================================================


class TestTrackerSerial:
    """4 keys go silent serially with 3s spacing; 4 callbacks in order."""

    def test_four_keys_serial_fires_in_order(self):
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )

        t0 = 10_000
        current = [t0]

        def now():
            return current[0]

        tracker._now_ms = now

        # s1 at t0, s2 at t0+3000, s3 at t0+6000, s4 at t0+9000
        tracker.touch("s1")
        current[0] = t0 + 3_000
        tracker.touch("s2")
        current[0] = t0 + 6_000
        tracker.touch("s3")
        current[0] = t0 + 9_000
        tracker.touch("s4")

        fire_log = []

        def tracking_callback(stream_name):
            fire_log.append((stream_name, current[0]))

        tracker._on_stream_timeout = tracking_callback

        # s1 fires at t0+6001
        current[0] = t0 + 6_001
        tracker.check_now()
        assert len(fire_log) == 1
        assert fire_log[0][0] == "s1"

        # s2 fires at t0+9001
        current[0] = t0 + 9_001
        tracker.check_now()
        assert len(fire_log) == 2
        assert fire_log[1][0] == "s2"

        # s3 fires at t0+12001
        current[0] = t0 + 12_001
        tracker.check_now()
        assert len(fire_log) == 3
        assert fire_log[2][0] == "s3"

        # s4 fires at t0+15001
        current[0] = t0 + 15_001
        tracker.check_now()
        assert len(fire_log) == 4
        assert fire_log[3][0] == "s4"

        # 3s spacing between fires
        for i in range(1, 4):
            spacing_ms = fire_log[i][1] - fire_log[i - 1][1]
            assert (
                spacing_ms == 3_000
            ), f"Spacing between fire {i - 1} and {i}: {spacing_ms}ms"

    def test_serial_keys_no_premature_fire(self):
        callback = MagicMock()
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=callback
        )

        t0 = 10_000
        _freeze_now(tracker, t0)
        tracker.touch("s1")
        _freeze_now(tracker, t0 + 3_000)
        tracker.touch("s2")

        _freeze_now(tracker, t0 + 5_999)  # s1 has 5999ms, s2 has 2999ms
        tracker.check_now()
        callback.assert_not_called()


# =============================================================================
# 5. Re-arm tests (migrated from TestStreamTimeoutRearm)
# =============================================================================


class TestTrackerRearm:
    """A fired key's next touch resurrects it as a fresh stream."""

    def test_rearm_produces_five_callbacks(self):
        fire_log = []
        current = [0]

        def tracking_callback(stream_name):
            fire_log.append((stream_name, current[0]))

        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=tracking_callback
        )
        tracker._now_ms = lambda: current[0]

        t0 = 10_000
        current[0] = t0
        for key in ["s1", "s2", "s3", "s4"]:
            tracker.touch(key)

        # Keepalive s2-s4 at t0+2000 (s1 silent)
        current[0] = t0 + 2_000
        for key in ["s2", "s3", "s4"]:
            tracker.touch(key)

        current[0] = t0 + 4_000
        for key in ["s2", "s3", "s4"]:
            tracker.touch(key)

        # At t0+6001: s1 fires
        current[0] = t0 + 6_001
        tracker.check_now()
        assert len(fire_log) == 1
        assert fire_log[0][0] == "s1"

        # Keepalives continue for s2-s4
        current[0] = t0 + 6_000
        for key in ["s2", "s3", "s4"]:
            tracker.touch(key)
        current[0] = t0 + 8_000
        for key in ["s2", "s3", "s4"]:
            tracker.touch(key)

        # Re-arm s1 at t0+10000
        current[0] = t0 + 10_000
        tracker.touch("s1")
        for key in ["s2", "s3", "s4"]:
            tracker.touch(key)

        # All 4 stop at t0+12000 (last sends)
        current[0] = t0 + 12_000
        for key in ["s2", "s3", "s4"]:
            tracker.touch(key)

        # s1 should NOT re-fire yet (2s since re-arm)
        tracker.check_now()
        assert len(fire_log) == 1

        # At t0+16001: s1 re-fires (6001ms since re-arm at t0+10000)
        current[0] = t0 + 16_001
        tracker.check_now()
        assert len(fire_log) == 2
        assert fire_log[1][0] == "s1"

        # At t0+18001: s2/s3/s4 all fire (6001ms silent since t0+12000)
        current[0] = t0 + 18_001
        tracker.check_now()
        assert len(fire_log) == 5

        from collections import Counter

        counts = Counter(name for name, _ in fire_log)
        assert counts["s1"] == 2
        assert counts["s2"] == 1
        assert counts["s3"] == 1
        assert counts["s4"] == 1

        assert len(tracker._last_seen_by_stream) == 0

    def test_rearm_ts_ms_monotonicity(self):
        fire_times = []
        current = [0]

        def tracking_callback(stream_name):
            if stream_name == "s1":
                fire_times.append(current[0])

        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=tracking_callback
        )
        tracker._now_ms = lambda: current[0]

        t0 = 10_000
        current[0] = t0
        tracker.touch("s1")

        current[0] = t0 + 6_001
        tracker.check_now()

        current[0] = t0 + 10_000
        tracker.touch("s1")

        current[0] = t0 + 16_001
        tracker.check_now()

        assert len(fire_times) == 2
        assert fire_times[1] > fire_times[0]
        assert fire_times[1] - fire_times[0] >= 5_000


# =============================================================================
# 6. Disabled re-arm sequence (migrated from TestStreamTimeoutDisabledRearmSequence)
# =============================================================================


class TestTrackerDisabledRearmSequence:
    """Full re-arm producer sequence with feature disabled -> zero callbacks."""

    def test_disabled_rearm_sequence_no_callbacks(self):
        callback = MagicMock()
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=None, on_stream_timeout=None
        )
        assert tracker.enabled is False

        # Replicate the producer sequence from the enabled-rearm test
        for key in ["s1", "s2", "s3", "s4"]:
            tracker.touch(key)

        for _ in range(5):
            for key in ["s2", "s3", "s4"]:
                tracker.touch(key)

        tracker.touch("s1")

        for key in ["s2", "s3", "s4"]:
            tracker.touch(key)

        tracker.check_now()

        assert not hasattr(tracker, "_last_seen_by_stream")
        callback.assert_not_called()


# =============================================================================
# 7. Key pass-through tests
# =============================================================================


class TestTrackerKeyPassthrough:
    """Keys are stored and surfaced as-is; ``None`` is silently skipped."""

    def test_null_key_silently_skipped(self):
        callback = MagicMock()
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=callback
        )
        _freeze_now(tracker, 10_000)
        tracker.touch(None)
        assert len(tracker._last_seen_by_stream) == 0

    def test_bytes_key_stored_as_bytes(self):
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )
        _freeze_now(tracker, 10_000)
        tracker.touch(b"sensor-a")
        assert b"sensor-a" in tracker._last_seen_by_stream

    def test_str_key_stored_as_str(self):
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )
        _freeze_now(tracker, 10_000)
        tracker.touch("sensor-c")
        assert "sensor-c" in tracker._last_seen_by_stream

    def test_int_key_stored_as_int(self):
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )
        _freeze_now(tracker, 10_000)
        tracker.touch(42)
        assert 42 in tracker._last_seen_by_stream

    def test_bytes_and_str_with_same_content_are_distinct_keys(self):
        """Raw pass-through: ``b'x'`` and ``'x'`` do not collide."""
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )
        _freeze_now(tracker, 10_000)
        tracker.touch(b"sensor")
        tracker.touch("sensor")
        assert b"sensor" in tracker._last_seen_by_stream
        assert "sensor" in tracker._last_seen_by_stream
        assert len(tracker._last_seen_by_stream) == 2

    def test_callback_receives_raw_key(self):
        """The callback gets the exact object passed to ``touch``."""
        received = []
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=received.append
        )
        _freeze_now(tracker, 10_000)
        tracker.touch(b"raw-bytes")
        _freeze_now(tracker, 20_000)  # past threshold
        tracker.check_now()
        assert received == [b"raw-bytes"]


# =============================================================================
# 8. TTL-sweep tests
# =============================================================================


class TestTrackerTTLSweep:
    """Keys older than 3x threshold are dropped silently (no callback)."""

    def test_ttl_sweep_drops_without_callback(self, caplog):
        callback = MagicMock()
        tracker = _tracker(
            stream_timeout_ms=6000,
            on_stream_timeout=callback,
            logger=logging.getLogger("quixstreams.sinks.core.timeout_event_generator"),
        )

        # Seed a key 4x older than threshold (24_000 ms old).
        _freeze_now(tracker, 100_000)
        tracker._last_seen_by_stream["ancient"] = 100_000 - 24_000

        with caplog.at_level(
            logging.WARNING, logger="quixstreams.sinks.core.timeout_event_generator"
        ):
            tracker.check_now()

        callback.assert_not_called()
        assert "ancient" not in tracker._last_seen_by_stream
        assert any("TTL sweep" in rec.getMessage() for rec in caplog.records)

    def test_ttl_sweep_mixed_with_fire(self):
        """Some keys fire, some get TTL-swept — both happen in one pass."""
        fired = []
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: fired.append(s)
        )

        now = 100_000
        _freeze_now(tracker, now)
        # ancient: 24s old -> TTL sweep
        tracker._last_seen_by_stream["ancient"] = now - 24_000
        # fresh-fire: 6.5s old -> fire
        tracker._last_seen_by_stream["fresh-fire"] = now - 6_500
        # fresh: 1s old -> stays
        tracker._last_seen_by_stream["fresh"] = now - 1_000

        tracker.check_now()

        assert fired == ["fresh-fire"]
        assert "ancient" not in tracker._last_seen_by_stream
        assert "fresh-fire" not in tracker._last_seen_by_stream
        assert "fresh" in tracker._last_seen_by_stream


# =============================================================================
# 9. Callback-failure retain-and-retry
# =============================================================================


class TestTrackerCallbackFailure:
    """A raising callback leaves the key in the tracker for retry."""

    def test_raising_callback_retains_key(self):
        attempts = [0]

        def flaky(stream_name):
            attempts[0] += 1
            if attempts[0] == 1:
                raise RuntimeError("boom")

        tracker = _tracker(stream_timeout_ms=6000, on_stream_timeout=flaky)

        _freeze_now(tracker, 10_000)
        tracker.touch("s1")

        # First check: callback raises, key must stay.
        _freeze_now(tracker, 16_001)
        tracker.check_now()
        assert attempts[0] == 1
        assert "s1" in tracker._last_seen_by_stream

        # Second check: callback succeeds, key evicted.
        _freeze_now(tracker, 16_050)
        tracker.check_now()
        assert attempts[0] == 2
        assert "s1" not in tracker._last_seen_by_stream


# =============================================================================
# 10. Now-ms override
# =============================================================================


class TestTrackerNowMs:
    """Passing an explicit now_ms to touch/check_now overrides the clock."""

    def test_touch_now_ms_override(self):
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )
        _freeze_now(tracker, 10_000)
        tracker.touch("s1", now_ms=20_000)
        # Explicit now_ms wins over tracker._now_ms.
        assert tracker._last_seen_by_stream["s1"] == 20_000

    def test_check_now_ms_override(self):
        fired = []
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: fired.append(s)
        )
        _freeze_now(tracker, 10_000)
        tracker.touch("s1")  # stamps at 10_000
        # Use explicit now_ms past threshold.
        tracker.check_now(now_ms=16_001)
        assert fired == ["s1"]


# =============================================================================
# 11. Start/stop idempotency
# =============================================================================


class TestTrackerStartStop:
    """start() and stop() are idempotent and gated by enabled."""

    def test_start_idempotent_on_enabled(self):
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=100,
        )
        try:
            tracker.start()
            thread1 = tracker._timer_thread
            assert thread1 is not None
            # Second start does not spawn a new thread.
            tracker.start()
            assert tracker._timer_thread is thread1
        finally:
            tracker.stop()

    def test_stop_after_start_signals_thread(self):
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=50,
        )
        tracker.start()
        assert not tracker._stop.is_set()
        tracker.stop()
        assert tracker._stop.is_set()

    def test_stop_before_start_is_safe(self):
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )
        # Never started; stop() just sets the event.
        tracker.stop()
        assert tracker._stop.is_set()

    def test_start_after_stop_is_noop(self):
        """A prior stop() permanently disables the thread; start() must
        not resurrect it (cleanup is terminal).
        """
        tracker = StreamTimeoutTracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=50,
        )
        tracker.stop()
        tracker.start()
        assert tracker._timer_thread is None


# =============================================================================
# 12. Thread self-terminate + respawn
# =============================================================================


class TestTrackerThreadLifecycle:
    """Empty-for-N-cycles thread self-terminates; next touch respawns it."""

    def test_thread_self_terminates_when_empty(self):
        """Run a tracker whose idle_exit_cycles=1 so the first empty
        cycle terminates the thread. Then verify the thread is cleared.
        """
        import time as _time

        tracker = StreamTimeoutTracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=50,
            idle_exit_cycles=1,
        )
        tracker.start()
        # Give the loop a few cycles to notice it's empty and exit.
        deadline = _time.time() + 2.0
        while _time.time() < deadline and tracker._timer_thread is not None:
            _time.sleep(0.05)
        assert tracker._timer_thread is None
        tracker.stop()

    def test_touch_respawns_terminated_thread(self):
        """After self-terminate, a new touch() re-spawns the timer."""
        import time as _time

        tracker = StreamTimeoutTracker(
            stream_timeout_ms=6000,
            on_stream_timeout=lambda s: None,
            check_interval_ms=50,
            idle_exit_cycles=1,
        )
        tracker.start()
        deadline = _time.time() + 2.0
        while _time.time() < deadline and tracker._timer_thread is not None:
            _time.sleep(0.05)
        assert tracker._timer_thread is None

        # New touch: respawns.
        tracker.touch("s1")
        assert tracker._timer_thread is not None
        assert tracker._timer_thread.is_alive()
        tracker.stop()


# =============================================================================
# 13. on_paused analog: tracker has no on_paused; state unaffected by external calls
# =============================================================================


class TestTrackerStateIsolation:
    """The tracker exposes no on_paused hook; all state mutation flows
    through touch/check_now/stop.
    """

    def test_tracker_has_no_on_paused(self):
        tracker = _tracker(
            stream_timeout_ms=6000, on_stream_timeout=lambda s: None
        )
        # Explicit: the tracker does not claim to know about pause events.
        assert not hasattr(tracker, "on_paused")
