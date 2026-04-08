from unittest.mock import MagicMock, patch

import pytest

from quixstreams.internal_producer import InternalProducer
from quixstreams.models.topics import Topic, TopicManager
from quixstreams.processing.watermarking import WatermarkManager

# A large idle_timeout to disable idle detection in tests that don't need it
_NO_IDLE = 999_999.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manager(
    interval: float = 1.0,
    idle_timeout: float = _NO_IDLE,
) -> WatermarkManager:
    """Return a WatermarkManager with mocked dependencies."""
    producer = MagicMock(spec=InternalProducer)
    topic_manager = MagicMock(spec=TopicManager)

    watermarks_topic = MagicMock(spec=Topic)
    watermarks_topic.name = "watermarks__test-group--watermarks"
    topic_manager.watermarks_topic.return_value = watermarks_topic

    manager = WatermarkManager(
        producer=producer,
        topic_manager=topic_manager,
        interval=interval,
        idle_timeout=idle_timeout,
    )
    # Force watermarks_topic to be resolved so we don't need to worry about the lazy property
    _ = manager.watermarks_topic
    return manager


def _make_topic(name: str, num_partitions: int = 1) -> MagicMock:
    topic = MagicMock(spec=Topic)
    topic.name = name
    topic.broker_config.num_partitions = num_partitions
    return topic


# ---------------------------------------------------------------------------
# store()
# ---------------------------------------------------------------------------


class TestWatermarkManagerStore:
    def test_store_default_watermark(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 100, default=True)
        assert mgr._to_produce[("topic-a", 0)] == (100, True)

    def test_store_non_default_watermark(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 200, default=False)
        assert mgr._to_produce[("topic-a", 0)] == (200, False)

    def test_store_default_does_not_override_non_default(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 200, default=False)
        mgr.store("topic-a", 0, 999, default=True)
        # Non-default (200) must survive
        assert mgr._to_produce[("topic-a", 0)] == (200, False)

    def test_store_non_default_overrides_default(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 100, default=True)
        mgr.store("topic-a", 0, 50, default=False)
        # Non-default replaces default; max(100, 50) = 100 but non-default wins
        assert mgr._to_produce[("topic-a", 0)] == (100, False)

    def test_store_only_advances_forward(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 100, default=True)
        mgr.store("topic-a", 0, 50, default=True)
        assert mgr._to_produce[("topic-a", 0)] == (100, True)

    def test_store_two_defaults_takes_max(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 100, default=True)
        mgr.store("topic-a", 0, 200, default=True)
        assert mgr._to_produce[("topic-a", 0)] == (200, True)

    def test_store_two_non_defaults_takes_max(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 300, default=False)
        mgr.store("topic-a", 0, 500, default=False)
        assert mgr._to_produce[("topic-a", 0)] == (500, False)

    def test_store_negative_timestamp_raises(self):
        mgr = _make_manager()
        with pytest.raises(ValueError):
            mgr.store("topic-a", 0, -1, default=True)

    def test_store_independent_partitions(self):
        mgr = _make_manager()
        mgr.store("topic-a", 0, 100, default=True)
        mgr.store("topic-a", 1, 200, default=True)
        assert mgr._to_produce[("topic-a", 0)] == (100, True)
        assert mgr._to_produce[("topic-a", 1)] == (200, True)


# ---------------------------------------------------------------------------
# produce()
# ---------------------------------------------------------------------------


class TestWatermarkManagerProduce:
    def test_produce_publishes_pending_watermarks(self):
        mgr = _make_manager(interval=0)
        mgr.store("topic-a", 0, 100, default=True)
        mgr.store("topic-b", 1, 200, default=True)

        mgr.produce()

        assert mgr._producer.produce.call_count == 2
        calls = {
            call.kwargs["key"]: call.kwargs["value"]
            for call in mgr._producer.produce.call_args_list
        }
        assert "topic-a[0]" in calls
        assert "topic-b[1]" in calls

    def test_produce_clears_pending_after_flush(self):
        mgr = _make_manager(interval=0)
        mgr.store("topic-a", 0, 100, default=True)
        mgr.produce()
        assert mgr._to_produce == {}

    def test_produce_respects_interval(self):
        mgr = _make_manager(interval=60.0)
        # Prime _last_produced so the first call fires at t=0
        mgr._last_produced = -100.0
        mgr.store("topic-a", 0, 100, default=True)

        with patch("quixstreams.processing.watermarking.monotonic", return_value=0.0):
            mgr.produce()  # fires; _last_produced → 0.0

        # Second call within interval (t=0.5 < 0.0 + 60.0) must be skipped
        mgr._producer.produce.reset_mock()
        mgr.store("topic-a", 0, 200, default=True)

        with patch("quixstreams.processing.watermarking.monotonic", return_value=0.5):
            mgr.produce()

        mgr._producer.produce.assert_not_called()

    def test_produce_fires_after_interval_elapses(self):
        mgr = _make_manager(interval=1.0)
        # Prime _last_produced so the first call fires at t=0
        mgr._last_produced = -100.0
        mgr.store("topic-a", 0, 100, default=True)

        with patch("quixstreams.processing.watermarking.monotonic", return_value=0.0):
            mgr.produce()  # fires; _last_produced → 0.0

        mgr._producer.produce.reset_mock()
        mgr.store("topic-a", 0, 200, default=True)

        # t=1.5 >= 0.0 + 1.0 → fires
        with patch("quixstreams.processing.watermarking.monotonic", return_value=1.5):
            mgr.produce()

        mgr._producer.produce.assert_called_once()

    def test_produce_noop_when_nothing_pending(self):
        mgr = _make_manager(interval=0)
        mgr.produce()
        mgr._producer.produce.assert_not_called()

    def test_produce_uses_correct_topic_name(self):
        mgr = _make_manager(interval=0)
        mgr.store("topic-a", 0, 100, default=True)
        mgr.produce()

        call = mgr._producer.produce.call_args
        assert call.kwargs["topic"] == mgr.watermarks_topic.name


# ---------------------------------------------------------------------------
# receive()
# ---------------------------------------------------------------------------


class TestWatermarkManagerReceive:
    def test_receive_advances_global_watermark(self):
        mgr = _make_manager()
        mgr._watermarks = {("topic-a", 0): -1}

        result = mgr.receive({"topic": "topic-a", "partition": 0, "timestamp": 100})

        assert result == 100
        assert mgr._watermarks[("topic-a", 0)] == 100

    def test_receive_returns_none_when_slow_partition_blocks(self):
        mgr = _make_manager()
        # Two partitions: one already received, one still at -1
        mgr._watermarks = {("topic-a", 0): -1, ("topic-a", 1): -1}

        # Advance partition 0 only
        result = mgr.receive({"topic": "topic-a", "partition": 0, "timestamp": 100})

        # Global watermark is still min(-1 blocked by partition 1) → no advance
        assert result is None

    def test_receive_global_is_min_of_all_partitions(self):
        mgr = _make_manager()
        mgr._watermarks = {
            ("topic-a", 0): -1,
            ("topic-a", 1): -1,
            ("topic-a", 2): -1,
        }

        mgr.receive({"topic": "topic-a", "partition": 0, "timestamp": 500})
        mgr.receive({"topic": "topic-a", "partition": 1, "timestamp": 300})
        result = mgr.receive({"topic": "topic-a", "partition": 2, "timestamp": 400})

        # min(500, 300, 400) == 300
        assert result == 300

    def test_receive_unknown_tp_is_stored(self):
        mgr = _make_manager()
        mgr._watermarks = {}

        result = mgr.receive({"topic": "topic-x", "partition": 5, "timestamp": 42})

        assert mgr._watermarks[("topic-x", 5)] == 42
        assert result == 42

    def test_receive_does_not_regress(self):
        mgr = _make_manager()
        mgr._watermarks = {("topic-a", 0): 200}

        result = mgr.receive({"topic": "topic-a", "partition": 0, "timestamp": 50})

        # Stored value should not drop below 200
        assert mgr._watermarks[("topic-a", 0)] == 200
        assert result is None

    def test_receive_returns_none_when_watermark_does_not_advance(self):
        mgr = _make_manager()
        mgr._watermarks = {("topic-a", 0): 100}

        # Same timestamp — no advancement
        result = mgr.receive({"topic": "topic-a", "partition": 0, "timestamp": 100})

        assert result is None


# ---------------------------------------------------------------------------
# set_topics() and on_revoke()
# ---------------------------------------------------------------------------


class TestWatermarkManagerSetTopicsAndRevoke:
    def test_set_topics_primes_all_partitions_with_minus_one(self):
        mgr = _make_manager()
        topics = [
            _make_topic("topic-a", num_partitions=2),
            _make_topic("topic-b", num_partitions=1),
        ]
        mgr.set_topics(topics)

        assert mgr._watermarks == {
            ("topic-a", 0): -1,
            ("topic-a", 1): -1,
            ("topic-b", 0): -1,
        }

    def test_set_topics_clears_previous_state(self):
        mgr = _make_manager()
        mgr._watermarks = {("old-topic", 0): 999}
        mgr.set_topics([_make_topic("new-topic", num_partitions=1)])

        assert ("old-topic", 0) not in mgr._watermarks
        assert ("new-topic", 0) in mgr._watermarks

    def test_on_revoke_removes_tp_from_pending(self):
        mgr = _make_manager()
        mgr._to_produce[("topic-a", 0)] = (100, True)
        mgr._to_produce[("topic-a", 1)] = (200, True)

        mgr.on_revoke("topic-a", 0)

        assert ("topic-a", 0) not in mgr._to_produce
        assert ("topic-a", 1) in mgr._to_produce

    def test_on_revoke_removes_tp_from_watermarks(self):
        mgr = _make_manager()
        mgr._watermarks = {("topic-a", 0): 100, ("topic-a", 1): 200}
        mgr._last_updated = {("topic-a", 0): 0, ("topic-a", 1): 0}

        mgr.on_revoke("topic-a", 0)

        assert ("topic-a", 0) not in mgr._watermarks
        assert ("topic-a", 0) not in mgr._last_updated
        assert ("topic-a", 1) in mgr._watermarks

    def test_on_revoke_missing_tp_is_noop(self):
        mgr = _make_manager()
        # Should not raise even if TP was never tracked
        mgr.on_revoke("topic-z", 99)


# ---------------------------------------------------------------------------
# _get_watermark()
# ---------------------------------------------------------------------------


class TestWatermarkManagerGetWatermark:
    def test_returns_minus_one_when_no_partitions(self):
        mgr = _make_manager()
        mgr._watermarks = {}
        assert mgr._get_watermark() == -1

    def test_returns_min_across_partitions(self):
        mgr = _make_manager()
        mgr._watermarks = {
            ("topic-a", 0): 500,
            ("topic-a", 1): 100,
            ("topic-a", 2): 300,
        }
        assert mgr._get_watermark() == 100

    def test_returns_single_value_when_one_partition(self):
        mgr = _make_manager()
        mgr._watermarks = {("topic-a", 0): 42}
        assert mgr._get_watermark() == 42


# ---------------------------------------------------------------------------
# Idle partition detection
# ---------------------------------------------------------------------------


class TestWatermarkManagerIdleDetection:
    def test_idle_partition_excluded_from_global_watermark(self):
        mgr = _make_manager(idle_timeout=5.0)
        # Partition 0 is active, partition 1 is idle (never updated)
        mgr._watermarks = {("topic-a", 0): 500, ("topic-a", 1): -1}
        mgr._last_updated = {
            ("topic-a", 0): 1_000_000.0,  # recent
            ("topic-a", 1): 0.0,  # ancient
        }

        with patch(
            "quixstreams.processing.watermarking.monotonic",
            return_value=1_000_000.0,
        ):
            assert mgr._get_watermark() == 500

    def test_all_active_partitions_use_min(self):
        mgr = _make_manager(idle_timeout=5.0)
        mgr._watermarks = {
            ("topic-a", 0): 500,
            ("topic-a", 1): 200,
            ("topic-a", 2): 300,
        }
        now = 1_000_000.0
        mgr._last_updated = {tp: now for tp in mgr._watermarks}

        with patch(
            "quixstreams.processing.watermarking.monotonic", return_value=now
        ):
            assert mgr._get_watermark() == 200

    def test_all_partitions_idle_returns_minus_one(self):
        mgr = _make_manager(idle_timeout=5.0)
        mgr._watermarks = {("topic-a", 0): -1, ("topic-a", 1): -1}
        mgr._last_updated = {("topic-a", 0): 0.0, ("topic-a", 1): 0.0}

        with patch(
            "quixstreams.processing.watermarking.monotonic",
            return_value=1_000_000.0,
        ):
            assert mgr._get_watermark() == -1

    def test_partition_becomes_idle_after_timeout(self):
        mgr = _make_manager(idle_timeout=10.0)
        mgr._watermarks = {("topic-a", 0): 500, ("topic-a", 1): 100}
        mgr._last_updated = {
            ("topic-a", 0): 100.0,
            ("topic-a", 1): 100.0,
        }

        # At t=105, partition 1 is still within timeout
        with patch(
            "quixstreams.processing.watermarking.monotonic", return_value=105.0
        ):
            assert mgr._get_watermark() == 100  # both active

        # At t=115, both partitions are past idle timeout
        # but partition 0 has ts=500, partition 1 has ts=100
        # both are idle, so neither is active
        with patch(
            "quixstreams.processing.watermarking.monotonic", return_value=115.0
        ):
            assert mgr._get_watermark() == -1

    def test_receive_resets_idle_timer(self):
        mgr = _make_manager(idle_timeout=10.0)
        mgr._watermarks = {("topic-a", 0): 100, ("topic-a", 1): -1}
        mgr._last_updated = {("topic-a", 0): 50.0, ("topic-a", 1): 0.0}

        # Receiving a watermark for partition 1 should reset its idle timer
        with patch(
            "quixstreams.processing.watermarking.monotonic", return_value=55.0
        ):
            mgr.receive({"topic": "topic-a", "partition": 1, "timestamp": 200})

        assert mgr._last_updated[("topic-a", 1)] == 55.0

    def test_store_resets_idle_timer(self):
        mgr = _make_manager(idle_timeout=10.0)
        mgr._watermarks = {("topic-a", 0): -1}
        mgr._last_updated = {("topic-a", 0): 0.0}

        with patch(
            "quixstreams.processing.watermarking.monotonic", return_value=99.0
        ):
            mgr.store("topic-a", 0, 100, default=True)

        assert mgr._last_updated[("topic-a", 0)] == 99.0

    def test_set_topics_initializes_last_updated(self):
        mgr = _make_manager(idle_timeout=10.0)
        topics = [_make_topic("topic-a", num_partitions=2)]

        with patch(
            "quixstreams.processing.watermarking.monotonic", return_value=42.0
        ):
            mgr.set_topics(topics)

        assert mgr._last_updated == {
            ("topic-a", 0): 42.0,
            ("topic-a", 1): 42.0,
        }

    def test_idle_partition_unblocks_receive(self):
        """Key scenario: partition 2 has no data, should not block watermark advancement."""
        mgr = _make_manager(idle_timeout=5.0)
        mgr._watermarks = {
            ("topic-a", 0): -1,
            ("topic-a", 1): -1,
            ("topic-a", 2): -1,  # will never get data
        }
        now = 100.0
        mgr._last_updated = {tp: now for tp in mgr._watermarks}

        # Receive watermarks for partitions 0 and 1 only
        with patch(
            "quixstreams.processing.watermarking.monotonic", return_value=now
        ):
            mgr.receive({"topic": "topic-a", "partition": 0, "timestamp": 500})
            result = mgr.receive(
                {"topic": "topic-a", "partition": 1, "timestamp": 300}
            )

        # Partition 2 is still active (within timeout), so it blocks at -1
        assert result is None

        # After idle timeout, partition 2 is excluded.
        # The first receive reactivates partition 1 and unblocks the watermark.
        with patch(
            "quixstreams.processing.watermarking.monotonic",
            return_value=now + 6.0,
        ):
            result = mgr.receive(
                {"topic": "topic-a", "partition": 1, "timestamp": 301}
            )

        # Partition 2 is idle; partition 0 is also idle but partition 1 just
        # got updated.  Global watermark = min(active) = 301
        assert result == 301


# ---------------------------------------------------------------------------
# TopicManager.watermarks_topic()
# ---------------------------------------------------------------------------


class TestTopicManagerWatermarksTopic:
    def _make_topic_manager(self, consumer_group: str = "my-group") -> TopicManager:
        topic_manager = MagicMock(spec=TopicManager)
        # Restore the real method under test while keeping Kafka calls mocked
        topic_manager.watermarks_topic = TopicManager.watermarks_topic.__get__(
            topic_manager
        )
        topic_manager._consumer_group = consumer_group
        topic_manager.default_replication_factor = 1

        # Mock _internal_name to return a predictable value
        topic_manager._internal_name = lambda t, n, s: f"{t}__{consumer_group}--{s}"

        # Mock Kafka-touching methods to return the topic unchanged
        topic_manager._get_or_create_broker_topic = lambda t: t
        topic_manager._configure_topic = lambda t, b: t
        topic_manager._watermarks_topics = {}

        return topic_manager

    def test_watermarks_topic_has_single_partition(self):
        tm = self._make_topic_manager()
        topic = tm.watermarks_topic()
        assert topic.create_config.num_partitions == 1

    def test_watermarks_topic_name_includes_consumer_group(self):
        tm = self._make_topic_manager(consumer_group="my-group")
        topic = tm.watermarks_topic()
        assert "my-group" in topic.name

    def test_watermarks_topic_is_registered(self):
        tm = self._make_topic_manager()
        topic = tm.watermarks_topic()
        assert topic.name in tm._watermarks_topics
