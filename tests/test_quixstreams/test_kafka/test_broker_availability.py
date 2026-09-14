import json
import time
import uuid
from unittest.mock import patch

import pytest
from confluent_kafka import KafkaError

from quixstreams.kafka.consumer import BaseConsumer
from quixstreams.kafka.exceptions import KafkaBrokerUnavailableError
from quixstreams.kafka.producer import Producer


class TestProducerBrokerAvailability:
    """Tests for Producer detecting prolonged broker unavailability."""

    def _simulate_all_brokers_down(self, producer: Producer):
        """Trigger the producer's error callback with _ALL_BROKERS_DOWN."""
        error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
        producer._error_cb(error)

    def test_all_brokers_down_sets_timestamp(self):
        """When _ALL_BROKERS_DOWN fires, the producer should record the timestamp."""
        producer = Producer(broker_address="localhost:9092")
        assert producer._broker_unavailable_since is None

        self._simulate_all_brokers_down(producer)
        assert producer._broker_unavailable_since is not None

    def test_all_brokers_down_keeps_first_timestamp(self):
        """Repeated _ALL_BROKERS_DOWN should keep the original timestamp, not update it."""
        producer = Producer(broker_address="localhost:9092")

        with patch("time.monotonic", return_value=100.0):
            self._simulate_all_brokers_down(producer)
        first_ts = producer._broker_unavailable_since

        with patch("time.monotonic", return_value=200.0):
            self._simulate_all_brokers_down(producer)
        assert producer._broker_unavailable_since == first_ts

    def test_raise_if_broker_unavailable_raises_after_timeout(self):
        """Should raise after _ALL_BROKERS_DOWN has persisted beyond the timeout
        and the active metadata probe also fails."""
        from unittest.mock import MagicMock

        producer = Producer(broker_address="localhost:9092")

        with patch("time.monotonic", return_value=100.0):
            self._simulate_all_brokers_down(producer)

        # Install a fake inner producer so the metadata probe fails
        fake = MagicMock()
        fake.list_topics.side_effect = Exception("brokers down")
        producer._inner_producer = fake

        with patch("time.monotonic", return_value=280.0):
            with pytest.raises(
                KafkaBrokerUnavailableError,
                match="broker_availability_timeout",
            ):
                producer.raise_if_broker_unavailable(timeout=120.0)

    def test_raise_if_broker_unavailable_no_raise_before_timeout(self):
        """Should NOT raise if timeout hasn't elapsed yet."""
        producer = Producer(broker_address="localhost:9092")

        with patch("time.monotonic", return_value=100.0):
            self._simulate_all_brokers_down(producer)

        with patch("time.monotonic", return_value=150.0):
            # 50s elapsed, timeout is 120s — should not raise
            producer.raise_if_broker_unavailable(timeout=120.0)

    def test_raise_if_broker_unavailable_no_raise_when_brokers_ok(self):
        """Should NOT raise if no _ALL_BROKERS_DOWN was ever reported."""
        producer = Producer(broker_address="localhost:9092")
        producer.raise_if_broker_unavailable(timeout=120.0)

    def test_broker_available_resets_timestamp(self):
        """Calling broker_available() should reset the unavailable timestamp."""
        producer = Producer(broker_address="localhost:9092")

        self._simulate_all_brokers_down(producer)
        assert producer._broker_unavailable_since is not None

        producer._broker_available()
        assert producer._broker_unavailable_since is None

    def test_broker_available_prevents_raise_after_reset(self):
        """After a reset via broker_available(), the timeout check should not raise."""
        producer = Producer(broker_address="localhost:9092")

        with patch("time.monotonic", return_value=100.0):
            self._simulate_all_brokers_down(producer)

        # Broker comes back
        producer._broker_available()

        # Even with enough elapsed time, should not raise because reset happened
        with patch("time.monotonic", return_value=300.0):
            producer.raise_if_broker_unavailable(timeout=120.0)

    def test_error_cb_still_handles_other_errors(self):
        """Non-ALL_BROKERS_DOWN errors should still be logged normally,
        not treated as broker unavailability."""
        producer = Producer(broker_address="localhost:9092")

        # Simulate a different error (e.g., _TRANSPORT)
        error = KafkaError(KafkaError._TRANSPORT)
        producer._error_cb(error)

        # Should not set broker unavailable timestamp
        assert producer._broker_unavailable_since is None

    def test_error_cb_destroy_does_not_set_unavailable(self):
        """_DESTROY errors should be ignored like before, not setting unavailable."""
        producer = Producer(broker_address="localhost:9092")

        error = KafkaError(KafkaError._DESTROY)
        producer._error_cb(error)

        assert producer._broker_unavailable_since is None

    def test_active_probe_resets_timer_when_brokers_recover(self):
        """If the metadata probe succeeds, the timer should be reset instead of raising."""
        from unittest.mock import MagicMock

        producer = Producer(broker_address="localhost:9092")

        with patch("time.monotonic", return_value=100.0):
            self._simulate_all_brokers_down(producer)

        # Install a fake inner producer where the probe succeeds
        fake = MagicMock()
        fake.list_topics.return_value = MagicMock()  # success
        producer._inner_producer = fake

        with patch("time.monotonic", return_value=280.0):
            # Should NOT raise because probe succeeds
            producer.raise_if_broker_unavailable(timeout=120.0)

        # Timer should have been reset
        assert producer._broker_unavailable_since is None

    def test_custom_error_callback_still_tracks_brokers(self):
        """A custom error_callback should still get broker availability tracking."""
        calls = []

        def my_error_cb(error):
            calls.append(error.code())

        producer = Producer(broker_address="localhost:9092", error_callback=my_error_cb)

        error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
        producer._error_cb(error)

        # Custom callback was called
        assert KafkaError._ALL_BROKERS_DOWN in calls
        # AND tracking was set
        assert producer._broker_unavailable_since is not None

    def test_error_message_includes_parameter_name(self):
        """The error message should mention broker_availability_timeout for discoverability."""
        from unittest.mock import MagicMock

        producer = Producer(broker_address="localhost:9092")

        with patch("time.monotonic", return_value=100.0):
            self._simulate_all_brokers_down(producer)

        fake = MagicMock()
        fake.list_topics.side_effect = Exception("down")
        producer._inner_producer = fake

        with patch("time.monotonic", return_value=280.0):
            with pytest.raises(
                KafkaBrokerUnavailableError,
                match="broker_availability_timeout.*set to 0 to disable",
            ):
                producer.raise_if_broker_unavailable(timeout=120.0)


class TestAppBrokerAvailability:
    """Tests for Application raising on prolonged broker unavailability.

    These are unit tests that verify the wiring between Application and
    Producer's broker availability tracking, without requiring Kafka.
    """

    def _make_app(self, **kwargs):
        """Create an Application with mocked Kafka connections."""
        from quixstreams.app import Application

        defaults = dict(
            broker_address="localhost:9092",
            consumer_group="test-group",
            auto_create_topics=False,
            use_changelog_topics=False,
        )
        defaults.update(kwargs)
        return Application(**defaults)

    def test_app_raises_when_brokers_unavailable_beyond_timeout(self):
        """The app should raise KafkaBrokerUnavailableError when brokers have been
        down for longer than broker_availability_timeout."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        from quixstreams.internal_consumer import InternalConsumer
        from quixstreams.models.topics.admin import TopicAdmin
        from quixstreams.models.topics.topic import TopicConfig

        app = self._make_app(broker_availability_timeout=0.1)

        # Simulate _ALL_BROKERS_DOWN in the past
        app._producer._producer._broker_unavailable_since = time.monotonic() - 10.0

        # Make the active metadata probe fail (brokers truly down)
        fake_confluent = MagicMock()
        fake_confluent.list_topics.side_effect = Exception("brokers down")
        app._producer._producer._inner_producer = fake_confluent

        def fake_inspect(topic_names, timeout=None):
            return {
                n: TopicConfig(num_partitions=1, replication_factor=1)
                for n in topic_names
            }

        with mock_patch.object(TopicAdmin, "inspect_topics", side_effect=fake_inspect):
            topic = app.topic(str(uuid.uuid4()))
        app.dataframe(topic)

        def mock_poll_row(self_consumer, *args, **kwargs):
            return None

        with mock_patch.object(InternalConsumer, "poll_row", mock_poll_row):
            with mock_patch.object(InternalConsumer, "_subscribe"):
                with pytest.raises(KafkaBrokerUnavailableError):
                    app.run()

    def test_app_does_not_raise_when_check_disabled(self):
        """With broker_availability_timeout=0, the app should NOT check
        broker availability."""
        from unittest.mock import patch as mock_patch

        from quixstreams.internal_consumer import InternalConsumer
        from quixstreams.models.topics.admin import TopicAdmin
        from quixstreams.models.topics.topic import TopicConfig

        app = self._make_app(broker_availability_timeout=0)

        # Simulate _ALL_BROKERS_DOWN in the past
        app._producer._producer._broker_unavailable_since = time.monotonic() - 9999.0

        def fake_inspect(topic_names, timeout=None):
            return {
                n: TopicConfig(num_partitions=1, replication_factor=1)
                for n in topic_names
            }

        with mock_patch.object(TopicAdmin, "inspect_topics", side_effect=fake_inspect):
            topic = app.topic(str(uuid.uuid4()))
        app.dataframe(topic)

        poll_count = 0

        def mock_poll_row(self_consumer, *args, **kwargs):
            nonlocal poll_count
            poll_count += 1
            if poll_count >= 3:
                app.stop()
            return None

        with mock_patch.object(InternalConsumer, "poll_row", mock_poll_row):
            with mock_patch.object(InternalConsumer, "_subscribe"):
                # Should NOT raise — just run and stop normally
                app.run()

    def test_internal_producer_broker_available_passthrough(self):
        """InternalProducer._broker_available() should reset the tracker
        on the underlying Producer."""
        app = self._make_app()

        # Set the tracker as if brokers were down
        app._producer._producer._broker_unavailable_since = time.monotonic() - 10.0

        # Call via InternalProducer passthrough
        app._producer._broker_available()

        assert app._producer._producer._broker_unavailable_since is None

    def test_default_timeout_is_120_seconds(self):
        """The default broker_availability_timeout should be 120s."""
        app = self._make_app()
        assert app._broker_availability_timeout == 120.0

    def test_negative_timeout_raises_value_error(self):
        """A negative broker_availability_timeout should raise ValueError."""
        with pytest.raises(
            ValueError, match="broker_availability_timeout must be >= 0"
        ):
            self._make_app(broker_availability_timeout=-1)


class TestConsumerBrokerAvailability:
    """Tests for BaseConsumer detecting prolonged broker unavailability."""

    def test_all_brokers_down_sets_timestamp(self):
        """When _ALL_BROKERS_DOWN fires, the consumer should record the timestamp."""
        from quixstreams.kafka.consumer import BaseConsumer

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )
        assert consumer._broker_unavailable_since is None

        error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
        consumer._error_cb(error)
        assert consumer._broker_unavailable_since is not None

    def test_broker_available_resets_timestamp(self):
        """_broker_available() should reset the unavailable timestamp."""
        from quixstreams.kafka.consumer import BaseConsumer

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )
        error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
        consumer._error_cb(error)
        assert consumer._broker_unavailable_since is not None

        consumer._broker_available()
        assert consumer._broker_unavailable_since is None

    def test_raise_if_broker_unavailable_raises_after_timeout(self):
        """Should raise after timeout with failed probe."""
        from unittest.mock import MagicMock

        from quixstreams.kafka.consumer import BaseConsumer

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )

        with patch("time.monotonic", return_value=100.0):
            error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
            consumer._error_cb(error)

        fake = MagicMock()
        fake.list_topics.side_effect = Exception("down")
        consumer._inner_consumer = fake

        with patch("time.monotonic", return_value=280.0):
            with pytest.raises(
                KafkaBrokerUnavailableError,
                match="broker_availability_timeout",
            ):
                consumer.raise_if_broker_unavailable(timeout=120.0)

    def test_active_probe_resets_timer_when_brokers_recover(self):
        """If the metadata probe succeeds, the timer should reset."""
        from unittest.mock import MagicMock

        from quixstreams.kafka.consumer import BaseConsumer

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )

        with patch("time.monotonic", return_value=100.0):
            error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
            consumer._error_cb(error)

        fake = MagicMock()
        fake.list_topics.return_value = MagicMock()
        consumer._inner_consumer = fake

        with patch("time.monotonic", return_value=280.0):
            consumer.raise_if_broker_unavailable(timeout=120.0)

        assert consumer._broker_unavailable_since is None

    def test_custom_error_callback_still_tracks_brokers(self):
        """Custom error callback should still get broker tracking."""
        from quixstreams.kafka.consumer import BaseConsumer

        calls = []

        def my_error_cb(error):
            calls.append(error.code())

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
            error_callback=my_error_cb,
        )

        error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
        consumer._error_cb(error)

        assert KafkaError._ALL_BROKERS_DOWN in calls
        assert consumer._broker_unavailable_since is not None


class TestRecoveryBrokerAvailability:
    """Tests for broker availability checks during state recovery."""

    def test_recovery_loop_raises_when_broker_unavailable(self):
        """The recovery loop should raise KafkaBrokerUnavailableError when
        brokers have been down longer than the configured timeout."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        from quixstreams.kafka.consumer import BaseConsumer
        from quixstreams.models.topics import TopicManager
        from quixstreams.state.recovery import RecoveryManager

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )

        topic_manager = MagicMock(spec=TopicManager)
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=topic_manager,
            broker_availability_timeout=0.1,
        )

        # Simulate broker down for a long time
        consumer._broker_unavailable_since = time.monotonic() - 10.0

        # Make the active metadata probe fail
        fake_confluent = MagicMock()
        fake_confluent.list_topics.side_effect = Exception("brokers down")
        fake_confluent.poll.return_value = None
        consumer._inner_consumer = fake_confluent

        # Make recovery loop think it has work (so it enters the while loop)
        fake_rp = MagicMock()
        recovery_manager._recovery_partitions = {0: {"store": fake_rp}}
        recovery_manager._running = True

        # Mock _update_recovery_status to isolate the broker check behavior
        with mock_patch.object(recovery_manager, "_update_recovery_status"):
            with pytest.raises(KafkaBrokerUnavailableError):
                recovery_manager._recovery_loop()

    def test_recovery_loop_no_check_when_timeout_disabled(self):
        """With broker_availability_timeout=0 (disabled), recovery loop should
        NOT check broker availability."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        from quixstreams.kafka.consumer import BaseConsumer
        from quixstreams.models.topics import TopicManager
        from quixstreams.state.recovery import RecoveryManager

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )

        topic_manager = MagicMock(spec=TopicManager)
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=topic_manager,
            broker_availability_timeout=0,
        )

        # Simulate broker down for a long time
        consumer._broker_unavailable_since = time.monotonic() - 9999.0

        # Make the active metadata probe fail
        fake_confluent = MagicMock()
        fake_confluent.list_topics.side_effect = Exception("brokers down")
        fake_confluent.poll.return_value = None
        consumer._inner_consumer = fake_confluent

        # Make recovery loop think it has work
        fake_rp = MagicMock()
        recovery_manager._recovery_partitions = {0: {"store": fake_rp}}
        recovery_manager._running = True

        poll_count = 0

        def stop_after_3(*args, **kwargs):
            nonlocal poll_count
            poll_count += 1
            if poll_count >= 3:
                recovery_manager._running = False
            return None

        fake_confluent.poll.side_effect = stop_after_3

        # Mock _update_recovery_status to isolate the broker check behavior
        with mock_patch.object(recovery_manager, "_update_recovery_status"):
            # Should NOT raise — just loop and stop
            recovery_manager._recovery_loop()
        assert poll_count >= 3

    def test_recovery_loop_resets_timer_on_changelog_message(self):
        """Successfully consuming a changelog message during recovery should
        reset the broker unavailability timer, preventing false positives."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        from quixstreams.kafka.consumer import BaseConsumer
        from quixstreams.models.topics import TopicManager
        from quixstreams.state.recovery import RecoveryManager

        consumer = BaseConsumer(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )

        topic_manager = MagicMock(spec=TopicManager)
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=topic_manager,
            broker_availability_timeout=0.1,
        )

        # Simulate broker was down (but has since recovered — messages are flowing)
        consumer._broker_unavailable_since = time.monotonic() - 10.0

        # Make the metadata probe fail — only changelog consumption should reset
        fake_confluent = MagicMock()
        fake_confluent.list_topics.side_effect = Exception("brokers down")
        consumer._inner_consumer = fake_confluent

        # Set up recovery partition
        fake_rp = MagicMock()
        fake_rp.changelog_name = "test-changelog"
        fake_rp.partition_num = 0
        recovery_manager._recovery_partitions = {0: {"test-changelog": fake_rp}}
        recovery_manager._running = True

        poll_count = 0

        def poll_then_stop(*args, **kwargs):
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                # First poll: return a fake changelog message
                msg = MagicMock()
                msg.error.return_value = None
                msg.partition.return_value = 0
                msg.topic.return_value = "test-changelog"
                return msg
            # Second poll: stop the loop
            recovery_manager._running = False
            return None

        fake_confluent.poll.side_effect = poll_then_stop

        with mock_patch.object(recovery_manager, "_update_recovery_status"):
            recovery_manager._recovery_loop()

        # Timer should have been reset by the successful changelog consumption
        assert consumer._broker_unavailable_since is None


def _make_stats_json(brokers: dict) -> str:
    """Helper to build a stats JSON string with the given broker entries."""
    return json.dumps({"brokers": brokers})


class TestProducerPerBrokerTracking:
    """Tests for Producer per-broker connectivity tracking via stats_cb."""

    def test_initial_up_no_log(self, caplog):
        """First time a broker is seen as UP should not produce a log."""
        producer = Producer(broker_address="localhost:9092")
        stats = _make_stats_json(
            {
                "broker1:9092/1": {"nodeid": 1, "state": "UP"},
            }
        )
        with caplog.at_level("DEBUG"):
            producer._stats_cb(stats)

        assert "broker1:9092/1" in producer._brokers_seen_up
        assert "UP again" not in caplog.text
        assert "is UP" not in caplog.text

    def test_up_to_down_logs_reassurance_when_others_up(self, caplog):
        """UP -> non-UP should log INFO with count of remaining brokers."""
        producer = Producer(broker_address="localhost:9092")
        # Two brokers UP
        producer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                    "broker2:9092/2": {"nodeid": 2, "state": "UP"},
                }
            )
        )

        with caplog.at_level("INFO"):
            producer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "DOWN"},
                        "broker2:9092/2": {"nodeid": 2, "state": "UP"},
                    }
                )
            )

        assert "went down" in caplog.text
        assert "1 other broker(s) still available" in caplog.text

    def test_up_to_down_no_reassurance_when_all_down(self, caplog):
        """UP -> non-UP with no other brokers UP should not log reassurance."""
        producer = Producer(broker_address="localhost:9092")
        producer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                }
            )
        )

        with caplog.at_level("INFO"):
            producer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "DOWN"},
                    }
                )
            )

        assert "went down" not in caplog.text

    def test_down_to_up_after_seen_up_logs_info(self, caplog):
        """non-UP -> UP for a previously-UP broker should log info."""
        producer = Producer(broker_address="localhost:9092")
        # UP -> DOWN -> UP
        producer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                }
            )
        )
        producer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "DOWN"},
                }
            )
        )

        with caplog.at_level("INFO"):
            producer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                    }
                )
            )

        assert "UP again" in caplog.text
        assert "was DOWN" in caplog.text

    def test_init_to_up_no_restored_log(self, caplog):
        """INIT -> UP (never been UP before) should not log "restored"."""
        producer = Producer(broker_address="localhost:9092")
        # First seen as INIT
        producer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "INIT"},
                }
            )
        )

        with caplog.at_level("DEBUG"):
            producer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                    }
                )
            )

        assert "UP again" not in caplog.text
        assert "broker1:9092/1" in producer._brokers_seen_up

    def test_bootstrap_nodeid_minus_1_skipped(self):
        """Brokers with nodeid -1 should be skipped entirely."""
        producer = Producer(broker_address="localhost:9092")
        producer._stats_cb(
            _make_stats_json(
                {
                    "bootstrap:9092": {"nodeid": -1, "state": "UP"},
                }
            )
        )

        assert producer._broker_states == {}
        assert producer._brokers_seen_up == set()

    def test_user_stats_cb_chained(self):
        """User-provided stats_cb should be called with the raw JSON."""
        calls = []
        producer = Producer(
            broker_address="localhost:9092",
            extra_config={"stats_cb": lambda s: calls.append(s)},
        )
        stats = _make_stats_json({"broker1:9092/1": {"nodeid": 1, "state": "UP"}})
        producer._stats_cb(stats)

        assert len(calls) == 1
        assert calls[0] == stats

    def test_user_stats_cb_called_on_parse_error(self):
        """User stats_cb should still be called even if JSON parsing fails."""
        calls = []
        producer = Producer(
            broker_address="localhost:9092",
            extra_config={"stats_cb": lambda s: calls.append(s)},
        )
        producer._stats_cb("not valid json {{{")

        assert len(calls) == 1
        assert calls[0] == "not valid json {{{"

    def test_invalid_json_does_not_crash(self):
        """Invalid JSON should not raise an exception."""
        producer = Producer(broker_address="localhost:9092")
        # Should not raise
        producer._stats_cb("not valid json {{{")

    def test_statistics_interval_default(self):
        """Default statistics.interval.ms should be 30000."""
        producer = Producer(broker_address="localhost:9092")
        assert producer._producer_config["statistics.interval.ms"] == 30000

    def test_statistics_interval_user_lower_wins(self):
        """If user sets a lower positive value, theirs should be used."""
        producer = Producer(
            broker_address="localhost:9092",
            extra_config={"statistics.interval.ms": 10000},
        )
        assert producer._producer_config["statistics.interval.ms"] == 10000

    def test_statistics_interval_user_zero_disables(self):
        """If user sets 0, stats are disabled — respected."""
        producer = Producer(
            broker_address="localhost:9092",
            extra_config={"statistics.interval.ms": 0},
        )
        assert producer._producer_config["statistics.interval.ms"] == 0

    def test_extra_config_not_mutated(self):
        """The caller's extra_config dict should not be modified."""
        original = {"some.setting": "value", "stats_cb": lambda s: None}
        extra = dict(original)
        Producer(broker_address="localhost:9092", extra_config=extra)

        assert "stats_cb" in extra
        assert extra == original


class TestConsumerPerBrokerTracking:
    """Tests for BaseConsumer per-broker connectivity tracking via stats_cb."""

    def _make_consumer(self, **kwargs):
        defaults = dict(
            broker_address="localhost:9092",
            consumer_group="test",
            auto_offset_reset="latest",
        )
        defaults.update(kwargs)
        return BaseConsumer(**defaults)

    def test_initial_up_no_log(self, caplog):
        consumer = self._make_consumer()
        stats = _make_stats_json(
            {
                "broker1:9092/1": {"nodeid": 1, "state": "UP"},
            }
        )
        with caplog.at_level("DEBUG"):
            consumer._stats_cb(stats)

        assert "broker1:9092/1" in consumer._brokers_seen_up
        assert "UP again" not in caplog.text

    def test_up_to_down_logs_reassurance_when_others_up(self, caplog):
        consumer = self._make_consumer()
        consumer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                    "broker2:9092/2": {"nodeid": 2, "state": "UP"},
                }
            )
        )

        with caplog.at_level("INFO"):
            consumer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "DOWN"},
                        "broker2:9092/2": {"nodeid": 2, "state": "UP"},
                    }
                )
            )

        assert "went down" in caplog.text
        assert "1 other broker(s) still available" in caplog.text

    def test_up_to_down_no_reassurance_when_all_down(self, caplog):
        consumer = self._make_consumer()
        consumer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                }
            )
        )

        with caplog.at_level("INFO"):
            consumer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "DOWN"},
                    }
                )
            )

        assert "went down" not in caplog.text

    def test_down_to_up_after_seen_up_logs_info(self, caplog):
        consumer = self._make_consumer()
        consumer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                }
            )
        )
        consumer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "DOWN"},
                }
            )
        )

        with caplog.at_level("INFO"):
            consumer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                    }
                )
            )

        assert "UP again" in caplog.text
        assert "was DOWN" in caplog.text

    def test_init_to_up_no_restored_log(self, caplog):
        consumer = self._make_consumer()
        consumer._stats_cb(
            _make_stats_json(
                {
                    "broker1:9092/1": {"nodeid": 1, "state": "INIT"},
                }
            )
        )

        with caplog.at_level("DEBUG"):
            consumer._stats_cb(
                _make_stats_json(
                    {
                        "broker1:9092/1": {"nodeid": 1, "state": "UP"},
                    }
                )
            )

        assert "UP again" not in caplog.text
        assert "broker1:9092/1" in consumer._brokers_seen_up

    def test_bootstrap_nodeid_minus_1_skipped(self):
        consumer = self._make_consumer()
        consumer._stats_cb(
            _make_stats_json(
                {
                    "bootstrap:9092": {"nodeid": -1, "state": "UP"},
                }
            )
        )

        assert consumer._broker_states == {}
        assert consumer._brokers_seen_up == set()

    def test_user_stats_cb_chained(self):
        calls = []
        consumer = self._make_consumer(
            extra_config={"stats_cb": lambda s: calls.append(s)},
        )
        stats = _make_stats_json({"broker1:9092/1": {"nodeid": 1, "state": "UP"}})
        consumer._stats_cb(stats)

        assert len(calls) == 1
        assert calls[0] == stats

    def test_user_stats_cb_called_on_parse_error(self):
        calls = []
        consumer = self._make_consumer(
            extra_config={"stats_cb": lambda s: calls.append(s)},
        )
        consumer._stats_cb("not valid json {{{")

        assert len(calls) == 1

    def test_invalid_json_does_not_crash(self):
        consumer = self._make_consumer()
        consumer._stats_cb("not valid json {{{")

    def test_statistics_interval_default(self):
        consumer = self._make_consumer()
        assert consumer._consumer_config["statistics.interval.ms"] == 30000

    def test_statistics_interval_user_lower_wins(self):
        consumer = self._make_consumer(
            extra_config={"statistics.interval.ms": 10000},
        )
        assert consumer._consumer_config["statistics.interval.ms"] == 10000

    def test_statistics_interval_user_zero_disables(self):
        consumer = self._make_consumer(
            extra_config={"statistics.interval.ms": 0},
        )
        assert consumer._consumer_config["statistics.interval.ms"] == 0

    def test_extra_config_not_mutated(self):
        original = {"some.setting": "value", "stats_cb": lambda s: None}
        extra = dict(original)
        self._make_consumer(extra_config=extra)

        assert "stats_cb" in extra
        assert extra == original
