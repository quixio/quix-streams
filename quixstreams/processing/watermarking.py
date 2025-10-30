import logging
from time import monotonic
from typing import TYPE_CHECKING, Optional, TypedDict

from confluent_kafka import TopicPartition

from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka.consumer import raise_for_msg_error
from quixstreams.models import Topic
from quixstreams.models.topics.manager import TopicManager
from quixstreams.utils.format import format_timestamp
from quixstreams.utils.json import dumps

if TYPE_CHECKING:
    from quixstreams.kafka import BaseConsumer

logger = logging.getLogger(__name__)

__all__ = ("WatermarkManager", "WatermarkMessage")


class WatermarkMessage(TypedDict):
    topic: str
    partition: int
    timestamp: int


class WatermarkManager:
    def __init__(
        self,
        producer: InternalProducer,
        topic_manager: TopicManager,
        interval: float = 1.0,
    ):
        self._interval = interval
        self._last_produced = 0
        self._watermarks: dict[tuple[str, int], int] = {}
        self._producer = producer
        self._topic_manager = topic_manager
        self._watermarks_topic: Optional[Topic] = None
        self._to_produce: dict[tuple[str, int], tuple[int, bool]] = {}

    def set_topics(self, topics: list[Topic]):
        """
        Set topics to be used as sources of watermarks
        (normally, topics consumed by the application).

        This method must be called before processing the watermarks.
        It will clear the existing TP watermarks and prime the internal
        state to know which partitions the app is expected to consume.
        """
        # Prime the watermarks with -1 for each expected topic partition
        # to make sure we have all TP watermarks before calculating the main watemark.

        self._watermarks = {
            (topic.name, partition): -1
            for topic in topics
            for partition in range(topic.broker_config.num_partitions or 1)
        }

    @property
    def watermarks_topic(self) -> Topic:
        """
        A topic with watermarks updates.
        """
        if self._watermarks_topic is None:
            self._watermarks_topic = self._topic_manager.watermarks_topic()
        return self._watermarks_topic

    def on_revoke(self, topic: str, partition: int):
        """
        Remove the TP from tracking (e.g. when partition is revoked).
        """
        tp = (topic, partition)
        self._to_produce.pop(tp, None)

    def store(self, topic: str, partition: int, timestamp: int, default: bool):
        """
        Store the new watermark.

        :param topic: topic name.
        :param partition: partition number.
        :param timestamp: watermark timestamp.
        :param default: whether the watermark is set by the default mechanism
            (i.e. extracted from the Kafka message timestamp or via Topic `timestamp_extractor`).
            Non-default watermarks always override the defaults.
            Default watermarks never override the non-default ones.
        """
        if timestamp < 0:
            raise ValueError("Watermark cannot be negative.")
        tp = (topic, partition)
        stored_watermark, stored_default = self._to_produce.get(tp, (-1, True))
        new_watermark = max(stored_watermark, timestamp)

        if default and not stored_default:
            # Skip watermark update if the non-default watermark is set.
            return
        elif not default and stored_default:
            # Always override the default watermark
            self._to_produce[tp] = (new_watermark, default)
        elif new_watermark > stored_watermark:
            # Schedule the updated watermark to be produced on the next cycle
            # if it's tracked and larger than the previous one.
            self._to_produce[tp] = (new_watermark, default)

    def produce(self):
        """
        Produce updated watermarks to the watermarks topic.
        """
        if monotonic() >= self._last_produced + self._interval:
            # Produce watermarks only for those partitions that are tracked by this application
            # to avoid re-publishing the same watermarks.
            for (topic, partition), (timestamp, _) in self._to_produce.items():
                msg: WatermarkMessage = {
                    "topic": topic,
                    "partition": partition,
                    "timestamp": timestamp,
                }
                logger.debug(
                    f"Produce watermark {format_timestamp(timestamp)}. "
                    f"topic={topic} partition={partition} timestamp={timestamp}"
                )
                key = f"{topic}[{partition}]"
                self._producer.produce(
                    topic=self._watermarks_topic.name, value=dumps(msg), key=key
                )
            self._last_produced = monotonic()
            self._to_produce.clear()

    def receive(self, message: WatermarkMessage) -> Optional[int]:
        """
        Receive and store the consumed watermark message.
        Returns True if the new watermark is larger the existing one.
        """
        topic, partition, timestamp = (
            message["topic"],
            message["partition"],
            message["timestamp"],
        )
        logger.debug(
            f"Received watermark {format_timestamp(timestamp)}. topic={topic} partition={partition} timestamp={timestamp}"
        )
        current_watermark = self._get_watermark()
        if current_watermark is None:
            current_watermark = -1

        # Store the updated TP watermark
        tp = (topic, partition)
        current_tp_watermark = self._watermarks.get(tp, -1)
        self._watermarks[tp] = max(current_tp_watermark, timestamp)

        # Check if the new TP watemark updates the overall watermark, and return it
        # if it does.
        new_watermark = self._get_watermark()
        if new_watermark > current_watermark:
            return new_watermark
        return None

    def _get_watermark(self) -> int:
        watermark = -1
        if watermarks := self._watermarks.values():
            watermark = min(watermarks)
        return watermark

    def bootstrap_watermarks(self, consumer: "BaseConsumer") -> None:
        """
        Bootstrap watermarks by reading the watermarks topic progressively.

        This method uses an exponential backoff strategy:
        1. Try to read N messages from the end of the topic
        2. If not all topic-partitions are found, seek further back exponentially
        3. Continue until all TPs have watermarks or the beginning is reached

        :param consumer: The Kafka consumer to use for reading watermarks
        """
        watermarks_topic_name = self.watermarks_topic.name
        watermarks_partition = 0  # Watermarks topic always has 1 partition

        # Get the expected topic-partitions that need watermarks
        expected_tps = set(self._watermarks.keys())
        if not expected_tps:
            logger.info("No topic-partitions to bootstrap watermarks for")
            return

        logger.info(
            f"Bootstrapping watermarks for {len(expected_tps)} topic-partitions "
            f"from topic '{watermarks_topic_name}'. Expected TPs: {expected_tps}"
        )

        # Get the high watermark (end offset) of the watermarks topic
        tp = TopicPartition(watermarks_topic_name, watermarks_partition)
        logger.debug(f"Getting watermark offsets for {watermarks_topic_name}...")
        try:
            _, high_offset = consumer.get_watermark_offsets(tp, timeout=5.0)
            logger.debug(f"Watermarks topic high offset: {high_offset}")
        except Exception as e:
            # If we can't get watermark offsets, the topic might not be ready yet
            # Log a warning but allow the application to start with -1 watermarks
            logger.warning(
                f"Failed to get watermark offsets for topic {watermarks_topic_name}: {e}. "
                f"Watermarks will start at -1 and be updated as messages arrive."
            )
            return

        if high_offset == 0:
            logger.info("Watermarks topic is empty, no bootstrapping needed")
            return

        # Progressive search parameters
        initial_lookback = 100  # Start by looking at last 100 messages
        lookback_step = min(initial_lookback, high_offset)
        found_tps: set[tuple[str, int]] = set()
        seek_offset = max(0, high_offset - lookback_step)

        iteration_count = 0
        max_iterations = 20  # Safety limit to prevent infinite loops
        while found_tps != expected_tps:
            iteration_count += 1
            if iteration_count > max_iterations:
                missing_tps = expected_tps - found_tps
                raise RuntimeError(
                    f"Bootstrap failed: exceeded {max_iterations} iterations. "
                    f"Found {len(found_tps)}/{len(expected_tps)} topic-partitions. "
                    f"Missing: {missing_tps}. This suggests a bug in the bootstrap logic."
                )
            logger.info(
                f"Bootstrap iteration {iteration_count}: seeking to offset {seek_offset} "
                f"(lookback_step={lookback_step}, found {len(found_tps)}/{len(expected_tps)} TPs)"
            )

            # Seek to the calculated position
            tp_with_offset = TopicPartition(
                watermarks_topic_name, watermarks_partition, seek_offset
            )
            try:
                consumer.seek(tp_with_offset)
                logger.debug(f"Seeked to offset {seek_offset}")
            except Exception as e:
                logger.error(f"Failed to seek to offset {seek_offset}: {e}")
                raise

            # Read messages from seek_offset towards previous seek_offset
            # or until all TPs are found
            messages_read = 0
            max_messages_to_read = lookback_step

            # Timeout for this specific seek iteration (30 seconds)
            iteration_timeout = 30.0
            iteration_start_time = monotonic()
            consecutive_poll_timeouts = 0
            max_consecutive_poll_timeouts = 5  # Stop after 5 consecutive empty polls

            while messages_read < max_messages_to_read:
                # Check if this iteration has timed out
                if monotonic() - iteration_start_time > iteration_timeout:
                    missing_tps = expected_tps - found_tps
                    raise TimeoutError(
                        f"Bootstrap failed: polling timeout after {iteration_timeout}s for seek offset {seek_offset}. "
                        f"Found {len(found_tps)}/{len(expected_tps)} topic-partitions. "
                        f"Missing: {missing_tps}. Cannot start application without complete watermark state."
                    )

                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    consecutive_poll_timeouts += 1
                    # If we've had many consecutive timeouts, assume we've read all available messages
                    # in this range and move to the next iteration
                    if consecutive_poll_timeouts >= max_consecutive_poll_timeouts:
                        logger.info(
                            f"No more messages available after {consecutive_poll_timeouts} empty polls at offset {seek_offset}, "
                            f"moving to next iteration (read {messages_read}/{max_messages_to_read} messages)"
                        )
                        break
                    continue

                # Reset consecutive timeout counter when we get a message
                consecutive_poll_timeouts = 0

                # Skip messages from other topics (shouldn't happen but be safe)
                if msg.topic() != watermarks_topic_name:
                    continue

                messages_read += 1

                # Deserialize and process the watermark message
                try:
                    # Raise if message has an error
                    msg = raise_for_msg_error(msg)
                    watermark_msg = self.watermarks_topic.deserialize(msg).value
                    tp_key = (watermark_msg["topic"], watermark_msg["partition"])

                    # Only track if it's an expected TP
                    if tp_key in expected_tps:
                        timestamp = watermark_msg["timestamp"]
                        # Update the watermark (use max to handle out-of-order reads)
                        current = self._watermarks.get(tp_key, -1)
                        self._watermarks[tp_key] = max(current, timestamp)
                        found_tps.add(tp_key)

                        logger.debug(
                            f"Bootstrapped watermark for {watermark_msg['topic']}[{watermark_msg['partition']}]: "
                            f"{format_timestamp(timestamp)}"
                        )

                    # Stop if we've found all TPs
                    if found_tps == expected_tps:
                        logger.info(
                            f"Successfully bootstrapped all {len(expected_tps)} topic-partitions "
                            f"after reading {messages_read} messages"
                        )
                        return

                except Exception as e:
                    logger.warning(f"Failed to deserialize watermark message: {e}")
                    continue

            # If we've read everything and still missing TPs, expand lookback exponentially
            if found_tps != expected_tps:
                if seek_offset == 0:
                    # We've read the entire topic from the beginning
                    missing_tps = expected_tps - found_tps
                    logger.warning(
                        f"Reached beginning of watermarks topic but {len(missing_tps)} "
                        f"topic-partitions still have no watermarks: {missing_tps}. "
                        f"They will remain at -1 until new watermarks arrive."
                    )
                    return

                # Double the step and seek further back from current position
                lookback_step = min(lookback_step * 2, seek_offset)
                seek_offset = max(0, seek_offset - lookback_step)

        logger.info(
            f"Finished bootstrapping watermarks: found {len(found_tps)}/{len(expected_tps)} topic-partitions"
        )
