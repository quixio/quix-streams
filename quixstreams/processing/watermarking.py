import logging
from time import monotonic
from typing import Optional, TypedDict

from quixstreams.internal_producer import InternalProducer
from quixstreams.models import Topic
from quixstreams.models.topics.manager import TopicManager
from quixstreams.utils.format import format_timestamp
from quixstreams.utils.json import dumps

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
        idle_timeout: float = 30.0,
    ):
        self._interval = interval
        self._idle_timeout = idle_timeout
        self._last_produced = 0
        self._watermarks: dict[tuple[str, int], int] = {}
        self._last_updated: dict[tuple[str, int], float] = {}
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

        now = monotonic()
        self._watermarks = {
            (topic.name, partition): -1
            for topic in topics
            for partition in range(topic.broker_config.num_partitions or 1)
        }
        self._last_updated = {tp: now for tp in self._watermarks}

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
        self._watermarks.pop(tp, None)
        self._last_updated.pop(tp, None)

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

        if default and not stored_default:
            # Skip watermark update if the non-default watermark is set.
            return
        elif not default and stored_default:
            # Override the default watermark with the custom (non-default) timestamp.
            # Use the custom timestamp directly — do NOT take max with the previous
            # default watermark, because the custom timestamp reflects the actual event
            # time used by windows/aggregations, which may be intentionally lower than
            # the Kafka ingest timestamp.
            if stored_watermark > timestamp:
                logger.debug(
                    f"Watermark override: custom_ts={timestamp} < kafka_ts={stored_watermark} "
                    f"for {tp}; using custom_ts (delta={stored_watermark - timestamp}ms)"
                )
            self._to_produce[tp] = (timestamp, default)
        else:
            # Same default-ness: advance to the higher watermark.
            new_watermark = max(stored_watermark, timestamp)
            if new_watermark > stored_watermark:
                self._to_produce[tp] = (new_watermark, default)

        self._last_updated[tp] = monotonic()

    def produce(self) -> bool:
        """
        Produce updated watermarks to the watermarks topic.

        :return: ``True`` if any watermarks were actually produced to the topic,
            ``False`` otherwise (either because the rate-limit interval has not
            elapsed yet, or because there was nothing new to produce).
        """
        if monotonic() >= self._last_produced + self._interval:
            produced = bool(self._to_produce)
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
            return produced
        return False

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
        self._last_updated[tp] = monotonic()

        # Check if the new TP watemark updates the overall watermark, and return it
        # if it does.
        new_watermark = self._get_watermark()
        if new_watermark > current_watermark:
            logger.info(
                f"Global watermark advanced: {format_timestamp(current_watermark)} -> "
                f"{format_timestamp(new_watermark)}. "
                f"All TP watermarks: {dict(self._watermarks)}"
            )
            return new_watermark
        return None

    def _get_watermark(self) -> int:
        now = monotonic()
        active = []
        for tp, ts in self._watermarks.items():
            if now - self._last_updated.get(tp, 0) < self._idle_timeout:
                active.append(ts)
            elif ts == -1:
                # Partition was primed but never received any data and has gone
                # idle — exclude it so it doesn't block the global watermark.
                logger.debug(
                    "Excluding idle partition %s from watermark calculation",
                    tp,
                )
        if active:
            return min(active)
        return -1
