import logging
from time import monotonic
from typing import Optional, TypedDict

from quixstreams.internal_producer import InternalProducer
from quixstreams.models import Topic
from quixstreams.models.topics.manager import TopicManager
from quixstreams.utils.json import dumps

logger = logging.getLogger(__name__)


class WatermarkMessage(TypedDict):
    topic: str
    partition: int
    timestamp: int


class WatermarkManager:
    def __init__(
        self,
        producer: InternalProducer,
        topic_manager: TopicManager,
        interval: float = 0.2,
    ):
        self._interval = interval
        self._last_produced = 0
        self._watermarks: dict[tuple[str, int], int] = {}
        self._producer = producer
        self._watermarks_topic = topic_manager.watermarks_topic()
        self._to_produce: dict[tuple[str, int], int] = {}

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
        self._watermarks.clear()
        for topic in topics:
            for partition in range(topic.broker_config.num_partitions):
                self._watermarks[(topic.name, partition)] = -1

    @property
    def watermarks_topic(self) -> Topic:
        """
        A topic with watermarks updates.
        """
        return self._watermarks_topic

    def untrack(self, topic: str, partition: int):
        """
        Remove the TP from tracking (e.g. when partition is revoked).
        """
        tp = (topic, partition)
        self._to_produce.pop(tp, None)

    def store(self, topic: str, partition: int, timestamp: int):
        """
        Store the new watermark.
        """
        if timestamp < 0:
            raise ValueError("Watermark cannot be negative.")
        tp = (topic, partition)
        stored_watermark = self._to_produce.get(tp, -1)
        new_watermark = max(stored_watermark, timestamp)
        if new_watermark > stored_watermark:
            # Schedule the updated watermark to be produced on the next cycle
            # if it's tracked and larger than the previous one.
            self._to_produce[tp] = new_watermark

    def produce(self):
        """
        Produce updated watermarks to the watermarks topic.
        """
        if monotonic() >= self._last_produced + self._interval:
            # Produce watermarks only for those partitions that are tracked by this application
            # to avoid re-publishing the same watermarks.
            for (topic, partition), timestamp in self._to_produce.items():
                msg: WatermarkMessage = {
                    "topic": topic,
                    "partition": partition,
                    "timestamp": timestamp,
                }
                logger.debug(
                    f"Produce watermark update. "
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
            f"Received watermark update. topic={topic} partition={partition} timestamp={timestamp}"
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
