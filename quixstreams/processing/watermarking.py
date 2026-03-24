import logging
import time as _time
from time import monotonic
from typing import Dict, Optional, Set, Tuple, TypedDict

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
        idle_partition_timeout: Optional[float] = None,
        idle_advance_after: Optional[float] = None,
    ):
        self._interval = interval
        self._last_produced = 0
        self._watermarks: dict[tuple[str, int], int] = {}
        self._producer = producer
        self._topic_manager = topic_manager
        self._watermarks_topic: Optional[Topic] = None
        self._to_produce: dict[tuple[str, int], tuple[int, bool]] = {}
        # Tracks whether any data has ever been processed by this replica,
        # used to avoid spurious caught-up advances before processing starts.
        self._ever_stored: bool = False
        # TPs that this replica has actually processed (via store()).
        # _get_watermark() only considers these, so unassigned partitions
        # from other replicas don't drag down the global min.
        self._owned_tps: set[tuple[str, int]] = set()
        # idle_partition_timeout: after this many seconds at watermark -1, a TP is
        # excluded from the global min so other TPs can advance unblocked.
        self._idle_partition_timeout = idle_partition_timeout
        # Tracks wall-clock time when each TP first received data (first store() call).
        self._tp_owned_at: Dict[Tuple[str, int], float] = {}
        # idle_advance_after: if global watermark hasn't advanced for this many seconds,
        # force-advance all owned TPs to wall-clock now and publish heartbeat watermarks.
        self._idle_advance_after = idle_advance_after
        # Wall-clock time of the last global watermark advancement.
        self._last_watermark_advanced_wall: float = monotonic()

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
        self._owned_tps.discard(tp)
        self._watermarks.pop(tp, None)
        self._tp_owned_at.pop(tp, None)

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
        self._ever_stored = True
        # NOTE: we deliberately do NOT reset _last_watermark_advanced_wall here.
        # When watermark messages from Kafka are starved by high data volume,
        # resetting the timer on every data message prevents idle_advance from
        # ever firing, causing 0 window expiry in continuous streaming mode.
        # The timer is still reset by receive() (when Kafka watermarks arrive)
        # and by produce() (when caught-up or idle-advance fires), which is
        # sufficient to prevent premature advances during normal operation.
        if tp not in self._owned_tps:
            self._owned_tps.add(tp)
            self._tp_owned_at[tp] = monotonic()
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

    def produce(self, caught_up: bool = False) -> Optional[int]:
        """
        Produce updated watermarks to the watermarks topic.

        When ``caught_up=True`` (all assigned data partitions have reached their
        current end offset), directly advances all tracked TP watermarks to
        wall-clock now — no Kafka message is produced (avoids EOS transaction
        issues when the checkpoint is empty).  Returns the new global watermark
        when it advances, otherwise returns ``None``.
        """
        now_wall = monotonic()
        if now_wall >= self._last_produced + self._interval:
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
                    topic=self.watermarks_topic.name, value=dumps(msg), key=key
                )
            self._last_produced = now_wall
            self._to_produce.clear()

        # When all assigned data partitions are caught up (position >= high watermark),
        # advance watermarks locally to wall-clock now so pending windows can be flushed.
        # Advancing ALL tracked TPs (not just locally-processed ones) ensures the global
        # min is not stuck at past event-time values received from other replicas.
        if caught_up and self._ever_stored:
            now_ms = int(_time.time() * 1000)
            old_wm = self._get_watermark()
            for tp in list(self._watermarks.keys()):
                self._watermarks[tp] = max(self._watermarks.get(tp, -1), now_ms)
            new_wm = self._get_watermark()
            if new_wm > old_wm:
                logger.info(
                    f"[WM] Caught-up advance: watermark {format_timestamp(old_wm)} "
                    f"-> {format_timestamp(new_wm)} for {len(self._watermarks)} TP(s)"
                )
                self._last_watermark_advanced_wall = now_wall
                return new_wm

        # idle_advance_after: if the global watermark has not advanced for this many
        # wall-clock seconds, force-advance all owned TPs to now and publish heartbeat
        # watermarks so pending windows can be flushed even without a second data batch.
        if (
            self._idle_advance_after is not None
            and self._ever_stored
            and now_wall - self._last_watermark_advanced_wall >= self._idle_advance_after
        ):
            now_ms = int(_time.time() * 1000)
            old_wm = self._get_watermark()
            for tp in list(self._owned_tps):
                self._watermarks[tp] = max(self._watermarks.get(tp, -1), now_ms)
                # Queue a heartbeat watermark to be published to Kafka so that other
                # replicas also advance their view of this TP's watermark.
                self._to_produce[tp] = (now_ms, False)
            # Force immediate flush on the next produce() call.
            self._last_produced = 0
            self._last_watermark_advanced_wall = now_wall
            new_wm = self._get_watermark()
            if new_wm > old_wm:
                logger.info(
                    f"[WM] Idle-advance: watermark {format_timestamp(old_wm)} "
                    f"-> {format_timestamp(new_wm)} for {len(self._owned_tps)} owned TP(s)"
                )
                return new_wm

        return None

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
        # Use global_watermark (min over ALL known non-(-1) TPs, not just owned ones)
        # so that on_watermark only fires when ALL replicas have confirmed progress.
        # This prevents a single fast replica from advancing the expiry threshold and
        # causing late-data drops for slower replicas in multi-replica pipelines.
        current_watermark = self.global_watermark
        if current_watermark is None:
            current_watermark = -1

        # Store the updated TP watermark
        tp = (topic, partition)
        current_tp_watermark = self._watermarks.get(tp, -1)
        self._watermarks[tp] = max(current_tp_watermark, timestamp)

        # Check if the new TP watemark updates the overall watermark, and return it
        # if it does.
        new_watermark = self.global_watermark
        if new_watermark > current_watermark:
            self._last_watermark_advanced_wall = monotonic()
            return new_watermark
        return None

    @property
    def global_watermark(self) -> int:
        """
        Global minimum watermark across all known topic-partitions that have
        published at least one watermark (value != -1).
        Returns -1 if no TP has published a watermark yet.
        """
        non_neg = [v for v in self._watermarks.values() if v >= 0]
        return min(non_neg) if non_neg else -1

    def _get_watermark(self) -> int:
        if not self._owned_tps:
            return -1
        if self._idle_partition_timeout is not None:
            # Exclude owned TPs that are still at -1 and have been waiting longer
            # than the idle timeout.  This lets the global watermark advance even when
            # a partition is in _owned_tps but its watermark hasn't been received back
            # yet (e.g. slow feedback loop at startup).
            now = monotonic()
            active_values = [
                v
                for tp, v in self._watermarks.items()
                if tp in self._owned_tps and (
                    v != -1
                    or (now - self._tp_owned_at.get(tp, now)) < self._idle_partition_timeout
                )
            ]
            return min(active_values) if active_values else -1
        owned = [v for tp, v in self._watermarks.items() if tp in self._owned_tps]
        if not owned:
            return -1
        return min(owned)
