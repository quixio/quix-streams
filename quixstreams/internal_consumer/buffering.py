import enum
import logging
from collections import deque
from operator import attrgetter
from typing import Iterable, Optional

from confluent_kafka import TopicPartition

from quixstreams.models import SuccessfulConfluentKafkaMessageProto

logger = logging.getLogger(__name__)

__all__ = ("InternalConsumerBuffer",)

_next_timestamp_getter = attrgetter("next_timestamp")


class Idleness(enum.Enum):
    IDLE = 1  # The partition is idle and has no new messages to be consumed
    ACTIVE = 2  # The partition has more messages to be consumed
    UNKNOWN = 3  # The idleness is unknown


class PartitionBuffer:
    def __init__(
        self,
        partition: int,
        topic: str,
        max_size: int,
        non_blocking: bool = False,
    ):
        """
        A buffer that holds data for a single topic partition.

        :param partition: partition number.
        :param topic: topic name.
        :param max_size: the maximum size of the buffer when the buffer is considered full.
            It is a soft limit, and it may be exceeded in some cases
        :param non_blocking: when True, an empty-but-active buffer will not block
            the group's pop() from returning messages from other partitions.
            Use for control topics (e.g. watermarks) that should never stall data flow.
        """
        self.partition = partition
        self.topic = topic
        self.next_timestamp = float("inf")
        self._paused = False
        self._max_size = max_size
        self._max_offset = -1
        self._high_watermark = -1001
        self.non_blocking = non_blocking
        self._messages: deque[SuccessfulConfluentKafkaMessageProto] = deque()

    def set_high_watermark(self, offset: int):
        """
        Set high watermark offset of topic partition.

        :param offset: high watermark offset.
        """
        self._high_watermark = offset

    def set_consumer_position(self, position: int):
        """
        Inform the buffer of the consumer's current fetch position.

        When the position is >= the high watermark (consumer is at the end of
        the log), bump ``_max_offset`` so that ``idleness()`` returns ``IDLE``
        and the time-aligned buffer does not block on this empty partition.

        :param position: current Kafka consumer fetch offset for this partition.
        """
        if position >= 0 and self._high_watermark >= 0 and position >= self._high_watermark:
            self._max_offset = max(self._max_offset, self._high_watermark - 1)

    def idleness(self) -> Idleness:
        """
        Check if the partition is idle or has more data to be consumed from the broker.

        :returns:
         - `True` when the max offset+1 equals to the high watermark
         - `False` when the max offset is below the high watermark
         - `None` when the watermark is not known yet.
        """
        high_watermark = self._high_watermark
        if high_watermark < 0:
            # There's no valid highwater for this partition yet, the idleness is unknown
            return Idleness.UNKNOWN

        return (
            Idleness.IDLE if self._max_offset + 1 >= high_watermark else Idleness.ACTIVE
        )

    def append(self, message: SuccessfulConfluentKafkaMessageProto):
        """
        Append a new Kafka message to the buffer.
        The message is supposed to have `.error()` to be `None`.

        :param message: a successful Kafka message.
        """
        offset = message.offset()
        if offset <= self._max_offset:
            raise ValueError(
                f"Invalid offset {offset} (max offset is {self._max_offset})"
            )
        self._max_offset = offset
        if self.next_timestamp == float("inf"):
            _, self.next_timestamp = message.timestamp()
        self._messages.append(message)

    def popleft(self) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        """
        Pop the message from the start of the buffer.

        :returns: `None` if the buffer is empty, otherwise a Kafka message
        """
        messages = self._messages
        try:
            item = messages.popleft()
        except IndexError:
            # The buffer is empty
            return None

        try:
            self.next_timestamp = messages[0].timestamp()[1]
        except IndexError:
            # After popping the last message from the buffer,
            # reset its next_timestamp to infinity.
            self.next_timestamp = float("inf")

            # If the partition is empty, we may not know the real watermark
            # until the consumer resumes it and tries to fetch the data from the broker.
            # More data may have been produced to the partition if it was paused.
            # Set high_watermark to "-1001" to mark it as "unknown".
            self._high_watermark = -1001
        return item

    def empty(self) -> bool:
        """
        Check if the buffer is empty
        """
        return not self._messages

    def full(self) -> bool:
        """
        Check if the buffer is full
        """
        return len(self._messages) >= self._max_size

    @property
    def paused(self) -> bool:
        return self._paused

    def pause(self):
        """
        Mark the buffer as paused
        """
        self._paused = True

    def resume(self):
        """
        Mark the buffer as resumed
        """
        self._paused = False

    def clear(self):
        """
        Clear the buffer and reset its state.
        """
        self.next_timestamp = float("inf")
        self._max_offset = -1
        self._high_watermark = -1001
        self._messages.clear()
        self._paused = False


class PartitionBufferGroup:
    def __init__(self, partition: int, max_size: int, non_blocking_topics: frozenset = frozenset()):
        """
        A group of individual `PartitionBuffer`s by partition.

        :param partition: partition number.
        :param max_size: the maximum size of the underlying `PartitionBuffer`s.
        :param non_blocking_topics: topic names whose buffers will not block pop()
            when empty-but-active (e.g. the watermarks topic).
        """
        self._partition = partition
        self._max_size = max_size
        self._non_blocking_topics = non_blocking_topics
        self._partition_buffers: dict[str, PartitionBuffer] = {}
        self._partition_buffers_values = self._partition_buffers.values()

    @property
    def partition(self) -> int:
        return self._partition

    def assign_partition(self, topic: str):
        """
        Add a new partition to the buffer group.

        :param topic: topic name.
        """
        if topic in self._partition_buffers:
            return
        # New partition, need to create a new buffer and add it to the heap
        self._partition_buffers[topic] = PartitionBuffer(
            partition=self._partition,
            topic=topic,
            max_size=self._max_size,
            non_blocking=topic in self._non_blocking_topics,
        )

    def revoke_partition(self, topic: str):
        """
        Remove partition from the buffer group.

        :param topic: topic name.
        """
        self._partition_buffers.pop(topic)

    def set_high_watermarks(self, offsets: dict[str, int]):
        """
        Set high watermarks for assigned partitions.

        :param offsets: a mapping of {<topic>: <offset>} with the high watermarks
            for this group.
        """
        for topic, watermark in offsets.items():
            buffer = self._partition_buffers[topic]
            buffer.set_high_watermark(watermark)

    def set_consumer_positions(self, positions: dict[str, int]):
        """
        Inform each buffer of the consumer's current fetch position.

        Buffers whose consumer position is at or beyond the high watermark are
        marked IDLE so the time-aligned ``pop()`` does not block waiting for
        messages that will never arrive (e.g. a topic fully consumed that has
        no new data).

        :param positions: a mapping of {<topic>: <fetch_position>}.
        """
        for topic, position in positions.items():
            buffer = self._partition_buffers.get(topic)
            if buffer is not None:
                buffer.set_consumer_position(position)

    def append(self, message: SuccessfulConfluentKafkaMessageProto):
        """
        Add a new message to the buffer group.

        :param message: a successful Kafka message.
        """

        partition_buffer = self._partition_buffers[message.topic()]
        partition_buffer.append(message=message)

    def pop(self) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        """
        Pop a message from the partition buffer with the smallest next_timestamp
        and return it.

        How it works:

        - When the group has multiple partitions assigned, it will pop the message
            from the partition with the smallest next timestamp.
        - When there's only one partition in the group, it will pop a message
            from this partition buffer.
        """

        buffers = self._partition_buffers_values
        if len(buffers) == 1:
            # Use iter() here to avoid creating a new list each time
            buffer = next(iter(buffers))
            return buffer.popleft()
        elif len(buffers) > 1:
            # There's more than one partition in the group and one of them is empty
            # but not idle.
            # Wait until new messages are fetched or the highwater is set.
            # Non-blocking buffers (e.g. watermarks topic) are excluded from this
            # check so they never stall processing of data partitions.
            for buffer in buffers:
                if buffer.empty() and buffer.idleness() != Idleness.IDLE and not buffer.non_blocking:
                    return None

            buffer = min(buffers, key=_next_timestamp_getter)
            return buffer.popleft()
        else:
            # The buffer has no partitions yet
            return None

    @property
    def has_blocking_buffers(self) -> bool:
        """Return True if this group has at least one non-non_blocking buffer."""
        return any(not b.non_blocking for b in self._partition_buffers_values)

    def pause_full(self) -> list[tuple[str, int]]:
        """
        Pause the full `PartitionBuffer`s and return them as a list of tuples.

        If the group has only one topic partition, it will never be paused.
        """
        if len(self._partition_buffers_values) == 1:
            # Never pause a single-partition group
            return []

        tps = []
        for buffer in self._partition_buffers_values:
            # Pause consuming new messages for this partition
            # when the buffer is paused, full and not idle
            if (
                not buffer.paused
                and buffer.full()
                and buffer.idleness() == Idleness.ACTIVE
            ):
                buffer.pause()
                tps.append((buffer.topic, buffer.partition))
        return tps

    def resume_empty(self) -> list[tuple[str, int]]:
        """
        Resume the empty `PartitionBuffer`s and return them as a list of tuples.

        Only previously paused partitions are resumed.
        """
        tps = []
        for buffer in self._partition_buffers_values:
            # Resume consuming new messages for this partition
            # when the buffer is paused and either idle or not full
            if buffer.paused and (
                buffer.idleness() != Idleness.ACTIVE or not buffer.full()
            ):
                buffer.resume()
                tps.append((buffer.topic, buffer.partition))

        return tps

    def clear(self, topic: str):
        if buffer := self._partition_buffers.get(topic):
            buffer.clear()


class InternalConsumerBuffer:
    def __init__(self, max_partition_buffer_size: int = 10000):
        """
        A buffer to align messages across different topics by timestamps and consume them
        in-order across partitions.

        Under the hood, this class groups buffered messages by partition and
        provides API to get the message with the smallest timestamp across all assigned
        topics with the same partition number.

        **Note**: messages are not guaranteed to be in-order 100% of the time,
          and they can be consumed out-of-order when the producer sends new messages with a delay,
          and they arrive to the broker later.

        How it works:

        - The buffer gets partitions assigned.
        - The Consumer feeds messages to the buffer along with the high watermarks for all
            assigned partitions.
        - The Consumer calls `.pop()` to get the next message to be processed.
            If multiple partitions with the same number are assigned, the message will be
            popped from the partition with the smallest next timestamp, providing in-order reads.
        - The Consumer calls `.pause_full()` and `.resume_empty()` methods to balance the reads
            across all partitions.

        :param max_partition_buffer_size: the maximum size of the individual topic partition
            buffer when the buffer is considered full. It is a soft limit, and it may be exceeded
            in some cases. When individual buffer exceeds this limit, its TP can be paused
            to let other partitions to be consumed too.
        """
        self._partition_groups: dict[int, PartitionBufferGroup] = {}
        self._max_partition_buffer_size = max_partition_buffer_size

    def assign_partitions(self, topic_partitions: list[TopicPartition], non_blocking_topics: frozenset = frozenset()):
        """
        Assign new partitions to the buffer.

        :param topic_partitions: list of `confluent_kafka.TopicPartition`.
        :param non_blocking_topics: topic names that should not block pop() when
            their buffer is empty-but-active (e.g. the watermarks topic).
        """
        # Sort partitions by their number to process them
        # in a fixed order when popping the items over
        topic_partitions = sorted(topic_partitions, key=lambda t: t.partition)
        for tp in topic_partitions:
            partition_group = self._partition_groups.setdefault(
                tp.partition,
                PartitionBufferGroup(
                    partition=tp.partition,
                    max_size=self._max_partition_buffer_size,
                    non_blocking_topics=non_blocking_topics,
                ),
            )
            partition_group.assign_partition(topic=tp.topic)

    def revoke_partitions(self, topic_partitions: list[TopicPartition]):
        """
        Drop the partitions from the buffer.

        :param topic_partitions: list of `confluent_kafka.TopicPartition`.
        """
        for tp in topic_partitions:
            partition_group = self._partition_groups.get(tp.partition)
            if partition_group is not None:
                partition_group.revoke_partition(topic=tp.topic)

    def feed(
        self,
        messages: Iterable[SuccessfulConfluentKafkaMessageProto],
        high_watermarks: dict[tuple[str, int], int],
        consumer_positions: Optional[dict[tuple[str, int], int]] = None,
    ):
        """
        Feed new batch of messages to the buffer.

        :param messages: an iterable with successful `confluent_kafka.Message` objects (`.error()` is expected to be None).
        :param high_watermarks: a dictionary with high watermarks for all assigned topic partitions.
        :param consumer_positions: optional mapping of (topic, partition) -> current fetch
            position.  When provided, partitions whose position is at or beyond the high
            watermark are marked IDLE so the time-aligned pop() does not block on them.
        """
        for message in messages:
            partition_group = self._partition_groups[message.partition()]
            partition_group.append(message=message)

        for partition_group in self._partition_groups.values():
            group_watermarks = {
                topic: watermark
                for (topic, partition), watermark in high_watermarks.items()
                if partition == partition_group.partition
            }
            partition_group.set_high_watermarks(offsets=group_watermarks)

            if consumer_positions is not None:
                group_positions = {
                    topic: pos
                    for (topic, partition), pos in consumer_positions.items()
                    if partition == partition_group.partition
                }
                partition_group.set_consumer_positions(positions=group_positions)

    def pop(self) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        """
        Pop the next message from the buffer in the timestamp order.

        Data groups (those with at least one blocking buffer) are tried first.
        Non-blocking-only groups (e.g. a watermarks-only partition group) are
        tried last so they never starve data processing.

        :returns: `None` if all the buffers are empty or if the
        """
        groups = self._partition_groups.values()
        # Try data groups first, then non-blocking-only groups
        for partition_group in groups:
            if partition_group.has_blocking_buffers:
                message = partition_group.pop()
                if message is not None:
                    return message
        for partition_group in groups:
            if not partition_group.has_blocking_buffers:
                message = partition_group.pop()
                if message is not None:
                    return message
        return None

    def pause_full(self) -> list[tuple[str, int]]:
        """
        Pause the full partition buffers and return them as a list.
        """
        tps = []
        for partition_group in self._partition_groups.values():
            tps += partition_group.pause_full()
        return tps

    def resume_empty(self) -> list[tuple[str, int]]:
        """
        Resume the empty partition buffers and return them as a list.
        """
        tps = []
        for partition_group in self._partition_groups.values():
            tps += partition_group.resume_empty()
        return tps

    def clear(self, topic: str, partition: int):
        """
        Clear the buffer for the given topic partition and keep it assigned.
        """
        partition_group = self._partition_groups.get(partition)
        if partition_group is not None:
            partition_group.clear(topic)

    def is_empty(self) -> bool:
        """
        Return True when every data partition buffer across all groups has no messages
        pending to be popped and processed.

        Non-blocking buffers (e.g. the watermarks topic) are excluded: they are
        control topics that should never stall data flow or the caught-up check.

        Used by the caught-up idle-advance check to avoid firing the watermark advance
        while messages are still buffered-but-unprocessed (which would cause them to
        arrive at the windowing stage after their windows were already closed and be
        silently dropped as late data).
        """
        return all(
            buffer.empty()
            for group in self._partition_groups.values()
            for buffer in group._partition_buffers_values
            if not buffer.non_blocking
        )

    def is_eos_stuck(
        self, topic: str, partition: int, consumer_position: int, high_watermark: int
    ) -> bool:
        """
        Return True if this partition is stuck on EOS transaction control records.

        Under read_committed isolation, EOS markers are invisible to consumers:
        consume() returns 0 messages but the consumer position does not advance
        past the marker, leaving a permanent gap between position and high watermark.

        Three conditions must all hold:

        1. buffer.empty(): all buffered data has been consumed by the application.

        2. not buffer.paused: the partition is not paused at the buffer level.
           A paused partition gets 0 messages from consume() regardless of whether
           EOS markers are present; resume_empty() will handle it instead.

        3. consumer_position >= _max_offset + 1: the consumer is at or beyond the
           next offset after the last data message we ever saw, confirming that
           the record(s) between here and the high watermark are not data records.
           Using >= (not ==) handles consecutive EOS markers: after seeking past
           the first EOS marker, consumer_position advances by 1 but _max_offset
           stays the same, so we need >= to detect the next EOS marker.

        A gap > 10 guard filters out large gaps that are not EOS-stuck: those
        indicate new data is arriving (another producer is actively writing) and
        a seek would skip real messages.  EOS transaction control records produce
        exactly a gap of 1 (one invisible control record per transaction commit).
        """
        gap = high_watermark - consumer_position
        if gap <= 0 or gap > 10:
            return False
        group = self._partition_groups.get(partition)
        if group is None:
            return False
        buffer = group._partition_buffers.get(topic)
        if buffer is None:
            return False
        return (
            buffer.empty()
            and not buffer.paused
            and buffer._max_offset >= 0
            and consumer_position == buffer._max_offset + 1
        )

    def close(self):
        """
        Drop all partition buffers.
        """
        self._partition_groups.clear()
