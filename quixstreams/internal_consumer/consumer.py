import logging
from time import monotonic
from typing import Callable, Iterator, Optional, Union

from confluent_kafka import Consumer, KafkaError, TopicPartition

from quixstreams.error_callbacks import ConsumerErrorCallback, default_on_consumer_error
from quixstreams.exceptions import PartitionAssignmentError
from quixstreams.kafka import AutoOffsetReset, BaseConsumer, ConnectionConfig
from quixstreams.kafka.consumer import RebalancingCallback, raise_for_msg_error
from quixstreams.models import (
    RawConfluentKafkaMessageProto,
    Row,
    SuccessfulConfluentKafkaMessageProto,
    Topic,
)
from quixstreams.models.serializers.exceptions import IgnoreMessage

from .buffering import InternalConsumerBuffer

logger = logging.getLogger(__name__)

__all__ = ("InternalConsumer",)


def _validate_message_batch(
    messages: list[RawConfluentKafkaMessageProto], on_error: ConsumerErrorCallback
) -> Iterator[SuccessfulConfluentKafkaMessageProto]:
    for message in messages:
        try:
            message = raise_for_msg_error(message)
            yield message
        except Exception as exc:
            to_suppress = on_error(exc, message, logger)
            if to_suppress:
                continue
            raise


class InternalConsumer(BaseConsumer):
    _backpressure_resume_at: float

    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset,
        auto_commit_enable: bool = True,
        on_commit: Optional[
            Callable[[Optional[KafkaError], list[TopicPartition]], None]
        ] = None,
        extra_config: Optional[dict] = None,
        on_error: Optional[ConsumerErrorCallback] = None,
        max_partition_buffer_size: int = 10000,
    ):
        """
        A consumer class that is capable of deserializing Kafka messages to Rows
        according to the Topics deserialization settings.

        It overrides `.subscribe()` method of Consumer class to accept `Topic`
        objects instead of strings.

        :param broker_address: Connection settings for Kafka.
            Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
            or a ConnectionConfig object if authentication is required.
        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`
        :param auto_offset_reset: Consumer `auto.offset.reset` setting.
            Available values:
              - "earliest" - automatically reset the offset to the smallest offset
              - "latest" - automatically reset the offset to the largest offset
        :param auto_commit_enable: If true, periodically commit offset of
            the last message handed to the application. Default - `True`.
        :param on_commit: Offset commit result propagation callback.
            Passed as "offset_commit_cb" to `confluent_kafka.Consumer`.
        :param extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
            Note: values passed as arguments override values in `extra_config`.
        :param on_error: a callback triggered when InternalConsumer fails
            to get and deserialize a new message.
            If consumer fails and the callback returns `True`, the exception
            will be logged but not propagated.
            The default callback logs an exception and returns `False`.
        :param max_partition_buffer_size: the maximum number of messages to buffer per topic partition to consider it full.
            The buffering is used to consume messages in-order between different topics.
            Note that the actual number of buffered messages can be higher
            Default - `10000`.
        """
        super().__init__(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            on_commit=on_commit,
            extra_config=extra_config,
        )
        self._on_error: ConsumerErrorCallback = on_error or default_on_consumer_error
        self._topics: dict[str, Topic] = {}
        self._backpressurred_tps: set[TopicPartition] = set()
        self._max_partition_buffer_size = max_partition_buffer_size
        self._buffer = InternalConsumerBuffer(
            max_partition_buffer_size=self._max_partition_buffer_size
        )
        self.reset_backpressure()

    def subscribe(
        self,
        topics: list[Topic],
        on_assign: Optional[RebalancingCallback] = None,
        on_revoke: Optional[RebalancingCallback] = None,
        on_lost: Optional[RebalancingCallback] = None,
    ):
        """
        Set subscription to supplied list of topics.
        This replaces a previous subscription.

        This method also updates the internal mapping with topics that is used
        to deserialize messages to Rows.

        :param topics: list of `Topic` instances to subscribe to.
        :param callable on_assign: callback to provide handling of customized offsets
            on completion of a successful partition re-assignment.
        :param callable on_revoke: callback to provide handling of offset commits to
            a customized store on the start of a rebalance operation.
        :param callable on_lost: callback to provide handling in the case the partition
            assignment has been lost. Partitions that have been lost may already be
            owned by other members in the group and therefore committing offsets,
            for example, may fail.
        """
        topics_map = {t.name: t for t in topics}
        topics_names = list(topics_map.keys())

        def _on_assign(consumer: Consumer, partitions: list[TopicPartition]):
            buffer_partitions = [
                tp for tp in partitions if not self._topics[tp.topic].is_changelog
            ]
            self._buffer.assign_partitions(buffer_partitions)
            if on_assign is not None:
                on_assign(consumer, partitions)

        def _on_revoke(consumer: Consumer, partitions: list[TopicPartition]):
            buffer_partitions = [
                tp for tp in partitions if not self._topics[tp.topic].is_changelog
            ]
            self._buffer.revoke_partitions(buffer_partitions)
            if on_revoke is not None:
                on_revoke(consumer, partitions)

        def _on_lost(consumer: Consumer, partitions: list[TopicPartition]):
            buffer_partitions = [
                tp for tp in partitions if not self._topics[tp.topic].is_changelog
            ]
            self._buffer.revoke_partitions(buffer_partitions)
            if on_lost is not None:
                on_lost(consumer, partitions)

        super()._subscribe(
            topics=topics_names,
            on_assign=_on_assign,
            on_revoke=_on_revoke,
            on_lost=_on_lost,
        )
        self._topics = topics_map

    @property
    def consumer_exists(self) -> bool:
        return self._inner_consumer is not None

    def poll_row(
        self, timeout: Optional[float] = None, buffered: bool = False
    ) -> Union[Row, list[Row], None]:
        """
        Consumes a single message and deserialize it to Row or a list of Rows.

        The message is deserialized according to the corresponding Topic.
        If deserializer raises `IgnoreValue` exception, this method will return None.
        If Kafka returns an error, it will be raised as exception.

        :param timeout: poll timeout seconds
        :param buffered: when `True`, the consumer will read messages in batches and buffer them for the timestamp-order processing
            across multiple topic partitions with the same number.
            Normally, it should be True if the application uses joins or concatenates topics.
            Note: buffered and non-buffered calls should not be mixed within the same application.
            Default - `False`.
        :return: single Row, list of Rows or None
        """
        if buffered:
            msg = self._poll_buffered(timeout=timeout)
        else:
            msg = self._poll_unbuffered(timeout=timeout)

        if msg is None:
            return None

        topic_name = msg.topic()
        try:
            topic = self._topics[topic_name]
            row_or_rows = topic.row_deserialize(message=msg)
            return row_or_rows
        except IgnoreMessage:
            # Deserializer decided to ignore the message
            return None
        except Exception as exc:
            to_suppress = self._on_error(exc, msg, logger)
            if to_suppress:
                return None
            raise

    def close(self):
        super().close()
        self.reset_backpressure()
        self._buffer.close()
        self._inner_consumer = None

    @property
    def backpressured_tps(self) -> set[TopicPartition]:
        return self._backpressurred_tps

    def trigger_backpressure(
        self,
        offsets_to_seek: dict[tuple[str, int], int],
        resume_after: float,
    ):
        """
        Pause all partitions for the certain period of time and seek the partitions
        provided in the `offsets_to_seek` dict.

        This method is supposed to be called in case of backpressure from Sinks.
        """
        resume_at = monotonic() + resume_after
        self._backpressure_resume_at = min(self._backpressure_resume_at, resume_at)

        changelog_topics = {k for k, v in self._topics.items() if v.is_changelog}
        for tp in self.assignment():
            # Pause only data TPs excluding changelog TPs
            if tp.topic in changelog_topics:
                continue

            position, *_ = self.position([tp])
            logger.debug(
                f'Pausing topic partition "{tp.topic}[{tp.partition}]" for {resume_after}s; '
                f"position={position.offset}"
            )
            self.pause(partitions=[tp])
            self._buffer.clear(topic=tp.topic, partition=tp.partition)

            # Seek the TP back to the "offset_to_seek" to start from it on resume.
            # The "offset_to_seek" is provided by the Checkpoint and is expected to be the
            # first offset processed in the checkpoint.
            # There may be no offset for the TP if no message has been processed yet.
            seek_offset = offsets_to_seek.get((tp.topic, tp.partition))
            if seek_offset is not None:
                logger.debug(
                    f'Seek the paused partition "{tp.topic}[{tp.partition}]" back to '
                    f"offset {seek_offset}"
                )
                self.seek(
                    partition=TopicPartition(
                        topic=tp.topic, partition=tp.partition, offset=seek_offset
                    )
                )

            self._backpressurred_tps.add(tp)

    def resume_backpressured(self):
        """
        Resume consuming from assigned data partitions after the wait period has elapsed.
        """
        if self._backpressure_resume_at > monotonic():
            return

        # Resume the previously backpressured TPs
        for tp in self._backpressurred_tps:
            logger.debug(f'Resuming topic partition "{tp.topic}[{tp.partition}]"')
            self.resume(partitions=[tp])
        self.reset_backpressure()

    def reset_backpressure(self):
        # Reset the timeout back to its initial state
        self._backpressure_resume_at = float("inf")
        self._backpressurred_tps.clear()

    def _poll_unbuffered(
        self, timeout: Optional[float] = None
    ) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        """
        Poll messages in a usual way and validate the errors
        """
        try:
            # Poll messages the usual way and validate them
            raw_msg = self.poll(timeout=timeout)
            if raw_msg is None:
                return None
            return raise_for_msg_error(raw_msg)
        except PartitionAssignmentError:
            # Always propagate errors happened during assignment
            raise
        except Exception as exc:
            to_suppress = self._on_error(exc, None, logger)
            if to_suppress:
                return None
            raise

    def _poll_buffered(
        self, timeout: Optional[float] = None
    ) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        """
        Poll messages in a buffered way to provide in-order reads across multiple
        topic partitions with the same partition number.
        """

        # Probe the buffer and return immediately if there's data available
        msg = self._buffer.pop()
        if msg is None:
            # If the buffer is empty, feed it and try the probing again
            self._feed_buffer(timeout=timeout)
            msg = self._buffer.pop()

        return msg

    def _feed_buffer(self, timeout: Optional[float] = None):
        """
        Feed the internal buffer and pause or resume the assigned partitions for the
        balanced consumption.

        :param timeout: the maximum time in seconds to wait for the next batch of messages.
        """
        try:
            messages = self.consume(
                num_messages=self._max_partition_buffer_size, timeout=timeout
            )
        except PartitionAssignmentError:
            # Always propagate errors happened during assignment
            raise
        except Exception as exc:
            to_suppress = self._on_error(exc, None, logger)
            if not to_suppress:
                raise
            messages = []

        # Get the recent cached high watermarks
        high_watermarks: dict[tuple[str, int], int] = {}
        for tp in self.assignment():
            topic_obj = self._topics[tp.topic]
            if not topic_obj.is_changelog:
                _, high = self.get_watermark_offsets(partition=tp, cached=True)
                high_watermarks[(tp.topic, tp.partition)] = high

        # Create a generator to validate messages
        valid_messages = _validate_message_batch(messages, on_error=self._on_error)

        # Feed the batch and the watermarks to the buffer
        self._buffer.feed(messages=valid_messages, high_watermarks=high_watermarks)

        # Resume partitions with empty buffers
        for topic, partition in self._buffer.resume_empty():
            tp = TopicPartition(topic=topic, partition=partition)
            # Make sure we don't resume partitions if they're backpressured
            if not tp in self._backpressurred_tps:
                self.resume([tp])

        # Pause partitions with full buffers to consume data
        # from other partitions on the next call
        for topic, partition in self._buffer.pause_full():
            self.pause([TopicPartition(topic=topic, partition=partition)])
