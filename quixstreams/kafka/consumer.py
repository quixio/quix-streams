import functools
import logging
import typing
from typing import Any, Callable, List, Optional, Tuple, Union, cast

from confluent_kafka import (
    Consumer as ConfluentConsumer,
)
from confluent_kafka import (
    KafkaError,
    Message,
    TopicPartition,
)
from confluent_kafka.admin import ClusterMetadata, GroupMetadata

from quixstreams.exceptions import KafkaPartitionError, PartitionAssignmentError
from quixstreams.models.types import (
    RawConfluentKafkaMessageProto,
    SuccessfulConfluentKafkaMessageProto,
)

from .configuration import ConnectionConfig
from .exceptions import KafkaConsumerException

__all__ = (
    "BaseConsumer",
    "Consumer",
    "AutoOffsetReset",
    "RebalancingCallback",
    "raise_for_msg_error",
)

RebalancingCallback = Callable[[ConfluentConsumer, List[TopicPartition]], None]
OnCommitCallback = Callable[[Optional[KafkaError], List[TopicPartition]], None]
AutoOffsetReset = typing.Literal["earliest", "latest", "error"]

logger = logging.getLogger(__name__)


def _default_error_cb(error: KafkaError):
    error_code = error.code()
    logger.error(f'Kafka consumer error: {error.str()} code="{error_code}"')


def _default_on_commit_cb(
    error: Optional[KafkaError],
    partitions: List[TopicPartition],
    on_commit: Optional[OnCommitCallback] = None,
):
    if error is not None:
        logger.error(
            f"Kafka commit error: {error.str()} (code={error.code()})",
        )
    if on_commit is not None:
        on_commit(error, partitions)


def _wrap_assignment_errors(func):
    """
    Wrap exceptions raised from "on_assign", "on_revoke" and "on_lost" callbacks
    into `PartitionAssignmentError`
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            raise PartitionAssignmentError("Error during partition assignment") from exc

    return wrapper


def raise_for_msg_error(
    msg: RawConfluentKafkaMessageProto,
) -> SuccessfulConfluentKafkaMessageProto:
    if msg.error():
        raise KafkaConsumerException(error=msg.error())
    return cast(SuccessfulConfluentKafkaMessageProto, msg)


class BaseConsumer:
    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        consumer_group: Optional[str],
        auto_offset_reset: AutoOffsetReset,
        auto_commit_enable: bool = True,
        logger: logging.Logger = logger,
        error_callback: Callable[[KafkaError], None] = _default_error_cb,
        on_commit: Optional[
            Callable[[Optional[KafkaError], List[TopicPartition]], None]
        ] = None,
        extra_config: Optional[dict] = None,
    ):
        """
        A wrapper around `confluent_kafka.Consumer`.

        It initializes `confluent_kafka.Consumer` on demand
        avoiding network calls during `__init__`, provides typing info for methods
        and some reasonable defaults.

        :param broker_address: Connection settings for Kafka.
            Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
            or a ConnectionConfig object if authentication is required.
        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`
        :param auto_offset_reset: Consumer `auto.offset.reset` setting.
            Available values:
                <br>"earliest" - automatically reset the offset to the smallest offset
                <br>"latest" - automatically reset the offset to the largest offset
                <br>"error" - trigger an error (`ERR__AUTO_OFFSET_RESET`) which is
                    retrieved by consuming messages (used for testing)
        :param auto_commit_enable: If true, periodically commit offset of
            the last message handed to the application. Default - `True`.
        :param logger: a Logger instance to attach librdkafka logging to
        :param error_callback: callback used for consumer errors
        :param on_commit: Offset commit result propagation callback.
            Passed as "offset_commit_cb" to `confluent_kafka.Consumer`.
        :param extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
            Note: values passed as arguments override values in `extra_config`.
        """
        if isinstance(broker_address, str):
            broker_address = ConnectionConfig(bootstrap_servers=broker_address)

        self._consumer_config = {
            # previous Quix Streams defaults
            "enable.auto.offset.store": False,
            "partition.assignment.strategy": "range",
            **(extra_config or {}),
            **broker_address.as_librdkafka_dict(),
            **{
                "group.id": consumer_group,
                "enable.auto.commit": auto_commit_enable,
                "auto.offset.reset": auto_offset_reset,
                "logger": logger,
                "error_cb": error_callback,
                "on_commit": functools.partial(
                    _default_on_commit_cb, on_commit=on_commit
                ),
            },
        }
        self._inner_consumer: Optional[ConfluentConsumer] = None

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[RawConfluentKafkaMessageProto]:
        """
        Consumes a single message, calls callbacks and returns events.

        The application must check the returned :py:class:`Message`
        object's :py:func:`Message.error()` method to distinguish between proper
        messages (error() returns None), or an event or error.

        Note: a `RebalancingCallback` may be called from this method (
        `on_assign`, `on_revoke`, or `on_lost`).

        :param float timeout: Maximum time in seconds to block waiting for message,
            event or callback. None or -1 is infinite. Default: None.
        :return: A `Message` object or `None` on timeout
        :rtype: Optional[Message]
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.poll(timeout=timeout if timeout is not None else -1)

    def _subscribe(
        self,
        topics: List[str],
        on_assign: Optional[RebalancingCallback] = None,
        on_revoke: Optional[RebalancingCallback] = None,
        on_lost: Optional[RebalancingCallback] = None,
    ):
        """
        Set subscription to supplied list of topics
        This replaces a previous subscription.

        :param List[str] topics: List of topics (strings) to subscribe to.
        :param Optional[RebalancingCallback] on_assign: callback to provide handling of
            customized offsets on completion of a successful partition re-assignment.
        :param Optional[RebalancingCallback] on_revoke: callback to provide handling of
            offset commits to a customized store on the start of a rebalance operation.
        :param Optional[RebalancingCallback] on_lost: callback to provide handling in
            the case the partition assignment has been lost. Partitions that have been
            lost may already be owned by other members in the group and therefore
            committing offsets, for example, may fail.
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """

        @_wrap_assignment_errors
        def _on_assign_wrapper(consumer: Consumer, partitions: List[TopicPartition]):
            for partition in partitions:
                logger.debug(
                    'Assigning topic partition "%s[%s]"',
                    partition.topic,
                    partition.partition,
                )
                if partition.error:
                    raise KafkaPartitionError(
                        f"Kafka partition error: "
                        f'partition="{partition.topic}[{partition.partition}]" '
                        f'error="{partition.error}"'
                    )

            if on_assign is not None:
                on_assign(consumer, partitions)

        @_wrap_assignment_errors
        def _on_revoke_wrapper(consumer: Consumer, partitions: List[TopicPartition]):
            for partition in partitions:
                logger.debug(
                    'Revoking topic partition "%s[%s]"',
                    partition.topic,
                    partition.partition,
                )
                if partition.error:
                    raise KafkaPartitionError(
                        f"Kafka partition error: "
                        f'partition="{partition.topic}[{partition.partition}]" '
                        f'error="{partition.error}"'
                    )

            if on_revoke is not None:
                on_revoke(consumer, partitions)

        @_wrap_assignment_errors
        def _on_lost_wrapper(consumer: Consumer, partitions: List[TopicPartition]):
            for partition in partitions:
                logger.debug(
                    'Losing topic partition: topic="%s" partition="%s"',
                    partition.topic,
                    partition.partition,
                )
                if partition.error:
                    raise KafkaPartitionError(
                        f"Kafka partition error: "
                        f'partition="{partition.topic}[{partition.partition}]" '
                        f'error="{partition.error}"'
                    )
            if on_lost is not None:
                on_lost(consumer, partitions)

        return self._consumer.subscribe(
            topics=topics,
            on_assign=_on_assign_wrapper,
            on_revoke=_on_revoke_wrapper,
            on_lost=_on_lost_wrapper,
        )

    def unsubscribe(self):
        """
        Remove current subscription.

        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.unsubscribe()

    def store_offsets(
        self,
        message: Optional[Message] = None,
        offsets: Optional[List[TopicPartition]] = None,
    ):
        """
        Store offsets for a message or a list of offsets.

        `message` and `offsets` are mutually exclusive. The stored offsets
        will be committed according to 'auto.commit.interval.ms' or manual
        offset-less `commit`.
        Note that 'enable.auto.offset.store' must be set to False when using this API.

        :param confluent_kafka.Message message: Store message's offset+1.
        :param List[TopicPartition] offsets: List of topic+partitions+offsets to store.
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """

        if message is not None and offsets is not None:
            raise ValueError(
                'Parameters "message" and "offsets" are mutually exclusive'
            )
        if message is None and offsets is None:
            raise ValueError('One of "message" or "offsets" must be passed')

        if message:
            return self._consumer.store_offsets(message=message)
        else:
            return self._consumer.store_offsets(offsets=offsets)

    def commit(
        self,
        message: Optional[Message] = None,
        offsets: Optional[List[TopicPartition]] = None,
        asynchronous: bool = True,
    ) -> Optional[List[TopicPartition]]:
        """
        Commit a message or a list of offsets.

        The `message` and `offsets` parameters are mutually exclusive.
        If neither is set, the current partition assignment's offsets are used instead.
        Use this method to commit offsets if you have 'enable.auto.commit' set to False.

        :param Message message: Commit the message's offset+1.
            Note: By convention, committed offsets reflect the next message
            to be consumed, **not** the last message consumed.
        :param List[TopicPartition] offsets: List of topic+partitions+offsets to commit.
        :param bool asynchronous: If true, asynchronously commit, returning None
            immediately. If False, the commit() call will block until the commit
            succeeds or fails and the committed offsets will be returned (on success).
            Note that specific partitions may have failed and the .err field of
            each partition should be checked for success.
        :rtype: Optional[List[TopicPartition]]
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """

        if message is not None and offsets is not None:
            raise ValueError(
                'Parameters "message" and "offsets" are mutually exclusive'
            )

        kwargs: dict[str, Any] = {
            "asynchronous": asynchronous,
        }
        if offsets is not None:
            kwargs["offsets"] = offsets
        if message is not None:
            kwargs["message"] = message
        return self._consumer.commit(**kwargs)

    def committed(
        self, partitions: List[TopicPartition], timeout: Optional[float] = None
    ) -> List[TopicPartition]:
        """
        Retrieve committed offsets for the specified partitions.

        :param List[TopicPartition] partitions: List of topic+partitions to query for stored offsets.
        :param float timeout: Request timeout (seconds).
            None or -1 is infinite. Default: None
        :return: List of topic+partitions with offset and possibly error set.
        :rtype: List[TopicPartition]
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.committed(
            partitions, timeout=timeout if timeout is not None else -1
        )

    def get_watermark_offsets(
        self,
        partition: TopicPartition,
        timeout: Optional[float] = None,
        cached: bool = False,
    ) -> Tuple[int, int]:
        """
        Retrieve low and high offsets for the specified partition.

        :param TopicPartition partition: Topic+partition to return offsets for.
        :param float timeout: Request timeout (seconds). None or -1 is infinite.
            Ignored if cached=True. Default: None
        :param bool cached: Instead of querying the broker, use cached information.
            Cached values: The low offset is updated periodically
            (if statistics.interval.ms is set) while the high offset is updated on each
            message fetched from the broker for this partition.
        :return: Tuple of (low,high) on success or None on timeout.
            The high offset is the offset of the last message + 1.
        :rtype: Tuple[int, int]
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.get_watermark_offsets(
            partition, timeout=timeout if timeout is not None else -1, cached=cached
        )

    def list_topics(
        self, topic: Optional[str] = None, timeout: Optional[float] = None
    ) -> ClusterMetadata:
        """
        Request metadata from the cluster.
        This method provides the same information as
        listTopics(), describeTopics() and describeCluster() in  the Java Admin client.

        :param str topic: If specified, only request information about this topic,
            else return results for all topics in cluster.
            Warning: If auto.create.topics.enable is set to true on the broker and
            an unknown topic is specified, it will be created.
        :param float timeout: The maximum response time before timing out
            None or -1 is infinite. Default: None
        :rtype: ClusterMetadata
        :raises KafkaException: if a Kafka-based error occurs
        """
        return self._consumer.list_topics(
            topic=topic, timeout=timeout if timeout is not None else -1
        )

    def memberid(self) -> Optional[str]:
        """
        Return this client's broker-assigned group member id.

        The member id is assigned by the group coordinator and is propagated to
        the consumer during rebalance.

        :return: Member id string or None
        :rtype: Optional[string]
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.memberid()

    def offsets_for_times(
        self, partitions: List[TopicPartition], timeout: Optional[float] = None
    ) -> List[TopicPartition]:
        """

        Look up offsets by timestamp for the specified partitions.

        The returned offset for each partition is the earliest offset whose
        timestamp is greater than or equal to the given timestamp in the
        corresponding partition. If the provided timestamp exceeds that of the
        last message in the partition, a value of -1 will be returned.

        :param List[TopicPartition] partitions: topic+partitions with timestamps
            in the TopicPartition.offset field.
        :param float timeout: The maximum response time before timing out.
            None or -1 is infinite. Default: None
        :return: List of topic+partition with offset field set and possibly error set
        :rtype: List[TopicPartition]
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """

        return self._consumer.offsets_for_times(
            partitions, timeout=timeout if timeout is not None else -1
        )

    def pause(self, partitions: List[TopicPartition]):
        """
        Pause consumption for the provided list of partitions.

        Paused partitions must be tracked manually.

        Does NOT affect the result of `Consumer.assignment()`.

        :param List[TopicPartition] partitions: List of topic+partitions to pause.
        :raises KafkaException: if a Kafka-based error occurs
        """
        return self._consumer.pause(partitions)

    def resume(self, partitions: List[TopicPartition]):
        """
        Resume consumption for the provided list of partitions.

        :param List[TopicPartition] partitions: List of topic+partitions to resume.
        :rtype: None
        :raises KafkaException: if a Kafka-based error occurs
        """
        return self._consumer.resume(partitions)

    def position(self, partitions: List[TopicPartition]) -> List[TopicPartition]:
        """
        Retrieve current positions (offsets) for the specified partitions.

        :param List[TopicPartition] partitions: List of topic+partitions to return
            current offsets for. The current offset is the offset of
            the last consumed message + 1.
        :return: List of topic+partitions with offset and possibly error set.
        :rtype: List[TopicPartition]
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.position(partitions)

    def seek(self, partition: TopicPartition):
        """
        Set consume position for partition to offset.
        The offset may be an absolute (>=0) or a
        logical offset like `OFFSET_BEGINNING`.

        `seek()` may only be used to update the consume offset of an
        actively consumed partition (i.e., after `Consumer.assign()`),
        to set the starting offset of partition not being consumed instead
        pass the offset in an `assign()` call.

        :param TopicPartition partition: Topic+partition+offset to seek to.

        :raises KafkaException: if a Kafka-based error occurs
        """
        return self._consumer.seek(partition)

    def assignment(
        self,
    ) -> List[TopicPartition]:
        """
        Returns the current partition assignment.

        :return: List of assigned topic+partitions.
        :rtype: List[TopicPartition]
        :raises KafkaException: if a Kafka-based error occurs
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.assignment()

    def set_sasl_credentials(self, username: str, password: str):
        """
        Sets the SASL credentials used for this client.
        These credentials will overwrite the old ones, and will be used the next
        time the client needs to authenticate.
        This method will not disconnect existing broker connections that have been
        established with the old credentials.
        This method is applicable only to SASL PLAIN and SCRAM mechanisms.

        :param str username: your username
        :param str password: your password
        """
        return self._consumer.set_sasl_credentials(username, password)

    def incremental_assign(self, partitions: List[TopicPartition]):
        """
        Assign new partitions.

        Can be called outside the `Consumer` `on_assign` callback (multiple times).
        Partitions immediately show on `Consumer.assignment()`.

        Any additional partitions besides the ones passed during the `Consumer`
        `on_assign` callback will NOT be associated with the consumer group.

        :param List[TopicPartition] partitions: a list of topic partitions
        """
        return self._consumer.incremental_assign(partitions)

    def assign(self, partitions: List[TopicPartition]):
        """
        Set the consumer partition assignment to the provided list of `TopicPartition` and start consuming.

        :param List[TopicPartition] partitions: List of topic+partitions and optionally initial offsets to start consuming from.
        :raises: KafkaException
        :raises: RuntimeError if called on a closed consumer
        """
        return self._consumer.assign(partitions)

    def incremental_unassign(self, partitions: List[TopicPartition]):
        """
        Revoke partitions.

        Can be called outside an on_revoke callback.

        :param List[TopicPartition] partitions: a list of topic partitions
        """
        return self._consumer.incremental_unassign(partitions)

    def unassign(self):
        """
        Removes the current partition assignment and stops consuming.

        :raises KafkaException:
        :raises RuntimeError: if called on a closed consumer
        """
        return self._consumer.unassign()

    def close(self):
        """
        Close down and terminate the Kafka Consumer.

        Actions performed:

        - Stops consuming.
        - Commits offsets, unless the consumer property 'enable.auto.commit' is set to False.
        - Leaves the consumer group.

        Registered callbacks may be called from this method,
        see `poll()` for more info.
        """
        logger.debug("Closing Kafka consumer")
        self._consumer.close()
        logger.debug("Kafka consumer closed")

    def consumer_group_metadata(self) -> GroupMetadata:
        """
        Used by the producer during consumer offset sending for an EOS transaction.
        """
        return self._consumer.consumer_group_metadata()

    def consume(
        self, num_messages: int = 1, timeout: Optional[float] = None
    ) -> list[RawConfluentKafkaMessageProto]:
        """
        Consumes a list of messages (possibly empty on timeout).
        Callbacks may be executed as a side effect of calling this method.

        :param num_messages: The maximum number of messages to return.
            Default: `1`.
        :param timeout: The maximum time in seconds to block waiting for message, event or callback.
            Default: `None` (infinite).
        """
        return self._consumer.consume(
            num_messages=num_messages, timeout=timeout if timeout is not None else -1
        )

    @property
    def _consumer(self) -> ConfluentConsumer:
        """
        Initialize consumer on demand to avoid network calls on object __init__
        :return: confluent_kafka.Consumer
        """
        if self._inner_consumer is None:
            self._inner_consumer = ConfluentConsumer(self._consumer_config)

        return self._inner_consumer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Consumer(BaseConsumer):
    def subscribe(
        self,
        topics: List[str],
        on_assign: Optional[RebalancingCallback] = None,
        on_revoke: Optional[RebalancingCallback] = None,
        on_lost: Optional[RebalancingCallback] = None,
    ):
        return super()._subscribe(topics, on_assign, on_revoke, on_lost)
