import logging
from typing import Optional, Callable, List, Union, Mapping

from confluent_kafka import KafkaError, TopicPartition
from typing_extensions import Protocol

from .error_callbacks import ConsumerErrorCallback, default_on_consumer_error
from .exceptions import QuixException, PartitionAssignmentError
from .kafka import Consumer, AssignmentStrategy, AutoOffsetReset
from .kafka.consumer import RebalancingCallback
from .models import Topic, Row
from .models.serializers.exceptions import IgnoreMessage

logger = logging.getLogger(__name__)


class KafkaMessageError(QuixException):
    def __init__(self, error: KafkaError):
        self.error = error

    @property
    def code(self) -> int:
        return self.error.code()

    @property
    def description(self):
        return self.error.str()

    def __str__(self):
        return (
            f"<{self.__class__.__name__} "
            f'code="{self.code}" '
            f'description="{self.description}">'
        )

    def __repr__(self):
        return str(self)


class RowConsumerProto(Protocol):
    def commit(
        self,
        message=None,
        offsets: List[TopicPartition] = None,
        asynchronous: bool = True,
    ) -> Optional[List[TopicPartition]]:
        ...

    def subscribe(
        self,
        topics: List[Topic],
        on_assign: Optional[RebalancingCallback] = None,
        on_revoke: Optional[RebalancingCallback] = None,
        on_lost: Optional[RebalancingCallback] = None,
    ):
        ...


class RowConsumer(Consumer, RowConsumerProto):
    def __init__(
        self,
        broker_address: str,
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset,
        auto_commit_enable: bool = True,
        assignment_strategy: AssignmentStrategy = "range",
        on_commit: Callable[[Optional[KafkaError], List[TopicPartition]], None] = None,
        extra_config: Optional[dict] = None,
        on_error: Optional[ConsumerErrorCallback] = None,
    ):
        """
        A consumer class that is capable of deserializing Kafka messages to Rows
        according to the Topics deserialization settings.

        It overrides `.subscribe()` method of Consumer class to accept `Topic`
        objects instead of strings.


        :param broker_address: Kafka broker host and port in format `<host>:<port>`.
            Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`
        :param auto_offset_reset: Consumer `auto.offset.reset` setting.
            Available values:
              - "earliest" - automatically reset the offset to the smallest offset
              - "latest" - automatically reset the offset to the largest offset
        :param auto_commit_enable: If true, periodically commit offset of
            the last message handed to the application. Default - `True`.
        :param assignment_strategy: The name of a partition assignment strategy.
            Available values: "range", "roundrobin", "cooperative-sticky".
        :param on_commit: Offset commit result propagation callback.
            Passed as "offset_commit_cb" to `confluent_kafka.Consumer`.
        :param extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
            Note: values passed as arguments override values in `extra_config`.
        :param on_error: a callback triggered when `RowConsumer.poll_row` fails.
            If consumer fails and the callback returns `True`, the exception
            will be logged but not propagated.
            The default callback logs an exception and returns `False`.
        """
        super().__init__(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            on_commit=on_commit,
            extra_config=extra_config,
        )
        self._on_error: Optional[ConsumerErrorCallback] = (
            on_error or default_on_consumer_error
        )
        self._topics: Mapping[str, Topic] = {}

    def subscribe(
        self,
        topics: List[Topic],
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
        super().subscribe(
            topics=topics_names,
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_lost=on_lost,
        )
        self._topics = {t.name: t for t in topics}

    def poll_row(self, timeout: float = None) -> Union[Row, List[Row], None]:
        """
        Consumes a single message and deserialize it to Row or a list of Rows.

        The message is deserialized according to the corresponding Topic.
        If deserializer raises `IgnoreValue` exception, this method will return None.
        If Kafka returns an error, it will be raised as exception.

        :param timeout: poll timeout seconds
        :return: single Row, list of Rows or None
        """
        try:
            msg = self.poll(timeout=timeout)
        except PartitionAssignmentError:
            # Always propagate errors happened during assignment
            raise
        except Exception as exc:
            to_suppress = self._on_error(exc, None, logger)
            if to_suppress:
                return
            raise

        if msg is None:
            return

        topic_name, partition, offset = msg.topic(), msg.partition(), msg.offset()
        try:
            if msg.error():
                raise KafkaMessageError(error=msg.error())

            topic = self._topics[topic_name]

            row_or_rows = topic.row_deserialize(message=msg)
            return row_or_rows
        except IgnoreMessage:
            # Deserializer decided to ignore the message
            return
        except Exception as exc:
            to_suppress = self._on_error(exc, msg, logger)
            if to_suppress:
                return
            raise
