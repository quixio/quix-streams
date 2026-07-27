import logging
from typing import Any, Optional, Union

from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.models import Row, Topic, TopicAdmin
from quixstreams.models.messagecontext import MessageContext
from quixstreams.models.serializers import SerializerType
from quixstreams.models.types import HeadersTuples
from quixstreams.sinks import (
    BaseSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBackpressureError,
)

__all__ = ("KafkaReplicatorSink",)

logger = logging.getLogger(__name__)


class KafkaReplicatorSink(BaseSink):
    """
    A sink that produces data to an external Kafka cluster.

    This sink uses the same serialization approach as the Quix Application.

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sinks.community.kafka import KafkaReplicatorSink

    app = Application(
        consumer_group="group",
    )

    topic = app.topic("input-topic")

    # Define the external Kafka cluster configuration
    kafka_sink = KafkaReplicatorSink(
        broker_address="external-kafka:9092",
        topic_name="output-topic",
        value_serializer="json",
        key_serializer="bytes",
    )

    sdf = app.dataframe(topic=topic)
    sdf.sink(kafka_sink)

    app.run()
    ```
    """

    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        topic_name: str,
        value_serializer: SerializerType = "json",
        key_serializer: SerializerType = "bytes",
        producer_extra_config: Optional[dict] = None,
        flush_timeout: float = 10.0,
        origin_topic: Optional[Topic] = None,
        auto_create_sink_topic: bool = True,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        """
        :param broker_address: The connection settings for the external Kafka cluster.
            Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
            or a ConnectionConfig object if authentication is required.
        :param topic_name: The topic name to produce to on the external Kafka cluster.
        :param value_serializer: The serializer type for values.
            Default - `json`.
        :param key_serializer: The serializer type for keys.
            Default - `bytes`.
        :param producer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
            Default - `None`.
        :param flush_timeout: The time in seconds the producer waits for all messages
            to be delivered during flush.
            Default - 10.0.
        :param origin_topic: If auto-creating the sink topic, can optionally pass the
            source topic to use its configuration.
        :param auto_create_sink_topic: Whether to try to create the sink topic upon startup
            Default - True
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._broker_address = broker_address
        self._topic_name = topic_name
        self._value_serializer = value_serializer
        self._key_serializer = key_serializer
        self._producer_extra_config = producer_extra_config or {}
        self._flush_timeout = flush_timeout
        self._auto_create_sink_topic = auto_create_sink_topic
        self._origin_topic = origin_topic

        self._producer: Optional[InternalProducer] = None
        self._topic: Optional[Topic] = None

    def setup(self):
        """
        Initialize the InternalProducer and Topic for serialization.
        """
        logger.info(
            f"Setting up KafkaReplicatorSink: "
            f'broker_address="{self._broker_address}" '
            f'topic="{self._topic_name}" '
            f'value_serializer="{self._value_serializer}" '
            f'key_serializer="{self._key_serializer}"'
        )

        self._producer = InternalProducer(
            broker_address=self._broker_address,
            extra_config=self._producer_extra_config,
            flush_timeout=self._flush_timeout,
            transactional=False,
        )

        self._topic = Topic(
            name=self._topic_name,
            value_serializer=self._value_serializer,
            key_serializer=self._key_serializer,
            create_config=self._origin_topic.broker_config
            if self._origin_topic
            else None,
        )

        if self._auto_create_sink_topic:
            admin = TopicAdmin(
                broker_address=self._broker_address,
                extra_config=self._producer_extra_config,
            )
            admin.create_topics(topics=[self._topic])

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """
        Add a message to be produced to the external Kafka cluster.

        This method converts the provided data into a Row object and uses
        the InternalProducer to serialize and produce it.

        :param value: The message value.
        :param key: The message key.
        :param timestamp: The message timestamp in milliseconds.
        :param headers: The message headers.
        :param topic: The source topic name.
        :param partition: The source partition.
        :param offset: The source offset.
        """
        context = MessageContext(
            topic=topic,
            partition=partition,
            offset=offset,
            size=0,
            leader_epoch=None,
        )
        row = Row(
            value=value,
            key=key,
            timestamp=timestamp,
            context=context,
            headers=headers,
        )
        self._producer.produce_row(
            row=row,
            topic=self._topic,
            timestamp=timestamp,
        )

    def flush(self) -> None:
        """
        Flush the producer to ensure all messages are delivered.

        This method is triggered by the Checkpoint class when it commits.
        If flush fails, the checkpoint will be aborted.
        """
        logger.debug(f'Flushing KafkaReplicatorSink for topic "{self._topic_name}"')

        # Flush all pending messages
        result = self._producer.flush(timeout=self._flush_timeout)

        if result > 0:
            logger.warning(
                f"{result} messages were not delivered to Kafka topic "
                f'"{self._topic_name}" within the flush timeout of {self._flush_timeout}s'
            )
            raise SinkBackpressureError(retry_after=10.0)

        logger.debug(
            f'Successfully flushed KafkaReplicatorSink for topic "{self._topic_name}"'
        )
