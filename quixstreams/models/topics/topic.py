import dataclasses
import logging
from typing import List, Optional, Any, Callable, Union

from confluent_kafka.admin import NewTopic, ConfigResource  # type: ignore

from quixstreams.models.messagecontext import MessageContext
from quixstreams.models.messages import KafkaMessage
from quixstreams.models.rows import Row
from quixstreams.models.serializers import (
    SerializationContext,
    DeserializerIsNotProvidedError,
    SerializerIsNotProvidedError,
    BytesSerializer,
    BytesDeserializer,
    IgnoreMessage,
    SERIALIZERS,
    DESERIALIZERS,
    SerializerType,
    DeserializerType,
    Serializer,
    Deserializer,
)
from quixstreams.models.timestamps import MessageTimestamp, TimestampType
from quixstreams.models.types import (
    ConfluentKafkaMessageProto,
    Headers,
    MessageHeadersTuples,
)

__all__ = ("Topic", "TopicConfig", "TimestampExtractor")

logger = logging.getLogger(__name__)

TimestampExtractor = Callable[
    [Any, Optional[MessageHeadersTuples], int, TimestampType],
    int,
]


@dataclasses.dataclass(eq=True)
class TopicConfig:
    """
    Represents all kafka-level configuration for a kafka topic.

    Generally used by Topic and any topic creation procedures.
    """

    num_partitions: int
    replication_factor: int
    extra_config: dict = dataclasses.field(default_factory=dict)

    def as_dict(self):
        return dataclasses.asdict(self)


def _get_serializer(serializer: Optional[SerializerType]) -> Optional[Serializer]:
    if isinstance(serializer, str):
        try:
            return SERIALIZERS[serializer]()
        except KeyError:
            raise ValueError(
                f"Unknown deserializer option '{serializer}'; "
                f"valid options are {list(SERIALIZERS.keys())}"
            )
    return serializer


def _get_deserializer(
    deserializer: Optional[DeserializerType],
) -> Optional[Deserializer]:
    if isinstance(deserializer, str):
        try:
            return DESERIALIZERS[deserializer]()
        except KeyError:
            raise ValueError(
                f"Unknown deserializer option '{deserializer}'; "
                f"valid options are {list(DESERIALIZERS.keys())}"
            )
    return deserializer


class Topic:
    """
    A definition of a Kafka topic.

    Typically created with an `app = quixstreams.app.Application()` instance via
    `app.topic()`, and used by `quixstreams.dataframe.StreamingDataFrame`
    instance.
    """

    def __init__(
        self,
        name: str,
        config: TopicConfig,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = BytesDeserializer(),
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = BytesSerializer(),
        timestamp_extractor: Optional[TimestampExtractor] = None,
    ):
        """
        :param name: topic name
        :param config: topic configs via `TopicConfig` (creation/validation)
        :param value_deserializer: a deserializer type for values
        :param key_deserializer: a deserializer type for keys
        :param value_serializer: a serializer type for values
        :param key_serializer: a serializer type for keys
        :param timestamp_extractor: a callable that returns a timestamp in
            milliseconds from a deserialized message.
        """
        self._name = name
        self._config = config
        self._key_serializer = _get_serializer(key_serializer)
        self._key_deserializer = _get_deserializer(key_deserializer)
        self._value_serializer = _get_serializer(value_serializer)
        self._value_deserializer = _get_deserializer(value_deserializer)
        self._timestamp_extractor = timestamp_extractor

    @property
    def name(self) -> str:
        """
        Topic name
        """
        return self._name

    @property
    def config(self) -> TopicConfig:
        return self._config

    def row_serialize(self, row: Row, key: Any) -> KafkaMessage:
        """
        Serialize Row to a Kafka message structure
        :param row: Row to serialize
        :param key: message key to serialize
        :return: KafkaMessage object with serialized values
        """
        ctx = SerializationContext(topic=self.name, headers=row.headers)
        if self._key_serializer is None:
            raise SerializerIsNotProvidedError(
                f'Key serializer is not provided for topic "{self.name}"'
            )
        if self._value_serializer is None:
            raise SerializerIsNotProvidedError(
                f'Value serializer is not provided for topic "{self.name}"'
            )
        # Try to serialize the key only if it's not None
        # If key is None then pass it as is
        # Otherwise, different serializers may serialize None differently
        key_serialized = None if key is None else self._key_serializer(key, ctx=ctx)
        return KafkaMessage(
            key=key_serialized,
            value=self._value_serializer(row.value, ctx=ctx),
            headers=self._value_serializer.extra_headers,
        )

    def row_deserialize(
        self, message: ConfluentKafkaMessageProto
    ) -> Union[Row, List[Row], None]:
        """
        Deserialize incoming Kafka message to a Row.

        :param message: an object with interface of `confluent_kafka.Message`
        :return: Row, list of Rows or None if the message is ignored.
        """
        if self._key_deserializer is None:
            raise DeserializerIsNotProvidedError(
                f'Key deserializer is not provided for topic "{self.name}"'
            )
        if self._value_deserializer is None:
            raise DeserializerIsNotProvidedError(
                f'Value deserializer is not provided for topic "{self.name}"'
            )

        headers = message.headers()
        ctx = SerializationContext(topic=message.topic(), headers=headers)

        key_bytes = message.key()
        key_deserialized = (
            None
            if key_bytes is None
            else self._key_deserializer(value=key_bytes, ctx=ctx)
        )
        value_bytes = message.value()
        try:
            value_deserialized = (
                None
                if value_bytes is None
                else self._value_deserializer(value=message.value(), ctx=ctx)
            )
        except IgnoreMessage:
            # Ignore message completely if deserializer raised IgnoreValueError.
            logger.debug(
                'Ignore incoming message: partition="%s[%s]" offset="%s"',
                message.topic(),
                message.partition(),
                message.offset(),
            )
            return

        if self._value_deserializer.split_values:
            # The expected value from this serializer is Iterable and each item
            # should be processed as a separate message
            rows = []
            for item in value_deserialized:
                rows.append(
                    Row(
                        value=item,
                        context=self._create_message_context(
                            message=message,
                            key=key_deserialized,
                            headers=headers,
                            value=item,
                        ),
                    )
                )
            return rows

        return Row(
            value=value_deserialized,
            context=self._create_message_context(
                message=message,
                key=key_deserialized,
                headers=headers,
                value=value_deserialized,
            ),
        )

    def serialize(
        self,
        key: Optional[object] = None,
        value: Optional[object] = None,
        headers: Optional[Headers] = None,
        timestamp_ms: Optional[int] = None,
    ) -> KafkaMessage:
        ctx = SerializationContext(topic=self.name, headers=headers)
        if self._key_serializer:
            key = self._key_serializer(key, ctx=ctx)
        elif key is not None:
            raise SerializerIsNotProvidedError(
                f'Key serializer is not provided for topic "{self.name}"'
            )
        if self._value_serializer:
            value = self._value_serializer(value, ctx=ctx)
        elif value is not None:
            raise SerializerIsNotProvidedError(
                f'Value serializer is not provided for topic "{self.name}"'
            )
        return KafkaMessage(
            key=key,
            value=value,
            headers=headers,
            timestamp=timestamp_ms,
        )

    def deserialize(self, message: ConfluentKafkaMessageProto):
        ctx = SerializationContext(topic=message.topic(), headers=message.headers())
        key_bytes = message.key()
        value_bytes = message.value()
        return KafkaMessage(
            key=(
                None
                if key_bytes is None
                else self._key_deserializer(key_bytes, ctx=ctx)
            ),
            value=(
                None
                if value_bytes is None
                else self._value_serializer(value_bytes, ctx=ctx)
            ),
            headers=message.headers(),
            timestamp=message.timestamp()[1],
        )

    def __repr__(self):
        return f'<{self.__class__.__name__} name="{self._name}"> '

    def _create_message_context(
        self,
        message: ConfluentKafkaMessageProto,
        key: Any,
        headers: Optional[MessageHeadersTuples],
        value: Any,
    ) -> MessageContext:
        timestamp_type, timestamp_ms = message.timestamp()

        if self._timestamp_extractor:
            timestamp_ms = self._timestamp_extractor(
                value, headers, timestamp_ms, TimestampType(timestamp_type)
            )

        timestamp = MessageTimestamp.create(
            timestamp_type=timestamp_type, milliseconds=timestamp_ms
        )

        return MessageContext(
            key=key,
            headers=headers,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            size=len(message),
            timestamp=timestamp,
            latency=message.latency(),
            leader_epoch=message.leader_epoch(),
        )
