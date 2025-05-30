import copy
import dataclasses
import enum
import logging
from typing import Any, Callable, List, Optional, Union

from quixstreams.models.messagecontext import MessageContext
from quixstreams.models.messages import KafkaMessage
from quixstreams.models.rows import Row
from quixstreams.models.serializers import (
    DESERIALIZERS,
    SERIALIZERS,
    Deserializer,
    DeserializerType,
    IgnoreMessage,
    MessageField,
    SerializationContext,
    Serializer,
    SerializerType,
)
from quixstreams.models.timestamps import TimestampType
from quixstreams.models.topics.exceptions import TopicConfigurationError
from quixstreams.models.topics.utils import merge_headers
from quixstreams.models.types import (
    Headers,
    KafkaHeaders,
    SuccessfulConfluentKafkaMessageProto,
)

__all__ = ("Topic", "TopicConfig", "TimestampExtractor", "TopicType")

logger = logging.getLogger(__name__)

TimestampExtractor = Callable[
    [Any, KafkaHeaders, int, TimestampType],
    int,
]


@dataclasses.dataclass(eq=True, frozen=True)
class TopicConfig:
    """
    Represents all kafka-level configuration for a kafka topic.

    Generally used by Topic and any topic creation procedures.
    """

    num_partitions: Optional[int]
    replication_factor: Optional[int]
    extra_config: dict[str, str] = dataclasses.field(default_factory=dict)

    def as_dict(self):
        return dataclasses.asdict(self)


def _resolve_serializer(serializer: SerializerType) -> Serializer:
    if isinstance(serializer, str):
        try:
            return SERIALIZERS[serializer]()
        except KeyError:
            raise ValueError(
                f'Unknown serializer option "{serializer}"; '
                f"valid options are {list(SERIALIZERS.keys())}"
            )
    elif not isinstance(serializer, Serializer):
        raise ValueError(
            f"Serializer must be either one of {list(SERIALIZERS.keys())} "
            f'or a subclass of Serializer; got "{serializer}"'
        )
    return serializer


def _resolve_deserializer(
    deserializer: DeserializerType,
) -> Deserializer:
    if isinstance(deserializer, str):
        try:
            return DESERIALIZERS[deserializer]()
        except KeyError:
            raise ValueError(
                f'Unknown deserializer option "{deserializer}"; '
                f"valid options are {list(DESERIALIZERS.keys())}"
            )
    elif not isinstance(deserializer, Deserializer):
        raise ValueError(
            f"Deserializer must be either one of {list(DESERIALIZERS.keys())} "
            f'or a subclass of Deserializer; got "{deserializer}"'
        )
    return deserializer


class TopicType(enum.Enum):
    REGULAR = 1
    REPARTITION = 2
    CHANGELOG = 3


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
        topic_type: TopicType = TopicType.REGULAR,
        create_config: Optional[TopicConfig] = None,
        value_deserializer: DeserializerType = "json",
        key_deserializer: DeserializerType = "bytes",
        value_serializer: SerializerType = "json",
        key_serializer: SerializerType = "bytes",
        timestamp_extractor: Optional[TimestampExtractor] = None,
        quix_name: str = "",
    ):
        """
        :param name: topic name
        :param topic_type: a type of the topic, can be one of:
            - `TopicType.REGULAR` - the regular input and output topics
            - `TopicType.REPARTITION` - a repartition topic used for re-keying the data
            - `TopicType.CHANGELOG` - a changelog topic to back up the state stores.

            Default - `TopicType.REGULAR`.

        :param create_config: a `TopicConfig` to create a new topic if it does not exist
        :param value_deserializer: a deserializer type for values
        :param key_deserializer: a deserializer type for keys
        :param value_serializer: a serializer type for values
        :param key_serializer: a serializer type for keys
        :param timestamp_extractor: a callable that returns a timestamp in
            milliseconds from a deserialized message.
        :param quix_name: a name of the topic in the Quix Cloud.
            It is set only by `QuixTopicManager`.
        """
        self.name = name
        self.quix_name = quix_name or name
        self._create_config = copy.deepcopy(create_config)
        self._broker_config: Optional[TopicConfig] = None
        self._value_deserializer = _resolve_deserializer(value_deserializer)
        self._key_deserializer = _resolve_deserializer(key_deserializer)
        self._value_serializer = _resolve_serializer(value_serializer)
        self._key_serializer = _resolve_serializer(key_serializer)
        self._timestamp_extractor = timestamp_extractor
        self._type = topic_type

    def __clone__(
        self,
        name: str,
        create_config: Optional[TopicConfig] = None,
    ):
        return self.__class__(
            name=name,
            quix_name=self.quix_name,
            create_config=create_config or self._create_config,
            value_deserializer=self._value_deserializer,
            key_deserializer=self._key_deserializer,
            value_serializer=self._value_serializer,
            key_serializer=self._key_serializer,
            timestamp_extractor=self._timestamp_extractor,
            topic_type=self._type,
        )

    @property
    def create_config(self) -> Optional[TopicConfig]:
        """
        A config to create the topic
        """
        return self._create_config

    @create_config.setter
    def create_config(self, config: Optional[TopicConfig]):
        self._create_config = config

    @property
    def broker_config(self) -> TopicConfig:
        """
        A topic config obtained from the Kafka broker
        """
        if self._broker_config is None:
            raise TopicConfigurationError(
                f'The broker topic configuration is missing for the topic "{self.name}"'
            )
        return self._broker_config

    @broker_config.setter
    def broker_config(self, config: TopicConfig):
        self._broker_config = copy.deepcopy(config)

    @property
    def is_changelog(self) -> bool:
        return self._type == TopicType.CHANGELOG

    @property
    def is_regular(self) -> bool:
        return self._type == TopicType.REGULAR

    @property
    def is_repartition(self) -> bool:
        return self._type == TopicType.REPARTITION

    def row_serialize(self, row: Row, key: Any) -> KafkaMessage:
        """
        Serialize Row to a Kafka message structure
        :param row: Row to serialize
        :param key: message key to serialize
        :return: KafkaMessage object with serialized values
        """

        serialization_ctx = SerializationContext(
            topic=self.name, field=MessageField.KEY, headers=row.headers
        )
        # Try to serialize the key only if it's not None
        # If key is None then pass it as is
        # Otherwise, different serializers may serialize None differently
        if key is None:
            key_serialized = None
        else:
            key_serialized = self._key_serializer(key, ctx=serialization_ctx)

        # Update message headers with headers supplied by the value serializer.
        extra_headers = self._value_serializer.extra_headers
        headers = merge_headers(row.headers, extra_headers)
        serialization_ctx.field = MessageField.VALUE
        value_serialized = self._value_serializer(row.value, ctx=serialization_ctx)

        return KafkaMessage(
            key=key_serialized,
            value=value_serialized,
            headers=headers,
        )

    def row_deserialize(
        self, message: SuccessfulConfluentKafkaMessageProto
    ) -> Union[Row, List[Row], None]:
        """
        Deserialize incoming Kafka message to a Row.

        :param message: an object with interface of `confluent_kafka.Message`
        :return: Row, list of Rows or None if the message is ignored.
        """
        headers = message.headers()
        topic = message.topic()
        partition = message.partition()
        offset = message.offset()

        serialization_ctx = SerializationContext(
            topic=topic, field=MessageField.KEY, headers=headers
        )
        if (key_bytes := message.key()) is None:
            key_deserialized = None
        else:
            key_deserialized = self._key_deserializer(
                value=key_bytes, ctx=serialization_ctx
            )

        if (value_bytes := message.value()) is None:
            value_deserialized = None
        else:
            # Reuse the SerializationContext object here to avoid creating a new
            # one with almost the same fields
            serialization_ctx.field = MessageField.VALUE
            try:
                value_deserialized = self._value_deserializer(
                    value=value_bytes, ctx=serialization_ctx
                )
            except IgnoreMessage:
                # Ignore message completely if deserializer raised IgnoreValueError.
                logger.debug(
                    'Ignore incoming message: partition="%s[%s]" offset="%s"',
                    topic,
                    partition,
                    offset,
                )
                return None

        timestamp_type, timestamp_ms = message.timestamp()
        message_context = MessageContext(
            topic=topic,
            partition=partition,
            offset=offset,
            size=len(message),
            leader_epoch=message.leader_epoch(),
        )

        if value_deserialized is not None and self._value_deserializer.split_values:
            # The expected value from this serializer is Iterable and each item
            # should be processed as a separate message
            rows = []
            for item in value_deserialized:
                if self._timestamp_extractor:
                    timestamp_ms = self._timestamp_extractor(
                        item, headers, timestamp_ms, TimestampType(timestamp_type)
                    )
                rows.append(
                    Row(
                        value=item,
                        key=key_deserialized,
                        timestamp=timestamp_ms,
                        headers=headers,
                        context=message_context,
                    )
                )
            return rows

        if (timestamp_extractor := self._timestamp_extractor) is not None:
            timestamp_ms = timestamp_extractor(
                value_deserialized, headers, timestamp_ms, TimestampType(timestamp_type)
            )

        return Row(
            value=value_deserialized,
            timestamp=timestamp_ms,
            key=key_deserialized,
            headers=headers,
            context=message_context,
        )

    def serialize(
        self,
        key: Optional[object] = None,
        value: Optional[object] = None,
        headers: Optional[Headers] = None,
        timestamp_ms: Optional[int] = None,
    ) -> KafkaMessage:
        serialization_ctx = SerializationContext(
            topic=self.name, field=MessageField.KEY, headers=headers
        )
        key = self._key_serializer(key, ctx=serialization_ctx)
        serialization_ctx.field = MessageField.VALUE
        value = self._value_serializer(value, ctx=serialization_ctx)

        return KafkaMessage(
            key=key,
            value=value,
            headers=headers,
            timestamp=timestamp_ms,
        )

    def deserialize(self, message: SuccessfulConfluentKafkaMessageProto):
        serialization_ctx = SerializationContext(
            topic=message.topic(),
            field=MessageField.KEY,
            headers=message.headers(),
        )
        if (key := message.key()) is not None:
            key = self._key_deserializer(key, ctx=serialization_ctx)

        if (value := message.value()) is not None:
            serialization_ctx.field = MessageField.VALUE
            value = self._value_deserializer(value, ctx=serialization_ctx)

        return KafkaMessage(
            key=key,
            value=value,
            headers=message.headers(),
            timestamp=message.timestamp()[1],
        )

    def __repr__(self):
        return f'<{self.__class__.__name__} name="{self.name}">'
