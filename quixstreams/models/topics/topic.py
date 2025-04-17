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
    BytesDeserializer,
    BytesSerializer,
    Deserializer,
    DeserializerIsNotProvidedError,
    DeserializerType,
    IgnoreMessage,
    MessageField,
    SerializationContext,
    Serializer,
    SerializerIsNotProvidedError,
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
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = BytesDeserializer(),
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = BytesSerializer(),
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
        self._value_deserializer = _get_deserializer(value_deserializer)
        self._key_deserializer = _get_deserializer(key_deserializer)
        self._value_serializer = _get_serializer(value_serializer)
        self._key_serializer = _get_serializer(key_serializer)
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
        if key is None:
            key_serialized = None
        else:
            key_ctx = SerializationContext(
                topic=self.name, field=MessageField.KEY, headers=row.headers
            )
            key_serialized = self._key_serializer(key, ctx=key_ctx)

        # Update message headers with headers supplied by the value serializer.
        extra_headers = self._value_serializer.extra_headers
        headers = merge_headers(row.headers, extra_headers)
        value_ctx = SerializationContext(
            topic=self.name, field=MessageField.VALUE, headers=row.headers
        )

        return KafkaMessage(
            key=key_serialized,
            value=self._value_serializer(row.value, ctx=value_ctx),
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
        if self._key_deserializer is None:
            raise DeserializerIsNotProvidedError(
                f'Key deserializer is not provided for topic "{self.name}"'
            )
        if self._value_deserializer is None:
            raise DeserializerIsNotProvidedError(
                f'Value deserializer is not provided for topic "{self.name}"'
            )

        headers = message.headers()

        if (key_bytes := message.key()) is None:
            key_deserialized = None
        else:
            key_ctx = SerializationContext(
                topic=message.topic(), field=MessageField.KEY, headers=headers
            )
            key_deserialized = self._key_deserializer(value=key_bytes, ctx=key_ctx)

        if (value_bytes := message.value()) is None:
            value_deserialized = None
        else:
            value_ctx = SerializationContext(
                topic=message.topic(), field=MessageField.VALUE, headers=headers
            )
            try:
                value_deserialized = self._value_deserializer(
                    value=value_bytes, ctx=value_ctx
                )
            except IgnoreMessage:
                # Ignore message completely if deserializer raised IgnoreValueError.
                logger.debug(
                    'Ignore incoming message: partition="%s[%s]" offset="%s"',
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                return None

        timestamp_type, timestamp_ms = message.timestamp()
        message_context = MessageContext(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
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

        if self._timestamp_extractor:
            timestamp_ms = self._timestamp_extractor(
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
        if self._key_serializer:
            key_ctx = SerializationContext(
                topic=self.name, field=MessageField.KEY, headers=headers
            )
            key = self._key_serializer(key, ctx=key_ctx)
        elif key is not None:
            raise SerializerIsNotProvidedError(
                f'Key serializer is not provided for topic "{self.name}"'
            )
        if self._value_serializer:
            value_ctx = SerializationContext(
                topic=self.name, field=MessageField.VALUE, headers=headers
            )
            value = self._value_serializer(value, ctx=value_ctx)
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

    def deserialize(self, message: SuccessfulConfluentKafkaMessageProto):
        if (key := message.key()) is not None:
            if self._key_deserializer:
                key_ctx = SerializationContext(
                    topic=message.topic(),
                    field=MessageField.KEY,
                    headers=message.headers(),
                )
                key = self._key_deserializer(key, ctx=key_ctx)
            else:
                raise DeserializerIsNotProvidedError(
                    f'Key deserializer is not provided for topic "{self.name}"'
                )
        if (value := message.value()) is not None:
            if self._value_deserializer:
                value_ctx = SerializationContext(
                    topic=message.topic(),
                    field=MessageField.VALUE,
                    headers=message.headers(),
                )
                value = self._value_deserializer(value, ctx=value_ctx)
            else:
                raise DeserializerIsNotProvidedError(
                    f'Value deserializer is not provided for topic "{self.name}"'
                )
        return KafkaMessage(
            key=key,
            value=value,
            headers=message.headers(),
            timestamp=message.timestamp()[1],
        )

    def __repr__(self):
        return f'<{self.__class__.__name__} name="{self.name}">'
