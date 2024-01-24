import dataclasses
import logging
from typing import Union, List, Optional, Any, Mapping, Iterable, Dict
from typing_extensions import TypeAlias

from confluent_kafka.admin import NewTopic, ConfigResource  # type: ignore

from .messagecontext import MessageContext
from .messages import KafkaMessage
from .rows import Row
from .serializers import (
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
from .timestamps import MessageTimestamp
from .types import (
    ConfluentKafkaMessageProto,
    MessageHeadersTuples,
)

logger = logging.getLogger(__name__)

__all__ = ("Topic", "TopicConfig")


@dataclasses.dataclass(eq=True)
class TopicConfig:
    """
    Represents all kafka-level configuration for a kafka topic.

    Generally used by Topic and any topic creation procedures.
    """

    num_partitions: int
    replication_factor: int
    extra_config: Optional[Mapping] = None

    def update_extra_config(
        self, defaults: Mapping = None, allowed: Optional[Iterable[str]] = None
    ):
        """
        A convenience method for updating self.extra_config in specific ways.

        :param defaults: Add these new configs without replacing any current
        :param allowed: Remove any keys that are not in this provided list
        """
        extra_update = {
            **(defaults or {}),
            **(self.extra_config or {}),
        }
        if allowed is not None:
            extra_update = {k: v for k, v in extra_update.items() if k in allowed}
        self.extra_config = extra_update


def _get_serializer(serializer: SerializerType) -> Serializer:
    if isinstance(serializer, str):
        try:
            return SERIALIZERS[serializer]()
        except KeyError:
            raise ValueError(
                f"Unknown deserializer option '{serializer}'; "
                f"valid options are {list(SERIALIZERS.keys())}"
            )
    return serializer


def _get_deserializer(deserializer: DeserializerType) -> Deserializer:
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
    A representation of a Kafka topic and its expected data format via
    designated key and value serializers/deserializers.

    Typically created with an `app = quixstreams.app.Application()` instance via
    `app.topic()`, and used by `quixstreams.dataframe.StreamingDataFrame`
    instance.
    """

    def __init__(
        self,
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = BytesDeserializer(),
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = BytesSerializer(),
        config: Optional[TopicConfig] = None,
    ):
        """
        Can specify serialization that should be used when consuming/producing
        to the topic in the form of a string name (i.e. "json" for JSON) or a
        serialization class instance directly, like JSONSerializer().


        Example Snippet:

        ```python
        from quixstreams.dataframe import StreamingDataFrame
        from quixstreams.models import Topic, JSONSerializer

        # Specify an input and output topic for a `StreamingDataFrame` instance,
        # where the output topic requires adjusting the key serializer.
        input_topic = Topic("input-topic", value_deserializer="json")
        output_topic = Topic(
            "output-topic", key_serializer="str", value_serializer=JSONSerializer()
        )
        sdf = StreamingDataFrame(input_topic)
        sdf.to_topic(output_topic)
        ```


        :param name: topic name
        :param value_deserializer: a deserializer type for values
        :param key_deserializer: a deserializer type for keys
        :param value_serializer: a serializer type for values
        :param key_serializer: a serializer type for keys
        :param config: optional topic configs via `TopicConfig` (creation/validation)
        """
        self._name = name
        self._key_serializer = _get_serializer(key_serializer)
        self._key_deserializer = _get_deserializer(key_deserializer)
        self._value_serializer = _get_serializer(value_serializer)
        self._value_deserializer = _get_deserializer(value_deserializer)
        self._config = config

    @property
    def name(self) -> str:
        """
        Topic name
        """
        return self._name

    @property
    def config(self) -> TopicConfig:
        return self._config

    def row_serialize(self, row: Row, key: Optional[Any] = None) -> KafkaMessage:
        """
        Serialize Row to a Kafka message structure
        :param row: Row to serialize
        :param key: message key to serialize, optional. Default - current Row key.
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

        return KafkaMessage(
            key=self._key_serializer(key or row.key, ctx=ctx),
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
        key = self._key_deserializer(value=message.key(), ctx=ctx)

        timestamp_type, timestamp_ms = message.timestamp()
        timestamp = MessageTimestamp.create(
            timestamp_type=timestamp_type, milliseconds=timestamp_ms
        )

        context = MessageContext(
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

        try:
            value = self._value_deserializer(value=message.value(), ctx=ctx)
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
            for item in value:
                if not isinstance(item, dict):
                    raise TypeError(f'Row value must be a dict, but got "{type(item)}"')
                rows.append(Row(value=item, context=context))
            return rows

        if not isinstance(value, dict):
            raise TypeError(f'Row value must be a dict, but got "{type(value)}"')
        return Row(value=value, context=context)

    def serialize(
        self,
        key: Optional[object] = None,
        value: Optional[object] = None,
        headers: Optional[Union[Mapping, MessageHeadersTuples]] = None,
        timestamp_ms: int = None,
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
        # TODO: confirm this is a valid context
        return KafkaMessage(
            key=key,
            value=value,
            headers=headers,
            timestamp=timestamp_ms,
        )

    def deserialize(self, message: ConfluentKafkaMessageProto):
        # TODO: confirm this is a valid context
        ctx = SerializationContext(topic="", headers=message.headers())
        return KafkaMessage(
            key=self._key_deserializer(key, ctx=ctx)
            if (key := message.key())
            else None,
            value=self._value_serializer(value, ctx=ctx)
            if (value := message.value())
            else None,
            headers=message.headers(),
            timestamp=message.timestamp(),
        )

    def __repr__(self):
        return f'<{self.__class__} name="{self._name}"> '


TopicList: TypeAlias = List[Topic]
TopicMap: TypeAlias = Dict[str, Topic]
