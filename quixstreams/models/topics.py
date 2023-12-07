import logging
import warnings
from copy import deepcopy
from typing import Union, List, Dict, Optional, Any, Mapping
from typing_extensions import Self

from confluent_kafka.admin import NewTopic  # type: ignore

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
    MessageKey,
    MessageValue,
    MessageHeadersTuples,
)

logger = logging.getLogger(__name__)

__all__ = ("Topic", "TopicKafkaConfigs")


class TopicKafkaConfigs:
    _quix_optionals_defaults = {
        "retention.ms": f"{10080 * 60000}",  # minutes converted to ms
        "retention.bytes": "52428800",
    }

    def __init__(
        self,
        name: Optional[str] = None,  # Required when not created by a Quix App.
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        optionals: Optional[dict] = None,
        is_quix_topic: bool = False,
    ):
        # TODO: make as properties
        self._is_quix_topic = is_quix_topic
        self.name = name
        self.num_partitions = num_partitions or 2
        self.replication_factor = replication_factor or (2 if is_quix_topic else 1)
        self._optionals = optionals or {}

    @property
    def is_quix_topic(self) -> bool:
        return self._is_quix_topic

    @property
    def optionals(self) -> Dict[str, str]:
        return self._optionals

    def _set_quix_optional_defaults(self):
        for k in self._quix_optionals_defaults:
            self._optionals.setdefault(k, self._quix_optionals_defaults[k])

    def _filter_quix_optionals(self):
        invalid_optionals = [
            k for k in self.optionals if k not in self._quix_optionals_defaults
        ]
        self._optionals = {
            k: v
            for k, v in self.optionals.items()
            if k in self._quix_optionals_defaults
        }
        if invalid_optionals:
            warnings.warn(
                f"There are optionals provided that are not supported by "
                f"the Quix API and thus will be removed from the optionals:"
                f"{invalid_optionals}"
            )

    def _as_quix_create(self) -> Self:
        new = deepcopy(self)
        new._filter_quix_optionals()
        new._set_quix_optional_defaults()
        return new

    def _as_create(self) -> NewTopic:
        if self.is_quix_topic:
            raise Exception(
                "This option is not supported for Quix topics; use the"
                "Quix API to generate topics on the Quix platform"
            )
        return NewTopic(
            topic=self.name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            config=self.optionals,
        )

    def as_creation_config(self) -> Union[Self, NewTopic]:
        return self._as_quix_create() if self.is_quix_topic else self._as_create()


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
        kafka_configs: Optional[TopicKafkaConfigs] = None,
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
        """
        self._name = name
        self._key_serializer = _get_serializer(key_serializer)
        self._key_deserializer = _get_deserializer(key_deserializer)
        self._value_serializer = _get_serializer(value_serializer)
        self._value_deserializer = _get_deserializer(value_deserializer)
        self._kafka_configs = kafka_configs

    @property
    def name(self) -> str:
        """
        Topic name
        """
        return self._name

    @property
    def kafka_configs(self) -> TopicKafkaConfigs:
        return self._kafka_configs

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
        key: Optional[MessageKey] = None,
        value: Optional[MessageValue] = None,
        headers: Optional[Union[Mapping, MessageHeadersTuples]] = None,
        timestamp_ms: int = None,
    ) -> KafkaMessage:
        # TODO: Implement SerDes for raw messages (also to produce primitive values)
        raise NotImplementedError

    def deserialize(self, message: ConfluentKafkaMessageProto):
        # TODO: Implement SerDes for raw messages
        raise NotImplementedError

    def __repr__(self):
        return f'<{self.__class__} name="{self._name}"> '
