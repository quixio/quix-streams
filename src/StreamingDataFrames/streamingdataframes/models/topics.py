import logging
from typing import Union, List, Mapping, Optional

from .messages import KafkaMessage
from .rows import Row
from .serializers import (
    Deserializer,
    SerializationContext,
    Serializer,
    DeserializerIsNotProvidedError,
    SerializerIsNotProvidedError,
    BytesSerializer,
    BytesDeserializer,
    IgnoreMessage,
)
from .timestamps import MessageTimestamp
from .types import (
    ConfluentKafkaMessageProto,
    MessageKey,
    MessageValue,
    MessageHeadersTuples,
)

logger = logging.getLogger(__name__)


class Topic:
    def __init__(
        self,
        name: str,
        real_name: str = None,
        value_deserializer: Optional[Deserializer] = None,
        key_deserializer: Optional[Deserializer] = BytesDeserializer(),
        value_serializer: Optional[Serializer] = None,
        key_serializer: Optional[Serializer] = BytesSerializer(),
    ):
        """
        A definition of Topic.

        :param name: topic name
        :param real_name: the actual topic name in the cluster, else uses name
        :param value_deserializer: a deserializer for values
        :param key_deserializer: a deserializer for keys
        :param value_serializer: a serializer for values
        :param key_serializer: a serializer for keys
        """
        self._name = name
        self._real_name = real_name
        self._key_serializer = key_serializer
        self._key_deserializer = key_deserializer
        self._value_serializer = value_serializer
        self._value_deserializer = value_deserializer

    @property
    def name(self) -> str:
        """
        Topic name
        """
        return self._name

    @property
    def real_name(self) -> str:
        return self._real_name or self._name

    @real_name.setter
    def real_name(self, value: str):
        self._real_name = value

    def row_serialize(self, row: Row) -> KafkaMessage:
        """
        Serialize Row to a Kafka message structure
        :param row: Row to serialize
        :return: KafkaMessage object with serialized values
        """
        ctx = SerializationContext(topic=row.topic, headers=row.headers)
        if self._key_serializer is None:
            raise SerializerIsNotProvidedError(
                f'Key serializer is not provided for topic "{self.name}"'
            )
        if self._value_serializer is None:
            raise SerializerIsNotProvidedError(
                f'Value serializer is not provided for topic "{self.name}"'
            )

        return KafkaMessage(
            key=self._key_serializer(row.key, ctx=ctx),
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

        row_kwargs = {
            "key": key,
            "headers": headers,
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "size": len(message),
            "timestamp": timestamp,
            "latency": message.latency(),
            "leader_epoch": message.leader_epoch(),
        }

        try:
            value = self._value_deserializer(value=message.value(), ctx=ctx)
        except IgnoreMessage:
            # Ignore message completely if the deserializer raised IgnoreValueError.
            logger.debug(
                "Ignore incoming message",
                extra={
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                },
            )
            return

        if self._value_deserializer.split_values:
            # The expected value from this serializer is Iterable and each item
            # should be processed as a separate message
            rows = []
            for item in value:
                if not isinstance(item, dict):
                    raise TypeError(f'Row value must be a dict, but got "{type(item)}"')
                rows.append(Row(value=item, **row_kwargs))
            return rows

        if not isinstance(value, dict):
            raise TypeError(f'Row value must be a dict, but got "{type(value)}"')
        return Row(value=value, **row_kwargs)

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
