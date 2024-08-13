from typing import Dict, Iterable, Mapping, Union

from google.protobuf.json_format import MessageToDict, ParseDict, ParseError
from google.protobuf.message import DecodeError, EncodeError, Message

from .base import Deserializer, SerializationContext, Serializer
from .exceptions import SerializationError

__all__ = ("ProtobufSerializer", "ProtobufDeserializer")


class ProtobufSerializer(Serializer):
    def __init__(
        self,
        msg_type: Message,
        deterministic: bool = False,
        ignore_unknown_fields: bool = False,
    ):
        """
        Serializer that returns data in protobuf format.

        Serialisation from a python dictionary can have a significant performance impact. An alternative is to pass the serializer an object of the `msg_type` class.

        :param msg_type: protobuf message class.
        :param deterministic: If true, requests deterministic serialization of the protobuf, with predictable ordering of map keys
            Default - `False`
        :param ignore_unknown_fields: If True, do not raise errors for unknown fields.
            Default - `False`
        """
        super().__init__()
        self._msg_type = msg_type

        self._deterministic = deterministic
        self._ignore_unknown_fields = ignore_unknown_fields

    def __call__(
        self, value: Union[Dict, Message], ctx: SerializationContext
    ) -> Union[str, bytes]:

        try:
            if isinstance(value, self._msg_type):
                return value.SerializeToString(deterministic=self._deterministic)

            msg = self._msg_type()
            return ParseDict(
                value, msg, ignore_unknown_fields=self._ignore_unknown_fields
            ).SerializeToString(deterministic=self._deterministic)
        except (EncodeError, ParseError) as exc:
            raise SerializationError(str(exc)) from exc


class ProtobufDeserializer(Deserializer):
    def __init__(
        self,
        msg_type: Message,
        use_integers_for_enums: bool = False,
        preserving_proto_field_name: bool = False,
        to_dict: bool = True,
    ):
        """
        Deserializer that parses protobuf data into a dictionary suitable for a StreamingDataframe.

        Deserialisation to a python dictionary can have a significant performance impact. You can disable this behavior using `to_dict`, in that case the protobuf message will be used as the StreamingDataframe row value.

        :param msg_type: protobuf message class.
        :param use_integers_for_enums: If true, use integers instead of enum names.
            Default - `False`
        :param preserving_proto_field_name: If True, use the original proto field names as
            defined in the .proto file. If False, convert the field names to
            lowerCamelCase.
            Default - `False`
        :param to_dict: If false, return the protobuf message instead of a dict.
            Default - `True`
        """
        super().__init__()
        self._msg_type = msg_type
        self._to_dict = to_dict

        self._use_integers_for_enums = use_integers_for_enums
        self._preserving_proto_field_name = preserving_proto_field_name

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping, Message]:
        msg = self._msg_type()

        try:
            msg.ParseFromString(value)
        except DecodeError as exc:
            raise SerializationError(str(exc)) from exc

        if not self._to_dict:
            return msg

        return MessageToDict(
            msg,
            always_print_fields_with_no_presence=True,
            use_integers_for_enums=self._use_integers_for_enums,
            preserving_proto_field_name=self._preserving_proto_field_name,
        )
