from typing import Callable, Union, Mapping, Optional, Any, Iterable

from quixstreams.utils.json import (
    dumps as default_dumps,
    loads as default_loads,
)
from .base import Serializer, Deserializer, SerializationContext
from .exceptions import SerializationError

__all__ = ("JSONSerializer", "JSONDeserializer")


class JSONSerializer(Serializer):
    def __init__(
        self,
        dumps: Callable[[Any], Union[str, bytes]] = default_dumps,
    ):
        """
        Serializer that returns data in json format.
        :param dumps: a function to serialize objects to json.
            Default - :py:func:`quixstreams.utils.json.dumps`
        """
        self._dumps = dumps

    def __call__(self, value: Any, ctx: SerializationContext) -> Union[str, bytes]:
        return self._to_json(value)

    def _to_json(self, value: Any):
        try:
            return self._dumps(value)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc


class JSONDeserializer(Deserializer):
    def __init__(
        self,
        column_name: Optional[str] = None,
        loads: Callable[[Union[bytes, bytearray]], Any] = default_loads,
    ):
        """
        Deserializer that parses data from JSON

        :param column_name: if provided, the deserialized value will be wrapped into
            dictionary with `column_name` as a key.
        :param loads: function to parse json from bytes.
            Default - :py:func:`quixstreams.utils.json.loads`.
        """
        super().__init__(column_name=column_name)
        self._loads = loads

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping]:
        try:
            deserialized = self._loads(value)
            return self._to_dict(deserialized)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc
