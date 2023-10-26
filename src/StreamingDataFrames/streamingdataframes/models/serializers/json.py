import json
from typing import Callable, Union, List, Mapping, Optional, Any, Iterable

from .base import Serializer, Deserializer, SerializationContext
from .exceptions import SerializationError

__all__ = ("JSONSerializer", "JSONDeserializer")


class JSONSerializer(Serializer):
    def __init__(
        self,
        dumps: Optional[Callable[[Any, None], Union[str, bytes]]] = None,
        dumps_kwargs: Optional[Mapping] = None,
    ):
        """
        Serializer that returns data in json format.
        :param dumps: a function to serialize objects to json. Default - `json.dumps`
        :param dumps_kwargs: a dict with keyword arguments for `dumps()` function.
        """
        self._dumps = dumps or json.dumps
        self._dumps_kwargs = {"separators": (",", ":"), **(dumps_kwargs or {})}

    def __call__(self, value: Any, ctx: SerializationContext) -> Union[str, bytes]:
        return self._to_json(value)

    def _to_json(self, value: Any):
        try:
            return self._dumps(value, **self._dumps_kwargs)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc


class JSONDeserializer(Deserializer):
    def __init__(
        self,
        column_name: Optional[str] = None,
        loads: Optional[
            Callable[[Union[str, bytes, bytearray], None], Union[List, Mapping]]
        ] = None,
        loads_kwargs: Optional[Mapping] = None,
    ):
        """
        Deserializer that parses data from JSON

        :param column_name: if provided, the deserialized value will be wrapped into
            dictionary with `column_name` as a key.
        :param loads: function to parse json from bytes. Default - `json.loads`.
        :param loads_kwargs: dict with named arguments for `loads` function.
        """
        super().__init__(column_name=column_name)
        self._loads = loads or json.loads
        self._loads_kwargs = loads_kwargs or {}

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping]:
        try:
            deserialized = self._loads(value, **self._loads_kwargs)
            return self._to_dict(deserialized)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc
