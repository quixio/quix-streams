from typing import Any

from streamingdataframes.state.types import DumpsFunc, LoadsFunc
from .exceptions import StateSerializationError

__all__ = (
    "serialize",
    "deserialize",
    "serialize_key",
)


def serialize(value: Any, dumps: DumpsFunc) -> bytes:
    try:
        return dumps(value)
    except Exception as exc:
        raise StateSerializationError(f'Failed to serialize value: "{value}"') from exc


def deserialize(value: bytes, loads: LoadsFunc) -> Any:
    try:
        return loads(value)
    except Exception as exc:
        raise StateSerializationError(
            f'Failed to deserialize value: "{value}"'
        ) from exc


def serialize_key(
    key: Any,
    dumps: DumpsFunc,
    prefix: bytes = b"",
) -> bytes:
    return prefix + serialize(key, dumps=dumps)
