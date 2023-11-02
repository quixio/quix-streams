from typing import Any

import orjson

from streamingdataframes.state.types import DumpsFunc, LoadsFunc
from .exceptions import StateSerializationError

__all__ = (
    "serialize",
    "deserialize",
    "serialize_key",
    "default_dumps",
    "default_loads",
)

_ORJSON_OPTIONS = orjson.OPT_PASSTHROUGH_DATETIME | orjson.OPT_PASSTHROUGH_DATACLASS


def default_dumps(value: Any) -> bytes:
    return orjson.dumps(value, option=_ORJSON_OPTIONS)


def default_loads(value: bytes) -> Any:
    return orjson.loads(value)


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
