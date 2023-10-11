import json
from typing import Any, Optional

from .exceptions import StateSerializationError
from .types import DumpsFunc, LoadsFunc

__all__ = (
    "serialize",
    "deserialize",
    "serialize_key",
)


def _default_dumps(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _default_loads(value: bytes) -> Any:
    return json.loads(value)


def serialize(value: Any, dumps: Optional[DumpsFunc] = None) -> bytes:
    dumps = dumps or _default_dumps
    try:
        return dumps(value)
    except Exception as exc:
        raise StateSerializationError(f'Failed to serialize value: "{value}"') from exc


def deserialize(value: bytes, loads: Optional[LoadsFunc] = None) -> Any:
    loads = loads or _default_loads
    try:
        return loads(value)
    except Exception as exc:
        raise StateSerializationError(
            f'Failed to deserialize value: "{value}"'
        ) from exc


def serialize_key(
    key: Any, prefix: bytes = b"", dumps: Optional[DumpsFunc] = None
) -> bytes:
    dumps = dumps or _default_dumps
    return prefix + serialize(key, dumps=dumps)
