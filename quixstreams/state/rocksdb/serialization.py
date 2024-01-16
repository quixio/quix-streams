import struct
from typing import Any

from quixstreams.state.types import DumpsFunc, LoadsFunc
from .exceptions import StateSerializationError

__all__ = (
    "serialize",
    "deserialize",
    "float_to_double_bytes",
    "float_from_double_bytes",
    "int_to_int64_bytes",
    "int_from_int64_bytes",
)


_float_packer = struct.Struct(">d")
_float_pack = _float_packer.pack
_float_unpack = _float_packer.unpack

_int_packer = struct.Struct(">q")
_int_pack = _int_packer.pack
_int_unpack = _int_packer.unpack


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


def float_to_double_bytes(value: float) -> bytes:
    return _float_pack(value)


def float_from_double_bytes(value: bytes) -> int:
    return _float_unpack(value)[0]


def int_to_int64_bytes(value: int) -> bytes:
    return _int_pack(value)


def int_from_int64_bytes(value: bytes) -> int:
    return _int_unpack(value)[0]
