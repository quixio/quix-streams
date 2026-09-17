"""
The stored representation of one withheld record.

`get_interval()` returns values without their store keys, so everything needed
to emit a record later - arrival time, event timestamp, origin topic and offset
- travels inside the value.

Envelopes go through the store's own serializer, orjson by default, which
rejects shapes a record value may legitimately hold. The envelope lifts those
out on the way in and puts them back on the way out, addressing them **by path**
so that no shape a user can put in a value is mistaken for a marker:

- `bytes`, `bytearray` and `memoryview` are base64-encoded, recorded in
  `ENVELOPE_BYTES`. Header values are base64-encoded in place instead.
- `tuple`, `set` and `frozenset` become JSON arrays with their kind recorded in
  `ENVELOPE_CONTAINERS`. orjson encodes a tuple as an array by itself, and would
  return it as a `list`.
- `datetime`, `date` and `Decimal` become strings with their kind recorded in
  `ENVELOPE_SCALARS`: `isoformat()` for the first two, `str()` for `Decimal`.
  A `PostgresLookup` field writes these into the value from a `timestamptz` or
  a `numeric` column, after the point upstream normalization can reach.

Accepted losses: `bytearray` and `memoryview` come back as `bytes`, a subclass
(a `NamedTuple`, an `OrderedDict`) comes back as its builtin base, and a
`datetime` in a named zone comes back on the fixed offset that zone was at.

Out of reach of any encoding here: arbitrary objects, non-`str` dict keys,
integers outside 64 bits, reference cycles.
"""

import base64
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Mapping, NamedTuple, Optional, Union

__all__ = (
    "ENVELOPE_BYTES",
    "ENVELOPE_CONTAINERS",
    "ENVELOPE_HEADERS",
    "ENVELOPE_OFFSET",
    "ENVELOPE_RECEIVED",
    "ENVELOPE_SCALARS",
    "ENVELOPE_TIMESTAMP",
    "ENVELOPE_TOPIC",
    "ENVELOPE_VALUE",
    "NO_OFFSET",
    "Emission",
    "decode_headers",
    "emit_tuple",
    "encode_envelope",
    "envelope_value",
)

# Envelope field names are one byte each: every withheld record pays for them,
# in the store and in the changelog.
ENVELOPE_VALUE = "v"
ENVELOPE_TIMESTAMP = "t"
ENVELOPE_RECEIVED = "r"
ENVELOPE_HEADERS = "h"
ENVELOPE_HEADERS_MAPPING = "m"
ENVELOPE_BYTES = "b"
ENVELOPE_CONTAINERS = "c"
ENVELOPE_SCALARS = "s"
ENVELOPE_TOPIC = "n"
ENVELOPE_OFFSET = "o"

NO_OFFSET = -1

_HEADER_BYTES = "b"
_HEADER_STR = "s"
_HEADER_NONE = "n"

_CONTAINER_TUPLE = "t"
_CONTAINER_SET = "s"
_CONTAINER_FROZENSET = "f"

_SCALAR_DATETIME = "dt"
_SCALAR_DATE = "d"
_SCALAR_DECIMAL = "n"

_SCALAR_DECODERS: dict[str, Callable[[str], Any]] = {
    _SCALAR_DATETIME: datetime.fromisoformat,
    _SCALAR_DATE: date.fromisoformat,
    _SCALAR_DECIMAL: Decimal,
}

_BINARY = (bytes, bytearray, memoryview)

_Step = Union[str, int]


class Emission(NamedTuple):
    """One record on its way downstream, with the origin to emit it under."""

    value: Any
    key: Any
    timestamp: int
    headers: Any
    topic: Optional[str]
    offset: int


def encode_envelope(
    value: Any,
    timestamp: int,
    receive_ms: int,
    headers: Any,
    topic: Optional[str] = None,
    offset: int = NO_OFFSET,
) -> dict[str, Any]:
    """
    Build the stored form of one record.

    :param value: The record value, after `lookup.join()` has run on it.
    :param timestamp: The record's event timestamp.
    :param receive_ms: Arrival time, the store key this is written under.
    :param headers: The record headers, mapping or pairs.
    :param topic: The topic the record came from.
    :param offset: The record's offset.
    :return: The envelope, not yet offered to the store's serializer.
    """
    encoded_headers, is_mapping = _encode_headers(headers)
    stored_value, byte_paths, container_paths, scalar_paths = _lift_value(value)
    return {
        ENVELOPE_VALUE: stored_value,
        ENVELOPE_TIMESTAMP: timestamp,
        ENVELOPE_RECEIVED: receive_ms,
        ENVELOPE_HEADERS: encoded_headers,
        ENVELOPE_HEADERS_MAPPING: is_mapping,
        ENVELOPE_BYTES: byte_paths,
        ENVELOPE_CONTAINERS: container_paths,
        ENVELOPE_SCALARS: scalar_paths,
        ENVELOPE_TOPIC: topic,
        ENVELOPE_OFFSET: offset,
    }


def envelope_value(envelope: Mapping[str, Any]) -> Any:
    """
    :param envelope: A stored envelope.
    :return: The record value with its lifted shapes restored.
    """
    value = _restore_bytes(envelope[ENVELOPE_VALUE], envelope.get(ENVELOPE_BYTES))
    value = _restore_scalars(value, envelope.get(ENVELOPE_SCALARS))
    return _restore_containers(value, envelope.get(ENVELOPE_CONTAINERS))


def emit_tuple(envelope: Mapping[str, Any], key: Any) -> Emission:
    """
    :param envelope: A stored envelope.
    :param key: The message key its prefix decodes to.
    :return: The emission to hand downstream, carrying its own origin.
    """
    return Emission(
        envelope_value(envelope),
        key,
        envelope[ENVELOPE_TIMESTAMP],
        decode_headers(envelope),
        envelope.get(ENVELOPE_TOPIC),
        envelope.get(ENVELOPE_OFFSET, NO_OFFSET),
    )


def decode_headers(envelope: Mapping[str, Any]) -> Any:
    """
    :param envelope: A stored envelope.
    :return: The headers in the shape they were received in, mapping or pairs.
    """
    encoded = envelope[ENVELOPE_HEADERS]
    if encoded is None:
        return None

    items: list[tuple[Any, Any]] = []
    for name, kind, payload in encoded:
        if kind == _HEADER_BYTES:
            items.append((name, base64.b64decode(payload)))
        elif kind == _HEADER_NONE:
            items.append((name, None))
        else:
            items.append((name, payload))

    if envelope[ENVELOPE_HEADERS_MAPPING]:
        return dict(items)
    return items


def _encode_headers(headers: Any) -> tuple[Optional[list[list[Any]]], bool]:
    if headers is None:
        return None, False

    if isinstance(headers, Mapping):
        pairs: Any = headers.items()
        is_mapping = True
    else:
        pairs = headers
        is_mapping = False

    encoded: list[list[Any]] = []
    for name, value in pairs:
        if isinstance(value, _BINARY):
            encoded.append(
                [name, _HEADER_BYTES, base64.b64encode(bytes(value)).decode()]
            )
        elif value is None:
            encoded.append([name, _HEADER_NONE, None])
        else:
            encoded.append([name, _HEADER_STR, value])
    return encoded, is_mapping


class _Lifted(NamedTuple):
    """The paths lifted out of one value, one list per kind of lift."""

    byte_paths: list[list[Any]]
    container_paths: list[list[Any]]
    scalar_paths: list[list[Any]]


def _lift_value(
    value: Any,
) -> tuple[
    Any,
    Optional[list[list[Any]]],
    Optional[list[list[Any]]],
    Optional[list[list[Any]]],
]:
    if not _needs_lift(value):
        return value, None, None, None

    lifted = _Lifted([], [], [])
    body = _strip(value, [], lifted)
    return (
        body,
        lifted.byte_paths or None,
        lifted.container_paths or None,
        lifted.scalar_paths or None,
    )


def _needs_lift(value: Any) -> bool:
    if (
        isinstance(value, _BINARY)
        or _container_kind(value) is not None
        or _scalar_kind(value) is not None
    ):
        return True
    if isinstance(value, dict):
        return any(_needs_lift(item) for item in value.values())
    if isinstance(value, list):
        return any(_needs_lift(item) for item in value)
    return False


def _container_kind(value: Any) -> Optional[str]:
    if isinstance(value, tuple):
        return _CONTAINER_TUPLE
    if isinstance(value, frozenset):
        return _CONTAINER_FROZENSET
    if isinstance(value, set):
        return _CONTAINER_SET
    return None


def _scalar_kind(value: Any) -> Optional[str]:
    # `datetime` first: it is a subclass of `date`.
    if isinstance(value, datetime):
        return _SCALAR_DATETIME
    if isinstance(value, date):
        return _SCALAR_DATE
    if isinstance(value, Decimal):
        return _SCALAR_DECIMAL
    return None


def _encode_scalar(value: Any, kind: str) -> str:
    if kind == _SCALAR_DECIMAL:
        return str(value)
    return value.isoformat()


def _strip(value: Any, path: list[_Step], lifted: _Lifted) -> Any:
    if isinstance(value, _BINARY):
        lifted.byte_paths.append([list(path), base64.b64encode(bytes(value)).decode()])
        return None

    scalar = _scalar_kind(value)
    if scalar is not None:
        lifted.scalar_paths.append([list(path), scalar])
        return _encode_scalar(value, scalar)

    if isinstance(value, dict):
        copied: dict[Any, Any] = {}
        for name, item in value.items():
            path.append(name)
            copied[name] = _strip(item, path, lifted)
            path.pop()
        return copied

    kind = _container_kind(value)
    if kind is not None or isinstance(value, list):
        if kind is not None:
            lifted.container_paths.append([list(path), kind])
        items: list[Any] = []
        for index, item in enumerate(value):
            path.append(index)
            items.append(_strip(item, path, lifted))
            path.pop()
        return items

    return value


def _restore_bytes(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    if not paths:
        return value

    for path, payload in paths:
        decoded = base64.b64decode(payload)
        if not path:
            return decoded
        target = value
        for step in path[:-1]:
            target = target[step]
        target[path[-1]] = decoded
    return value


def _restore_scalars(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    if not paths:
        return value

    for path, kind in paths:
        decode = _SCALAR_DECODERS[kind]
        if not path:
            return decode(value)
        target = value
        for step in path[:-1]:
            target = target[step]
        target[path[-1]] = decode(target[path[-1]])
    return value


def _restore_containers(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    if not paths:
        return value

    # Deepest paths first: rebuilding an outer tuple would freeze the inner
    # lists before they could be rebuilt in place.
    for path, kind in sorted(paths, key=lambda entry: len(entry[0]), reverse=True):
        if not path:
            return _rebuild_container(value, kind)
        parent = value
        for step in path[:-1]:
            parent = parent[step]
        parent[path[-1]] = _rebuild_container(parent[path[-1]], kind)
    return value


def _rebuild_container(items: Any, kind: str) -> Any:
    if kind == _CONTAINER_TUPLE:
        return tuple(items)
    if kind == _CONTAINER_SET:
        return set(items)
    if kind == _CONTAINER_FROZENSET:
        return frozenset(items)
    return items
