"""
The stored representation of one withheld record.

An envelope has to be self-describing, because
`TimestampedPartitionTransaction.get_interval()` returns values without their
store keys: the arrival time and the original event timestamp are unrecoverable
unless they travel inside the value.

Envelopes are serialized by the store's own `dumps`/`loads`, which default to
orjson (`quixstreams/state/rocksdb/options.py`). orjson cannot encode `bytes`,
and a record path that crashes on one is worse than useless - the crash happens
*before* the offset is committed, so redelivery reproduces it forever. Both the
header values and any `bytes` inside the record value are therefore base64-
encoded on the way in and restored on the way out.

The index of which keys currently hold withheld records lives in
`buffer_state.py`; the operator logic that decides when to write or read an
envelope lives in `buffer_operator.py`.
"""

import base64
from typing import Any, Mapping, Optional, Union

__all__ = (
    "ENVELOPE_BYTES",
    "ENVELOPE_RECEIVED",
    "ENVELOPE_TIMESTAMP",
    "ENVELOPE_VALUE",
    "decode_headers",
    "emit_tuple",
    "encode_envelope",
    "envelope_value",
)

# Envelope field names. Kept to one character each: every withheld record pays
# for them twice, once in RocksDB and once in the changelog topic.
ENVELOPE_VALUE = "v"
ENVELOPE_TIMESTAMP = "t"
ENVELOPE_RECEIVED = "r"
ENVELOPE_HEADERS = "h"
ENVELOPE_HEADERS_MAPPING = "m"
ENVELOPE_BYTES = "b"

# Header value encodings used inside the envelope.
_HEADER_BYTES = "b"
_HEADER_STR = "s"
_HEADER_NONE = "n"

# One step of a path into the record value: a dict key or a list index.
_Step = Union[str, int]


def encode_envelope(
    value: Any,
    timestamp: int,
    receive_ms: int,
    headers: Any,
) -> dict[str, Any]:
    """
    Build the stored representation of one withheld record.

    :param value: The record value *after* `lookup.join()` — so already resolved
        through every field's `missing()`. That is what makes emitting a
        timed-out record free: the stored value is already the right answer.
    :param timestamp: The original event timestamp, in milliseconds. Emitted
        verbatim on both the release and the timeout path.
    :param receive_ms: Wall-clock arrival time, in milliseconds. Drives the
        deadline and the pending index's earliest-arrival bound.
    :param headers: The record headers as received.
    :return: An orjson-serializable dict.
    """
    encoded_headers, is_mapping = _encode_headers(headers)
    stored_value, byte_paths = _extract_bytes(value)
    return {
        ENVELOPE_VALUE: stored_value,
        ENVELOPE_TIMESTAMP: timestamp,
        ENVELOPE_RECEIVED: receive_ms,
        ENVELOPE_HEADERS: encoded_headers,
        ENVELOPE_HEADERS_MAPPING: is_mapping,
        ENVELOPE_BYTES: byte_paths,
    }


def envelope_value(envelope: Mapping[str, Any]) -> Any:
    """
    Restore a withheld record's value from its envelope.

    The `bytes` field is read with `get()` so that an envelope recovered from a
    changelog written before it existed reads back as "nothing to restore"
    rather than raising.

    :param envelope: An envelope produced by `encode_envelope()`.
    :return: The value as it was when the record was withheld, with any `bytes`
        put back where they were.
    """
    return _restore_bytes(envelope[ENVELOPE_VALUE], envelope.get(ENVELOPE_BYTES))


def emit_tuple(envelope: Mapping[str, Any], key: Any) -> tuple[Any, Any, int, Any]:
    """
    Build the downstream tuple for one stored record.

    :param envelope: An envelope produced by `encode_envelope()`.
    :param key: The message key to emit under.
    :return: A `(value, key, timestamp, headers)` tuple carrying the stored
        record's own event timestamp and headers rather than those of whatever
        record happened to release it.
    """
    return (
        envelope_value(envelope),
        key,
        envelope[ENVELOPE_TIMESTAMP],
        decode_headers(envelope),
    )


def decode_headers(envelope: Mapping[str, Any]) -> Any:
    """
    Restore a withheld record's headers from its envelope.

    :param envelope: An envelope produced by `encode_envelope()`.
    :return: The headers in their original shape - `None`, a list of
        `(name, value)` tuples, or a mapping.
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
    """
    Encode headers into an orjson-safe form, preserving duplicates and order.

    At runtime headers reach a dataframe callback as `Optional[List[Tuple[str,
    bytes]]]` (`Row.headers`), but the public `Headers` type also allows a
    mapping, so the container shape is recorded alongside the items.

    :param headers: The headers as received.
    :return: A `(items, is_mapping)` pair, where `items` is `None` for headers
        that were `None`.
    """
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
        if isinstance(value, bytes):
            encoded.append([name, _HEADER_BYTES, base64.b64encode(value).decode()])
        elif value is None:
            encoded.append([name, _HEADER_NONE, None])
        else:
            encoded.append([name, _HEADER_STR, value])
    return encoded, is_mapping


def _extract_bytes(value: Any) -> tuple[Any, Optional[list[list[Any]]]]:
    """
    Split a record value into an orjson-safe body and its `bytes` leaves.

    The leaves are addressed by path rather than by an in-band marker so that no
    shape a user can put in a record value can be mistaken for one: a
    `lookup.bytes_field()` resolving to real binary content (a certificate, say)
    has to survive buffering, and the record value is otherwise arbitrary.

    A value with no `bytes` in it is returned unchanged and uncopied, so the
    common case costs one read-only walk.

    :param value: The record value, already joined.
    :return: A `(body, paths)` pair. `paths` is `None` when there was nothing to
        extract, otherwise a list of `[path, base64]` entries where `path` is a
        list of dict keys and list indices.
    """
    if not _contains_bytes(value):
        return value, None

    found: list[list[Any]] = []
    body = _replace_bytes(value, [], found)
    return body, found


def _contains_bytes(value: Any) -> bool:
    """
    Report whether a value has any `bytes` leaf.

    :param value: Any part of a record value.
    :return: `True` if a `bytes` object is reachable from it.
    """
    if isinstance(value, bytes):
        return True
    if isinstance(value, dict):
        return any(_contains_bytes(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_bytes(item) for item in value)
    return False


def _replace_bytes(value: Any, path: list[_Step], found: list[list[Any]]) -> Any:
    """
    Copy a value with every `bytes` leaf replaced by `None`, recording its path.

    :param value: Any part of a record value.
    :param path: The path walked so far, mutated in place during the walk.
    :param found: The accumulator of `[path, base64]` entries.
    :return: The orjson-safe copy.
    """
    if isinstance(value, bytes):
        found.append([list(path), base64.b64encode(value).decode()])
        return None

    if isinstance(value, dict):
        copied: dict[Any, Any] = {}
        for name, item in value.items():
            path.append(name)
            copied[name] = _replace_bytes(item, path, found)
            path.pop()
        return copied

    if isinstance(value, list):
        items: list[Any] = []
        for index, item in enumerate(value):
            path.append(index)
            items.append(_replace_bytes(item, path, found))
            path.pop()
        return items

    return value


def _restore_bytes(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    """
    Put the `bytes` leaves recorded by `_extract_bytes()` back into a value.

    :param value: The body read out of the envelope.
    :param paths: The `[path, base64]` entries, or `None` if there were none.
    :return: The value with its `bytes` restored.
    """
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
