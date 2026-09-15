"""
The stored representation of one withheld record.

An envelope has to be self-describing, because
`TimestampedPartitionTransaction.get_interval()` returns values without their
store keys: the arrival time and the original event timestamp are unrecoverable
unless they travel inside the value. The originating topic and offset travel
with it for the same reason, one step further removed - the deadline tick
(`buffer_tick.py`) emits a record with no input record in hand and has to
rebuild its `MessageContext` from somewhere.

Envelopes are serialized by the store's own `dumps`/`loads`, which default to
orjson (`quixstreams/state/rocksdb/options.py`). A record path that crashes on a
value orjson will not take is worse than useless - the crash happens *before* the
offset is committed, so redelivery reproduces it forever, on the same record.

So the envelope **lifts** the shapes orjson rejects out of the record value on
the way in and puts them back on the way out:

- `bytes`, `bytearray` and `memoryview` (orjson rejects all three) are
  base64-encoded and recorded by path. Header values are base64-encoded in
  place, by the same reasoning.
- `tuple`, `set` and `frozenset` are stored as JSON arrays with their type
  recorded by path. The tag is not decoration in the tuple's case: orjson *does*
  encode a tuple, as an array, and it would come back a `list` - a silent type
  change in a value a user put there deliberately.

Two losses are accepted rather than encoded: `bytearray` and `memoryview` come
back as `bytes`, and a subclass (a `NamedTuple`, an `OrderedDict`) comes back as
its builtin base. Recording the exact class would mean importing and constructing
it at restore time, on the record path, from data that has been through a
changelog.

What is still not survivable, because no encoding here can make it so: a value
holding an arbitrary object (a `datetime`, a `Decimal`, a custom class), a dict
with non-`str` keys, an integer outside 64 bits, or a reference cycle. Each of
those raises inside `orjson.dumps` exactly as a `bytes` leaf used to, and each is
a property of every orjson-backed store in the SDK rather than of this buffer.
Filed as `dev-planning/lookup-deadline-tick/open-points.md` §4, because what a
buffer should *do* with a record it cannot store is a spec question.

Both lifts address their targets **by path** rather than with an in-band marker,
so no shape a user can put in a record value can be mistaken for one.

The index of which keys currently hold withheld records lives in
`buffer_state.py`; the operator logic that decides when to write or read an
envelope lives in `buffer_operator.py`.
"""

import base64
from typing import Any, Mapping, NamedTuple, Optional, Union

__all__ = (
    "ENVELOPE_BYTES",
    "ENVELOPE_CONTAINERS",
    "ENVELOPE_OFFSET",
    "ENVELOPE_RECEIVED",
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

# Envelope field names. Kept to one character each: every withheld record pays
# for them twice, once in RocksDB and once in the changelog topic.
ENVELOPE_VALUE = "v"
ENVELOPE_TIMESTAMP = "t"
ENVELOPE_RECEIVED = "r"
ENVELOPE_HEADERS = "h"
ENVELOPE_HEADERS_MAPPING = "m"
ENVELOPE_BYTES = "b"
ENVELOPE_CONTAINERS = "c"
# The originating topic name and offset. They exist only so the deadline tick
# can rebuild a truthful `MessageContext` for a record it emits with no input
# record in hand (`buffer_tick.py`). Caching the last-seen context per
# partition instead would not survive the case the tick exists for: a cold
# process that recovers a buffer from the changelog and then sees no traffic
# has no last-seen context.
ENVELOPE_TOPIC = "n"
ENVELOPE_OFFSET = "o"

# Stand-in offset for an envelope written before `ENVELOPE_OFFSET` existed.
# Matches librdkafka's "unset" convention and is what `MessageContext.offset`
# reports for such a record.
NO_OFFSET = -1

# Header value encodings used inside the envelope.
_HEADER_BYTES = "b"
_HEADER_STR = "s"
_HEADER_NONE = "n"

# Container types that are stored as a JSON array and rebuilt on the way out.
_CONTAINER_TUPLE = "t"
_CONTAINER_SET = "s"
_CONTAINER_FROZENSET = "f"

# Binary types that are stored as base64. `bytearray` and `memoryview` are
# coerced to `bytes` and come back as `bytes`.
_BINARY = (bytes, bytearray, memoryview)

# One step of a path into the record value: a dict key or a sequence index.
_Step = Union[str, int]


class Emission(NamedTuple):
    """
    One record on its way downstream, as the buffer hands it over.

    The first four fields are the `(value, key, timestamp, headers)` a composed
    `Stream` executor takes. `topic` and `offset` are the record's **originals**,
    carried alongside rather than inside so the deadline tick can rebuild a
    `MessageContext` for a record emitted with no input record in hand. They are
    unused on the record path, which keeps emitting under the arriving record's
    own context.

    `topic` is `None` for an envelope written before `ENVELOPE_TOPIC` existed;
    the tick substitutes the dataframe's own topic name.
    """

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
    Build the stored representation of one withheld record.

    :param value: The record value *after* `lookup.join()` — so already resolved
        through every field's `missing()`. That is what makes emitting a
        timed-out record free: the stored value is already the right answer.
    :param timestamp: The original event timestamp, in milliseconds. Emitted
        verbatim on both the release and the timeout path.
    :param receive_ms: Wall-clock arrival time, in milliseconds. Drives the
        deadline and the pending index's earliest-arrival bound.
    :param headers: The record headers as received.
    :param topic: The name of the topic the record arrived on.
    :param offset: The record's offset on that topic-partition.
    :return: An orjson-serializable dict.
    """
    encoded_headers, is_mapping = _encode_headers(headers)
    stored_value, byte_paths, container_paths = _lift_value(value)
    return {
        ENVELOPE_VALUE: stored_value,
        ENVELOPE_TIMESTAMP: timestamp,
        ENVELOPE_RECEIVED: receive_ms,
        ENVELOPE_HEADERS: encoded_headers,
        ENVELOPE_HEADERS_MAPPING: is_mapping,
        ENVELOPE_BYTES: byte_paths,
        ENVELOPE_CONTAINERS: container_paths,
        ENVELOPE_TOPIC: topic,
        ENVELOPE_OFFSET: offset,
    }


def envelope_value(envelope: Mapping[str, Any]) -> Any:
    """
    Restore a withheld record's value from its envelope.

    Both lift fields are read with `get()` so that an envelope recovered from a
    changelog written before either existed reads back as "nothing to restore"
    rather than raising.

    The order is fixed: the binary leaves go back first, while every container
    on their path is still the mutable list the store returned, and the
    containers are rebuilt afterwards.

    :param envelope: An envelope produced by `encode_envelope()`.
    :return: The value as it was when the record was withheld, with its binary
        leaves and container types put back.
    """
    value = _restore_bytes(envelope[ENVELOPE_VALUE], envelope.get(ENVELOPE_BYTES))
    return _restore_containers(value, envelope.get(ENVELOPE_CONTAINERS))


def emit_tuple(envelope: Mapping[str, Any], key: Any) -> Emission:
    """
    Build the downstream emission for one stored record.

    The topic and offset are read with `get()` so that an envelope recovered
    from a changelog written before those fields existed reads back as "unknown
    origin" rather than raising.

    :param envelope: An envelope produced by `encode_envelope()`.
    :param key: The message key to emit under.
    :return: An `Emission` carrying the stored record's own event timestamp,
        headers and origin rather than those of whatever record happened to
        release it.
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

    A value is base64-encoded if it is binary at all, not only if it is exactly
    `bytes`: `HeadersValue` is `str | bytes`, but nothing enforces that on a
    header a `sdf.apply()` has just written, and the branch this would otherwise
    fall through to stores the object verbatim for orjson to reject.

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
        if isinstance(value, _BINARY):
            encoded.append(
                [name, _HEADER_BYTES, base64.b64encode(bytes(value)).decode()]
            )
        elif value is None:
            encoded.append([name, _HEADER_NONE, None])
        else:
            encoded.append([name, _HEADER_STR, value])
    return encoded, is_mapping


def _lift_value(
    value: Any,
) -> tuple[Any, Optional[list[list[Any]]], Optional[list[list[Any]]]]:
    """
    Split a record value into an orjson-safe body and the two path lists that
    describe what had to be taken out of it.

    Both are addressed by path rather than by an in-band marker so that no shape
    a user can put in a record value can be mistaken for one: a
    `lookup.bytes_field()` resolving to real binary content (a certificate, say)
    has to survive buffering, and the record value is otherwise arbitrary.

    A value orjson can already take verbatim is returned unchanged and uncopied,
    so the common case costs one read-only walk and nothing else.

    :param value: The record value, already joined.
    :return: A `(body, byte_paths, container_paths)` triple. Each path list is
        `None` when there was nothing of that kind. `byte_paths` holds
        `[path, base64]` entries, `container_paths` holds `[path, kind]` entries,
        and a `path` is a list of dict keys and sequence indices.
    """
    if not _needs_lift(value):
        return value, None, None

    byte_paths: list[list[Any]] = []
    container_paths: list[list[Any]] = []
    body = _strip(value, [], byte_paths, container_paths)
    return body, byte_paths or None, container_paths or None


def _needs_lift(value: Any) -> bool:
    """
    Report whether a value holds anything the envelope has to lift out of it.

    :param value: Any part of a record value.
    :return: `True` if a binary leaf, a `tuple`, a `set` or a `frozenset` is
        reachable from it.
    """
    if isinstance(value, _BINARY) or _container_kind(value) is not None:
        return True
    if isinstance(value, dict):
        return any(_needs_lift(item) for item in value.values())
    if isinstance(value, list):
        return any(_needs_lift(item) for item in value)
    return False


def _container_kind(value: Any) -> Optional[str]:
    """
    Return the tag for a container that has to be stored as a JSON array.

    :param value: Any part of a record value.
    :return: The kind tag, or `None` for anything else - `list` included, which
        is already what it will be read back as.
    """
    if isinstance(value, tuple):
        return _CONTAINER_TUPLE
    if isinstance(value, frozenset):
        return _CONTAINER_FROZENSET
    if isinstance(value, set):
        return _CONTAINER_SET
    return None


def _strip(
    value: Any,
    path: list[_Step],
    byte_paths: list[list[Any]],
    container_paths: list[list[Any]],
) -> Any:
    """
    Copy a value into its orjson-safe form, recording what was taken out.

    Binary leaves are replaced by `None` and `tuple`/`set`/`frozenset` by a
    `list`; both record their path. A set's iteration order here is what defines
    its elements' indices, so the paths recorded inside it stay valid.

    :param value: Any part of a record value.
    :param path: The path walked so far, mutated in place during the walk.
    :param byte_paths: The accumulator of `[path, base64]` entries.
    :param container_paths: The accumulator of `[path, kind]` entries.
    :return: The orjson-safe copy.
    """
    if isinstance(value, _BINARY):
        byte_paths.append([list(path), base64.b64encode(bytes(value)).decode()])
        return None

    if isinstance(value, dict):
        copied: dict[Any, Any] = {}
        for name, item in value.items():
            path.append(name)
            copied[name] = _strip(item, path, byte_paths, container_paths)
            path.pop()
        return copied

    kind = _container_kind(value)
    if kind is not None or isinstance(value, list):
        if kind is not None:
            container_paths.append([list(path), kind])
        items: list[Any] = []
        for index, item in enumerate(value):
            path.append(index)
            items.append(_strip(item, path, byte_paths, container_paths))
            path.pop()
        return items

    return value


def _restore_bytes(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    """
    Put the binary leaves recorded by `_lift_value()` back into a value.

    :param value: The body read out of the envelope.
    :param paths: The `[path, base64]` entries, or `None` if there were none.
    :return: The value with its binary leaves restored, always as `bytes`.
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


def _restore_containers(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    """
    Rebuild the containers recorded by `_lift_value()`, deepest path first.

    The order is what makes one pass enough: a container is rebuilt only once
    everything inside it already has its final type, and its parent is still the
    mutable list or dict the store returned when the rebuilt object is written
    back into it.

    :param value: The body, with its binary leaves already restored.
    :param paths: The `[path, kind]` entries, or `None` if there were none.
    :return: The value with its container types restored.
    """
    if not paths:
        return value

    for path, kind in sorted(paths, key=lambda entry: len(entry[0]), reverse=True):
        if not path:
            # The whole value was a container. Its path is the shortest there
            # is, so this is the last entry and there is nothing left to do.
            return _rebuild_container(value, kind)
        parent = value
        for step in path[:-1]:
            parent = parent[step]
        parent[path[-1]] = _rebuild_container(parent[path[-1]], kind)
    return value


def _rebuild_container(items: Any, kind: str) -> Any:
    """
    Turn one stored array back into the container it came from.

    :param items: The list read out of the envelope.
    :param kind: The tag recorded by `_container_kind()`.
    :return: The rebuilt container, or the list unchanged for a kind this
        version does not know - which only a newer writer's envelope replayed
        from the changelog can produce, and where the stored list is a better
        answer than a crash on the record path.
    """
    if kind == _CONTAINER_TUPLE:
        return tuple(items)
    if kind == _CONTAINER_SET:
        return set(items)
    if kind == _CONTAINER_FROZENSET:
        return frozenset(items)
    return items
