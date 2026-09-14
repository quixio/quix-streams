"""
Storage-level helpers for the non-blocking lookup buffer.

Everything here is about *how* a withheld record and the per-partition index of
keys with withheld records are represented inside a `TimestampedStore`. The
operator logic that decides *when* to write or read them lives in
`buffer_operator.py`; the public knobs live in `buffer.py`.

Two representations are defined:

- **The envelope** — one withheld record, stored under the message key's prefix
  at its wall-clock arrival time. It has to be self-describing, because
  `TimestampedPartitionTransaction.get_interval()` returns values without their
  store keys, so the arrival time and the original event timestamp are
  unrecoverable unless they travel inside the value.
- **The pending index** — one entry per partition listing which prefixes
  currently hold withheld records, the earliest arrival time known for each, and
  enough information to rebuild the original message key object. It is durable
  because a restart must not make a recovered buffer invisible.

Both are serialized by the store's own `dumps`/`loads`, which default to orjson
(`quixstreams/state/rocksdb/options.py`). orjson cannot serialize `bytes`, so
header values and prefixes are base64-encoded on the way in.
"""

import base64
from typing import Any, Mapping, Optional, Union

from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

__all__ = (
    "ENVELOPE_RECEIVED",
    "ENVELOPE_TIMESTAMP",
    "ENVELOPE_VALUE",
    "INDEX_KEY",
    "INDEX_PREFIX",
    "PendingIndex",
    "decode_headers",
    "decode_prefix",
    "emit_tuple",
    "encode_envelope",
    "encode_prefix",
    "key_from_prefix",
    "key_kind",
    "prefix_for_key",
)

# Envelope field names. Kept to one character each: every withheld record pays
# for them twice, once in RocksDB and once in the changelog topic.
ENVELOPE_VALUE = "v"
ENVELOPE_TIMESTAMP = "t"
ENVELOPE_RECEIVED = "r"
ENVELOPE_HEADERS = "h"
ENVELOPE_HEADERS_MAPPING = "m"

# Header value encodings used inside the envelope.
_HEADER_BYTES = "b"
_HEADER_STR = "s"
_HEADER_NONE = "n"

# How the original message key is rebuilt from the prefix bytes on the sweep
# path, where the key object itself is not available.
KEY_KIND_BYTES = "b"
KEY_KIND_STR = "s"

# The pending index lives in the same store as the buffered records, under a
# prefix that a message key must never collide with.
INDEX_PREFIX = b"__lookup_buffer_index__"
INDEX_KEY = b"pending"


def prefix_for_key(key: Any) -> bytes:
    """
    Return the store prefix for a message key.

    Passing `bytes` through unchanged makes `TimestampedPartitionTransaction.
    _ensure_bytes()` a no-op, so the prefix is exactly these bytes and the
    pending index maps back onto it exactly.

    :param key: The message key.
    :return: The prefix to store this key's withheld records under.
    :raises ValueError: If the key is not `bytes` or `str`, or collides with the
        pending index's own prefix.
    """
    if isinstance(key, bytes):
        prefix = key
    elif isinstance(key, str):
        prefix = key.encode()
    else:
        raise ValueError(
            f"Cannot buffer a record with a message key of type "
            f"{type(key).__name__!r}: `join_lookup(..., buffer=...)` stores "
            f"withheld records under the message key, which must be `bytes` or "
            f"`str`."
        )
    if prefix == INDEX_PREFIX:
        raise ValueError(
            f"The message key {key!r} collides with the lookup buffer's "
            f"internal index key. Use a different key or a different "
            f"`store_name` for the LookupBuffer."
        )
    return prefix


def key_kind(key: Any) -> str:
    """
    Return the tag recording whether a message key was `bytes` or `str`.

    :param key: The message key, already accepted by `prefix_for_key()`.
    :return: `"b"` for `bytes`, `"s"` for `str`.
    """
    return KEY_KIND_BYTES if isinstance(key, bytes) else KEY_KIND_STR


def key_from_prefix(prefix: bytes, kind: str) -> Union[bytes, str]:
    """
    Rebuild the original message key from a prefix and its recorded kind.

    Used on the sweep path, which emits another key's withheld records and has
    only the prefix to work from. The kind is stored rather than guessed,
    because a `str` key and its UTF-8 `bytes` are indistinguishable afterwards.

    :param prefix: The store prefix.
    :param kind: The tag produced by `key_kind()`.
    :return: The message key to emit under.
    """
    return prefix.decode() if kind == KEY_KIND_STR else prefix


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
    return {
        ENVELOPE_VALUE: value,
        ENVELOPE_TIMESTAMP: timestamp,
        ENVELOPE_RECEIVED: receive_ms,
        ENVELOPE_HEADERS: encoded_headers,
        ENVELOPE_HEADERS_MAPPING: is_mapping,
    }


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
        envelope[ENVELOPE_VALUE],
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


class PendingIndex:
    """
    The durable, per-partition set of prefixes that currently hold withheld
    records.

    It answers three questions that the buffer cannot answer any other way:

    1. *Does this key already have withheld records?* If it does, the incoming
       record joins the queue even when its own lookup resolves, which is what
       keeps within-key order intact. An in-process-only answer would be wrong
       after a restart, releasing a resolvable record ahead of its own recovered
       predecessors.
    2. *Which prefixes might have something past its deadline?* The sweep
       selects on the recorded earliest arrival without deserializing anything.
    3. *Which key object do those records get emitted under?* From the recorded
       key kind.

    The recorded earliest arrival is a **lower bound**, not an exact value:
    reading it too low only costs a wasted range read, while reading it too high
    would hide withheld records from the sweep forever.

    The index is read lazily at most once per record and written back only when
    something changed, both through the caller's store transaction so the writes
    reach the changelog like any other state.
    """

    def __init__(self, transaction: TimestampedPartitionTransaction) -> None:
        self._transaction = transaction
        self._entries: Optional[dict[str, list]] = None
        self._changed = False

    def entries(self) -> dict[str, list]:
        """
        Return the index, loading it from the store on first use.

        :return: A mapping of `base64(prefix)` to `[earliest_receive_ms,
            key_kind]`. Mutating it directly will not be persisted - use the
            other methods.
        """
        if self._entries is None:
            self._entries = self._transaction.get(INDEX_KEY, prefix=INDEX_PREFIX) or {}
        return self._entries

    def get(self, prefix: bytes) -> Optional[list]:
        """
        Return the index entry for a prefix, or `None` if it holds nothing.

        :param prefix: The store prefix.
        :return: `[earliest_receive_ms, key_kind]` or `None`.
        """
        return self.entries().get(encode_prefix(prefix))

    def ensure(self, prefix: bytes, key: Any, receive_ms: int) -> None:
        """
        Record that a prefix holds withheld records.

        An existing entry is left alone: its earliest arrival is already at or
        below `receive_ms`, and lowering it is never useful.

        :param prefix: The store prefix.
        :param key: The original message key, for the key-kind tag.
        :param receive_ms: The arrival time of the record being withheld.
        """
        entries = self.entries()
        encoded = encode_prefix(prefix)
        if encoded not in entries:
            entries[encoded] = [receive_ms, key_kind(key)]
            self._changed = True

    def set_earliest(self, prefix: bytes, receive_ms: int) -> None:
        """
        Update a prefix's earliest-arrival lower bound.

        :param prefix: The store prefix.
        :param receive_ms: The new lower bound, in milliseconds.
        """
        entry = self.entries().get(encode_prefix(prefix))
        if entry is not None and entry[0] != receive_ms:
            entry[0] = receive_ms
            self._changed = True

    def drop(self, prefix: bytes) -> None:
        """
        Forget a prefix, which must hold nothing anymore.

        :param prefix: The store prefix.
        """
        if self.entries().pop(encode_prefix(prefix), None) is not None:
            self._changed = True

    def flush(self) -> None:
        """Persist the index through the transaction if anything changed."""
        if self._changed and self._entries is not None:
            self._transaction.set(INDEX_KEY, self._entries, prefix=INDEX_PREFIX)
            self._changed = False


def encode_prefix(prefix: bytes) -> str:
    """
    Encode a prefix for use as an index key.

    The index is serialized as JSON, whose object keys must be strings, so the
    raw prefix bytes are base64-encoded.

    :param prefix: The store prefix.
    :return: The base64 form.
    """
    return base64.b64encode(prefix).decode()


def decode_prefix(encoded: str) -> bytes:
    """
    Decode an index key back into a store prefix.

    :param encoded: The base64 form produced by `encode_prefix()`.
    :return: The store prefix.
    """
    return base64.b64decode(encoded)
