"""
The durable index of which message-key prefixes currently hold withheld records.

Two namespaces inside the buffer's store, both written only by
`PendingIndex.flush()`:

- `INDEX_PREFIX`: one marker per prefix, `[earliest_receive_ms, key_kind]`.
- `QUEUE_PREFIX`: the same prefixes keyed by that arrival time, so the sweep
  finds what is due with a range read instead of a scan.

The recorded arrival time is a lower bound. Too low costs a wasted range read;
too high would hide withheld records from the sweep for good.
"""

import base64
from typing import Any, Optional, Union

from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction
from quixstreams.state.serialization import int_to_bytes

__all__ = (
    "INDEX_PREFIX",
    "MAX_RECEIVE_MS",
    "NULL_KEY_PREFIX",
    "QUEUE_PREFIX",
    "PendingIndex",
    "decode_prefix",
    "encode_prefix",
    "key_from_prefix",
    "key_kind",
    "prefix_for_key",
)

MAX_RECEIVE_MS = 2**63 - 1

KEY_KIND_BYTES = "b"
KEY_KIND_STR = "s"
KEY_KIND_NONE = "n"

INDEX_PREFIX = b"__lookup_buffer_index__"
QUEUE_PREFIX = b"__lookup_buffer_queue__"

# A store key is `<prefix> SEPARATOR <encoded timestamp>`, so a prefix holding a
# SEPARATOR byte sorts inside the range a scan for a shorter prefix walks
# (`b"tenant|device"` inside `b"tenant"`'s). The store does not defend against
# this for any of its users - issue #1148 - so prefixes are escaped before they
# reach it. The escape byte is escaped first, which makes the encoding
# injective, and is 0x7f so the encoding is the identity for a prefix holding
# neither byte.
PREFIX_ESCAPE = b"\x7f"
_ESCAPED_SEPARATOR = PREFIX_ESCAPE + b"\x01"
_ESCAPED_ESCAPE = PREFIX_ESCAPE + b"\x02"

# Free as a sentinel: no escaped prefix can begin `7f 00`, because every 0x7f in
# one starts a sequence whose second byte is 0x01 or 0x02.
NULL_KEY_PREFIX = PREFIX_ESCAPE + b"\x00"


def _escape_prefix(prefix: bytes) -> bytes:
    if SEPARATOR not in prefix and PREFIX_ESCAPE not in prefix:
        return prefix
    return prefix.replace(PREFIX_ESCAPE, _ESCAPED_ESCAPE).replace(
        SEPARATOR, _ESCAPED_SEPARATOR
    )


def _unescape_prefix(prefix: bytes) -> bytes:
    if PREFIX_ESCAPE not in prefix:
        return prefix
    return prefix.replace(_ESCAPED_SEPARATOR, SEPARATOR).replace(
        _ESCAPED_ESCAPE, PREFIX_ESCAPE
    )


def prefix_for_key(key: Any) -> bytes:
    """
    Map a message key to the store prefix its records are held under.

    `LookupBuffer.validate_key_deserializers()` turns the `ValueError` below into
    a build-time failure for the topic-wide case.

    :param key: The message key.
    :return: The escaped store prefix.
    :raises ValueError: if the key is not `bytes`, `str` or `None`, or if it
        encodes to one of the index's own namespaces.
    """
    if key is None:
        return NULL_KEY_PREFIX
    if isinstance(key, bytes):
        prefix = key
    elif isinstance(key, str):
        prefix = key.encode()
    else:
        raise ValueError(
            f"Cannot buffer a record with a message key of type "
            f"{type(key).__name__!r}: `join_lookup(..., buffer=...)` stores "
            f"withheld records under the message key, which must be `bytes`, "
            f"`str` or `None`."
        )
    prefix = _escape_prefix(prefix)
    if prefix in (INDEX_PREFIX, QUEUE_PREFIX):
        raise ValueError(
            f"The message key {key!r} collides with the lookup buffer's "
            f"internal index. Use a different key or a different `store_name` "
            f"for the LookupBuffer."
        )
    return prefix


def key_kind(key: Any) -> str:
    if key is None:
        return KEY_KIND_NONE
    return KEY_KIND_BYTES if isinstance(key, bytes) else KEY_KIND_STR


def key_from_prefix(prefix: bytes, kind: str) -> Optional[Union[bytes, str]]:
    if kind == KEY_KIND_NONE:
        return None
    key = _unescape_prefix(prefix)
    return key.decode() if kind == KEY_KIND_STR else key


def encode_prefix(prefix: bytes) -> str:
    # base64 because the prefix travels inside index values, which go through
    # the store's serializer, and orjson takes `str` but not `bytes`.
    return base64.b64encode(prefix).decode()


def decode_prefix(encoded: str) -> bytes:
    return base64.b64decode(encoded)


def _marker_key(encoded: str) -> bytes:
    # The `k` tag keeps an empty message key's marker off the store key
    # `INDEX_PREFIX + SEPARATOR`, which sorts below the zero timestamp
    # `TimestampedPartitionTransaction._expire()` uses as its lower bound, and
    # clear of the store's own `__min_eligible_timestamps__` key.
    return b"k" + encoded.encode()


def _queue_key(receive_ms: int, encoded: str) -> bytes:
    # `int_to_bytes` is big-endian, so entries sort by deadline; the encoded
    # prefix keeps two prefixes arriving in the same millisecond apart.
    return int_to_bytes(receive_ms) + SEPARATOR + encoded.encode()


class PendingIndex:
    """One store transaction's view of the pending index."""

    def __init__(self, transaction: TimestampedPartitionTransaction) -> None:
        self._transaction = transaction
        self._markers: dict[str, Optional[list]] = {}
        self._queued: dict[str, Optional[int]] = {}
        self._changed: set[str] = set()
        self._stale: list[tuple[str, int]] = []

    def get(self, prefix: bytes) -> Optional[list]:
        """
        :param prefix: The store prefix.
        :return: `[earliest_receive_ms, key_kind]`, or `None` if the prefix
            holds nothing.
        """
        return self.entry(encode_prefix(prefix))

    def entry(self, encoded: str) -> Optional[list]:
        """`get()` for a prefix already in its base64 form."""
        if encoded not in self._markers:
            marker = self._transaction.get(_marker_key(encoded), prefix=INDEX_PREFIX)
            self._markers[encoded] = marker
            self._queued[encoded] = marker[0] if marker else None
        return self._markers[encoded]

    def due(self, cutoff: int, limit: int) -> list[list]:
        """
        :param cutoff: Arrival time at or below which a prefix is due.
        :param limit: Cap on entries read.
        :return: `[encoded_prefix, queued_receive_ms]` pairs, earliest first.
        """
        if cutoff < 0:
            return []
        return self._transaction.get_interval(
            start=0, end=cutoff + 1, prefix=QUEUE_PREFIX, limit=limit
        )

    def earliest(self) -> Optional[int]:
        """:return: The earliest queued arrival time in the partition, if any."""
        queued = self._transaction.get_interval(
            start=0, end=MAX_RECEIVE_MS, prefix=QUEUE_PREFIX, limit=1
        )
        if not queued:
            return None
        return queued[0][1]

    def entries(self) -> dict[str, list]:
        materialised: dict[str, list] = {}
        queued = self._transaction.get_interval(
            start=0, end=MAX_RECEIVE_MS, prefix=QUEUE_PREFIX
        )
        for encoded, _ in queued:
            marker = self.entry(encoded)
            if marker is not None:
                materialised[encoded] = marker
        return materialised

    def ensure(self, prefix: bytes, key: Any, receive_ms: int) -> None:
        encoded = encode_prefix(prefix)
        if self.entry(encoded) is None:
            self._markers[encoded] = [receive_ms, key_kind(key)]
            self._changed.add(encoded)

    def set_earliest(self, prefix: bytes, receive_ms: int) -> None:
        """
        Move an existing marker to a new arrival time. No-op if there is none.

        :param prefix: The store prefix.
        :param receive_ms: The prefix's earliest surviving arrival time.
        """
        encoded = encode_prefix(prefix)
        marker = self.entry(encoded)
        if marker is not None and marker[0] != receive_ms:
            marker[0] = receive_ms
            self._changed.add(encoded)

    def drop(self, prefix: bytes) -> None:
        encoded = encode_prefix(prefix)
        if self.entry(encoded) is not None:
            self._markers[encoded] = None
            self._changed.add(encoded)

    def unqueue(self, encoded: str, receive_ms: int) -> None:
        """
        Schedule the removal of a queue entry whose marker no longer claims it.

        :param encoded: The prefix in its base64 form.
        :param receive_ms: The arrival time the entry is queued at.
        """
        self._stale.append((encoded, receive_ms))

    def flush(self) -> None:
        """
        Write back every entry this instance changed.

        The only writer of either namespace: the previously persisted queue
        entry is deleted before the new one is written, so a prefix is never
        queued twice or queued at a deadline its marker no longer claims.
        Entries handed to `unqueue()` go first, so a prefix repaired and
        rewritten in the same callback keeps its new entry.
        """
        for stale_encoded, stale_ms in self._stale:
            self._transaction.delete(
                _queue_key(stale_ms, stale_encoded), prefix=QUEUE_PREFIX
            )
        self._stale.clear()

        for encoded in self._changed:
            marker = self._markers[encoded]
            queued_at = self._queued[encoded]
            if queued_at is not None:
                self._transaction.delete(
                    _queue_key(queued_at, encoded), prefix=QUEUE_PREFIX
                )

            if marker is None:
                self._transaction.delete(_marker_key(encoded), prefix=INDEX_PREFIX)
                self._queued[encoded] = None
            else:
                self._transaction.set(_marker_key(encoded), marker, prefix=INDEX_PREFIX)
                self._transaction.set(
                    _queue_key(marker[0], encoded),
                    [encoded, marker[0]],
                    prefix=QUEUE_PREFIX,
                )
                self._queued[encoded] = marker[0]
        self._changed.clear()
