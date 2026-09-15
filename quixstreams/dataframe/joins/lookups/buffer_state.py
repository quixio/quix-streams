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

PREFIX_ESCAPE = b"\x7f"
_ESCAPED_SEPARATOR = PREFIX_ESCAPE + b"\x01"
_ESCAPED_ESCAPE = PREFIX_ESCAPE + b"\x02"

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
    return base64.b64encode(prefix).decode()


def decode_prefix(encoded: str) -> bytes:
    return base64.b64decode(encoded)


def _marker_key(encoded: str) -> bytes:
    return b"k" + encoded.encode()


def _queue_key(receive_ms: int, encoded: str) -> bytes:
    return int_to_bytes(receive_ms) + SEPARATOR + encoded.encode()


class PendingIndex:
    def __init__(self, transaction: TimestampedPartitionTransaction) -> None:
        self._transaction = transaction
        self._markers: dict[str, Optional[list]] = {}
        self._queued: dict[str, Optional[int]] = {}
        self._changed: set[str] = set()
        self._stale: list[tuple[str, int]] = []

    def get(self, prefix: bytes) -> Optional[list]:
        return self.entry(encode_prefix(prefix))

    def entry(self, encoded: str) -> Optional[list]:
        if encoded not in self._markers:
            marker = self._transaction.get(_marker_key(encoded), prefix=INDEX_PREFIX)
            self._markers[encoded] = marker
            self._queued[encoded] = marker[0] if marker else None
        return self._markers[encoded]

    def due(self, cutoff: int, limit: int) -> list[list]:
        if cutoff < 0:
            return []
        return self._transaction.get_interval(
            start=0, end=cutoff + 1, prefix=QUEUE_PREFIX, limit=limit
        )

    def earliest(self) -> Optional[int]:
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
        self._stale.append((encoded, receive_ms))

    def flush(self) -> None:
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
