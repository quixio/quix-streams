"""
The durable index of which keys currently hold withheld records.

Everything here is about *which* prefixes the buffer is holding something for and
*when* the oldest of those records arrived. The withheld records themselves are
represented in `buffer_envelope.py`, the operator logic that decides when to
write or read them lives in `buffer_operator.py`, and the public knobs live in
`buffer.py`.

This module also owns the message key <-> store prefix mapping
(`prefix_for_key()` / `key_from_prefix()`), which is not the identity: a key
containing the store's `|` separator would otherwise share a shorter key's scan
range. See `PREFIX_ESCAPE` below. Everything downstream - the record path, the
sweep, the two index namespaces and the in-process bookkeeping - works in the
escaped form, and `key_from_prefix()` is the single place it is undone.

The index answers three questions that the buffer cannot answer any other way:

1. *Does this key already have withheld records?* If it does, the incoming
   record joins the queue even when its own lookup resolves, which is what keeps
   within-key order intact. An in-process-only answer would be wrong after a
   restart, releasing a resolvable record ahead of its own recovered
   predecessors.
2. *Which prefixes have something past their deadline?* Answered by a range read
   over a deadline-ordered queue, so the cost is proportional to what is due, not
   to how many keys are waiting.
3. *Which key object do those records get emitted under?* From the recorded key
   kind, because a `str` key and its UTF-8 `bytes` are indistinguishable
   afterwards.

It is durable - it lives in the same `TimestampedStore` as the records, so it
reaches the changelog like any other state - because a restart must not make a
recovered buffer invisible.

Two namespaces inside that store hold it, neither of which a message key may
collide with:

- `__lookup_buffer_index__` — one entry per waiting prefix,
  `"k" + base64(prefix) -> [earliest_receive_ms, key_kind]`. Point get/set/delete,
  so a record touches exactly its own key's entry and a checkpoint produces one
  small changelog message per prefix that actually changed.
- `__lookup_buffer_queue__` — the same prefixes keyed by their deadline,
  `int_to_bytes(earliest_receive_ms) | base64(prefix) -> [base64(prefix),
  earliest_receive_ms]`. Scanned with `get_interval(0, cutoff + 1)`, which
  returns exactly the prefixes that are past their deadline, oldest first. The
  key embeds the prefix so two prefixes sharing an arrival millisecond cannot
  overwrite each other, and the value repeats the deadline so a reader holding
  only the scan's result can address the entry it came from.

The invariant tying them together is: **every marker has exactly one queue entry,
at the millisecond the marker records.** `flush()` is the only writer, and it
maintains that invariant by deleting the previously persisted queue entry before
writing the new one. A reader that finds the invariant broken - which a
checkpoint whose changelog messages were only partly produced before the process
died can leave behind - repairs it with `unqueue()`.
"""

import base64
from typing import Any, Optional, Union

from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction
from quixstreams.state.serialization import int_to_bytes

__all__ = (
    "INDEX_PREFIX",
    "MAX_RECEIVE_MS",
    "QUEUE_PREFIX",
    "PendingIndex",
    "decode_prefix",
    "encode_prefix",
    "key_from_prefix",
    "key_kind",
    "prefix_for_key",
)

# Upper bound for "everything, however late it arrived". Deliberately not `now`:
# a clock step backwards can leave stored arrival times in the future, and those
# records must still be found rather than stranded.
MAX_RECEIVE_MS = 2**63 - 1

# How the original message key is rebuilt from the prefix bytes on the sweep
# path, where the key object itself is not available.
KEY_KIND_BYTES = "b"
KEY_KIND_STR = "s"

# The index lives in the same store as the buffered records, under prefixes that
# a message key must never collide with.
INDEX_PREFIX = b"__lookup_buffer_index__"
QUEUE_PREFIX = b"__lookup_buffer_queue__"

# A `TimestampedStore` key is `<prefix> SEPARATOR <encoded timestamp>`, so a
# prefix that itself contains the SEPARATOR is ambiguous with a shorter one:
# `b"tenant|device"`'s keys start with `b"tenant" + SEPARATOR` and therefore sort
# *inside* the byte range every range scan for `b"tenant"` walks. Whether they
# come back depends only on the scan's upper bound, and this buffer scans to
# `MAX_RECEIVE_MS`, whose big-endian first byte is 0x7F - above everything a
# realistic millisecond bound stops at. So for the buffer the collision is not
# theoretical: `b"tenant"`'s release would read, emit under the wrong key and
# then delete `b"tenant|device"`'s withheld records.
#
# The store does not defend against this for any of its users (issue #1148), so
# the buffer escapes the SEPARATOR out of its own prefixes before they ever reach
# the store. An escaped prefix contains no SEPARATOR byte, so no prefix's
# encoding can be another prefix's encoding followed by a SEPARATOR, whatever the
# scan bounds are.
#
# The escape byte is 0x7F rather than something printable so that the encoding is
# the *identity* for every key containing neither `|` nor 0x7F - the buffer's own
# reserved namespaces included.
PREFIX_ESCAPE = b"\x7f"
_ESCAPED_SEPARATOR = PREFIX_ESCAPE + b"\x01"
_ESCAPED_ESCAPE = PREFIX_ESCAPE + b"\x02"


def _escape_prefix(prefix: bytes) -> bytes:
    """
    Encode a prefix so that it cannot contain the store's key SEPARATOR.

    The escape byte is escaped first, so the encoding is injective: `b"a|b"` and
    `b"a\\x7f\\x01b"` map to different byte strings.

    :param prefix: The raw message-key bytes.
    :return: The prefix with no SEPARATOR byte in it.
    """
    if SEPARATOR not in prefix and PREFIX_ESCAPE not in prefix:
        return prefix
    return prefix.replace(PREFIX_ESCAPE, _ESCAPED_ESCAPE).replace(
        SEPARATOR, _ESCAPED_SEPARATOR
    )


def _unescape_prefix(prefix: bytes) -> bytes:
    """
    Invert `_escape_prefix()`.

    Two passes of `replace()` are exact here: every 0x7F left in an escaped
    prefix starts an escape sequence (the raw ones were all doubled), and no
    sequence's second byte is 0x7F, so no match can straddle a boundary. The
    first pass only introduces `|`, so it cannot manufacture a sequence for the
    second.

    :param prefix: A prefix produced by `_escape_prefix()`.
    :return: The raw message-key bytes.
    """
    if PREFIX_ESCAPE not in prefix:
        return prefix
    return prefix.replace(_ESCAPED_SEPARATOR, SEPARATOR).replace(
        _ESCAPED_ESCAPE, PREFIX_ESCAPE
    )


def prefix_for_key(key: Any) -> bytes:
    """
    Return the store prefix for a message key.

    The key's bytes are escaped (see `PREFIX_ESCAPE` above) so that a key
    containing the store's `|` separator gets a namespace of its own instead of
    sharing a shorter key's scan range. The encoding is the identity for every
    key that contains neither `|` nor 0x7F, so the common case stores the key's
    own bytes verbatim. `key_from_prefix()` inverts it.

    :param key: The message key.
    :return: The prefix to store this key's withheld records under.
    :raises ValueError: If the key is not `bytes` or `str`, or collides with one
        of the buffer's own reserved prefixes.
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
    prefix = _escape_prefix(prefix)
    if prefix in (INDEX_PREFIX, QUEUE_PREFIX):
        raise ValueError(
            f"The message key {key!r} collides with the lookup buffer's "
            f"internal index. Use a different key or a different `store_name` "
            f"for the LookupBuffer."
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
    only the prefix to work from. This is the one place a prefix travels back
    out of the store's namespace, so it is the one place `prefix_for_key()`'s
    escaping is undone. The kind is stored rather than guessed, because a `str`
    key and its UTF-8 `bytes` are indistinguishable afterwards.

    :param prefix: The store prefix, as produced by `prefix_for_key()`.
    :param kind: The tag produced by `key_kind()`.
    :return: The message key to emit under.
    """
    key = _unescape_prefix(prefix)
    return key.decode() if kind == KEY_KIND_STR else key


def encode_prefix(prefix: bytes) -> str:
    """
    Encode a prefix for use as an index key.

    The index's values are serialized as JSON and its store keys have to be
    printable enough to embed in the deadline queue's key, so the raw prefix
    bytes are base64-encoded.

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


def _marker_key(encoded: str) -> bytes:
    """
    Build the marker store key for one prefix.

    The tag byte exists so that the entry for an *empty* message key is not the
    empty store key: that would serialize to `INDEX_PREFIX + SEPARATOR`, which
    sorts below the zero timestamp `TimestampedPartitionTransaction._expire()`
    uses as its lower bound, and the store would delete the marker as though it
    were an expired record. It also keeps every marker key clear of the store's
    own `__min_eligible_timestamps__` key, which shares the namespace.

    :param encoded: The prefix in its base64 form.
    :return: The store key, inside the `INDEX_PREFIX` namespace.
    """
    return b"k" + encoded.encode()


def _queue_key(receive_ms: int, encoded: str) -> bytes:
    """
    Build the deadline queue's store key for one prefix.

    `int_to_bytes` is big-endian, so the entries sort by deadline; appending the
    encoded prefix keeps two prefixes that arrived in the same millisecond from
    overwriting each other.

    :param receive_ms: The prefix's earliest arrival time, in milliseconds.
    :param encoded: The prefix in its base64 form.
    :return: The store key, inside the `QUEUE_PREFIX` namespace.
    """
    return int_to_bytes(receive_ms) + SEPARATOR + encoded.encode()


class PendingIndex:
    """
    The durable, per-partition set of prefixes that currently hold withheld
    records, seen through one store transaction.

    The recorded earliest arrival is a **lower bound**, not an exact value:
    reading it too low only costs a wasted range read, while reading it too high
    would hide withheld records from the sweep forever.

    Entries are read lazily and at most once per prefix per instance, and written
    back only when something changed, both through the caller's store transaction
    so the writes reach the changelog like any other state.
    """

    def __init__(self, transaction: TimestampedPartitionTransaction) -> None:
        self._transaction = transaction
        # {base64(prefix): [earliest_receive_ms, key_kind] or None if it holds
        # nothing}, for the prefixes this instance has looked at.
        self._markers: dict[str, Optional[list]] = {}
        # {base64(prefix): the deadline its queue entry is currently stored at},
        # so `flush()` knows which entry to remove when a deadline moves.
        self._queued: dict[str, Optional[int]] = {}
        self._changed: set[str] = set()
        # [(base64(prefix), deadline)] of queue entries with no matching marker.
        self._stale: list[tuple[str, int]] = []

    def get(self, prefix: bytes) -> Optional[list]:
        """
        Return the index entry for a prefix, or `None` if it holds nothing.

        :param prefix: The store prefix.
        :return: `[earliest_receive_ms, key_kind]` or `None`.
        """
        return self.entry(encode_prefix(prefix))

    def entry(self, encoded: str) -> Optional[list]:
        """
        Return the index entry for an already-encoded prefix.

        :param encoded: The base64 form produced by `encode_prefix()`.
        :return: `[earliest_receive_ms, key_kind]` or `None`.
        """
        if encoded not in self._markers:
            marker = self._transaction.get(_marker_key(encoded), prefix=INDEX_PREFIX)
            self._markers[encoded] = marker
            self._queued[encoded] = marker[0] if marker else None
        return self._markers[encoded]

    def due(self, cutoff: int) -> list[list]:
        """
        Return the prefixes whose earliest withheld record is past its deadline.

        This is the whole reason the deadline queue exists: the answer costs a
        range read over what is actually due, instead of a scan of every waiting
        key on the partition.

        :param cutoff: Arrival times at or below this are past their deadline.
        :return: `[encoded_prefix, queued_receive_ms]` pairs, oldest deadline
            first.
        """
        if cutoff < 0:
            return []
        return self._transaction.get_interval(
            start=0, end=cutoff + 1, prefix=QUEUE_PREFIX
        )

    def entries(self) -> dict[str, list]:
        """
        Materialise the whole index.

        Not on the record path - the record path only ever touches one prefix's
        entry (`get()`) or the prefixes that are actually due (`due()`). This is
        the introspection view, and it costs one range read plus one point read
        per waiting prefix.

        :return: A mapping of `base64(prefix)` to `[earliest_receive_ms,
            key_kind]`. Mutating it directly will not be persisted - use the
            other methods.
        """
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
        """
        Record that a prefix holds withheld records.

        An existing entry is left alone: its earliest arrival is already at or
        below `receive_ms`, and lowering it is never useful.

        :param prefix: The store prefix.
        :param key: The original message key, for the key-kind tag.
        :param receive_ms: The arrival time of the record being withheld.
        """
        encoded = encode_prefix(prefix)
        if self.entry(encoded) is None:
            self._markers[encoded] = [receive_ms, key_kind(key)]
            self._changed.add(encoded)

    def set_earliest(self, prefix: bytes, receive_ms: int) -> None:
        """
        Update a prefix's earliest-arrival lower bound.

        :param prefix: The store prefix.
        :param receive_ms: The new lower bound, in milliseconds.
        """
        encoded = encode_prefix(prefix)
        marker = self.entry(encoded)
        if marker is not None and marker[0] != receive_ms:
            marker[0] = receive_ms
            self._changed.add(encoded)

    def drop(self, prefix: bytes) -> None:
        """
        Forget a prefix, which must hold nothing anymore.

        :param prefix: The store prefix.
        """
        encoded = encode_prefix(prefix)
        if self.entry(encoded) is not None:
            self._markers[encoded] = None
            self._changed.add(encoded)

    def unqueue(self, encoded: str, receive_ms: int) -> None:
        """
        Forget a queue entry that its marker no longer claims.

        The marker and its queue entry are written in the same `flush()`, so this
        only fires on a partially applied checkpoint. Without it the entry would
        be returned by every `due()` from then on, for a prefix that holds
        nothing.

        :param encoded: The base64 form produced by `encode_prefix()`.
        :param receive_ms: The deadline the entry is stored at.
        """
        self._stale.append((encoded, receive_ms))

    def flush(self) -> None:
        """
        Persist every entry this instance changed, through the transaction.

        This is the only writer, which is what keeps the marker and its queue
        entry in step: the previously persisted queue entry is removed before the
        new one is written, so a prefix is never queued at two deadlines and
        never queued at a deadline its marker no longer claims. Entries handed to
        `unqueue()` are removed first, so a prefix that is repaired and rewritten
        in the same callback keeps its new entry.
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
