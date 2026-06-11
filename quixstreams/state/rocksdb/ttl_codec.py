"""
Codecs for the per-write TTL feature.

Two concerns live here:

1. The *value codec* — every value stored in a TTL-aware main column family is
   prefixed with an 8-byte big-endian uint64 millisecond expiry timestamp.
   Layout: ``expires_at_u64_be (8 bytes) || value_bytes``. The sentinel value
   ``SENTINEL_NEVER`` (``0xFFFFFFFFFFFFFFFF``) encodes "never expires"; entries
   carrying the sentinel always read back, never enter the secondary expiry
   index, and are never seen as expired by the read-time filter.

2. The *index codec* — every entry in the local-only ``__ttl_index__`` column
   family is keyed by ``expires_at_u64_be (8 bytes) || user_key_bytes`` and has
   an empty value. RocksDB's default byte-wise comparator yields expiry order,
   so a forward iterator on the index walks "oldest first". Index entries are
   only written for values whose stamp is *not* the sentinel.

See ``dev-planning/state-ttl/architecture.md`` for the design.
"""

import struct

__all__ = (
    "SENTINEL_NEVER",
    "TTL_STAMP_BYTES",
    "encode_ttl_value",
    "decode_ttl_value",
    "encode_index_key",
    "decode_index_key",
    "is_sentinel",
)


# 8-byte big-endian unsigned 64-bit integer. Same packer used elsewhere in
# the state serialization layer; duplicated locally to avoid importing the
# whole serialization module from the codec.
_stamp_packer = struct.Struct(">Q")
_stamp_pack = _stamp_packer.pack
_stamp_unpack_from = _stamp_packer.unpack_from
TTL_STAMP_BYTES = _stamp_packer.size

# Sentinel stamp value meaning "never expires". Chosen as the maximum uint64
# so that a sentinel-stamped entry sorts after every realistic expiry in the
# secondary index (where it is in fact never written) and fails the
# ``stamp <= now`` read-time filter for any plausible event-time clock.
SENTINEL_NEVER: int = 0xFFFFFFFFFFFFFFFF


def is_sentinel(stamp: int) -> bool:
    """Return ``True`` if ``stamp`` is the never-expires sentinel."""
    return stamp == SENTINEL_NEVER


def encode_ttl_value(expires_at_ms: int, value: bytes) -> bytes:
    """
    Prefix a serialized main-CF value with its 8-byte big-endian expiry stamp.

    :param expires_at_ms: absolute event-time expiry in milliseconds, or
        :data:`SENTINEL_NEVER` for entries that should never expire.
    :param value: already-serialized value bytes.
    :return: stamped blob suitable for writing to the main CF.
    """
    return _stamp_pack(expires_at_ms) + value


def decode_ttl_value(blob: bytes) -> tuple[int, bytes]:
    """
    Strip the 8-byte big-endian expiry stamp from a TTL main-CF blob.

    :param blob: bytes previously produced by :func:`encode_ttl_value`.
    :return: ``(expires_at_ms, value_bytes)``. ``expires_at_ms`` equals
        :data:`SENTINEL_NEVER` for "never expires" entries.
    :raises ValueError: if the blob is shorter than the stamp prefix.
    """
    if len(blob) < TTL_STAMP_BYTES:
        raise ValueError(
            f"TTL-stamped value is shorter than {TTL_STAMP_BYTES} bytes; "
            "the store may have been opened with TTL enabled on data that "
            "was originally written without TTL"
        )
    (expires_at_ms,) = _stamp_unpack_from(blob, 0)
    return expires_at_ms, blob[TTL_STAMP_BYTES:]


def encode_index_key(expires_at_ms: int, user_key: bytes) -> bytes:
    """
    Build a sortable index-CF key: ``expires_at_be || user_key``.

    Sorting on the encoded key gives ``(expires_at, user_key)`` order, which
    means a forward iterator on the index column family naturally yields
    the oldest expiries first.

    :param expires_at_ms: absolute event-time expiry, in milliseconds. Must
        not be :data:`SENTINEL_NEVER` — sentinel-stamped entries skip the
        index entirely.
    :param user_key: serialized user key (already prefix-encoded by the
        transaction layer).
    :return: index-CF key bytes.
    """
    return _stamp_pack(expires_at_ms) + user_key


def decode_index_key(blob: bytes) -> tuple[int, bytes]:
    """
    Inverse of :func:`encode_index_key`.

    :param blob: bytes previously produced by :func:`encode_index_key`.
    :return: ``(expires_at_ms, user_key_bytes)``.
    :raises ValueError: if the blob is too short to contain the expiry stamp.
    """
    if len(blob) < TTL_STAMP_BYTES:
        raise ValueError(f"TTL index key is shorter than {TTL_STAMP_BYTES} bytes")
    (expires_at_ms,) = _stamp_unpack_from(blob, 0)
    return expires_at_ms, blob[TTL_STAMP_BYTES:]
