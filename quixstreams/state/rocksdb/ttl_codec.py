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
    "clamp_additive_expiry",
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

# Inclusive bounds of the unsigned 8-byte stamp domain. ``_stamp_pack`` is a
# ``>Q`` (unsigned) packer, so anything outside ``[0, 2**64-1]`` raises a raw
# ``struct.error``. ``SENTINEL_NEVER`` is the upper bound and stays valid.
_MIN_STAMP = 0
_MAX_STAMP = 0xFFFFFFFFFFFFFFFF

# Upper bound (exclusive) for a "plausible" epoch-ms expiry stamp: ~year 33658,
# far beyond any realistic event-time clock. Single source of truth for the
# read-side strict validator (``_safe_decode_stamp``), the write-side
# ``_compute_stamp`` reject (both backends), and ``RocksDBOptions`` validation,
# so a stamp that encodes fine (``< 2**64-1``) but exceeds this bound can never
# be written — such a value is refused symmetrically at write time rather than
# becoming a permanently-unreadable record. Lives here (with the codec) because
# the read-side validator conceptually belongs with the codec.
_MAX_PLAUSIBLE_STAMP_MS = 10**15


def is_sentinel(stamp: int) -> bool:
    """Return ``True`` if ``stamp`` is the never-expires sentinel."""
    return stamp == SENTINEL_NEVER


def clamp_additive_expiry(expiry_ms: int) -> int:
    """
    Clamp an *additive* legacy-backfill / recovery-completion expiry
    (``enable_time + ttl``) to :data:`SENTINEL_NEVER` when it would land
    ``>= _MAX_PLAUSIBLE_STAMP_MS`` and therefore be refused by the read-side
    strict validator (``_safe_decode_stamp``) on every subsequent read.

    The per-write ``_compute_stamp`` path *rejects* an implausible stamp — there
    it is a caller error. The backfill / completion paths instead derive the
    expiry additively from an operator-supplied ``legacy_records_ttl`` (or the
    batch-implicit ttl) plus the enable-time high-water, so an individually
    plausible ttl can still sum past the readable bound. Rejecting there would
    strand the entire migration (and every legacy record with it); instead the
    record is kept never-expiring — still readable, and never mass-deleted,
    honoring the no-state-reset guarantee. Callers emit a WARN when the clamp
    fires. Returns ``expiry_ms`` unchanged when it is already below the bound.
    """
    if expiry_ms >= _MAX_PLAUSIBLE_STAMP_MS:
        return SENTINEL_NEVER
    return expiry_ms


def _check_stamp_range(expires_at_ms: int) -> None:
    """Reject a stamp outside the unsigned 8-byte domain with a descriptive
    ``ValueError`` BEFORE it reaches the ``>Q`` packer (#12, review batch 3).

    A negative expiry (Kafka ``NO_TIMESTAMP = -1`` / pre-epoch event-time flowing
    through ``timestamp + ttl``) or an out-of-range value would otherwise raise a
    bare ``struct.error`` that recurs on every replay of the offending record — a
    crash-loop with no diagnosable cause. Callers that compute expiries
    (``_compute_stamp``) reject negatives earlier; this is defense-in-depth so any
    future caller gets a named, catchable error naming the offending value.
    """
    if not (_MIN_STAMP <= expires_at_ms <= _MAX_STAMP):
        raise ValueError(
            f"TTL expiry stamp {expires_at_ms} is outside the valid unsigned "
            f"8-byte range [{_MIN_STAMP}, {_MAX_STAMP}]; a negative expiry "
            "(e.g. from a pre-epoch event-time or Kafka NO_TIMESTAMP) or an "
            "out-of-range value cannot be encoded."
        )


def encode_ttl_value(expires_at_ms: int, value: bytes) -> bytes:
    """
    Prefix a serialized main-CF value with its 8-byte big-endian expiry stamp.

    :param expires_at_ms: absolute event-time expiry in milliseconds, or
        :data:`SENTINEL_NEVER` for entries that should never expire.
    :param value: already-serialized value bytes.
    :return: stamped blob suitable for writing to the main CF.
    :raises ValueError: if ``expires_at_ms`` is outside ``[0, 2**64-1]``.
    """
    _check_stamp_range(expires_at_ms)
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
    :raises ValueError: if ``expires_at_ms`` is outside ``[0, 2**64-1]``.
    """
    _check_stamp_range(expires_at_ms)
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
