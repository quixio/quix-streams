import logging
from datetime import timedelta
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast

from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.metadata import Marker
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc, append_integer

from .exceptions import IncompatibleStateStoreError
from .metadata import (
    GLOBAL_COUNTER_CF_NAME,
    GLOBAL_COUNTER_KEY,
    TTL_INDEX_CF_NAME,
)
from .ttl_codec import (
    SENTINEL_NEVER,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)

if TYPE_CHECKING:
    from .partition import RocksDBStorePartition

__all__ = ("RocksDBPartitionTransaction",)

logger = logging.getLogger(__name__)

MAX_UINT64 = 2**64 - 1  # 18446744073709551615


def _ttl_to_ms(ttl: timedelta) -> int:
    """Convert a strictly positive ``timedelta`` to integer milliseconds.

    Sub-millisecond TTLs are rounded *up* to 1 ms to keep ``ttl > 0`` honest
    after conversion. Caller is expected to validate ``ttl > timedelta(0)``.
    """
    micros = ttl.days * 86_400_000_000 + ttl.seconds * 1_000_000 + ttl.microseconds
    return -(-micros // 1000)


class RocksDBPartitionTransaction(PartitionTransaction[bytes, Any]):
    """
    Default RocksDB transaction.

    Implements the per-write TTL feature: every value written to the user-facing
    ``default`` column family is prefixed with an 8-byte big-endian millisecond
    expiry stamp. A ``state.set(key, value)`` call (no ``ttl=``) writes the
    sentinel ``SENTINEL_NEVER`` meaning "never expires"; ``state.set(key, value,
    ttl=timedelta(...))`` writes ``record.timestamp + ttl``. Non-sentinel writes
    also emit a ``(expires_at || serialized_user_key)`` entry to the local-only
    ``__ttl_index__`` column family so the bounded sweep on every flush can
    reclaim expired values.

    Subclasses (e.g. ``WindowedRocksDBPartitionTransaction``,
    ``TimestampedPartitionTransaction``) opt out by setting
    ``_uses_ttl_stamps = False`` on their partition so writes bypass the
    stamp / index machinery — those stores have their own retention model.
    """

    def __init__(
        self,
        partition: "RocksDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"] = None,
    ) -> None:
        super().__init__(
            partition=partition,
            dumps=dumps,
            loads=loads,
            changelog_producer=changelog_producer,
        )
        self._partition: RocksDBStorePartition = cast(
            "RocksDBStorePartition", self._partition
        )
        self._counter: Optional[int] = None

        # Set to True the first time a ``state.set(..., ttl=...)`` lands in
        # this transaction. Used by ``_flush`` to decide whether the batch
        # needs the flush-time detection / flip-or-reject step. Cheap O(1)
        # boolean OR maintained per ``set`` — the spec's hot-path branch.
        self._batch_has_ttl_writes: bool = False
        # Per-key TTL stamps recorded on a not-yet-flipped partition so the
        # flush path can re-encode default-CF cache entries if the flip
        # succeeds. Map shape: ``{(prefix, serialized_key): stamp_int}``.
        # Keys present in ``self._update_cache`` but absent from this map
        # are treated as sentinel (un-stamped legacy semantics) at flip
        # time. Untouched on flipped-from-start transactions (no extra work
        # for the steady-state TTL store).
        self._pending_stamps: dict[tuple[bytes, bytes], int] = {}

    # ------------------------------------------------------------------
    # TTL-aware write / read overrides.
    # ------------------------------------------------------------------

    def _stamps_enabled(self, cf_name: str) -> bool:
        """
        Return True if writes to ``cf_name`` should be TTL-stamped **inline**
        in :meth:`set` / :meth:`set_bytes`. Stamps are applied only on the
        user-facing default CF and only when the partition is already
        flipped into TTL mode at the moment the write is queued.

        For unflipped partitions (the 99% case) this returns False and the
        write goes through the legacy path; the flush-time detector decides
        whether the batch needs to flip and re-encode.
        """
        return cf_name == "default" and self._partition.uses_ttl_stamps

    def _compute_stamp(self, ttl: Optional[timedelta], timestamp: Optional[int]) -> int:
        if ttl is None:
            # Rule 3 (spec §0 / §6.7): once a partition is flipped into TTL
            # mode AND ``legacy_records_ttl`` is configured, a write with no
            # explicit ``ttl=`` is floored to ``event_time + legacy_records_ttl``
            # instead of ``SENTINEL_NEVER`` — so nothing ever-expires while the
            # feature is active in that partition.
            #
            # Gated on the **runtime** flip flag (Rule 1 activation gate): a
            # partition that never took a ``ttl=`` write is unflipped
            # (``uses_ttl_stamps`` is False) and falls straight through to the
            # sentinel, byte-identical to v3.23.6. Windowed / timestamped
            # partitions nail ``uses_ttl_stamps = False`` at the class level and
            # never reach this method on the stamped path, so they never floor.
            legacy_records_ttl = self._partition.legacy_records_ttl
            if self._partition.uses_ttl_stamps and legacy_records_ttl is not None:
                if timestamp is None:
                    # Cannot floor without an event-time to anchor the expiry.
                    # The State wrapper injects ``timestamp`` on every record
                    # (see ``state/base/state.py`` ``set``), so this is
                    # belt-and-suspenders; mirror ``_compute_stamp``'s existing
                    # "flooring requires event-time" stance rather than invent a
                    # wall-clock expiry, and fall back to never-expires.
                    return SENTINEL_NEVER
                return timestamp + _ttl_to_ms(legacy_records_ttl)
            return SENTINEL_NEVER
        if ttl <= timedelta(0):
            raise ValueError(f"ttl must be a positive timedelta or None, got {ttl!r}")
        if timestamp is None:
            raise ValueError(
                "ttl=... on state.set() requires the current record's "
                "event-time timestamp; the framework injects it via the "
                "stateful StreamingDataFrame wrapper. If you are calling "
                "state.set() outside an apply/update/filter callback (e.g. "
                "from a custom Source), use as_state(prefix, timestamp=...)."
            )
        return timestamp + _ttl_to_ms(ttl)

    def set(
        self,
        key: Any,
        value: Any,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
        ttl: Optional[timedelta] = None,
    ) -> None:
        if cf_name != "default" or not type(self._partition).uses_ttl_stamps:
            # Non-default CF, or a subclass-level opt-out (windowed,
            # timestamped). Always-legacy path.
            super().set(
                key=key,
                value=value,
                prefix=prefix,
                cf_name=cf_name,
                timestamp=timestamp,
                ttl=ttl,
            )
            return

        if self._partition.uses_ttl_stamps:
            # Already-flipped partition: stamp inline, exactly the v2 path.
            self._set_default_cf_stamped(
                key=key,
                value=value,
                prefix=prefix,
                timestamp=timestamp,
                ttl=ttl,
            )
            return

        # Unflipped partition. Defer the stamping decision to ``flush()``;
        # if no TTL writes ever land in this batch we keep the legacy
        # un-stamped layout (byte-identical to v3.23.6). If a TTL write
        # *does* land we flip the partition (or reject loudly) and the
        # flush path re-encodes the cache.
        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

        super().set_bytes(
            key=key, value=value_serialized, prefix=prefix, cf_name="default"
        )

        if ttl is not None:
            # Validate eagerly so the user gets a clear error at the call
            # site rather than from inside ``flush()``.
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            self._batch_has_ttl_writes = True
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            # Advance the high-water on TTL writes only — we don't want a
            # plain ``state.set(k, v)`` on an unflipped store to start
            # writing the TTL high-water marker (legacy stores must stay
            # marker-free).
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)

    def set_bytes(
        self,
        key: Any,
        value: bytes,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
        ttl: Optional[timedelta] = None,
    ) -> None:
        if cf_name != "default" or not type(self._partition).uses_ttl_stamps:
            super().set_bytes(
                key=key,
                value=value,
                prefix=prefix,
                cf_name=cf_name,
                timestamp=timestamp,
                ttl=ttl,
            )
            return

        if not isinstance(value, bytes):
            self._status = PartitionTransactionStatus.FAILED
            raise StateSerializationError("Value must be bytes")

        if self._partition.uses_ttl_stamps:
            self._set_bytes_default_cf_stamped(
                key=key,
                value=value,
                prefix=prefix,
                timestamp=timestamp,
                ttl=ttl,
            )
            return

        super().set_bytes(key=key, value=value, prefix=prefix, cf_name="default")

        if ttl is not None:
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            self._batch_has_ttl_writes = True
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)

    def _set_default_cf_stamped(
        self,
        key: Any,
        value: Any,
        prefix: bytes,
        timestamp: Optional[int],
        ttl: Optional[timedelta],
    ) -> None:
        """Stamp + cache write path used when the partition is flipped."""
        if timestamp is not None:
            self._partition.advance_high_water(timestamp)
        stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise
        stamped = encode_ttl_value(stamp, value_serialized)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._update_cache.set(
                key=encode_index_key(stamp, key_serialized),
                value=b"",
                prefix=b"",
                cf_name=TTL_INDEX_CF_NAME,
            )

    def _set_bytes_default_cf_stamped(
        self,
        key: Any,
        value: bytes,
        prefix: bytes,
        timestamp: Optional[int],
        ttl: Optional[timedelta],
    ) -> None:
        """``set_bytes`` variant of :meth:`_set_default_cf_stamped`."""
        if timestamp is not None:
            self._partition.advance_high_water(timestamp)
        stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        stamped = encode_ttl_value(stamp, value)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._update_cache.set(
                key=encode_index_key(stamp, key_serialized),
                value=b"",
                prefix=b"",
                cf_name=TTL_INDEX_CF_NAME,
            )

    def _get_bytes(
        self,
        key: Any,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
    ) -> Union[bytes, Literal[Marker.UNDEFINED, Marker.DELETED]]:
        if not self._stamps_enabled(cf_name):
            return super()._get_bytes(
                key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
            )

        if timestamp is not None:
            self._partition.advance_high_water(timestamp)

        raw = super()._get_bytes(
            key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
        )
        if raw is Marker.UNDEFINED or raw is Marker.DELETED:
            return raw

        try:
            stamp, payload = decode_ttl_value(cast(bytes, raw))
        except ValueError:
            # Malformed entry; treat as missing rather than crashing the
            # caller. The format-version guard at open time is the primary
            # line of defense; this branch is purely belt-and-suspenders.
            return Marker.UNDEFINED

        # Sentinel-stamped entries always pass; "no TTL" is the common case.
        if stamp == SENTINEL_NEVER:
            return payload

        now = self._partition.high_water_ms
        if now is not None and stamp <= now:
            return Marker.UNDEFINED

        return payload

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def exists(
        self,
        key: Any,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
    ) -> bool:
        if not self._stamps_enabled(cf_name):
            return super().exists(
                key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
            )
        result = self._get_bytes(
            key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
        )
        return result is not Marker.UNDEFINED and result is not Marker.DELETED

    # ------------------------------------------------------------------
    # Existing helpers (unchanged from pre-TTL behavior).
    # ------------------------------------------------------------------

    def _get_items(
        self,
        start: int,
        end: int,
        prefix: bytes,
        backwards: bool = False,
        cf_name: str = "default",
    ) -> list[tuple[bytes, bytes]]:
        """
        Get all items that start between `start` and `end`
        within the specified prefix.

        This function also checks the update cache for any updates not yet
        committed to RocksDB.

        :param start: Start of the range, inclusive.
        :param end: End of the range, exclusive.
        :param prefix: The key prefix for filtering items.
        :param backwards: If True, returns items in reverse order.
        :param cf_name: The RocksDB column family name.
        :return: A sorted list of key-value pairs.
        """
        start = max(start, 0)
        if start > end:
            return []

        seek_from_key = append_integer(base_bytes=prefix, integer=start)
        seek_to_key = append_integer(base_bytes=prefix, integer=end)

        # Create an iterator over the state store
        db_items = self._partition.iter_items(
            lower_bound=seek_from_key,
            upper_bound=seek_to_key,
            cf_name=cf_name,
        )

        cache = self._update_cache
        update_cache = cache.get_updates(cf_name=cf_name).get(prefix, {})
        delete_cache = cache.get_deletes(cf_name=cf_name)

        # Get cached updates with matching keys
        updated_items = (
            (key, value)
            for key, value in update_cache.items()
            if seek_from_key < key <= seek_to_key
        )

        # Iterate over stored and cached items and merge them to a single dict
        merged_items = {}
        for key, value in chain(db_items, updated_items):
            if key not in delete_cache:
                merged_items[key] = value

        # Sort items merged from the cache and store
        return sorted(merged_items.items(), key=lambda kv: kv[0], reverse=backwards)

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def prepare(self, processed_offsets: Optional[dict[str, int]] = None) -> None:
        """
        Persist the counter, run the flush-time TTL detection / flip-or-reject
        step (so the cache is stamped before the parent ``prepare()`` produces
        changelog records from it), then delegate to the parent.

        :param processed_offsets: the dict with <topic: offset> of the latest processed message
        """
        self._persist_counter()
        self._maybe_flip_or_reject(processed_offsets=processed_offsets)
        super().prepare(processed_offsets=processed_offsets)

    def _maybe_flip_or_reject(
        self, processed_offsets: Optional[dict[str, int]] = None
    ) -> None:
        """
        Flush-time TTL detection.

        Four terminal cases:

        1. Partition is already flipped → no work; the cache was stamped
           inline by :meth:`set` / :meth:`set_bytes`.
        2. Partition is not flipped and the batch has no TTL writes → no
           work; the cache stays un-stamped (legacy layout, byte-identical
           to v3.23.6 on disk and on the changelog).
        3. Partition is not flipped, the batch has a TTL write, and the
           default CF is **empty** → flip (empty-store fast path). Every
           default-CF entry in the cache is re-encoded with its stamp,
           index entries are queued for non-sentinel writes, and metadata
           flag writes are added to the cache so the partition's ``write()``
           commits everything atomically.
        4. Partition is not flipped, the batch has a TTL write, and the
           default CF is **populated**:
           - if ``legacy_records_ttl`` is set → **backfill** every
             pre-existing record with a uniform ``high_water +
             legacy_records_ttl`` expiry in bounded chunks (each chunk
             persisted + produced before the next is read; peak memory ≈ one
             chunk), then write the in-batch stamps and the flip metadata LAST;
           - otherwise → reject loudly (with the operator-callable message
             pointing at ``legacy_records_ttl``).

        :param processed_offsets: ``<topic: offset>`` of the latest processed
            message, forwarded to the chunked backfill for changelog headers.
        """
        if not self._batch_has_ttl_writes:
            return
        if self._partition.uses_ttl_stamps:
            return

        populated = self._partition.main_cf_has_user_data()
        legacy_records_ttl = self._partition.legacy_records_ttl

        if populated and legacy_records_ttl is None:
            self._status = PartitionTransactionStatus.FAILED
            raise self._partition.reject_ttl_on_populated_store()

        restamped = 0
        staged_default_keys: set[bytes] = set()
        if populated:
            # Backfill branch: re-stamp every pre-existing on-disk record with a
            # uniform expiry in bounded chunks (memory ≈ one chunk), producing
            # and committing each chunk before reading the next. The genuine
            # in-batch user writes are skipped here (passed as
            # ``staged_default_keys``) and re-stamped with their own true pending
            # stamp by ``_restamp_default_cf_cache_for_flip`` below. The flip
            # metadata is written LAST (flag-last ordering, spec §3.3): a crash
            # before ``_write_flip_metadata_to_cache`` lands leaves the partition
            # legacy and the backfill re-runs cleanly.
            for prefix_updates in self._update_cache.get_updates(
                cf_name="default"
            ).values():
                staged_default_keys.update(prefix_updates.keys())

            expires_at_ms = self._compute_legacy_expiry(legacy_records_ttl)
            restamped = self._partition.backfill_legacy_records(
                expires_at_ms=expires_at_ms,
                changelog_producer=self._changelog_producer,
                processed_offsets=processed_offsets,
                staged_default_keys=staged_default_keys,
                chunk_size=self._partition.legacy_backfill_chunk_size,
            )

        # Re-encode the in-batch cache with stamps + flip the partition flag in
        # the same logical batch. The actual on-disk ``WriteBatch`` is built
        # later in ``RocksDBStorePartition.write``; we mutate the transaction
        # cache here so both the changelog producer (in ``super().prepare()``)
        # and the partition writer see the stamped values. The pre-existing
        # records have already been persisted+produced by the chunk loop above,
        # so only the (small) in-batch keys flow through the cache here, and
        # every one of them must be stamped with its own pending stamp — there
        # are no pre-existing backfilled keys left in the cache to skip.
        self._restamp_default_cf_cache_for_flip()
        self._write_flip_metadata_to_cache()
        # Flip the **runtime** flag now so the partition's write path takes
        # the TTL-aware branch (high-water persist, sweep, index-CF use).
        self._partition.uses_ttl_stamps = True
        self._partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        if populated:
            logger.info(
                "Backfilled %d legacy records and flipped state store "
                "partition into TTL mode (legacy_records_ttl) path=%s",
                restamped,
                getattr(self._partition, "path", "<memory>"),
            )
        else:
            logger.info(
                "Flipping state store partition into TTL mode (empty-store "
                "fast path) path=%s",
                getattr(self._partition, "path", "<memory>"),
            )

    def _compute_legacy_expiry(self, legacy_records_ttl: timedelta) -> int:
        """
        Compute the uniform expiry for backfilled legacy records:
        ``enable_time + legacy_records_ttl`` in event-time milliseconds
        (spec §8.1).

        ``enable_time`` is the partition's event-time high-water. The TTL
        write that triggers the flip advanced the high-water with its own
        record timestamp (``transaction.py`` ``set`` / ``set_bytes``), so the
        high-water also *is* that triggering record's timestamp in the normal
        single-write-at-flip case. If the high-water is ``None`` here, the
        triggering TTL write carried no record timestamp — which the ``ttl=``
        validation in :meth:`_compute_stamp` should already have rejected — so
        we hard-error rather than invent a wall-clock expiry (spec §8.1).
        """
        ttl_ms = _ttl_to_ms(legacy_records_ttl)
        enable_time_ms = self._partition.high_water_ms
        if enable_time_ms is None:
            raise IncompatibleStateStoreError(
                "Cannot backfill legacy records: no event-time high-water is "
                "available at flip (a ttl= write carried no record timestamp). "
                "This should have been rejected at the state.set(..., ttl=...) "
                "call site. Refusing to invent a wall-clock expiry."
            )
        return enable_time_ms + ttl_ms

    def _restamp_default_cf_cache_for_flip(
        self, skip_keys: "Optional[set[bytes]]" = None
    ) -> None:
        """
        Re-encode every default-CF cache entry with its TTL stamp, queue
        index entries for non-sentinel writes, and persist the partition's
        high-water if the batch advanced it.

        Called from :meth:`_maybe_flip_or_reject` on both the empty-store flip
        path and the backfill path. The cache walk is O(batch); typical
        first-flush batches are tens to hundreds of entries.

        With the chunked backfill the pre-existing on-disk records are persisted
        and produced directly by
        :meth:`RocksDBStorePartition.backfill_legacy_records` and never enter
        this cache, so the cache holds only the genuine in-batch user writes —
        all of which must be stamped. ``skip_keys`` is therefore unused by the
        current caller and retained only as a defensive hook.

        :param skip_keys: optional serialized default-CF keys to leave
            untouched (not re-stamped). The current caller passes none.
        """
        skip_keys = skip_keys or set()
        cache = self._update_cache
        updates = cache.get_updates(cf_name="default")
        # Snapshot prefixes/keys before mutating; we update in place.
        prefixes = list(updates.keys())
        for prefix in prefixes:
            entries = list(updates[prefix].items())
            for serialized_key, raw_value in entries:
                if serialized_key in skip_keys:
                    continue
                stamp = self._pending_stamps.get(
                    (prefix, serialized_key), SENTINEL_NEVER
                )
                stamped = encode_ttl_value(stamp, raw_value)
                cache.set(
                    key=serialized_key,
                    value=stamped,
                    prefix=prefix,
                    cf_name="default",
                )
                if stamp != SENTINEL_NEVER:
                    cache.set(
                        key=encode_index_key(stamp, serialized_key),
                        value=b"",
                        prefix=b"",
                        cf_name=TTL_INDEX_CF_NAME,
                    )

    def _write_flip_metadata_to_cache(self) -> None:
        """
        Queue ``__ttl_enabled__`` and ``__ttl_format_version__`` writes into
        the metadata CF cache so they land in the same WriteBatch as the
        first stamped user writes (atomic flip semantics required by spec
        §6.4 and §6.6).

        We import locally to avoid a circular import with ``partition.py``.
        """
        from quixstreams.state.metadata import METADATA_CF_NAME
        from quixstreams.state.serialization import int_to_bytes

        from .metadata import (
            STATE_FORMAT_VERSION,
            STATE_FORMAT_VERSION_KEY,
            TTL_ENABLED_KEY,
        )

        self._update_cache.set(
            key=TTL_ENABLED_KEY,
            value=b"\x01",
            prefix=b"",
            cf_name=METADATA_CF_NAME,
        )
        self._update_cache.set(
            key=STATE_FORMAT_VERSION_KEY,
            value=int_to_bytes(STATE_FORMAT_VERSION),
            prefix=b"",
            cf_name=METADATA_CF_NAME,
        )

    def _increment_counter(self) -> int:
        """
        Increment the global counter.

        The counter will reset to 0 if it reaches the maximum unsigned 64-bit
        integer value (18446744073709551615) to prevent overflow.

        :return: Next sequential counter value
        """
        if self._counter is None:
            self._counter = self.get(
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                default=-1,
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
        self._counter = self._counter + 1 if self._counter < MAX_UINT64 else 0
        return self._counter

    def _persist_counter(self) -> None:
        if self._counter is not None:
            self.set(
                value=self._counter,
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
