import logging
from datetime import timedelta
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, NoReturn, Optional, Set, Union, cast

from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.exceptions import StateMigrationError, StateSerializationError
from quixstreams.state.metadata import Marker
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc, append_integer

from .exceptions import IncompatibleStateStoreError
from .metadata import (
    GLOBAL_COUNTER_CF_NAME,
    GLOBAL_COUNTER_KEY,
    TTL_ENABLED_KEY,
    TTL_FORCE_FLIP_ENV_VAR,
    TTL_INDEX_CF_NAME,
    TTL_ROLLBACK_ENV_VAR,
)
from .ttl_codec import (
    _MAX_PLAUSIBLE_STAMP_MS,
    SENTINEL_NEVER,
    TTL_STAMP_BYTES,
    clamp_additive_expiry,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)

if TYPE_CHECKING:
    from .partition import RocksDBStorePartition

__all__ = ("RocksDBPartitionTransaction",)

logger = logging.getLogger(__name__)

MAX_UINT64 = 2**64 - 1  # 18446744073709551615

# ``_MAX_PLAUSIBLE_STAMP_MS`` is the single source of truth for the read-side
# strict validator and the write-side reject; it lives in ``ttl_codec`` next to
# the codec and is re-exported here so existing importers of
# ``quixstreams.state.rocksdb.transaction._MAX_PLAUSIBLE_STAMP_MS`` keep working.


def _safe_decode_stamp(value: bytes) -> Optional[tuple[int, bytes]]:
    """
    Strict, fail-safe stamp validator for the flipped-partition read path.

    Returns ``(stamp, payload)`` ONLY when ``value`` robustly looks like a real
    TTL stamp, else ``None`` so the caller can degrade (return the value RAW as
    never-expires) instead of unconditionally stripping 8 bytes off a value that
    may be a genuine un-stamped legacy payload.

    "Robustly looks like a stamp" means:

    - ``len(value) >= TTL_STAMP_BYTES`` (an 8-byte stamp prefix can exist), and
    - the leading 8 big-endian bytes decode to either ``SENTINEL_NEVER`` or a
      plausible epoch-ms expiry ``0 < stamp < 10**15``.

    A genuine stamp is always accepted: real stamps are ``>= 8`` bytes and carry
    either the sentinel or an expiry well below the plausibility cap, so no
    genuinely-stamped value is ever mis-read as legacy. The residual is
    the safe-direction one: a legacy value whose first 8 bytes coincidentally
    decode to a plausible expiry is still mis-treated as stamped — but the
    backfill completeness guarantee means a fully backfilled store has no
    un-stamped values left to mis-classify, so this only bites stores written
    before the backfill fix.
    """
    if len(value) < TTL_STAMP_BYTES:
        return None
    try:
        stamp, payload = decode_ttl_value(value)
    except ValueError:
        return None
    if stamp == SENTINEL_NEVER:
        return stamp, payload
    if 0 < stamp < _MAX_PLAUSIBLE_STAMP_MS:
        return stamp, payload
    return None


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
        # boolean OR maintained per ``set`` — the hot-path branch.
        self._batch_has_ttl_writes: bool = False
        # Per-key TTL stamps recorded on a not-yet-flipped partition so the
        # flush path can re-encode default-CF cache entries if the flip
        # succeeds. Map shape: ``{(prefix, serialized_key): stamp_int}``.
        # Keys present in ``self._update_cache`` but absent from this map
        # are treated as sentinel (un-stamped legacy semantics) at flip
        # time. Untouched on flipped-from-start transactions (no extra work
        # for the steady-state TTL store).
        self._pending_stamps: dict[tuple[bytes, bytes], int] = {}
        # Max ``ttl=`` duration (ms) among the default-CF TTL writes staged on a
        # not-yet-flipped partition. When a populated legacy store
        # flips WITHOUT ``legacy_records_ttl`` configured, this is the implicit
        # legacy ttl fed into ``high_water + implicit_ttl`` — the triggering
        # batch's own declared window. ``None`` until the first ``ttl=`` write
        # lands; only maintained on the unflipped write path, so a steady-state
        # TTL store never touches it.
        self._max_batch_ttl_ms: Optional[int] = None

        # Set True by :meth:`_maybe_corroborate_adoption` when THIS transaction
        # corroborates a provisional cold adoption. Consulted after
        # ``super().prepare()`` succeeds to run the DEFERRED, irreversible backup-CF
        # teardown — never before the commit barrier, so an aborted
        # prepare leaves the rollback backup intact.
        self._corroborated_this_tx: bool = False

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
            # A ``state.set(...)`` with no explicit ``ttl=`` always writes the
            # never-expires sentinel — TTL is strictly per-write. This matches
            # the v3.23.6 semantics: ``legacy_records_ttl`` does NOT
            # impose a store-wide default on steady-state writes. (The
            # no-``ttl=`` floor was removed by design — ``legacy_records_ttl``
            # remains ONLY a one-time migration knob for pre-existing legacy
            # records, applied via the backfill path, never on new writes.)
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
        expiry = timestamp + _ttl_to_ms(ttl)
        if expiry <= 0:
            # Reject a NON-POSITIVE
            # expiry loudly at the source. A negative expiry (pre-epoch / negative
            # event-time) cannot be represented by the unsigned stamp encoder at
            # all. An expiry of EXACTLY 0 (e.g. timestamp=-1 + ttl=1ms) does encode
            # as ``\x00`` * 8, but the strict read validator (``_safe_decode_stamp``)
            # only accepts ``0 < stamp``, so a stamp-0 value reads back as raw bytes
            # -> StateSerializationError on every read and never sweeps. We reject
            # at write time rather than widen the read validator to ``0 <=`` on
            # purpose: the ``0 <`` guard is what stops a genuine un-stamped legacy
            # value whose first 8 bytes happen to be ``\x00`` * 8 from being
            # mis-read as an expire-at-epoch stamp and silently stripped/swept.
            # Caught by the validate-before-stage + FAILED handling, so nothing
            # is committed. We reject rather than clamp: a clamped expiry would
            # silently change the record's TTL semantics.
            raise ValueError(
                f"ttl=... produced a non-positive expiry ({expiry} ms) from "
                f"timestamp={timestamp} and ttl={ttl!r}; a pre-epoch, negative, or "
                "exactly-zero (epoch-ms 0) event-time expiry cannot be TTL-stamped."
            )
        if expiry >= _MAX_PLAUSIBLE_STAMP_MS:
            # Symmetric upper bound: a stamp this large
            # encodes fine (``< 2**64-1``) but the strict read validator
            # (``_safe_decode_stamp``) refuses it on every read, so the record
            # would be permanently unreadable (StateSerializationError per key)
            # and its index entry sweep-stranded. Reject at write time with the
            # same ValueError class as the negative-expiry / bad-ttl rejects
            # above (the codec/decode layer keeps StateSerializationError).
            raise ValueError(
                f"ttl=... produced an implausible expiry ({expiry} ms >= "
                f"{_MAX_PLAUSIBLE_STAMP_MS}) from timestamp={timestamp} and "
                f"ttl={ttl!r}; this exceeds the maximum representable TTL stamp "
                "and would be unreadable. Check for a nanosecond/second vs "
                "millisecond timestamp mistake or an unbounded ttl."
            )
        return expiry

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
        #
        # Compute + validate the stamp BEFORE staging the
        # value (``super().set_bytes`` below), and fail the transaction on a stamp
        # ValueError (bad ttl, missing timestamp, negative expiry).
        # Otherwise a caught ValueError leaves the already-staged value behind to
        # commit as a stray never-expiring legacy record. The TTL bookkeeping is
        # grouped with the validated stamp so nothing is staged on failure.
        if ttl is not None:
            try:
                stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            except ValueError:
                self._status = PartitionTransactionStatus.FAILED
                raise
            # Only a write that can ANCHOR the flip may trigger it. A negative
            # event-time (Kafka NO_TIMESTAMP is -1) is not a position in event
            # time, and ``advance_high_water`` ignores it, so such a write would
            # mark the batch as flip-triggering while leaving the high-water
            # unset -- and the backfill then raises IncompatibleStateStoreError
            # out of the flush, uncommitted and redelivered forever. The write
            # itself still succeeds and is still re-stamped if some OTHER write
            # in this batch carries a real timestamp and flips the store; only
            # the flip trigger is withheld.
            if timestamp is not None and timestamp >= 0:
                self._batch_has_ttl_writes = True
            self._track_batch_ttl_ms(ttl)
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            # Advance the high-water on TTL writes only — we don't want a
            # plain ``state.set(k, v)`` on an unflipped store to start
            # writing the TTL high-water marker (legacy stores must stay
            # marker-free).
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)
        elif self._pending_stamps:
            # Last-write-wins. A plain (no-ttl) write
            # of a key that had an earlier ttl= write staged in this same
            # unflipped batch must clear that key's pending stamp; otherwise the
            # flip stamps the final never-expires value with the stale finite
            # expiry and the next sweep deletes a record whose last write asked
            # for never-expires. Gated on a non-empty map so the pure-legacy hot
            # path pays no key-serialization cost.
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps.pop((prefix, key_serialized), None)

        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

        super().set_bytes(
            key=key, value=value_serialized, prefix=prefix, cf_name="default"
        )

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

        # Validate the stamp BEFORE staging (see :meth:`set`).
        if ttl is not None:
            try:
                stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            except ValueError:
                self._status = PartitionTransactionStatus.FAILED
                raise
            # Only a write that can ANCHOR the flip may trigger it. A negative
            # event-time (Kafka NO_TIMESTAMP is -1) is not a position in event
            # time, and ``advance_high_water`` ignores it, so such a write would
            # mark the batch as flip-triggering while leaving the high-water
            # unset -- and the backfill then raises IncompatibleStateStoreError
            # out of the flush, uncommitted and redelivered forever. The write
            # itself still succeeds and is still re-stamped if some OTHER write
            # in this batch carries a real timestamp and flips the store; only
            # the flip trigger is withheld.
            if timestamp is not None and timestamp >= 0:
                self._batch_has_ttl_writes = True
            self._track_batch_ttl_ms(ttl)
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)
        elif self._pending_stamps:
            # Last-write-wins — clear a stale pending
            # stamp for this key (see :meth:`set`). Gated on a non-empty map.
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps.pop((prefix, key_serialized), None)

        super().set_bytes(key=key, value=value, prefix=prefix, cf_name="default")

    def delete(self, key: Any, prefix: bytes, cf_name: str = "default") -> None:
        # Keep ``_pending_stamps`` consistent with
        # the cache on the unflipped path. A deleted key carries no TTL intent, so
        # a delete()→set(no-ttl) sequence must not resurrect a stale pending stamp
        # at flip time; pop it here before delegating. Gated on a non-empty map
        # (always empty on the flipped inline path, so this is a no-op there).
        # ``super().delete`` performs the cache mutation and STARTED-status check.
        if self._pending_stamps:
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps.pop((prefix, key_serialized), None)
        super().delete(key=key, prefix=prefix, cf_name=cf_name)

    def _track_batch_ttl_ms(self, ttl: timedelta) -> None:
        """
        Record the maximum ``ttl=`` duration (ms) seen among default-CF TTL
        writes on a not-yet-flipped partition. Called only from the
        unflipped ``ttl is not None`` write branch — after ``_compute_stamp`` has
        already validated ``ttl > 0`` — so it adds no work to the hot non-TTL
        path and no work to a steady-state (already-flipped) TTL store.

        Feeds the implicit legacy expiry when a populated legacy store flips
        WITHOUT ``legacy_records_ttl`` configured: the leftover records are
        stamped at ``high_water + max(ttl=)``. Max is the safe choice — it never
        expires an old record earlier than any live write in the same batch. For
        dedup (one constant window) it is exactly that window.
        """
        ttl_ms = _ttl_to_ms(ttl)
        if self._max_batch_ttl_ms is None or ttl_ms > self._max_batch_ttl_ms:
            self._max_batch_ttl_ms = ttl_ms

    def _delete_stale_index_entry(
        self, key_serialized: bytes, prefix: bytes, new_stamp: int
    ) -> None:
        """
        Inline-delete a key's OLD ``__ttl_index__`` entry
        when it is refreshed with a new stamp, so a refresh-heavy store (dedup
        sliding window) does not accumulate ghost index entries and grow the
        below-cutoff index unboundedly. MUST be called BEFORE the new stamped
        value is staged (it reads the OLD value).

        Fast-path (free): a within-batch prior write of this key is already in the
        update cache — decode the old stamp from there, no disk read. Otherwise
        one point-get of the committed main value: a genuinely new key is a
        bloom-filter negative and a hot refresh key is a block-cache hit. No-op
        when there is no old value (new key), the old stamp is the sentinel
        (unindexed), or the stamp is unchanged. The ``__ttl_index__`` CF is
        LOCAL_ONLY, so the delete never reaches the changelog. The
        visit-budget sweep is retained as the backstop for pre-existing ghosts
        (older code, recovery/backfill re-stamps, externally-mutated entries).
        """
        cached = self._update_cache.get(
            key=key_serialized, prefix=prefix, cf_name="default"
        )
        if cached is Marker.DELETED:
            return
        if cached is Marker.UNDEFINED:
            committed = self._partition.get(key_serialized, cf_name="default")
            if committed is Marker.UNDEFINED:
                return
            old_raw = cast(bytes, committed)
        else:
            old_raw = cast(bytes, cached)
        decoded = _safe_decode_stamp(old_raw)
        if decoded is None:
            return
        old_stamp, _ = decoded
        if old_stamp == SENTINEL_NEVER or old_stamp == new_stamp:
            return
        self._update_cache.delete(
            key=encode_index_key(old_stamp, key_serialized),
            prefix=b"",
            cf_name=TTL_INDEX_CF_NAME,
        )

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
        # Fail the transaction on a stamp ValueError (parity with the
        # serialize-error path below and the unflipped write path). The stamp is
        # computed before any staging, so nothing is committed on failure.
        try:
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        except ValueError:
            self._status = PartitionTransactionStatus.FAILED
            raise
        if ttl is not None:
            # A genuine live ``ttl=`` write on an already-flipped partition (the
            # stamp is non-sentinel by construction). Record it so the
            # adoption-corroboration hook in :meth:`prepare` can confirm a provisional
            # cold-adoption. Harmless on a normal flipped store (the hook no-ops
            # unless ``_adopt_provisional`` is set).
            self._batch_has_ttl_writes = True
        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise
        key_serialized = self._serialize_key(key, prefix=prefix)
        # Delete the stale index entry BEFORE staging the new value.
        self._delete_stale_index_entry(key_serialized, prefix, stamp)
        stamped = encode_ttl_value(stamp, value_serialized)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
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
        try:
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        except ValueError:
            self._status = PartitionTransactionStatus.FAILED
            raise
        if ttl is not None:
            # See :meth:`_set_default_cf_stamped`: mark the batch as carrying a
            # live ttl= write so adoption corroboration can fire.
            self._batch_has_ttl_writes = True
        key_serialized = self._serialize_key(key, prefix=prefix)
        self._delete_stale_index_entry(key_serialized, prefix, stamp)
        stamped = encode_ttl_value(stamp, value)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
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
            raw_legacy = super()._get_bytes(
                key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
            )
            # Legacy (un-flipped) read path. Normally byte-identical to v3.23.6:
            # the value is returned exactly as stored. The one exception is a
            # store the open-time resolution ARMED as unreconciled: TTL
            # bookkeeping on disk, no ``__ttl_enabled__`` flag, and nothing that
            # flipped it -- either the automatic repair declined (see
            # ``RocksDBStorePartition._repair_unflagged_stamped_store``) or the
            # rollback lever turned a provisionally-adopted store legacy again
            # while leaving its adopted originals byte-identical (see
            # ``RocksDBStorePartition._rollback_provisional_adopt``). On such a
            # store a value that still decodes as a LIVE stamp is a genuine TTL
            # value being read in legacy mode: returning it hands ``8B||payload``
            # to the value deserializer, which is the sc-74843 crash loop
            # (2026-09-04: ``StateSerializationError`` / orjson "surrogates not
            # allowed" on every restart). Fail loud instead -- see
            # :meth:`_raise_unreconciled_stamped_read` for why this does not strip.
            #
            # The armed flag is checked FIRST and is an open-time snapshot that is
            # False for every store without TTL bookkeeping, so the 99% legacy read
            # path does not gain a single byte comparison. The liveness condition
            # (sentinel or a stamp still ahead of the wallclock) is what keeps a
            # genuine legacy ``set_bytes()`` dedup store -- whose values are
            # 8-byte big-endian PAST epoch-ms and therefore decode as plausible
            # stamps -- reading back verbatim rather than raising.
            if (
                cf_name == "default"
                and self._partition._ttl_stamped_reads_unflagged  # noqa: SLF001
                and isinstance(raw_legacy, bytes)
            ):
                decoded_legacy = _safe_decode_stamp(raw_legacy)
                if decoded_legacy is not None:
                    stamp_legacy = decoded_legacy[0]
                    now_ms = self._partition._now_ms()  # noqa: SLF001
                    if stamp_legacy == SENTINEL_NEVER or stamp_legacy > now_ms:
                        self._raise_unreconciled_stamped_read(raw_legacy, prefix)
            return raw_legacy

        if timestamp is not None:
            self._partition.advance_high_water(timestamp)

        raw = super()._get_bytes(
            key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
        )
        if raw is Marker.UNDEFINED or raw is Marker.DELETED:
            return raw

        raw_bytes = cast(bytes, raw)

        # Fail-safe decode: only
        # strip the 8-byte stamp when a strict validator confirms the prefix is
        # a real stamp. A genuine un-stamped legacy value in a flipped partition
        # (the live-incident corruption: a JSON value whose first 8 bytes are NOT
        # a plausible stamp) must NOT be mis-stripped — degrade by returning it
        # RAW (treat as never-expires), log once, never corrupt.
        #
        # A value SHORTER than the 8-byte prefix routes here too, and must: it
        # cannot be a stamp, so it is necessarily an un-stamped legacy payload —
        # precisely the class this fail-safe protects. ``_safe_decode_stamp``
        # already returns None for ``len < TTL_STAMP_BYTES``, so short values
        # degrade raw alongside long ones. An earlier length check used to return
        # ``Marker.UNDEFINED`` here, silently reading every sub-8-byte legacy
        # survivor (``1``, ``true``, ``null``, ``"ab"``) back as MISSING while its
        # long counterpart survived.
        decoded = _safe_decode_stamp(raw_bytes)
        if decoded is None:
            # Warn once PER PARTITION: the guard lives on the
            # partition, not this checkpoint transaction, so a persistently
            # degraded value logs a single WARNING for the partition's lifetime
            # instead of re-warning on every checkpoint.
            if not self._partition._unstamped_read_warned:  # noqa: SLF001
                self._partition._unstamped_read_warned = True  # noqa: SLF001
                logger.warning(
                    "Fail-safe TTL read: a flipped partition holds a value that "
                    "does not decode to a valid stamp (key prefix=%r) at "
                    "path=%s; returning it raw and treating it as never-expires "
                    "rather than stripping 8 bytes. This should be impossible on "
                    "a fully backfilled store and signals a store written before "
                    "the backfill-completeness fix or an externally-mutated value.",
                    prefix[:16],
                    getattr(self._partition, "path", "<memory>"),
                )
            return raw_bytes

        stamp, payload = decoded

        # Sentinel-stamped entries always pass; "no TTL" is the common case.
        if stamp == SENTINEL_NEVER:
            return payload

        now = self._partition.high_water_ms
        if now is not None and stamp <= now:
            return Marker.UNDEFINED

        return payload

    def _raise_unreconciled_stamped_read(self, raw: bytes, prefix: bytes) -> NoReturn:
        """
        Refuse a ``default``-CF read that returned a live TTL-stamped value on a
        store flagged LEGACY, with an actionable :class:`StateMigrationError`.

        **Why not strip the stamp here.** Stripping would mask a store-wide
        mis-flag: the partition would keep writing UN-stamped values (it is legacy,
        so ``_stamps_enabled`` is False) while reading stamped ones, so the store
        would drift further apart on every flush and no operator would ever learn.
        Worse, the strip cannot be proven safe per value: ``_safe_decode_stamp``
        documents its residual, a genuine legacy value whose first eight bytes
        happen to decode, and eight bytes removed from such a value are gone for
        good. Open time is the place where a flip IS decidable (the repair samples
        the default CF and persists the decision); by the time a single read gets
        here that decision has already been taken -- the repair looked and
        declined, or the rollback lever asserted legacy intent -- so a per-value
        guess here can only contradict it.

        **Why not return it raw.** That is precisely the crash being fixed: the
        caller deserializes ``8B||json`` and dies, with a traceback pointing at
        ``orjson`` rather than at the state store. A named error naming the lever
        turns an undiagnosable crash loop into a one-variable fix.

        The message carries the value LENGTH and the hex of the 8-byte stamp
        prefix, never the payload: a state value can hold user data, and this
        message goes to logs.

        The WARNING is rate-limited to once per partition (the offending value is
        persistent, so every read of it would otherwise re-log); the RAISE is not,
        because suppressing it would return stamped bytes to the caller.
        """
        if not self._partition._stamped_legacy_read_warned:  # noqa: SLF001
            self._partition._stamped_legacy_read_warned = True  # noqa: SLF001
            logger.warning(
                "Unreconciled TTL state at path=%s: a default-CF read returned a "
                "TTL-stamped value while the partition is flagged LEGACY. "
                "Refusing the read (see StateMigrationError) instead of handing "
                "the stamp to the value deserializer. Set "
                "RocksDBOptions(ttl_force_flip=True) (or %s=1) and restart to "
                "flip this store into TTL mode.",
                getattr(self._partition, "path", "<unknown>"),
                TTL_FORCE_FLIP_ENV_VAR,
            )
        raise StateMigrationError(
            f"State store at path="
            f"{getattr(self._partition, 'path', '<unknown>')!r} is flagged LEGACY "
            f"(no {TTL_ENABLED_KEY!r} in its metadata column family) but its "
            f"default column family holds TTL-stamped values: a read of key "
            f"prefix={prefix[:16]!r} returned {len(raw)} byte(s) whose 8-byte "
            f"prefix {raw[:TTL_STAMP_BYTES].hex()} decodes as a live expiry "
            f"stamp. Returning it would hand the stamp to the value deserializer "
            f"(the crash this guard replaces) and stripping it could truncate a "
            f"genuine legacy value, so neither is done. The store opened carrying "
            f"TTL bookkeeping -- an interrupted legacy-TTL migration, or a "
            f"v3.24.0 adoption -- and nothing flipped it: either the automatic "
            f"open-time repair found no live stamp in its sample of the default "
            f"CF, or the rollback lever (RocksDBOptions.ttl_rollback / "
            f"{TTL_ROLLBACK_ENV_VAR}=1) asserted legacy intent -- suppressing "
            f"that repair, or reverting an adoption while keeping its originals "
            f"byte-identical. Set RocksDBOptions(ttl_force_flip=True) (or "
            f"{TTL_FORCE_FLIP_ENV_VAR}=1) and restart to flip the store into TTL "
            f"mode -- clearing the rollback lever first if it is set, since the "
            f"two together refuse to open -- or clear this partition's state and "
            f"let it rebuild from the changelog."
        )

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
        # The TTL hooks run BEFORE ``super().prepare()`` (they stamp the cache the
        # parent then produces from). The base ``prepare()`` sets status=FAILED on
        # any exception from ``_prepare``, but a hook raising here would otherwise
        # leave the transaction STARTED — violating the base-class contract that a
        # raised ``prepare()`` transitions to FAILED. Mirror that contract for the
        # hooks explicitly: a failed flip / counter persist must fail
        # the transaction, not leave it reusable.
        try:
            self._persist_counter()
            self._maybe_flip_or_reject(processed_offsets=processed_offsets)
            self._maybe_corroborate_adoption()
            self._sweep_expired_into_cache_if_enabled()
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise
        super().prepare(processed_offsets=processed_offsets)
        # Run the DEFERRED, irreversible corroboration teardown (drop
        # the ``__ttl_adopt_backup__`` CF) ONLY after ``super().prepare()`` has
        # produced the changelog records. A producer error there raises above and
        # skips this, so the backup stays intact and rollback is still possible
        # after a failed prepare. A crash between here and the flush is reconciled at
        # the next open (the durable done-marker proves corroboration succeeded).
        if self._corroborated_this_tx:
            self._partition.finalize_corroboration_teardown()

    def _maybe_corroborate_adoption(self) -> None:
        """
        Adoption-corroboration hook. A live ``state.set(..., ttl=...)`` write (which
        sets ``_batch_has_ttl_writes`` and always produces a non-sentinel stamp)
        on a PROVISIONALLY cold-adopted partition confirms the adoption is genuine.
        Runs AFTER :meth:`_maybe_flip_or_reject` (so the runtime flip state is
        settled) and BEFORE the sweep (so the corroborating flush's sweep, now
        un-suppressed, can reclaim now-past adopted records). A plain ``set()``
        (sentinel, no ``ttl=``) never sets ``_batch_has_ttl_writes`` and so never
        corroborates — legacy apps do plain sets too. One-time per partition.
        """
        partition = self._partition
        if (
            self._batch_has_ttl_writes
            and partition.uses_ttl_stamps
            and partition._adopt_provisional  # noqa: SLF001
        ):
            partition.corroborate_adoption()
            # Defer the irreversible backup drop to after ``super().prepare()`` —
            # see :meth:`prepare`.
            self._corroborated_this_tx = True

    def _sweep_expired_into_cache_if_enabled(self) -> None:
        """
        Prepare-time TTL sweep on the ON path.

        Runs AFTER :meth:`_maybe_flip_or_reject` (so a flip's runtime state and
        freshly-stamped cache writes are visible to the guards) and BEFORE
        ``super().prepare()`` (so staged tombstones ride the same changelog batch
        as the user writes). Skipped when:

        - the escape hatch ``ttl_changelog_tombstones=False`` is set (the OFF path
          runs the local-only :meth:`RocksDBStorePartition._run_sweep` in
          ``write()`` instead), or
        - the partition is not flipped into TTL mode (no index to sweep), or
        - the update cache is empty — a read-only transaction never swept before
          (the sweep lived in ``write()``, which ``_flush`` skips on an empty
          cache), and gating here preserves that coupling and the empty-tx no-I/O
          optimization.

        The ``staged_*`` guard sets are the transaction cache's own pending writes
        — the identical source ``write()`` reads on the OFF path, just one phase
        earlier — so the #1129 same-flush protections are byte-identical.
        """
        partition = self._partition
        if not partition.ttl_changelog_tombstones:
            return
        if not partition.uses_ttl_stamps:
            return
        if self._update_cache.is_empty():
            return

        staged_default = {
            key
            for prefix_map in self._update_cache.get_updates("default").values()
            for key in prefix_map
        }
        staged_ttl_index = {
            key
            for prefix_map in self._update_cache.get_updates(TTL_INDEX_CF_NAME).values()
            for key in prefix_map
        }
        partition.sweep_expired_into_cache(
            self._update_cache, staged_default, staged_ttl_index
        )

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
           default CF is **populated** → **auto-finish the migration**:
           backfill every pre-existing record with a uniform expiry in
           bounded chunks (each chunk's VALUES persisted + produced before the
           next is read; peak memory is O(census key count) for the key list,
           see :meth:`RocksDBStorePartition.backfill_legacy_records`), then write
           the in-batch stamps and the flip metadata LAST. The uniform expiry
           source is:
           - ``legacy_records_ttl`` set → ``high_water + legacy_records_ttl``
             (explicit config always wins, unchanged);
           - ``legacy_records_ttl`` absent → ``high_water + max(ttl=)`` where
             ``max(ttl=)`` is the largest ``ttl=`` duration in the triggering
             batch (the *implicit* legacy ttl). A WARN precedes the flip to
             flag that the window was implicit and how to override it.

           The populated-no-config path no longer rejects: a later revision
           replaced the ``reject_ttl_on_populated_store`` hard-error with this
           auto-backfill (finish the migration rather than error).
           The one surviving hard-error is the ``high_water is None`` framework
           guard in :meth:`_legacy_expiry_from_ttl_ms`.

        :param processed_offsets: ``<topic: offset>`` of the latest processed
            message, forwarded to the chunked backfill for changelog headers.
        """
        if not self._batch_has_ttl_writes:
            return
        if self._partition.uses_ttl_stamps:
            return

        populated = self._partition.main_cf_has_user_data()
        legacy_records_ttl = self._partition.legacy_records_ttl

        restamped = 0
        # Set to the implicit ttl (ms) only on the populated-no-config path, so
        # the post-flip logging can emit the WARN with the derived window.
        implicit_ttl_ms: Optional[int] = None
        staged_default_keys: set[bytes] = set()
        if populated:
            # Backfill branch: re-stamp every pre-existing on-disk record with a
            # uniform expiry in bounded chunks (values chunk-bounded), producing
            # and committing each chunk before reading the next. The genuine
            # in-batch user writes are skipped here (passed as
            # ``staged_default_keys``) and re-stamped with their own true pending
            # stamp by ``_restamp_default_cf_cache_for_flip`` below. The flip
            # metadata is written LAST (flag-last ordering): a crash
            # before ``_write_flip_metadata_to_cache`` lands leaves the partition
            # legacy and the backfill re-runs cleanly (re-deriving the implicit
            # ttl from the resuming write — identical for a constant dedup window).
            for prefix_updates in self._update_cache.get_updates(
                cf_name="default"
            ).values():
                staged_default_keys.update(prefix_updates.keys())

            if legacy_records_ttl is not None:
                # Explicit config wins (unchanged): high_water + legacy_records_ttl.
                expires_at_ms = self._compute_legacy_expiry(legacy_records_ttl)
            else:
                # Implicit default: high_water + max(ttl=) in this batch.
                implicit_ttl_ms = self._max_batch_ttl_ms
                expires_at_ms = self._legacy_expiry_from_ttl_ms(implicit_ttl_ms)

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
        # Durable done-flag: staged into the ``__ttl_system__`` CF here. The base
        # ``_prepare`` produces that system CF LAST within this flip flush by
        # EXPLICIT ordering (NOT by staging order — the staged CF set is unordered;
        # see ``base/transaction.py``), so the marker is the last record produced.
        # On both the empty-store and the populated-backfill path the migration is
        # complete once this flush commits, so the marker records "done, never
        # redo" for any future cold rebuild.
        self._write_migration_done_marker_to_cache()
        # Flip the **runtime** flag now so the partition's write path takes
        # the TTL-aware branch (high-water persist, sweep, index-CF use).
        self._partition.uses_ttl_stamps = True
        self._partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        if populated:
            if implicit_ttl_ms is not None:
                # The window was derived from the triggering write, not
                # from config. WARN (downgraded from the removed ERROR) with the
                # count, the implicit duration, the resulting absolute expiry,
                # and how to override. Precedes the INFO flip-confirmation line.
                logger.warning(
                    "Enabled TTL on a populated legacy store WITHOUT "
                    "legacy_records_ttl configured: auto-backfilled %d "
                    "pre-existing record(s) with an implicit expiry of "
                    "high_water + %d ms (= %d), derived from the triggering "
                    "state.set(..., ttl=...) write. To choose a different "
                    "uniform window for legacy records, set "
                    "RocksDBOptions(legacy_records_ttl=timedelta(...)) and "
                    "redeploy. path=%s",
                    restamped,
                    implicit_ttl_ms,
                    expires_at_ms,
                    getattr(self._partition, "path", "<memory>"),
                )
            logger.info(
                "Backfilled %d legacy records and flipped state store "
                "partition into TTL mode path=%s",
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
        Compute the uniform expiry for backfilled legacy records from the
        **explicit** ``legacy_records_ttl`` config: ``enable_time +
        legacy_records_ttl`` in event-time milliseconds. The
        implicit path uses :meth:`_legacy_expiry_from_ttl_ms` directly with the
        batch-derived ttl instead.
        """
        return self._legacy_expiry_from_ttl_ms(_ttl_to_ms(legacy_records_ttl))

    def _legacy_expiry_from_ttl_ms(self, ttl_ms: Optional[int]) -> int:
        """
        Resolve ``enable_time + ttl_ms`` for the legacy backfill, shared by the
        explicit-config path (:meth:`_compute_legacy_expiry`) and the
        implicit path (``ttl_ms`` = max ``ttl=`` in the triggering batch).

        ``enable_time`` is the partition's event-time high-water. The TTL write
        that triggers the flip advanced the high-water with its own record
        timestamp (``set`` / ``set_bytes``), so the high-water also *is* that
        triggering record's timestamp in the normal single-write-at-flip case.

        If the high-water is ``None`` here, the triggering TTL write carried no
        record timestamp — which the ``ttl=`` validation in
        :meth:`_compute_stamp` should already have rejected — so we hard-error
        rather than invent a wall-clock expiry (this is the one loud
        error kept, because it signals a framework bug, not an
        operator misconfiguration). ``ttl_ms is None`` is the same class of
        should-be-unreachable framework bug: ``_batch_has_ttl_writes`` is True
        here, so at least one ``ttl=`` write set ``_max_batch_ttl_ms``.
        """
        enable_time_ms = self._partition.high_water_ms
        if enable_time_ms is None:
            raise IncompatibleStateStoreError(
                "Cannot backfill legacy records: no event-time high-water is "
                "available at flip (a ttl= write carried no record timestamp). "
                "This should have been rejected at the state.set(..., ttl=...) "
                "call site. Refusing to invent a wall-clock expiry."
            )
        if ttl_ms is None:
            raise IncompatibleStateStoreError(
                "Cannot backfill legacy records: no ttl= duration was recorded "
                "for the triggering batch. This is a framework invariant "
                "violation (a flip requires at least one ttl= write)."
            )
        # Clamp the ADDITIVE sum: unlike the per-write
        # ``_compute_stamp`` path — which rejects an implausible stamp as a caller
        # error — the backfill derives its expiry from ``high_water + ttl``, so an
        # individually valid ttl can still sum ``>= _MAX_PLAUSIBLE_STAMP_MS`` and
        # become unreadable. Keep such records never-expiring (still readable)
        # rather than rejecting the whole flip. Plausible sums pass through
        # unchanged.
        expiry_ms = enable_time_ms + ttl_ms
        clamped = clamp_additive_expiry(expiry_ms)
        if clamped != expiry_ms:
            logger.warning(
                "Legacy-TTL backfill expiry high_water(%d) + ttl(%d) = %d exceeds "
                "the maximum readable stamp (%d) at path=%s; clamping to "
                "never-expire (SENTINEL) so the backfilled records stay readable. "
                "Configure a smaller legacy_records_ttl for a finite window.",
                enable_time_ms,
                ttl_ms,
                expiry_ms,
                _MAX_PLAUSIBLE_STAMP_MS,
                getattr(self._partition, "path", "<memory>"),
            )
        return clamped

    def _restamp_default_cf_cache_for_flip(
        self, skip_keys: Optional[Set[bytes]] = None
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
        first stamped user writes (atomic flip semantics).

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

    def _write_migration_done_marker_to_cache(self) -> None:
        """
        Queue the durable "migration done" marker into the
        replicated ``__ttl_system__`` CF cache. Because that CF is NOT in
        ``LOCAL_ONLY_CFS``, the base ``_prepare`` produces it to the changelog
        (so a cold rebuild onto a fresh volume learns "done, never redo"), and
        the partition's ``write()`` also commits it to disk in the SAME flush as
        the flip metadata and the first stamped user writes. The base ``_prepare``
        produces the ``__ttl_system__`` CF LAST in this flush by explicit ordering
        (the staged CF set is unordered; see ``base/transaction.py``), so the
        marker is the last record produced — regardless of staging order.

        Local imports mirror :meth:`_write_flip_metadata_to_cache` to avoid a
        circular import with ``partition.py``.
        """
        from quixstreams.state.metadata import (
            TTL_MIGRATION_DONE_KEY,
            TTL_SYSTEM_CF_NAME,
        )
        from quixstreams.state.serialization import int_to_bytes

        from .metadata import STATE_FORMAT_VERSION

        self._update_cache.set(
            key=TTL_MIGRATION_DONE_KEY,
            value=int_to_bytes(STATE_FORMAT_VERSION),
            prefix=b"",
            cf_name=TTL_SYSTEM_CF_NAME,
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
