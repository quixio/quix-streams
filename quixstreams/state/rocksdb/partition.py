import logging
import time
from threading import Event
from typing import (
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Union,
    cast,
)

from rocksdict import AccessType, ColumnFamily, Rdict, ReadOptions, WriteBatch

from quixstreams.state.base import (
    PartitionTransactionCache,
    StorePartition,
)
from quixstreams.state.metadata import LOCAL_ONLY_CFS, METADATA_CF_NAME, Marker
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.transaction import RocksDBPartitionTransaction
from quixstreams.state.serialization import int_from_bytes, int_to_bytes

from .exceptions import (
    IncompatibleStateStoreError,
    RocksDBCorruptedError,
    RocksDBOpenAborted,
)
from .metadata import (
    CHANGELOG_OFFSET_KEY,
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
)
from .options import RocksDBOptions
from .ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)
from .types import RocksDBOptionsType

__all__ = ("RocksDBStorePartition",)


logger = logging.getLogger(__name__)


class RocksDBStorePartition(StorePartition):
    """
    A base class to access state in RocksDB.
    It represents a single RocksDB database.

    Responsibilities:
     1. Managing access to the RocksDB instance
     2. Creating transactions to interact with data
     3. Flushing WriteBatches to the RocksDB
     4. Maintaining the per-write TTL machinery (8-byte expiry stamp on every
        value in the user-facing default CF, secondary expiry index in
        ``__ttl_index__``, partition high-water mark, bounded sweep on
        flush, recovery filter / index rebuild on changelog replay).

    It opens the RocksDB on `__init__`. If the db is locked by another process,
    it will retry according to `open_max_retries` and `open_retry_backoff` options.

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    """

    # Class-level switch that subclasses with their own retention model
    # (windowed, timestamped) flip to ``False`` to permanently opt out of the
    # TTL stamp machinery. For general-purpose partitions this stays ``True``
    # at the class level; the **per-instance** ``uses_ttl_stamps`` (set in
    # ``__init__``) decides whether the machinery is *active* — it starts
    # ``False`` on a fresh / legacy store and flips to ``True`` only when a
    # ``state.set(..., ttl=...)`` write is detected at flush time on an empty
    # default CF (see :meth:`_flip_into_ttl_mode`). This keeps no-TTL
    # workloads byte-identical to v3.23.6 on disk and on the changelog.
    uses_ttl_stamps: bool = True

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
        stop_event: Optional[Event] = None,
    ):
        if not options:
            options = RocksDBOptions()

        super().__init__(options.dumps, options.loads, changelog_producer)
        self._path = path
        self._options = options
        self._stop_event = stop_event
        self._rocksdb_options = self._options.to_options()
        self._open_max_retries = self._options.open_max_retries
        self._open_retry_backoff = self._options.open_retry_backoff
        self._max_evictions_per_flush = self._options.max_evictions_per_flush
        self._db = self._init_rocksdb()
        self._cf_cache: Dict[str, Rdict] = {}
        self._cf_handle_cache: Dict[str, ColumnFamily] = {}
        self._high_water_ms: Optional[int] = None

        # Resolve the **runtime** TTL flag. Subclasses that nail
        # ``uses_ttl_stamps = False`` at the class level (windowed,
        # timestamped) stay opted-out forever. Otherwise we read the
        # persisted ``__ttl_enabled__`` flag from the metadata CF: absent
        # means "legacy mode, behave like v3.23.6"; present-and-truthy means
        # "this partition flipped into TTL mode in a previous flush".
        class_uses_ttl_stamps = type(self).uses_ttl_stamps
        if class_uses_ttl_stamps:
            self.uses_ttl_stamps = self._load_ttl_enabled_flag()
        else:
            self.uses_ttl_stamps = False

        if self.uses_ttl_stamps:
            # Already-flipped store: validate the on-disk format and warm up
            # the TTL bookkeeping (high-water, index CF). For legacy stores
            # (the 99% case) we skip every line of this block — no extra CF
            # is created, no extra metadata read happens beyond the single
            # ``__ttl_enabled__`` probe above.
            self._enforce_format_version()
            self._load_high_water()
            # Pre-create the index CF so writes never race a CF creation.
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)

    @property
    def high_water_ms(self) -> Optional[int]:
        """
        Highest record event-time observed by any transaction on this
        partition since the process started, or ``None`` for cold start.
        """
        return self._high_water_ms

    @property
    def max_evictions_per_flush(self) -> int:
        """Cap on per-flush sweep evictions."""
        return self._max_evictions_per_flush

    def advance_high_water(self, timestamp: Optional[int]) -> None:
        """
        Advance the partition's high-water mark monotonically. Called by the
        transaction layer on every TTL-aware ``set`` / ``get`` that carries a
        timestamp. Late-arriving timestamps never roll the high-water back.
        """
        if timestamp is None:
            return
        if self._high_water_ms is None or timestamp > self._high_water_ms:
            self._high_water_ms = timestamp

    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        cf_handle = self.get_column_family_handle(cf_name)
        batch = WriteBatch(raw_mode=True)

        # Recovery flag-discovery (spec §6.6).
        #
        # The ``__ttl_enabled__`` key lives in the metadata CF, which is in
        # ``LOCAL_ONLY_CFS`` and therefore never produced to the changelog
        # topic — so a cold-restore recovery cannot read the flag from a
        # changelog message. We instead infer the partition's TTL mode from
        # the **first user-data message replayed**: if its value is
        # stamped (8-byte BE prefix decodable as the sentinel or a
        # plausible epoch-ms expiry), the source partition was flipped, so
        # we flip this recovery partition too and stay in TTL mode for the
        # rest of replay. This is the "peek at the first replayed value"
        # design called out as the fallback in spec §8.
        if (
            type(self).uses_ttl_stamps
            and not self.uses_ttl_stamps
            and cf_name == "default"
            and value
            and self._looks_like_stamped_value(value)
        ):
            logger.info(
                "Recovery: detected stamped default-CF replay; flipping "
                "partition path=%s into TTL mode for the rest of recovery.",
                self._path,
            )
            self.uses_ttl_stamps = True
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            # Stamp the on-disk format-version + flag now so a subsequent
            # process restart picks up TTL mode at open time.
            self._stamp_flip_metadata()

        if not self.uses_ttl_stamps:
            # Legacy / non-TTL partitions: replay the raw payload verbatim.
            # Identical to v3.23.6 behavior — no stamp wrapping, no index
            # rebuild, no recovery filter.
            if value is None:
                batch.delete(key, cf_handle)
            else:
                batch.put(key, value, cf_handle)
            self._update_changelog_offset(batch=batch, offset=offset)
            self._write(batch)
            return

        if cf_name in LOCAL_ONLY_CFS:
            # Local-only CFs should never appear on the changelog topic, but
            # if a bogus message arrives, ignore the payload and just roll
            # the offset forward.
            self._update_changelog_offset(batch=batch, offset=offset)
            self._write(batch)
            return

        # Only the user-facing "default" CF carries stamped values. Any other
        # CF (metadata, global counter, etc.) is replayed verbatim.
        is_main_cf = cf_name == "default"

        if value is None:
            batch.delete(key, cf_handle)
        elif is_main_cf:
            stamped, stamp = self._normalize_replay_value(value)
            recovery_now = self._high_water_ms
            if (
                stamp != SENTINEL_NEVER
                and recovery_now is not None
                and stamp <= recovery_now
            ):
                # Already-expired entry; skip both the main and the index
                # write. Roll the changelog offset forward so recovery
                # progresses.
                pass
            else:
                batch.put(key, stamped, cf_handle)
                if stamp != SENTINEL_NEVER:
                    index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
                    batch.put(encode_index_key(stamp, key), b"", index_handle)
        else:
            batch.put(key, value, cf_handle)

        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

        # Track the highest stamp seen as a recovery high-water — this is the
        # only event-time signal available on a fresh partition. Sentinel
        # stamps don't advance it.
        if value is not None and is_main_cf:
            _, stamp = self._normalize_replay_value(value)
            if stamp != SENTINEL_NEVER:
                self.advance_high_water(stamp)

    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
        batch: Optional[WriteBatch] = None,
    ):
        """
        Write data to RocksDB.

        For TTL-enabled partitions this also persists the high-water mark and
        runs the bounded sweep over the secondary expiry index, all within
        the same WriteBatch so the on-disk commit is atomic.

        For legacy / unflipped partitions (the 99% no-TTL workload) the path
        is byte-identical to v3.23.6: no stamp prefix, no high-water write,
        no sweep, no index CF use. The hot-path branch is a single Python
        attribute check.

        :param cache: The modified data
        :param changelog_offset: The changelog message offset of the data.
        :param batch: prefilled `rocksdict.WriteBatch`, optional.
        """
        if batch is None:
            batch = WriteBatch(raw_mode=True)

        column_families = cache.get_column_families()

        # Iterate over the transaction update cache and stage writes verbatim.
        # For unflipped partitions this commits the cache as-is — exactly the
        # v3.23.6 behavior. For flipped partitions the transaction layer has
        # already stamped values and emitted index-CF writes into the cache.
        # Keys re-written into the default CF in this same flush. The TTL sweep
        # reads committed disk state (not this uncommitted batch), so it must not
        # delete a key the batch just refreshed — otherwise the stale-read delete
        # clobbers the fresh write. Track TTL-index keys too: a refreshed value
        # can legitimately stage the same expiry index key the sweep is visiting.
        staged_default_keys: set[bytes] = set()
        staged_ttl_index_keys: set[bytes] = set()
        # Only track staged keys when a sweep will actually consume them. For
        # legacy / unflipped partitions (the 99% no-TTL workload) the sweep never
        # runs, so this keeps the inner write loop byte-identical to v3.23.6.
        track_staged = self.uses_ttl_stamps
        for cf_name in column_families:
            cf_handle = self.get_column_family_handle(cf_name)

            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    batch.put(key, value, cf_handle)
                    if track_staged:
                        if cf_name == "default":
                            staged_default_keys.add(key)
                        elif cf_name == TTL_INDEX_CF_NAME:
                            staged_ttl_index_keys.add(key)

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                batch.delete(key, cf_handle)

        if self.uses_ttl_stamps and self._high_water_ms is not None:
            batch.put(
                TTL_HIGH_WATER_KEY,
                int_to_bytes(self._high_water_ms),
                self.get_column_family_handle(METADATA_CF_NAME),
            )

        if self.uses_ttl_stamps:
            self._run_sweep(
                batch=batch,
                staged_default_keys=staged_default_keys,
                staged_ttl_index_keys=staged_ttl_index_keys,
            )

        # Save the latest changelog topic offset to know where to recover from
        # It may be None if changelog topics are disabled
        if changelog_offset is not None:
            self._update_changelog_offset(batch=batch, offset=changelog_offset)
        logger.debug(
            f"Flushing state changes to the disk "
            f'path="{self.path}" '
            f"changelog_offset={changelog_offset} "
            f"bytes_total={batch.size_in_bytes()}"
        )

        self._write(batch)

    # ------------------------------------------------------------------
    # TTL flip / probe helpers (used by the transaction at flush time).
    # ------------------------------------------------------------------

    def main_cf_has_user_data(self) -> bool:
        """
        Return True if the default column family already contains at least one
        entry. Used by the transaction layer at flush time to decide between
        the empty-store flip path and the populated-store rejection path.

        ``seek_to_first`` on the default CF runs once per partition lifetime
        (only on the flush that flips), so its cost is irrelevant.
        """
        return self._main_cf_has_user_data()

    def estimated_main_cf_key_count(self, cap: int = 10_000) -> int:
        """
        Best-effort count of the keys in the default CF, used in the
        rejection error message to give the operator a rough scale of the
        state they are about to wipe.

        rocksdict does not expose RocksDB's ``GetEstimatedNumKeys``, so we
        iterate up to ``cap`` keys; "saturated" means ">= cap". This runs
        once per partition lifetime (only on the rejection path), so the
        cost is irrelevant. Returns 0 only if the iteration fails for a
        reason other than emptiness — the operator-visible contract
        documents 0 as "unknown but non-zero".
        """
        try:
            cf = self.get_or_create_column_family("default")
            count = 0
            for _ in cf.items():
                count += 1
                if count >= cap:
                    break
            return count
        except Exception:
            return 0

    def flip_into_ttl_mode(self, batch: WriteBatch) -> None:
        """
        Atomically flip this partition into TTL mode.

        Called by the transaction layer from ``flush()`` when a TTL write is
        detected on a partition whose default CF is empty (the empty-store
        fast path). Writes ``__ttl_enabled__`` and ``__ttl_format_version__``
        to the metadata CF in the **same** ``batch`` as the first stamped
        user writes, so the change is atomic on disk and replayable through
        the changelog.

        After this call:

        - ``self.uses_ttl_stamps`` is True; the next transaction starts in
          TTL mode and stamps inline.
        - The ``__ttl_index__`` CF exists; subsequent writes can index
          non-sentinel entries.
        - The ``__ttl_format_version__`` marker is on disk; future opens
          take the TTL-aware branch in ``__init__``.
        """
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        batch.put(TTL_ENABLED_KEY, b"\x01", metadata_handle)
        batch.put(
            STATE_FORMAT_VERSION_KEY,
            int_to_bytes(STATE_FORMAT_VERSION),
            metadata_handle,
        )
        # Lazily create the index CF on first need.
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        self.uses_ttl_stamps = True

    def reject_ttl_on_populated_store(self) -> "IncompatibleStateStoreError":
        """
        Build (and log) the structured ERROR raised when a TTL write lands on
        a partition that has existing un-stamped data. The caller is expected
        to ``raise`` the returned exception; emitting the log line here keeps
        the message format in one place.

        Spec §6.4.1 — silent TTL drop is the worst possible failure mode for
        the dedup workload this feature exists for, so we halt loudly.
        """
        approx_keys = self.estimated_main_cf_key_count()
        msg = (
            f"IncompatibleStateStoreError: state store at {self._path!r} "
            f"has {approx_keys} un-stamped existing entries; cannot enable "
            "TTL on a populated store. To enable TTL: stop the application, "
            f"delete the state directory at {self._path!r}, restart — "
            "recovery will rebuild from the changelog with TTL enabled."
        )
        logger.error(
            msg,
            extra={
                "state_store_path": self._path,
                "approx_key_count": approx_keys,
                "operator_action": "delete_state_directory_and_recover",
            },
        )
        return IncompatibleStateStoreError(msg)

    def _write(self, batch: WriteBatch):
        """
        Write `WriteBatch` to RocksDB
        :param batch: an instance of `rocksdict.WriteBatch`
        """
        self._db.write(batch)

    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """
        Get a key from RocksDB.

        :param key: a key encoded to `bytes`
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the DB. Otherwise, `default`
        """
        result = self.get_or_create_column_family(cf_name).get(
            key, default=Marker.UNDEFINED
        )

        # RDict accept Any type as value but we only write bytes so we should only get bytes back.
        return cast(Union[bytes, Literal[Marker.UNDEFINED]], result)

    def iter_items(
        self,
        lower_bound: bytes,  # inclusive
        upper_bound: bytes,  # exclusive
        backwards: bool = False,
        cf_name: str = "default",
    ) -> Iterator[tuple[bytes, bytes]]:
        """
        Iterate over key-value pairs within a specified range in a column family.

        :param lower_bound: The lower bound key (inclusive) for the iteration range.
        :param upper_bound: The upper bound key (exclusive) for the iteration range.
        :param backwards: If `True`, iterate in reverse order (descending).
            Default is `False` (ascending).
        :param cf_name: The name of the column family to iterate over.
            Default is "default".
        :return: An iterator yielding (key, value) tuples.
        """
        cf = self.get_or_create_column_family(cf_name=cf_name)

        # Set iterator bounds to reduce IO by limiting the range of keys fetched
        read_opt = ReadOptions()
        read_opt.set_iterate_lower_bound(lower_bound)
        read_opt.set_iterate_upper_bound(upper_bound)

        from_key = upper_bound if backwards else lower_bound

        # RDict accepts Any type as value but we only write bytes so we should only get bytes back.
        items = cast(
            Iterator[tuple[bytes, bytes]],
            cf.items(from_key=from_key, read_opt=read_opt, backwards=backwards),
        )

        if not backwards:
            # NOTE: Forward iteration respects bounds correctly.
            # Also, we need to use yield from notation to replace RdictItems
            # with Python-native generator or else garbage collection
            # will make the result unpredictable.
            yield from items
        else:
            # NOTE: When iterating backwards, the `read_opt` lower bound
            # is not respected by Rdict for some reason. We need to manually
            # filter it here.
            for key, value in items:
                if key < lower_bound:
                    # Exit early if the key falls below the lower bound
                    break
                yield key, value

    def begin(self) -> RocksDBPartitionTransaction:
        return RocksDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the DB.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        cf_dict = self.get_or_create_column_family(cf_name)
        return key in cf_dict

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        offset_bytes = metadata_cf.get(CHANGELOG_OFFSET_KEY)
        if offset_bytes is None:
            return None

        return int_from_bytes(offset_bytes)

    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.

        To be used when we simply need to update the changelog offset without touching
        the actual data.

        :param offset: new changelog offset
        """
        batch = WriteBatch(raw_mode=True)
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    def close(self):
        """
        Close the underlying RocksDB
        """
        start = time.monotonic()
        logger.debug(f'Closing rocksdb partition on "{self._path}"')
        # Clean the column family caches to drop references
        # Otherwise the Rocksdb won't close properly
        self._cf_handle_cache = {}
        self._cf_cache = {}
        # Stop background flush/compaction before closing so that db.close()
        # does not block waiting for it to wind down. On large DBs this wait
        # dominates the revoke sequence and is what pushes it past
        # max.poll.interval.ms during a rebalance handover; cancelling it
        # releases the OS lock in ~milliseconds. Unflushed memtable data is
        # preserved by the WAL, and cancelled compaction debt simply resumes
        # under the next owner.
        # Guarded with getattr because cancel_all_background is not present in
        # every rocksdict release within our supported range; when it's absent
        # we simply fall back to the plain (slower) close.
        cancel_all_background = getattr(self._db, "cancel_all_background", None)
        if cancel_all_background is not None:
            cancel_all_background(True)
        try:
            self._db.close()
        except Exception as exc:
            # After cancel_all_background(), RocksDB reports "Shutdown in
            # progress" from close(). The DB still closes cleanly and the lock
            # is released, so this status is benign; re-raise anything else.
            if "shutdown in progress" not in str(exc).lower():
                raise
        elapsed = round(time.monotonic() - start, 4)
        logger.debug(f'Closed rocksdb partition on "{self._path}" in {elapsed}s')

    @property
    def path(self) -> str:
        """
        Absolute path to RocksDB database folder
        :return: file path
        """
        return self._path

    @classmethod
    def destroy(cls, path: str):
        """
        Delete underlying RocksDB database

        The database must be closed first.

        :param path: an absolute path to the RocksDB folder
        """
        Rdict.destroy(path=path)

    def get_column_family_handle(self, cf_name: str) -> ColumnFamily:
        """
        Get a column family handle to pass to it WriteBatch.
        This method will cache the CF handle instance to avoid creating them
        repeatedly.

        :param cf_name: column family name
        :return: instance of `rocksdict.ColumnFamily`
        """
        if (cf_handle := self._cf_handle_cache.get(cf_name)) is None:
            self.get_or_create_column_family(cf_name)
            cf_handle = self._db.get_column_family_handle(cf_name)
            self._cf_handle_cache[cf_name] = cf_handle
        return cf_handle

    def get_or_create_column_family(self, cf_name: str) -> Rdict:
        """
        Get a column family instance.
        This method will cache the CF instance to avoid creating them repeatedly.

        :param cf_name: column family name
        :return: instance of `rocksdict.Rdict` for the given column family
        """
        if (cf := self._cf_cache.get(cf_name)) is None:
            try:
                cf = self._db.get_column_family(cf_name)
            except Exception as exc:
                if "does not exist" not in str(exc):
                    raise
                cf = self._db.create_column_family(
                    cf_name, options=self._rocksdb_options
                )
            self._cf_cache[cf_name] = cf
        return cf

    def list_column_families(self) -> List[str]:
        return self._db.list_cf(self._path)

    # ------------------------------------------------------------------
    # TTL machinery (only used when ``uses_ttl_stamps`` is True).
    # ------------------------------------------------------------------

    def _run_sweep(
        self,
        batch: WriteBatch,
        staged_default_keys: "set[bytes] | None" = None,
        staged_ttl_index_keys: "set[bytes] | None" = None,
    ) -> None:
        """
        Bounded-budget sweep over the secondary expiry index.

        Called from :meth:`write` so any deletes go into the same batch as
        the user-driven writes for atomicity.

        ``staged_default_keys`` are the default-CF keys re-written in this same
        flush. The sweep reads committed disk state, which is stale for those
        keys, so it must never delete them here.

        ``staged_ttl_index_keys`` are the TTL-index entries written in this
        batch. If a fresh write reuses the same expiry stamp as the stale index
        entry being swept, deleting that index key would orphan the fresh value.
        """
        staged_default: "set[bytes] | frozenset[bytes]" = (
            staged_default_keys or frozenset()
        )
        staged_ttl_index: "set[bytes] | frozenset[bytes]" = (
            staged_ttl_index_keys or frozenset()
        )
        if self._high_water_ms is None:
            # Cold start: no event-time established yet — skip the sweep.
            return

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index_cf = self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        main_cf = self.get_or_create_column_family("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        main_handle = self.get_column_family_handle("default")

        def delete_index_if_not_staged(index_key: bytes) -> None:
            if index_key not in staged_ttl_index:
                batch.delete(index_key, index_handle)

        # Bound the iterator at the cutoff stamp to skip future expiries
        # without paying for the iterator step. Build the cutoff prefix as
        # 8 BE bytes equal to ``now_ms + 1`` so any entry whose first
        # 8 bytes equal exactly ``now_ms`` is still iterated.
        upper_bound = int_to_bytes(now_ms + 1) if now_ms < (2**64 - 1) else None
        read_opt = ReadOptions()
        if upper_bound is not None:
            read_opt.set_iterate_upper_bound(upper_bound)

        evicted = 0
        iterator: Iterator[tuple[bytes, bytes]] = cast(
            Iterator[tuple[bytes, bytes]],
            index_cf.items(from_key=b"", read_opt=read_opt),
        )
        for index_key, _ in iterator:
            if evicted >= budget:
                break

            try:
                idx_expires_at, user_key = decode_index_key(index_key)
            except ValueError:
                delete_index_if_not_staged(index_key)
                continue

            if idx_expires_at > now_ms:
                # Sorted by expiry — the rest is in the future.
                break

            main_value = main_cf.get(user_key, default=None)
            if main_value is None:
                # Ghost: user deleted the main entry but the index still
                # points at it. GC the orphaned index entry.
                delete_index_if_not_staged(index_key)
                continue

            try:
                main_expires_at, _ = decode_ttl_value(cast(bytes, main_value))
            except ValueError:
                logger.warning(
                    "Malformed TTL value at %s for key %r; dropping orphan "
                    "index entry but leaving main untouched.",
                    self._path,
                    user_key,
                )
                delete_index_if_not_staged(index_key)
                continue

            if main_expires_at == SENTINEL_NEVER:
                # Ghost: key was overwritten by a plain ``state.set`` and
                # is now permanent. Drop the stale index pointer only.
                delete_index_if_not_staged(index_key)
                continue

            if main_expires_at == idx_expires_at and user_key not in staged_default:
                batch.delete(user_key, main_handle)
                delete_index_if_not_staged(index_key)
                evicted += 1
            else:
                # Ghost: key was overwritten with a fresh expiry stamp — either
                # already committed, or re-written in this same batch (in which
                # case the committed read above is stale). Drop only the stale
                # index pointer; deleting the key would clobber the fresh write.
                delete_index_if_not_staged(index_key)

        if evicted:
            logger.debug(
                "TTL sweep evicted %d expired entries on partition path=%s "
                "now_ms=%d budget=%d",
                evicted,
                self._path,
                now_ms,
                budget,
            )

    def _enforce_format_version(self) -> None:
        """
        Validate the on-disk format-version marker on a partition that is
        already flipped into TTL mode (``__ttl_enabled__`` was True at open
        time). A flipped store must carry a marker whose value is at least
        :data:`STATE_FORMAT_VERSION`; anything older is forward-incompatible
        and the operator must reset the state directory.

        Stores that never enabled TTL never enter this method — they have no
        marker by design (and remain byte-identical to v3.23.6 on disk).
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(STATE_FORMAT_VERSION_KEY, default=None)
        if raw is None:
            # Defensive: a flipped store must have the marker. Treat its
            # absence as "needs reset" rather than silently re-stamp.
            raise IncompatibleStateStoreError(
                f'State store at "{self._path}" is marked TTL-enabled but '
                "is missing its format-version marker. Delete the state "
                "directory and let recovery rebuild from the changelog."
            )
        try:
            version = int_from_bytes(cast(bytes, raw))
        except Exception:
            version = -1
        if version < STATE_FORMAT_VERSION:
            raise IncompatibleStateStoreError(
                f'State store at "{self._path}" was written with an older '
                f"on-disk layout (version={version}); the running version "
                f"requires {STATE_FORMAT_VERSION}. Delete the state "
                "directory and let recovery rebuild from the changelog."
            )

    def _load_ttl_enabled_flag(self) -> bool:
        """
        Read the persistent ``__ttl_enabled__`` flag from the metadata CF.
        Absent / falsy = legacy mode (the 99% case, byte-identical to
        v3.23.6). Present-and-truthy = the partition flipped into TTL mode
        on a previous flush; resume the v2 stamped path on the next write.
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(TTL_ENABLED_KEY, default=None)
        if raw is None:
            return False
        # Any non-empty bytes value counts as "True" — we currently write
        # ``b"\x01"`` but stay liberal in what we accept.
        return bool(raw)

    def _stamp_flip_metadata(self) -> None:
        """
        Persist the TTL-enabled flag and the format-version marker to the
        metadata CF on disk **outside of a user write batch**. Used by the
        recovery flag-discovery path: when recovery detects a stamped
        replayed value, it flips the partition immediately so the rest of
        replay decodes correctly, and a subsequent process restart picks
        up TTL mode at open time.
        """
        batch = WriteBatch(raw_mode=True)
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        batch.put(TTL_ENABLED_KEY, b"\x01", metadata_handle)
        batch.put(
            STATE_FORMAT_VERSION_KEY,
            int_to_bytes(STATE_FORMAT_VERSION),
            metadata_handle,
        )
        self._write(batch)

    def _looks_like_stamped_value(self, value: bytes) -> bool:
        """
        Heuristic used by the recovery flag-discovery path to decide whether
        a replayed default-CF value originated from a flipped store.

        Conservative recognizer: the value must be at least 8 bytes long,
        and the leading 8 BE bytes must be either:

        - ``SENTINEL_NEVER`` (always-true marker for "never expires"), or
        - a plausible epoch-millisecond expiry — strictly positive and
          smaller than 10^15 ms (≈ year 33658) which comfortably bounds
          any realistic event-time TTL while excluding sentinel collisions
          and most "this is actually serialized user data" false positives.

        False negatives (a flipped store that produced an unrecognizable
        first value) would leave the recovery partition in legacy mode —
        which then writes un-stamped values back to disk and effectively
        clears TTL on that key. The 10^15 ms cap is generous enough that
        this is unlikely in practice; the documented operator action is
        still "delete state directory and let recovery rebuild" if the
        heuristic misfires.
        """
        if len(value) < 8:
            return False
        try:
            stamp, _ = decode_ttl_value(value)
        except ValueError:
            return False
        if stamp == SENTINEL_NEVER:
            return True
        # ~year 33658; far beyond any realistic event-time clock.
        return 0 < stamp < 10**15

    def _main_cf_has_user_data(self) -> bool:
        """
        Return True if the default column family contains at least one
        non-metadata user entry.
        """
        default_cf = self.get_or_create_column_family("default")
        for _ in default_cf.items():
            return True
        return False

    def _load_high_water(self) -> None:
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(TTL_HIGH_WATER_KEY, default=None)
        if raw is None:
            return
        try:
            self._high_water_ms = int_from_bytes(cast(bytes, raw))
        except Exception:
            logger.warning(
                "Failed to decode persisted TTL high-water at %s; "
                "treating it as undefined.",
                self._path,
            )

    def _normalize_replay_value(self, value: bytes) -> tuple[bytes, int]:
        """
        Decode a replayed main-CF value into ``(stamped_blob, stamp)``.

        - Already-stamped values (8 bytes prefix decodable) round-trip with
          their original stamp.
        - Unstamped legacy values (e.g. from a pre-v2 changelog topic) are
          wrapped with the sentinel so downstream reads round-trip
          natively.

        Heuristic: a value strictly shorter than 8 bytes cannot carry a
        stamp; one of length >= 8 is always treated as stamped, since the
        format-version guard prevents a populated unstamped on-disk store
        from ever reaching this method. Pre-v2 *changelog* values may still
        be unstamped, but those are replayed into a fresh (empty) on-disk
        store after the guard has stamped a fresh format-version marker —
        the guard's check ran on an empty default CF and passed. To
        distinguish them, we wrap any value whose first 8 bytes do not
        resemble a plausible stamp range. To stay safe we wrap *only* if
        the value is shorter than 8 bytes; anything else is assumed
        already-stamped. The risk: a pre-v2 changelog value of >=8 bytes
        will be misinterpreted. The mitigation, called out in the spec
        §6.8 / §8, is that operators must also rebuild the changelog topic
        when crossing the v3.23.6 → v2 boundary if they want clean replay.
        """
        if len(value) < 8:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        try:
            stamp, _ = decode_ttl_value(value)
        except ValueError:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        return value, stamp

    # ------------------------------------------------------------------
    # RocksDB lifecycle.
    # ------------------------------------------------------------------

    def _open_rocksdict(self) -> Rdict:
        options = self._rocksdb_options
        options.create_if_missing(True)
        options.create_missing_column_families(True)
        create_rdict = lambda: Rdict(
            path=self._path,
            options=options,
            access_type=AccessType.read_write(),
        )
        # TODO: Add docs

        try:
            rdict = create_rdict()
        except Exception as exc:
            if not str(exc).startswith("Corruption"):
                raise
            elif not self._changelog_producer:
                raise RocksDBCorruptedError(
                    f'State store at "{self._path}" is corrupted '
                    f"and cannot be recovered from the changelog topic: "
                    "`use_changelog_topics` is set to False."
                ) from exc
            elif not self._options.on_corrupted_recreate:
                raise RocksDBCorruptedError(
                    f'State store at "{self._path}" is corrupted '
                    f"but may be recovered from the changelog topic. "
                    "`on_corrupted_recreate` is set to False; "
                    "remove the override (or pass "
                    "`rocksdb_options=RocksDBOptions(..., on_corrupted_recreate=True)`) "
                    "to destroy the corrupted state "
                    "and recover it from the changelog."
                ) from exc

            logger.warning(f"Destroying corrupted RocksDB path={self._path}")
            Rdict.destroy(self._path)
            logger.warning(f"Recreating corrupted RocksDB path={self._path}")
            rdict = create_rdict()

        # Ensure metadata column family is created without defining it upfront
        try:
            rdict.get_column_family(METADATA_CF_NAME)
        except Exception as exc:
            if "does not exist" in str(exc):
                rdict.create_column_family(METADATA_CF_NAME, options=options)
            else:
                raise

        return rdict

    def _init_rocksdb(self) -> Rdict:
        attempt = 1
        while True:
            logger.debug(
                f'Opening rocksdb partition on "{self._path}" attempt={attempt}',
            )
            try:
                db = self._open_rocksdict()
                logger.debug(
                    f'Successfully opened rocksdb partition on "{self._path}"',
                )
                return db
            except Exception as exc:
                is_locked = str(exc).lower().startswith("io error")
                if not is_locked:
                    raise

                if self._open_max_retries <= 0 or attempt >= self._open_max_retries:
                    raise

                logger.warning(
                    f"Failed to open rocksdb partition , cannot acquire a lock. "
                    f"Retrying in {self._open_retry_backoff}sec."
                )

                attempt += 1
                # Wait for the backoff, but bail out immediately if the
                # application is stopping so a lock-waiting instance stays
                # promptly killable instead of sleeping through every retry.
                if self._stop_event is not None:
                    if self._stop_event.wait(self._open_retry_backoff):
                        raise RocksDBOpenAborted(
                            f'Aborted opening rocksdb partition on "{self._path}": '
                            f"the application is stopping"
                        ) from exc
                else:
                    time.sleep(self._open_retry_backoff)

    def _update_changelog_offset(self, batch: WriteBatch, offset: int):
        batch.put(
            CHANGELOG_OFFSET_KEY,
            int_to_bytes(offset),
            self.get_column_family_handle(METADATA_CF_NAME),
        )
