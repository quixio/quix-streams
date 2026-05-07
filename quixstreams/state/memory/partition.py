import functools
import logging
from datetime import timedelta
from typing import Any, Dict, Literal, Optional, Union, cast

from quixstreams.state import PartitionTransaction
from quixstreams.state.base import PartitionTransactionCache, StorePartition
from quixstreams.state.base.transaction import PartitionTransactionStatus
from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.metadata import (
    LOCAL_ONLY_CFS,
    METADATA_CF_NAME,
    TTL_INDEX_CF_NAME,
    Marker,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.metadata import TTL_HIGH_WATER_KEY
from quixstreams.state.rocksdb.transaction import _ttl_to_ms
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)
from quixstreams.state.serialization import int_from_bytes, int_to_bytes
from quixstreams.utils.json import dumps as json_dumps
from quixstreams.utils.json import loads as json_loads

logger = logging.getLogger(__name__)

__all__ = ("MemoryStorePartition",)

_DEFAULT_MAX_EVICTIONS_PER_FLUSH = 10_000


def _validate_partition_state():
    """
    Check that the store is not closed
    """

    def wrapper(func):
        @functools.wraps(func)
        def _wrapper(partition: "MemoryStorePartition", *args, **kwargs):
            if partition.closed:
                raise RuntimeError("partition is closed")

            return func(partition, *args, **kwargs)

        return _wrapper

    return wrapper


class MemoryStorePartition(StorePartition):
    """
    Class to access in-memory state.

    Responsibilities:
     1. Recovering from changelog messages
     2. Creating transaction to interact with data
     3. Track partition state in-memory
     4. Maintaining the same per-write TTL machinery as
        :class:`quixstreams.state.rocksdb.RocksDBStorePartition` so dev/test
        workflows that switch between MemoryStore and RocksDBStore see
        identical semantics.
    """

    uses_ttl_stamps: bool = True

    def __init__(
        self,
        changelog_producer: Optional[ChangelogProducer],
        max_evictions_per_flush: int = _DEFAULT_MAX_EVICTIONS_PER_FLUSH,
    ) -> None:
        super().__init__(
            dumps=json_dumps,
            loads=json_loads,
            changelog_producer=changelog_producer,
        )
        self._changelog_offset: Optional[int] = None
        self._state: Dict[str, Dict[bytes, Any]] = {
            "default": {},
            METADATA_CF_NAME: {},
        }
        self._closed = False
        self._max_evictions_per_flush = max_evictions_per_flush
        self._high_water_ms: Optional[int] = None
        if self.uses_ttl_stamps:
            self._state.setdefault(TTL_INDEX_CF_NAME, {})
            self._load_high_water()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def high_water_ms(self) -> Optional[int]:
        return self._high_water_ms

    @property
    def max_evictions_per_flush(self) -> int:
        return self._max_evictions_per_flush

    def advance_high_water(self, timestamp: Optional[int]) -> None:
        if timestamp is None:
            return
        if self._high_water_ms is None or timestamp > self._high_water_ms:
            self._high_water_ms = timestamp

    def close(self) -> None:
        self._closed = True

    def begin(self) -> PartitionTransaction:
        return MemoryPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    @_validate_partition_state()
    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
    ) -> None:
        """
        Write data to the state

        :param cache: The partition update cache
        :param changelog_offset: The changelog message offset of the data.
        """
        if changelog_offset is not None:
            self._changelog_offset = changelog_offset

        for cf_name in cache.get_column_families():
            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    self._state.setdefault(cf_name, {})[key] = value

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                self._state.setdefault(cf_name, {}).pop(key, None)

        if self.uses_ttl_stamps:
            if self._high_water_ms is not None:
                self._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = int_to_bytes(
                    self._high_water_ms
                )
            self._run_sweep()

    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ) -> None:
        if not self.uses_ttl_stamps:
            if value:
                self._state.setdefault(cf_name, {})[key] = value
            else:
                self._state.setdefault(cf_name, {}).pop(key, None)
            self._changelog_offset = offset
            return

        if cf_name in LOCAL_ONLY_CFS:
            self._changelog_offset = offset
            return

        is_main_cf = cf_name == "default"

        if value is None:
            self._state.setdefault(cf_name, {}).pop(key, None)
        elif is_main_cf:
            stamped, stamp = self._normalize_replay_value(value)
            recovery_now = self._high_water_ms
            if (
                stamp != SENTINEL_NEVER
                and recovery_now is not None
                and stamp <= recovery_now
            ):
                # already-expired entry; drop it
                pass
            else:
                self._state.setdefault(cf_name, {})[key] = stamped
                if stamp != SENTINEL_NEVER:
                    self._state.setdefault(TTL_INDEX_CF_NAME, {})[
                        encode_index_key(stamp, key)
                    ] = b""
                    self.advance_high_water(stamp)
        else:
            self._state.setdefault(cf_name, {})[key] = value

        self._changelog_offset = offset

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        return self._changelog_offset

    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.

        To be used when we simply need to update the changelog offset without touching
        the actual data.

        :param offset: new changelog offset
        """
        self._changelog_offset = offset

    @_validate_partition_state()
    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """
        Get a key from the store

        :param key: a key encoded to `bytes`
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the store. Otherwise, `default`
        """
        return self._state.get(cf_name, {}).get(key, Marker.UNDEFINED)

    @_validate_partition_state()
    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the store.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        return key in self._state.get(cf_name, {})

    # ------------------------------------------------------------------
    # TTL helpers (mirror of RocksDBStorePartition).
    # ------------------------------------------------------------------

    def _load_high_water(self) -> None:
        raw = self._state.get(METADATA_CF_NAME, {}).get(TTL_HIGH_WATER_KEY)
        if raw is None:
            return
        try:
            self._high_water_ms = int_from_bytes(cast(bytes, raw))
        except Exception:
            logger.warning("Failed to decode persisted TTL high-water; ignoring.")

    def _normalize_replay_value(self, value: bytes) -> tuple[bytes, int]:
        if len(value) < 8:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        try:
            stamp, _ = decode_ttl_value(value)
        except ValueError:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        return value, stamp

    def _run_sweep(self) -> None:
        if self._high_water_ms is None:
            return

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index = self._state.setdefault(TTL_INDEX_CF_NAME, {})
        main = self._state.setdefault("default", {})

        evicted = 0
        # Iterate in sorted order — index keys are big-endian-stamped so
        # ascending byte order equals ascending expiry order.
        for index_key in sorted(index.keys()):
            if evicted >= budget:
                break

            try:
                idx_expires_at, user_key = decode_index_key(index_key)
            except ValueError:
                index.pop(index_key, None)
                continue

            if idx_expires_at > now_ms:
                break

            main_value = main.get(user_key)
            if main_value is None:
                index.pop(index_key, None)
                continue

            try:
                main_expires_at, _ = decode_ttl_value(main_value)
            except ValueError:
                index.pop(index_key, None)
                continue

            if main_expires_at == SENTINEL_NEVER:
                index.pop(index_key, None)
                continue

            if main_expires_at == idx_expires_at:
                main.pop(user_key, None)
                index.pop(index_key, None)
                evicted += 1
            else:
                index.pop(index_key, None)


class MemoryPartitionTransaction(PartitionTransaction[bytes, Any]):
    """
    TTL-aware transaction for the in-memory store.

    Mirrors :class:`quixstreams.state.rocksdb.RocksDBPartitionTransaction`
    exactly except for the absence of a RocksDB write batch — writes go
    straight to the parent's transaction cache.
    """

    _partition: MemoryStorePartition  # type: ignore[assignment]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._partition = cast(MemoryStorePartition, self._partition)

    def _stamps_enabled(self, cf_name: str) -> bool:
        return cf_name == "default" and self._partition.uses_ttl_stamps

    def _compute_stamp(
        self, ttl: Optional[timedelta], timestamp: Optional[int]
    ) -> int:
        if ttl is None:
            return SENTINEL_NEVER
        if ttl <= timedelta(0):
            raise ValueError(
                f"ttl must be a positive timedelta or None, got {ttl!r}"
            )
        if timestamp is None:
            raise ValueError(
                "ttl=... on state.set() requires the current record's "
                "event-time timestamp; the framework injects it via the "
                "stateful StreamingDataFrame wrapper. If you are calling "
                "state.set() outside an apply/update/filter callback, use "
                "as_state(prefix, timestamp=...)."
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
        if not self._stamps_enabled(cf_name):
            super().set(
                key=key,
                value=value,
                prefix=prefix,
                cf_name=cf_name,
                timestamp=timestamp,
                ttl=ttl,
            )
            return

        if timestamp is not None:
            self._partition.advance_high_water(timestamp)

        stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)

        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

        stamped = encode_ttl_value(stamp, value_serialized)
        super().set_bytes(
            key=key, value=stamped, prefix=prefix, cf_name="default"
        )

        if stamp != SENTINEL_NEVER:
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._update_cache.set(
                key=encode_index_key(stamp, key_serialized),
                value=b"",
                prefix=b"",
                cf_name=TTL_INDEX_CF_NAME,
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
        if not self._stamps_enabled(cf_name):
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

        if timestamp is not None:
            self._partition.advance_high_water(timestamp)

        stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        stamped = encode_ttl_value(stamp, value)
        super().set_bytes(
            key=key, value=stamped, prefix=prefix, cf_name="default"
        )

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
            return Marker.UNDEFINED

        if stamp == SENTINEL_NEVER:
            return payload

        now = self._partition.high_water_ms
        if now is not None and stamp <= now:
            return Marker.UNDEFINED

        return payload

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
