from __future__ import annotations

import logging
import os
import time
from typing import Iterator, Literal, Optional, Union

from quixstreams.state.base import PartitionTransactionCache, StorePartition
from quixstreams.state.metadata import Marker
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import int_from_bytes, int_to_bytes

from .driver import SlateDBDriver, get_default_driver
from .exceptions import SlateDBLockError
from .options import SlateDBOptionsType
from .transaction import SlateDBPartitionTransaction

logger = logging.getLogger(__name__)

__all__ = ("SlateDBStorePartition",)


class SlateDBStorePartition(StorePartition):
    def __init__(
        self,
        path: str,
        options: Optional[SlateDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> None:
        if options is not None:
            dumps = options.dumps
            loads = options.loads
        else:
            from quixstreams.utils.json import dumps as json_dumps
            from quixstreams.utils.json import loads as json_loads

            dumps = json_dumps
            loads = json_loads
        super().__init__(
            dumps=dumps, loads=loads, changelog_producer=changelog_producer
        )
        self._path = path
        self._options = options
        self._lock_path = f"{self._path}.lock"
        # Ensure the DB directory exists before acquiring a lock file next to it
        dirpath = os.path.dirname(self._path)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        self._acquire_lock()
        self._driver: SlateDBDriver = get_default_driver()
        try:
            self._driver.open(path)
        except Exception as e:
            # Handle corruption if requested
            from .exceptions import SlateDBCorruptedError

            if isinstance(e, SlateDBCorruptedError) and (
                self._options and getattr(self._options, "on_corrupted_recreate", False)
            ):
                # Destroy the path and retry open once
                try:
                    import shutil

                    logger.info(
                        "Detected corrupted SlateDB at %s; recreating as per on_corrupted_recreate",
                        self._path,
                    )
                    if os.path.isdir(self._path):
                        shutil.rmtree(self._path, ignore_errors=True)
                    else:
                        try:
                            os.remove(self._path)
                        except FileNotFoundError:
                            pass
                except Exception:
                    # Best-effort cleanup; will retry open regardless
                    logger.warning(
                        "Failed to fully clean corrupted path %s; retrying open anyway",
                        self._path,
                    )
                # Retry open
                try:
                    self._driver.open(path)
                except Exception:
                    # Ensure we release the lock on failure before bubbling up
                    self._release_lock_safely()
                    raise
                logger.info("Recreated and reopened SlateDB at %s", self._path)
            else:
                # Release lock and re-raise
                self._release_lock_safely()
                raise
        logger.debug("SlateDB opened successfully at %s", self._path)
        self._meta_ns = b"__meta__::"
        self._cf_prefix = b"__cf::"

    def _release_lock_safely(self) -> None:
        try:
            os.remove(self._lock_path)
        except Exception:
            try:
                os.unlink(self._lock_path)
            except Exception:
                pass

    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        # Apply change to namespaced key and update offset
        nk = self._encode_cf_key(cf_name, key)
        if value is None:
            self._driver.delete(nk)
        else:
            self._driver.put(nk, value)
        self.write_changelog_offset(offset)

    def write(self, cache: PartitionTransactionCache, changelog_offset: Optional[int]):
        ops: list[tuple[str, bytes, Optional[bytes]]] = []
        # Apply updates with CF namespace prefix
        for cf_name in cache.get_column_families():
            updates = cache.get_updates(cf_name)
            for prefix, kvs in updates.items():
                for key, value in kvs.items():
                    nk = self._encode_cf_key(cf_name, key)
                    ops.append(("put", nk, value))
            deletes = cache.get_deletes(cf_name)
            for key in deletes:
                nk = self._encode_cf_key(cf_name, key)
                ops.append(("del", nk, None))
        # Write changelog offset if provided
        if changelog_offset is not None:
            ops.append(
                (
                    "put",
                    self._meta_ns + b"changelog_offset",
                    int_to_bytes(changelog_offset),
                )
            )
        self._driver.write_batch(ops, sync=False)

    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        nk = self._encode_cf_key(cf_name, key)
        val = self._driver.get(nk)
        return val if val is not None else Marker.UNDEFINED

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        nk = self._encode_cf_key(cf_name, key)
        return self._driver.get(nk) is not None

    def get_changelog_offset(self) -> Optional[int]:
        raw = self._driver.get(self._meta_ns + b"changelog_offset")
        if raw is None:
            return None
        try:
            return int_from_bytes(raw)
        except Exception:
            return None

    def write_changelog_offset(self, offset: int):
        self._driver.put(self._meta_ns + b"changelog_offset", int_to_bytes(offset))

    def begin(self) -> SlateDBPartitionTransaction:
        return SlateDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def iter_items(
        self,
        lower_bound: bytes,  # inclusive
        upper_bound: bytes,  # exclusive
        backwards: bool = False,
        cf_name: str = "default",
    ) -> Iterator[tuple[bytes, bytes]]:
        cp = self._cf_ns(cf_name)
        ns_lower = cp + lower_bound
        ns_upper = cp + upper_bound

        # Try to use driver bounds; driver itself will fallback if unsupported
        try:
            it = self._driver.iter(start=ns_lower, end=ns_upper, reverse=backwards)
            for k, v in it:
                if not k.startswith(cp):
                    continue
                # Enforce bounds, especially on reverse
                if k < ns_lower or k >= ns_upper:
                    continue
                yield k[len(cp) :], v
        except NotImplementedError:
            # Fallback: full scan and filter
            it = self._driver.iter(start=None, end=None, reverse=backwards)
            for k, v in it:
                if not k.startswith(cp):
                    continue
                if k < ns_lower or k >= ns_upper:
                    continue
                yield k[len(cp) :], v

    def _encode_cf_key(self, cf_name: str, key: bytes) -> bytes:
        # __cf::<name>::<key>
        return self._cf_ns(cf_name) + key

    def _cf_ns(self, cf_name: str) -> bytes:
        return self._cf_prefix + cf_name.encode("utf-8") + b"::"

    def _acquire_lock(self):
        max_retries = self._options.open_max_retries if self._options else 5
        backoff = self._options.open_retry_backoff if self._options else 0.2
        attempt = 1
        while True:
            try:
                # O_CREAT|O_EXCL to ensure exclusive creation
                fd = os.open(
                    self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644
                )
                os.close(fd)
                return
            except FileExistsError:
                if max_retries <= 0 or attempt >= max_retries:
                    raise SlateDBLockError(
                        f"Failed to acquire state lock at {self._lock_path}"
                    )
                time.sleep(backoff)
                attempt += 1

    def close(self):
        logger.debug(f'Closing slatedb partition on "{self._path}"')
        self._driver.close()
        # release lock
        try:
            os.remove(self._lock_path)
        except FileNotFoundError:
            pass
