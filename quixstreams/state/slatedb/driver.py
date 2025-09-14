from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Optional, Protocol, Tuple


class SlateDBDriver(Protocol):
    def open(self, path: str, create_if_missing: bool = True) -> None: ...
    def close(self) -> None: ...
    def get(self, key: bytes) -> Optional[bytes]: ...
    def put(self, key: bytes, value: bytes) -> None: ...
    def delete(self, key: bytes) -> None: ...
    def write_batch(
        self, ops: Iterable[Tuple[str, bytes, Optional[bytes]]], sync: bool = False
    ) -> None: ...
    def iter(
        self, start: Optional[bytes], end: Optional[bytes], reverse: bool = False
    ) -> Iterator[Tuple[bytes, bytes]]: ...


@dataclass
class InMemoryDriver:
    store: dict[bytes, bytes]

    def open(self, path: str, create_if_missing: bool = True) -> None:
        # path is ignored for in-memory fallback
        if self.store is None:
            self.store = {}

    def close(self) -> None:
        pass

    def get(self, key: bytes) -> Optional[bytes]:
        return self.store.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self.store[key] = value

    def delete(self, key: bytes) -> None:
        self.store.pop(key, None)

    def write_batch(
        self, ops: Iterable[Tuple[str, bytes, Optional[bytes]]], sync: bool = False
    ) -> None:
        for op, key, val in ops:
            if op == "put":
                if val is None:
                    raise ValueError("put operation requires a non-None value")
                self.put(key, val)
            elif op == "del":
                self.delete(key)
            else:
                raise ValueError(f"unknown op {op}")

    def iter(self, start: Optional[bytes], end: Optional[bytes], reverse: bool = False):
        items = sorted((k, v) for k, v in self.store.items())

        def in_range(k: bytes) -> bool:
            if start is not None and k < start:
                return False
            if end is not None and k >= end:
                return False
            return True

        items = [(k, v) for k, v in items if in_range(k)]
        if reverse:
            items.reverse()
        for kv in items:
            yield kv


class RealSlateDBDriver:
    def __init__(self) -> None:
        self._db = None

    def open(self, path: str, create_if_missing: bool = True) -> None:
        # slatedb always creates if missing by default for local path
        import slatedb  # type: ignore

        from .exceptions import SlateDBCorruptedError

        try:
            self._db = slatedb.SlateDB(path)
        except Exception as e:
            # Heuristic: map known corruption/open errors to SlateDBCorruptedError
            msg = str(e).lower()
            if any(
                token in msg
                for token in [
                    "corrupt",
                    "corrupted",
                    "invalid manifest",
                    "invalid sst",
                    "repair required",
                ]
            ):
                raise SlateDBCorruptedError(str(e))
            raise

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None

    def _ensure(self):
        if self._db is None:
            raise RuntimeError("SlateDB driver not opened")

    def get(self, key: bytes) -> Optional[bytes]:
        self._ensure()
        return self._db.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._ensure()
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("value must be bytes")
        self._db.put(key, value)

    def delete(self, key: bytes) -> None:
        self._ensure()
        self._db.delete(key)

    def write_batch(
        self, ops: Iterable[Tuple[str, bytes, Optional[bytes]]], sync: bool = False
    ) -> None:
        # SlateDB python API has no explicit batch in this package; emulate sequentially.
        # Future: use native batch if/when exposed.
        self._ensure()
        for op, key, val in ops:
            if op == "put":
                if val is None:
                    raise ValueError("put operation requires a non-None value")
                self._db.put(key, val)
            elif op == "del":
                self._db.delete(key)
            else:
                raise ValueError(f"unknown op {op}")

    def iter(self, start: Optional[bytes], end: Optional[bytes], reverse: bool = False):
        self._ensure()
        # Use scan_iter if available; assume forward order when reverse=False
        # Future: add bound parameters once API is confirmed.
        if hasattr(self._db, "scan_iter"):
            # Many APIs accept optional start/end; try kwargs fallback.
            try:
                it = self._db.scan_iter(start=start, end=end)
            except TypeError:
                it = self._db.scan_iter()
            if reverse:
                # Collect and reverse if reverse iteration unsupported
                items = list(it)
                items.reverse()
                for kv in items:
                    yield kv
            else:
                for kv in it:
                    yield kv
        else:
            # Fallback: no iteration support
            raise NotImplementedError("SlateDB iteration not supported by driver")


def get_default_driver() -> SlateDBDriver:
    try:
        import slatedb  # noqa: F401

        return RealSlateDBDriver()
    except Exception:
        return InMemoryDriver(store={})
