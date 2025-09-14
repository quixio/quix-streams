import logging
from typing import Iterator, Optional

import pytest

from quixstreams.state.slatedb.partition import SlateDBStorePartition


class FakeDriver:
    def __init__(self):
        self.store: dict[bytes, bytes] = {}

    def open(self, path: str, create_if_missing: bool = True) -> None:  # pragma: no cover
        return

    def close(self) -> None:  # pragma: no cover
        return

    def get(self, key: bytes) -> Optional[bytes]:
        return self.store.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self.store[key] = value

    def delete(self, key: bytes) -> None:
        self.store.pop(key, None)

    def write_batch(self, ops, sync: bool = False) -> None:
        for op, key, val in ops:
            if op == "put":
                assert val is not None
                self.put(key, val)
            elif op == "del":
                self.delete(key)
            else:  # pragma: no cover
                raise ValueError(op)

    def iter(self, start: Optional[bytes], end: Optional[bytes], reverse: bool = False) -> Iterator[tuple[bytes, bytes]]:
        # Simulate a driver that does not support bounded iteration, only full scan
        if start is not None or end is not None:
            raise NotImplementedError
        items = sorted(self.store.items())
        if reverse:
            items.reverse()
        for kv in items:
            yield kv


def test_iter_items_logs_fallback_on_not_implemented(tmp_path, caplog):
    # Arrange: create partition and swap in FakeDriver
    path = (tmp_path / "fallback").as_posix()
    p = SlateDBStorePartition(path=path)
    p._driver = FakeDriver()  # type: ignore[attr-defined]

    # Populate keys across and outside of bounds in CF 'default'
    cp = p._cf_ns("default")
    for k in [b"a", b"b", b"m", b"y", b"z"]:
        p._driver.put(cp + k, b"v")  # type: ignore[attr-defined]

    # Act: iterate with bounds b..z (exclusive upper bound)
    with caplog.at_level("INFO", logger="quixstreams.state.slatedb.partition"):
        out = list(p.iter_items(lower_bound=b"b", upper_bound=b"z", backwards=False))

    # Assert: fallback log present and correct items returned (b, m, y)
    logs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "Falling back to full scan for cf" in logs
    expected = [b"b", b"m", b"y"]
    assert [k for k, _ in out] == expected