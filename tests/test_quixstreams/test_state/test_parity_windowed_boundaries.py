import pytest

from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.windowed.partition import WindowedRocksDBStorePartition
from quixstreams.state.slatedb.options import SlateDBOptions
from quixstreams.state.slatedb.windowed.partition import WindowedSlateDBStorePartition


def _seed_three_windows(part):
    tx = part.begin()
    # Create windows [1000,2000), [2000,3000), [3000,4000)
    tx.update_window(1000, 2000, {"v": 1}, timestamp_ms=1000, prefix=b"p")
    tx.update_window(2000, 3000, {"v": 2}, timestamp_ms=2000, prefix=b"p")
    tx.update_window(3000, 4000, {"v": 3}, timestamp_ms=3000, prefix=b"p")
    tx.prepare()
    tx.flush()


def _collect(part, start_from, start_to):
    tx = part.begin()
    return [
        w[0]
        for w in tx.get_windows(
            start_from_ms=start_from, start_to_ms=start_to, prefix=b"p"
        )
    ]


@pytest.mark.parametrize("backend", ["rocksdb", "slatedb"])
def test_windowed_boundaries_smoke(tmp_path, backend):
    # Create partitions for each backend
    if backend == "rocksdb":
        rpart = WindowedRocksDBStorePartition(
            path=str(tmp_path / "r"), options=RocksDBOptions()
        )
        spart = None
        part = rpart
    else:
        spart = WindowedSlateDBStorePartition(
            path=str(tmp_path / "s"), options=SlateDBOptions()
        )
        rpart = None
        part = spart

    try:
        _seed_three_windows(part)
        # Lower exclusive, upper inclusive semantics examples:
        # - [1000, 2000] excludes start=1000, includes start=2000
        assert _collect(part, 1000, 2000) == [(2000, 3000)]
        # - Equal bounds yield empty (no start s.t. s > X and s <= X)
        assert _collect(part, 1000, 1000) == []
        assert _collect(part, 2000, 2000) == []
        # Mid-range: [1500, 2500] should include only window starting at 2000
        assert _collect(part, 1500, 2500) == [(2000, 3000)]
        # Full-ish range: include all windows
        assert _collect(part, -1, 5000) == [(1000, 2000), (2000, 3000), (3000, 4000)]
    finally:
        if rpart:
            rpart.close()
        if spart:
            spart.close()


def test_windowed_boundaries_parity(tmp_path):
    rocks = WindowedRocksDBStorePartition(
        path=str(tmp_path / "r"), options=RocksDBOptions()
    )
    slate = WindowedSlateDBStorePartition(
        path=str(tmp_path / "s"), options=SlateDBOptions()
    )
    try:
        _seed_three_windows(rocks)
        _seed_three_windows(slate)
        cases = [
            (1000, 2000),
            (1000, 1000),
            (2000, 2000),
            (1500, 2500),
            (-1, 5000),
        ]
        for start_from, start_to in cases:
            r = _collect(rocks, start_from, start_to)
            s = _collect(slate, start_from, start_to)
            assert r == s
    finally:
        rocks.close()
        slate.close()
