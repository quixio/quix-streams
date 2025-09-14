from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.timestamped import TimestampedStorePartition as RTsPart
from quixstreams.state.slatedb.options import SlateDBOptions
from quixstreams.state.slatedb.timestamped import (
    TimestampedSlateDBStorePartition as STsPart,
)


def _seed_same_ts(part, prefix: bytes):
    tx = part.begin()
    # Multiple writes at same timestamp with keep_duplicates=True
    tx.set_for_timestamp(2000, {"v": 1}, prefix=prefix)
    tx.set_for_timestamp(2000, {"v": 2}, prefix=prefix)
    tx.set_for_timestamp(2000, {"v": 3}, prefix=prefix)
    tx.prepare()
    tx.flush()


def _latest(part, ts: int, prefix: bytes):
    tx = part.begin()
    return tx.get_latest(ts, prefix=prefix)


def test_parity_timestamped_keep_duplicates_multi_entry(tmp_path):
    rocks = RTsPart(
        path=str(tmp_path / "r"),
        grace_ms=0,
        keep_duplicates=True,
        options=RocksDBOptions(),
    )
    slate = STsPart(
        path=str(tmp_path / "s"),
        grace_ms=0,
        keep_duplicates=True,
        options=SlateDBOptions(),
    )
    try:
        _seed_same_ts(rocks, b"p")
        _seed_same_ts(slate, b"p")
        # At exactly 2000, both should report the latest value at that timestamp
        assert _latest(rocks, 2000, b"p") == _latest(slate, 2000, b"p") == {"v": 3}
        # After 2000, still latest is {"v": 3}
        assert _latest(rocks, 3000, b"p") == _latest(slate, 3000, b"p") == {"v": 3}
    finally:
        rocks.close()
        slate.close()
