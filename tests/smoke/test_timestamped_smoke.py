import pytest


def _has_rocksdict():
    try:
        import rocksdict  # noqa: F401

        return True
    except Exception:
        return False


def _make_timestamped(backend: str):
    if backend == "rocksdb":
        if not _has_rocksdict():
            pytest.skip("rocksdict not available")
        from quixstreams.state.rocksdb.options import RocksDBOptions as Opt
        from quixstreams.state.rocksdb.timestamped import (
            TimestampedStorePartition as Part,
        )

        return Part(
            path="/tmp/rocks-ts-smoke", grace_ms=0, keep_duplicates=True, options=Opt()
        )
    else:
        from quixstreams.state import SlateDBOptions as Opt
        from quixstreams.state.slatedb.timestamped import (
            TimestampedSlateDBStorePartition as Part,
        )

        return Part(
            path="/tmp/slate-ts-smoke", grace_ms=0, keep_duplicates=True, options=Opt()
        )


@pytest.mark.parametrize("backend", ["rocksdb", "slatedb"])
def test_timestamped_basic(backend):
    part = _make_timestamped(backend)
    tx = part.begin()
    tx.set_for_timestamp(1000, {"v": 1}, prefix=b"p")
    tx.set_for_timestamp(1200, {"v": 2}, prefix=b"p")
    tx.set_for_timestamp(1100, {"v": 3}, prefix=b"p")
    tx.prepare()
    tx.flush()

    t2 = part.begin()
    assert t2.get_latest(2000, prefix=b"p") == {"v": 2}
    assert t2.get_latest(3000, prefix=b"p") == {"v": 2}

    part.close()
