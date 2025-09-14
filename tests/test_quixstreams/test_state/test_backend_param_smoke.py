import pytest


def _has_rocksdict():
    try:
        import rocksdict  # noqa: F401

        return True
    except Exception:
        return False


def _make_windowed(backend: str):
    if backend == "rocksdb":
        if not _has_rocksdict():
            pytest.skip("rocksdict not available")
        from quixstreams.state.rocksdb.options import RocksDBOptions as Opt
        from quixstreams.state.rocksdb.windowed.partition import (
            WindowedRocksDBStorePartition as Part,
        )

        return Part(path="/tmp/rocks-windowed-param", options=Opt())
    else:
        from quixstreams.state import SlateDBOptions as Opt
        from quixstreams.state.slatedb.windowed.partition import (
            WindowedSlateDBStorePartition as Part,
        )

        return Part(path="/tmp/slate-windowed-param", options=Opt())


def _make_timestamped(backend: str):
    if backend == "rocksdb":
        if not _has_rocksdict():
            pytest.skip("rocksdict not available")
        from quixstreams.state.rocksdb.options import RocksDBOptions as Opt
        from quixstreams.state.rocksdb.timestamped import (
            TimestampedStorePartition as Part,
        )

        return Part(
            path="/tmp/rocks-ts-param", grace_ms=0, keep_duplicates=True, options=Opt()
        )
    else:
        from quixstreams.state import SlateDBOptions as Opt
        from quixstreams.state.slatedb.timestamped import (
            TimestampedSlateDBStorePartition as Part,
        )

        return Part(
            path="/tmp/slate-ts-param", grace_ms=0, keep_duplicates=True, options=Opt()
        )


@pytest.mark.parametrize("backend", ["rocksdb", "slatedb"])
def test_backend_windowed_smoke(backend):
    part = _make_windowed(backend)
    tx = part.begin()
    tx.update_window(1000, 2000, {"v": 1}, 1500, prefix=b"p")
    tx.update_window(2000, 3000, {"v": 2}, 2500, prefix=b"p")
    tx.update_window(3000, 4000, {"v": 3}, 3500, prefix=b"p")
    tx.prepare()
    tx.flush()

    t2 = part.begin()
    fwd = t2.get_windows(0, 5000, prefix=b"p")
    assert [w[0] for w in fwd] == [(1000, 2000), (2000, 3000), (3000, 4000)]

    rev = t2.get_windows(0, 5000, prefix=b"p", backwards=True)
    assert [w[0] for w in rev] == [(3000, 4000), (2000, 3000), (1000, 2000)]

    txd = part.begin()
    txd.delete_window(2000, 3000, prefix=b"p")
    txd.prepare()
    txd.flush()
    fwd2 = t2.get_windows(0, 5000, prefix=b"p")
    assert [w[0] for w in fwd2] == [(1000, 2000), (3000, 4000)]

    exp = t2.expire_windows(
        max_start_time=2000, prefix=b"p", delete=True, collect=False
    )
    assert [w[0] for w in exp] == [(1000, 2000)]  # since (2000,3000) already deleted

    part.close()


@pytest.mark.parametrize("backend", ["rocksdb", "slatedb"])
def test_backend_timestamped_smoke(backend):
    part = _make_timestamped(backend)
    tx = part.begin()
    tx.set_for_timestamp(1000, {"v": 1}, prefix=b"p")
    tx.set_for_timestamp(1200, {"v": 2}, prefix=b"p")
    tx.set_for_timestamp(1100, {"v": 3}, prefix=b"p")
    tx.prepare()
    tx.flush()

    t2 = part.begin()
    # Compare at safe checkpoints
    assert t2.get_latest(2000, prefix=b"p") == {"v": 2}
    assert t2.get_latest(3000, prefix=b"p") == {"v": 2}

    part.close()
