import pytest


def _has_rocksdict():
    try:
        import rocksdict  # noqa: F401

        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_rocksdict(), reason="rocksdict not available")
def test_parity_windowed_smoke():
    from quixstreams.state import SlateDBOptions
    from quixstreams.state.rocksdb.options import RocksDBOptions
    from quixstreams.state.rocksdb.windowed.partition import (
        WindowedRocksDBStorePartition,
    )
    from quixstreams.state.slatedb.windowed.partition import (
        WindowedSlateDBStorePartition,
    )

    # Create partitions
    rocks = WindowedRocksDBStorePartition(
        path="/tmp/rocks-windowed-parity", options=RocksDBOptions()
    )
    slate = WindowedSlateDBStorePartition(
        path="/tmp/slate-windowed-parity", options=SlateDBOptions()
    )

    # Seed same windows
    for part in (rocks, slate):
        tx = part.begin()
        tx.update_window(1000, 2000, {"v": 1}, 1500, prefix=b"p")
        tx.update_window(2000, 3000, {"v": 2}, 2500, prefix=b"p")
        tx.update_window(3000, 4000, {"v": 3}, 3500, prefix=b"p")
        tx.prepare()
        tx.flush()

    # Compare get_windows forward
    rtx = rocks.begin()
    stx = slate.begin()
    r_fwd = rtx.get_windows(0, 5000, prefix=b"p")
    s_fwd = stx.get_windows(0, 5000, prefix=b"p")
    assert [w[0] for w in r_fwd] == [w[0] for w in s_fwd]

    # Delete a window on both
    for part in (rocks, slate):
        tx = part.begin()
        tx.delete_window(2000, 3000, prefix=b"p")
        tx.prepare()
        tx.flush()

    r_fwd2 = rtx.get_windows(0, 5000, prefix=b"p")
    s_fwd2 = stx.get_windows(0, 5000, prefix=b"p")
    assert [w[0] for w in r_fwd2] == [w[0] for w in s_fwd2]

    # Expire windows up to 2000
    r_exp = rtx.expire_windows(
        max_start_time=2000, prefix=b"p", delete=True, collect=False
    )
    s_exp = stx.expire_windows(
        max_start_time=2000, prefix=b"p", delete=True, collect=False
    )
    assert [w[0] for w in r_exp] == [w[0] for w in s_exp]

    rocks.close()
    slate.close()


@pytest.mark.skipif(not _has_rocksdict(), reason="rocksdict not available")
def test_parity_timestamped_smoke():
    from quixstreams.state import SlateDBOptions
    from quixstreams.state.rocksdb.options import RocksDBOptions
    from quixstreams.state.rocksdb.timestamped import (
        TimestampedStorePartition as RTsPart,
    )
    from quixstreams.state.slatedb.timestamped import (
        TimestampedSlateDBStorePartition as STsPart,
    )

    rocks = RTsPart(
        path="/tmp/rocks-ts-parity",
        grace_ms=0,
        keep_duplicates=True,
        options=RocksDBOptions(),
    )
    slate = STsPart(
        path="/tmp/slate-ts-parity",
        grace_ms=0,
        keep_duplicates=True,
        options=SlateDBOptions(),
    )

    for part in (rocks, slate):
        tx = part.begin()
        tx.set_for_timestamp(1000, {"v": 1}, prefix=b"p")
        tx.set_for_timestamp(1200, {"v": 2}, prefix=b"p")
        tx.set_for_timestamp(1100, {"v": 3}, prefix=b"p")
        tx.prepare()
        tx.flush()

    rtx = rocks.begin()
    stx = slate.begin()
    # Compare at clear boundary and at end
    assert rtx.get_latest(2000, prefix=b"p") == stx.get_latest(2000, prefix=b"p")
    assert rtx.get_latest(3000, prefix=b"p") == stx.get_latest(3000, prefix=b"p")

    rocks.close()
    slate.close()
