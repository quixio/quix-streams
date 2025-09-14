import pytest


def _has_rocksdict():
    try:
        import rocksdict  # noqa: F401

        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_rocksdict(), reason="rocksdict not available")
def test_parity_windowed_reverse(tmp_path):
    from quixstreams.state import SlateDBOptions
    from quixstreams.state.rocksdb.options import RocksDBOptions
    from quixstreams.state.rocksdb.windowed.partition import (
        WindowedRocksDBStorePartition,
    )
    from quixstreams.state.slatedb.windowed.partition import (
        WindowedSlateDBStorePartition,
    )

    rocks = WindowedRocksDBStorePartition(
        path=(tmp_path / "rocks-win").as_posix(),
        options=RocksDBOptions(open_max_retries=0),
    )
    slate = WindowedSlateDBStorePartition(
        path=(tmp_path / "slate-win").as_posix(), options=SlateDBOptions()
    )

    try:
        # Seed windows
        for part in (rocks, slate):
            tx = part.begin()
            tx.update_window(1000, 2000, {"v": 1}, 1500, prefix=b"p")
            tx.update_window(2000, 3000, {"v": 2}, 2500, prefix=b"p")
            tx.update_window(3000, 4000, {"v": 3}, 3500, prefix=b"p")
            tx.prepare()
            tx.flush()

        rtx = rocks.begin()
        stx = slate.begin()
        r_rev = rtx.get_windows(0, 5000, prefix=b"p", backwards=True)
        s_rev = stx.get_windows(0, 5000, prefix=b"p", backwards=True)
        assert [w[0] for w in r_rev] == [w[0] for w in s_rev]

        # Delete a middle window on both and compare again
        for part in (rocks, slate):
            tx = part.begin()
            tx.delete_window(2000, 3000, prefix=b"p")
            tx.prepare()
            tx.flush()

        r_rev2 = rtx.get_windows(0, 5000, prefix=b"p", backwards=True)
        s_rev2 = stx.get_windows(0, 5000, prefix=b"p", backwards=True)
        assert [w[0] for w in r_rev2] == [w[0] for w in s_rev2]

        # Empty range parity (start_from == start_to yields empty)
        assert (
            list(rtx.get_windows(2000, 2000, prefix=b"p", backwards=False))
            == list(stx.get_windows(2000, 2000, prefix=b"p", backwards=False))
            == []
        )
    finally:
        rocks.close()
        slate.close()
