from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.windowed.partition import WindowedSlateDBStorePartition


def test_windowed_expire_windows_delete(tmp_path):
    path = (tmp_path / "slatedb-devtest-windowed-expire").as_posix()
    part = WindowedSlateDBStorePartition(path=path, options=SlateDBOptions())
    tx = part.begin()
    # Seed windows with starts 1000, 2000, 3000
    tx.update_window(1000, 2000, {"v": 1}, 1500, prefix=b"p")
    tx.update_window(2000, 3000, {"v": 2}, 2500, prefix=b"p")
    tx.update_window(3000, 4000, {"v": 3}, 3500, prefix=b"p")
    tx.prepare()
    tx.flush()

    tx2 = part.begin()
    expired = list(
        tx2.expire_windows(max_start_time=2000, prefix=b"p", delete=True, collect=False)
    )
    # Should return two windows (start 1000, 2000)
    assert [w[0] for w in expired] == [(1000, 2000), (2000, 3000)]

    remaining = tx2.get_windows(start_from_ms=0, start_to_ms=5000, prefix=b"p")
    assert [w[0] for w in remaining] == [(3000, 4000)]
