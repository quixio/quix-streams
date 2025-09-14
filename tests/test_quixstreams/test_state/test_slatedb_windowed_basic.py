from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.windowed.partition import WindowedSlateDBStorePartition


def test_windowed_update_and_get(tmp_path):
    path = (tmp_path / "slatedb-devtest-windowed").as_posix()
    part = WindowedSlateDBStorePartition(path=path, options=SlateDBOptions())
    tx = part.begin()
    # Define a window [1000, 2000)
    tx.update_window(
        start_ms=1000, end_ms=2000, value={"sum": 3}, timestamp_ms=1500, prefix=b"p"
    )
    tx.prepare()
    tx.flush()

    tx2 = part.begin()
    val = tx2.get_window(start_ms=1000, end_ms=2000, prefix=b"p", default=None)
    assert val == {"sum": 3}
