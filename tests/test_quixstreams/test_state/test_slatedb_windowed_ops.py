from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.windowed.partition import WindowedSlateDBStorePartition


def test_windowed_keys_get_windows_delete(tmp_path):
    path = (tmp_path / "slatedb-devtest-windowed-ops").as_posix()
    part = WindowedSlateDBStorePartition(path=path, options=SlateDBOptions())
    tx = part.begin()
    # Create windows [1000,2000), [2000,3000), [3000,4000)
    tx.update_window(1000, 2000, {"v": 1}, 1500, prefix=b"p")
    tx.update_window(2000, 3000, {"v": 2}, 2500, prefix=b"p")
    tx.update_window(3000, 4000, {"v": 3}, 3500, prefix=b"p")
    tx.prepare()
    tx.flush()

    # keys() yields encoded window keys; ensure non-empty
    tx2 = part.begin()
    ks = list(tx2.keys())
    assert any(k.startswith(b"p|") for k in ks)

    # get_windows forward
    fwd = tx2.get_windows(
        start_from_ms=999, start_to_ms=4000, prefix=b"p", backwards=False
    )
    assert [w[0] for w in fwd] == [(1000, 2000), (2000, 3000), (3000, 4000)]

    # reverse
    rev = tx2.get_windows(
        start_from_ms=999, start_to_ms=4000, prefix=b"p", backwards=True
    )
    assert [w[0] for w in rev] == [(3000, 4000), (2000, 3000), (1000, 2000)]

    # delete a window
    tx3 = part.begin()
    tx3.delete_window(2000, 3000, prefix=b"p")
    tx3.prepare()
    tx3.flush()

    fwd2 = tx2.get_windows(
        start_from_ms=999, start_to_ms=4000, prefix=b"p", backwards=False
    )
    assert [w[0] for w in fwd2] == [(1000, 2000), (3000, 4000)]
