from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.timestamped import TimestampedSlateDBStorePartition


def test_timestamped_set_and_get_latest():
    part = TimestampedSlateDBStorePartition(
        path="/tmp/slatedb-devtest-ts",
        grace_ms=0,
        keep_duplicates=True,
        options=SlateDBOptions(),
    )

    tx = part.begin()
    tx.set_for_timestamp(timestamp=1000, value={"v": 1}, prefix=b"p")
    tx.set_for_timestamp(timestamp=1200, value={"v": 2}, prefix=b"p")
    tx.set_for_timestamp(timestamp=1100, value={"v": 3}, prefix=b"p")
    tx.prepare()
    tx.flush()

    tx2 = part.begin()
    assert tx2.get_latest(timestamp=1150, prefix=b"p") == {"v": 3}
    assert tx2.get_latest(timestamp=2000, prefix=b"p") == {"v": 2}

    part.close()
