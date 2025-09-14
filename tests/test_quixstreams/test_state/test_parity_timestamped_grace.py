import pytest


def _has_rocksdict():
    try:
        import rocksdict  # noqa: F401

        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_rocksdict(), reason="rocksdict not available")
def test_parity_timestamped_grace(tmp_path):
    from quixstreams.state import SlateDBOptions
    from quixstreams.state.rocksdb.options import RocksDBOptions
    from quixstreams.state.rocksdb.timestamped import (
        TimestampedStorePartition as RTsPart,
    )
    from quixstreams.state.slatedb.timestamped import (
        TimestampedSlateDBStorePartition as STsPart,
    )

    rocks = RTsPart(
        path=(tmp_path / "rocks-ts-grace").as_posix(),
        grace_ms=50,
        keep_duplicates=False,
        options=RocksDBOptions(open_max_retries=0),
    )
    slate = STsPart(
        path=(tmp_path / "slate-ts-grace").as_posix(),
        grace_ms=50,
        keep_duplicates=False,
        options=SlateDBOptions(),
    )

    try:
        # Set an initial value at t=1000
        for part in (rocks, slate):
            with part.begin() as tx:
                tx.set_for_timestamp(1000, {"v": 1}, prefix=b"p")

        # Attempt to set an older value beyond grace (t=900, grace=50 => 1000-900=100>50)
        for part in (rocks, slate):
            with part.begin() as tx:
                tx.set_for_timestamp(900, {"v": 0}, prefix=b"p")

        rtx = rocks.begin()
        stx = slate.begin()
        # Latest at t=2000 should still be the first value {"v":1}
        assert (
            rtx.get_latest(2000, prefix=b"p")
            == stx.get_latest(2000, prefix=b"p")
            == {"v": 1}
        )
    finally:
        rocks.close()
        slate.close()
