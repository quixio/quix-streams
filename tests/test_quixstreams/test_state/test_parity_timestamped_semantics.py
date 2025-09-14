import pytest


def _has_rocksdict():
    try:
        import rocksdict  # noqa: F401

        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_rocksdict(), reason="rocksdict not available")
def test_parity_timestamped_same_ts_overwrite(tmp_path):
    from quixstreams.state import SlateDBOptions
    from quixstreams.state.rocksdb.options import RocksDBOptions
    from quixstreams.state.rocksdb.timestamped import (
        TimestampedStorePartition as RTsPart,
    )
    from quixstreams.state.slatedb.timestamped import (
        TimestampedSlateDBStorePartition as STsPart,
    )

    rocks = RTsPart(
        path=(tmp_path / "rocks-ts").as_posix(),
        grace_ms=0,
        keep_duplicates=False,
        options=RocksDBOptions(open_max_retries=0),
    )
    slate = STsPart(
        path=(tmp_path / "slate-ts").as_posix(),
        grace_ms=0,
        keep_duplicates=False,
        options=SlateDBOptions(),
    )

    try:
        # Insert same timestamp twice with different values; expect overwrite
        for part in (rocks, slate):
            with part.begin() as tx:
                tx.set_for_timestamp(1000, {"v": 1}, prefix=b"p")
                tx.set_for_timestamp(1000, {"v": 2}, prefix=b"p")

        rtx = rocks.begin()
        stx = slate.begin()
        assert (
            rtx.get_latest(2000, prefix=b"p")
            == stx.get_latest(2000, prefix=b"p")
            == {"v": 2}
        )
    finally:
        rocks.close()
        slate.close()


@pytest.mark.skipif(not _has_rocksdict(), reason="rocksdict not available")
def test_parity_timestamped_keep_duplicates_true(tmp_path):
    from quixstreams.state import SlateDBOptions
    from quixstreams.state.rocksdb.options import RocksDBOptions
    from quixstreams.state.rocksdb.timestamped import (
        TimestampedStorePartition as RTsPart,
    )
    from quixstreams.state.slatedb.timestamped import (
        TimestampedSlateDBStorePartition as STsPart,
    )

    rocks = RTsPart(
        path=(tmp_path / "rocks-ts-dups").as_posix(),
        grace_ms=0,
        keep_duplicates=True,
        options=RocksDBOptions(open_max_retries=0),
    )
    slate = STsPart(
        path=(tmp_path / "slate-ts-dups").as_posix(),
        grace_ms=0,
        keep_duplicates=True,
        options=SlateDBOptions(),
    )

    try:
        for part in (rocks, slate):
            with part.begin() as tx:
                tx.set_for_timestamp(1000, {"v": 1}, prefix=b"p")
                tx.set_for_timestamp(1000, {"v": 2}, prefix=b"p")

        rtx = rocks.begin()
        stx = slate.begin()
        # Latest should be the second value per our RocksDB behavior
        assert (
            rtx.get_latest(2000, prefix=b"p")
            == stx.get_latest(2000, prefix=b"p")
            == {"v": 2}
        )
    finally:
        rocks.close()
        slate.close()
