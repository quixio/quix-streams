import pytest


def _has_rocksdict():
    try:
        import rocksdict  # noqa: F401

        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_rocksdict(), reason="rocksdict not available")
def test_parity_kv_iter_items(tmp_path):
    # Build RocksDB and SlateDB partitions
    from quixstreams.state import SlateDBOptions
    from quixstreams.state.rocksdb.options import RocksDBOptions
    from quixstreams.state.rocksdb.partition import RocksDBStorePartition as RPart
    from quixstreams.state.slatedb.partition import SlateDBStorePartition as SPart

    rpart = RPart(
        path=(tmp_path / "rocks-kv").as_posix(),
        options=RocksDBOptions(open_max_retries=0),
    )
    spart = SPart(path=(tmp_path / "slate-kv").as_posix(), options=SlateDBOptions())

    try:
        # Seed same keys in CF "D"
        for i in range(0, 10):
            key = f"k{i:02d}".encode()
            val = f"v{i}".encode()
            with rpart.begin() as rtx:
                rtx.set_bytes(key=key, value=val, prefix=b"", cf_name="D")
            with spart.begin() as stx:
                stx.set_bytes(key=key, value=val, prefix=b"", cf_name="D")

        def collect(part, lb: bytes, ub: bytes, rev: bool = False):
            return [
                (k, v)
                for k, v in part.iter_items(
                    lower_bound=lb, upper_bound=ub, backwards=rev, cf_name="D"
                )
            ]

        # Forward full range
        r_all = collect(rpart, b"", b"~")
        s_all = collect(spart, b"", b"~")
        assert [k for k, _ in r_all] == [k for k, _ in s_all]
        assert [v for _, v in r_all] == [v for _, v in s_all]

        # Subrange [k03, k07)
        r_sub = collect(rpart, b"k03", b"k07")
        s_sub = collect(spart, b"k03", b"k07")
        assert [k for k, _ in r_sub] == [k for k, _ in s_sub]

        # Reverse subrange [k03, k07)
        r_rev = collect(rpart, b"k03", b"k07", rev=True)
        s_rev = collect(spart, b"k03", b"k07", rev=True)
        assert [k for k, _ in r_rev] == [k for k, _ in s_rev]
    finally:
        rpart.close()
        spart.close()
