from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.partition import SlateDBStorePartition


def test_iteration_bounds_forward_and_reverse():
    part = SlateDBStorePartition(
        path="/tmp/slatedb-devtest-iter", options=SlateDBOptions()
    )

    # Seed some keys in CF "D"
    tx = part.begin()
    for i in range(0, 10):
        key = f"k{i:02d}".encode()
        val = f"v{i}".encode()
        tx.set_bytes(key=key, value=val, prefix=b"", cf_name="D")
    tx.prepare()
    tx.flush(changelog_offset=100)

    def collect(lb: bytes, ub: bytes, rev: bool = False):
        return [
            (k, v)
            for k, v in part.iter_items(
                lower_bound=lb, upper_bound=ub, backwards=rev, cf_name="D"
            )
        ]

    # Forward full range
    all_items = collect(b"", b"~")  # '~' > 'z' in ASCII; acts as high sentinel
    assert len(all_items) == 10
    assert all_items[0][0] == b"k00"
    assert all_items[-1][0] == b"k09"

    # Forward subrange [k03, k07)
    sub = collect(b"k03", b"k07")
    assert [k for k, _ in sub] == [b"k03", b"k04", b"k05", b"k06"]

    # Reverse subrange [k03, k07)
    sub_rev = collect(b"k03", b"k07", rev=True)
    assert [k for k, _ in sub_rev] == [b"k06", b"k05", b"k04", b"k03"]

    part.close()
