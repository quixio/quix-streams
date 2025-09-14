from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.partition import SlateDBStorePartition


def test_crud_exists_and_offsets_cycle1():
    part = SlateDBStorePartition(path="/tmp/slatedb-devtest", options=SlateDBOptions())
    # Before any writes
    assert (
        part.get(b"k1") is not None
    )  # Marker.UNDEFINED is not bytes; but we can check exists==False
    assert part.exists(b"k1") is False
    # Prepare a transaction via the base transaction
    tx = part.begin()
    tx.set_bytes(key=b"k1", value=b"v1", prefix=b"", cf_name="default")
    tx.set_bytes(key=b"k2", value=b"v2", prefix=b"", cf_name="default")
    tx.prepare()
    tx.flush(changelog_offset=42)

    assert part.exists(b"k1") is True
    assert part.get(b"k1") == b"v1"
    assert part.get_changelog_offset() == 42

    # Delete k1
    tx2 = part.begin()
    tx2.delete(key=b"k1", prefix=b"", cf_name="default")
    tx2.prepare()
    tx2.flush(changelog_offset=43)

    assert part.exists(b"k1") is False
    assert part.get_changelog_offset() == 43

    # Manual offset write
    part.write_changelog_offset(100)
    assert part.get_changelog_offset() == 100

    part.close()
