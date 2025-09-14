from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.partition import SlateDBStorePartition


def test_cf_namespaces_isolation_and_basic_ops():
    part = SlateDBStorePartition(
        path="/tmp/slatedb-devtest-cf", options=SlateDBOptions()
    )

    tx = part.begin()
    tx.set_bytes(key=b"k", value=b"vA", prefix=b"", cf_name="A")
    tx.set_bytes(key=b"k", value=b"vB", prefix=b"", cf_name="B")
    tx.prepare()
    tx.flush(changelog_offset=1)

    # Ensure isolation by CF name
    assert part.get(b"k", cf_name="A") == b"vA"
    assert part.get(b"k", cf_name="B") == b"vB"
    assert part.exists(b"k", cf_name="A") is True
    assert part.exists(b"k", cf_name="B") is True

    # Deleting only in CF A must keep CF B intact
    tx2 = part.begin()
    tx2.delete(key=b"k", prefix=b"", cf_name="A")
    tx2.prepare()
    tx2.flush(changelog_offset=2)

    assert part.exists(b"k", cf_name="A") is False
    assert part.get(b"k", cf_name="B") == b"vB"

    part.close()


def test_recovery_idempotent_put_and_delete():
    part = SlateDBStorePartition(
        path="/tmp/slatedb-devtest-recov", options=SlateDBOptions()
    )

    # Apply recovery message: put key in CF X
    part.recover_from_changelog_message(key=b"rk", value=b"rv", cf_name="X", offset=10)
    assert part.get(b"rk", cf_name="X") == b"rv"
    assert part.get_changelog_offset() == 10

    # Idempotent re-apply
    part.recover_from_changelog_message(key=b"rk", value=b"rv", cf_name="X", offset=11)
    assert part.get(b"rk", cf_name="X") == b"rv"
    assert part.get_changelog_offset() == 11

    # Delete via recovery
    part.recover_from_changelog_message(key=b"rk", value=None, cf_name="X", offset=12)
    assert part.exists(b"rk", cf_name="X") is False
    assert part.get_changelog_offset() == 12

    part.close()
