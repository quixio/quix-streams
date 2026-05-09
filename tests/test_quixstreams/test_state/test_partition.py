import pytest

from quixstreams.state.manager import SUPPORTED_STORES
from quixstreams.state.metadata import Marker
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_ttl_value,
    encode_ttl_value,
)


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestStorePartition:
    def test_open_db_close(self, store_partition_factory):
        with store_partition_factory():
            ...

    def test_get_db_closed_fails(self, store_partition_factory):
        store_partition = store_partition_factory()
        store_partition.close()
        with pytest.raises(Exception):
            store_partition.get(b"key")

    @pytest.mark.parametrize("store_value", [10, None])
    def test_recover_from_changelog_message_success(self, store_value, store_partition):
        # v3: a fresh partition replaying an unstamped legacy value stays in
        # legacy mode (the recovery flag-discovery heuristic only flips on a
        # plausibly-stamped first value). The on-disk payload is the raw
        # value, byte-identical to v3.23.6 — no sentinel wrapping.
        store_partition.recover_from_changelog_message(
            key=b"key", value=b"value", cf_name="default", offset=1
        )
        assert store_partition.get(b"key") == b"value"
        assert store_partition.uses_ttl_stamps is False
        assert store_partition.get_changelog_offset() == 1

    def test_recover_from_changelog_message_already_stamped(self, store_partition):
        # An already-stamped replay value (>= 8 bytes, sentinel stamp) flips
        # the recovery partition into TTL mode and is stored verbatim.
        stamped = encode_ttl_value(SENTINEL_NEVER, b"value")
        store_partition.recover_from_changelog_message(
            key=b"key", value=stamped, cf_name="default", offset=2
        )
        assert store_partition.get(b"key") == stamped
        assert store_partition.uses_ttl_stamps is True
        assert store_partition.get_changelog_offset() == 2

    def test_recover_from_changelog_message_missing_cf(self, store_partition):
        store_partition.recover_from_changelog_message(
            key=b"key", value=b"value", cf_name="some_cf", offset=1
        )
        assert store_partition.get(b"key") == Marker.UNDEFINED
        assert store_partition.get_changelog_offset() == 1

    def test_write_changelog_offset(self, store_partition):
        assert store_partition.get_changelog_offset() is None
        store_partition.write_changelog_offset(offset=1)
        assert store_partition.get_changelog_offset() == 1
