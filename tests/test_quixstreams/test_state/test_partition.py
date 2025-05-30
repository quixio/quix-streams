import pytest

from quixstreams.state.manager import SUPPORTED_STORES
from quixstreams.state.metadata import Marker


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
        store_partition.recover_from_changelog_message(
            key=b"key", value=b"value", cf_name="default", offset=1
        )
        assert store_partition.get(b"key") == b"value"
        assert store_partition.get_changelog_offset() == 1

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
