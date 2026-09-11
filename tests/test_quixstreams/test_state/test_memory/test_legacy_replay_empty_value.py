"""
Memory legacy-replay truthiness.

If ``MemoryStorePartition.recover_from_changelog_message`` routed a header-absent
(legacy) default-CF record through ``if value:``, it would treat a legitimate
empty-bytes value (``b""``) as falsy and delete the key, silently losing it.
RocksDB's equivalent legacy branch keys off ``if value is None:`` so only a
genuine tombstone (``None``) deletes. This asserts the memory backend mirrors
RocksDB: a real ``b""`` legacy value survives replay.
"""

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME


class TestLegacyReplayEmptyValue:
    def test_empty_bytes_legacy_value_survives_replay(self):
        """A header-absent legacy record whose value is ``b""`` must land in the
        default CF as ``b""`` (not be dropped as a tombstone).

        Only a real ``None`` tombstone deletes; a ``b""`` value must survive
        replay and remain present.
        """
        key = b"pfx|k0"
        partition = MemoryStorePartition(changelog_producer=None)
        # Un-flipped legacy partition (memory starts legacy after __init__).
        assert partition.uses_ttl_stamps is False

        partition.recover_from_changelog_message(
            key=key,
            value=b"",
            cf_name="default",
            offset=0,
            ttl_stamped=False,
        )

        default = partition._state.get("default", {})
        assert key in default, "empty-bytes legacy value must not be dropped"
        assert default[key] == b"", "the surviving value must be exactly b''"
        partition.close()

    def test_none_tombstone_still_deletes(self):
        """Sibling guard: a genuine tombstone (``value=None``) still deletes the
        key on the legacy branch (unchanged behavior)."""
        key = b"pfx|k0"
        partition = MemoryStorePartition(changelog_producer=None)

        # First land a real value, then tombstone it.
        partition.recover_from_changelog_message(
            key=key, value=b"payload", cf_name="default", offset=0, ttl_stamped=False
        )
        assert key in partition._state.get("default", {})
        partition.recover_from_changelog_message(
            key=key, value=None, cf_name="default", offset=1, ttl_stamped=False
        )

        assert key not in partition._state.get(
            "default", {}
        ), "a None tombstone must delete the key"
        partition.close()

    def test_empty_bytes_value_survives_complete_recovery(self):
        """The surviving ``b""`` value must not be discarded by
        ``complete_recovery`` (pure-legacy replay discards only the orphan
        pending census, never the default-CF value)."""
        key = b"pfx|k0"
        partition = MemoryStorePartition(changelog_producer=None)
        partition.recover_from_changelog_message(
            key=key, value=b"", cf_name="default", offset=0, ttl_stamped=False
        )
        partition.complete_recovery()

        default = partition._state.get("default", {})
        assert default.get(key) == b"", "b'' value must survive completion"
        # Pure-legacy hygiene: the orphan census is drained.
        assert not partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        partition.close()
