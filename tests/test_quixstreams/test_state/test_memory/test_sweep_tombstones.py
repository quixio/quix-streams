"""
Memory-backend parity for State-TTL sweep tombstones (spec ttl-changelog-tombstones
§5.4). The memory store's changelog is Kafka (same ``ChangelogProducer`` via base
``_prepare``), so a memory-backed store shrinks its changelog identically. Mirrors
the RocksDB black-box tests 2 (tombstone on eviction), 3 (re-written key not
tombstoned), and 5 (OFF escape hatch).
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
)
from quixstreams.state.rocksdb.ttl_codec import decode_ttl_value

TTL = timedelta(milliseconds=100)
FAR = timedelta(days=3650)


def _default_tombstones(mock):
    out = []
    for call in mock.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        if (
            call.kwargs.get("value") is None
            and headers.get(CHANGELOG_CF_MESSAGE_HEADER) == "default"
            and headers.get(CHANGELOG_TTL_STAMPED_HEADER)
        ):
            out.append(call.kwargs.get("key"))
    return out


def _flip(partition, prefix=b"pfx"):
    tx = partition.begin()
    tx.set(key="seed", value="s", prefix=prefix, timestamp=1000, ttl=FAR)
    tx.prepare(processed_offsets={"topic": 1})
    tx.flush(changelog_offset=1)


class TestMemorySweepTombstones:
    def test_eviction_produces_tombstone(self, changelog_producer_mock):
        prefix = b"pfx"
        partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _flip(partition, prefix)
        tx1 = partition.begin()
        tx1.set(key="k", value="v", prefix=prefix, timestamp=1000, ttl=TTL)
        user_key = tx1._serialize_key(key="k", prefix=prefix)
        tx1.prepare(processed_offsets={"topic": 2})
        tx1.flush(changelog_offset=2)

        changelog_producer_mock.produce.reset_mock()
        tx2 = partition.begin()
        tx2.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
        tx2.prepare(processed_offsets={"topic": 3})
        tx2.flush(changelog_offset=3)

        assert _default_tombstones(changelog_producer_mock) == [user_key]
        assert user_key not in partition._state.get("default", {})

    def test_rewritten_key_same_flush_not_tombstoned(self, changelog_producer_mock):
        prefix = b"pfx"
        partition = MemoryStorePartition(changelog_producer=changelog_producer_mock)
        _flip(partition, prefix)
        tx1 = partition.begin()
        tx1.set(key="k", value="v1", prefix=prefix, timestamp=1000, ttl=TTL)
        user_key = tx1._serialize_key(key="k", prefix=prefix)
        tx1.prepare(processed_offsets={"topic": 2})
        tx1.flush(changelog_offset=2)

        changelog_producer_mock.produce.reset_mock()
        tx2 = partition.begin()
        tx2.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
        tx2.set(key="k", value="v2", prefix=prefix, timestamp=5000, ttl=TTL)
        tx2.prepare(processed_offsets={"topic": 3})
        tx2.flush(changelog_offset=3)

        assert user_key not in _default_tombstones(changelog_producer_mock)
        _, payload = decode_ttl_value(partition._state["default"][user_key])
        assert payload == b'"v2"'

    def test_off_escape_hatch_no_tombstone(self, changelog_producer_mock):
        prefix = b"pfx"
        partition = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
            ttl_changelog_tombstones=False,
        )
        _flip(partition, prefix)
        tx1 = partition.begin()
        tx1.set(key="k", value="v", prefix=prefix, timestamp=1000, ttl=TTL)
        user_key = tx1._serialize_key(key="k", prefix=prefix)
        tx1.prepare(processed_offsets={"topic": 2})
        tx1.flush(changelog_offset=2)

        changelog_producer_mock.produce.reset_mock()
        tx2 = partition.begin()
        tx2.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
        tx2.prepare(processed_offsets={"topic": 3})
        tx2.flush(changelog_offset=3)

        # OFF path: local-only _run_sweep evicted K but produced NO tombstone.
        assert _default_tombstones(changelog_producer_mock) == []
        assert user_key not in partition._state.get("default", {})
