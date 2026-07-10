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
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
)

TTL = timedelta(milliseconds=100)
FAR = timedelta(days=3650)


def _index_expiries(partition):
    """Decoded expiry stamps of every in-RAM ``__ttl_index__`` entry (repeats)."""
    index = partition._state.get(TTL_INDEX_CF_NAME, {})
    return [decode_index_key(key)[0] for key in index]


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


class TestMemorySweepVisitBudget:
    """#7 (review batch 2), memory parity (§8 open question default = yes): every
    index-entry visit (ghost or genuine) counts against the eviction budget in
    both memory sweep methods."""

    def _build_ghosts(self, partition, prefix, n, short, long):
        """Inject ``n`` PRE-EXISTING ghost index entries out-of-band. #8 (review
        batch 3) now inline-deletes refresh-minted stale entries at the source, so
        the retained visit-budget backstop is exercised with genuine injected
        ghosts: k0..k{n-1} stamped at E2 (long) in the main dict, plus a stale E1
        (short) ``__ttl_index__`` entry each (main stamp E2 != idx stamp E1)."""
        e1 = 1000 + int(short / timedelta(milliseconds=1))
        tx = partition.begin()
        for i in range(n):
            tx.set(key=f"k{i}", value="v", prefix=prefix, timestamp=1000, ttl=long)
        tx.prepare(processed_offsets={"topic": 2})
        tx.flush(changelog_offset=2)

        serializer = partition.begin()
        index = partition._state.setdefault(TTL_INDEX_CF_NAME, {})
        for i in range(n):
            key_serialized = serializer._serialize_key(f"k{i}", prefix=prefix)
            index[encode_index_key(e1, key_serialized)] = b""

    def test_on_path_ghost_visits_count_against_budget(self, changelog_producer_mock):
        """RED (HEAD): all N ghosts GC'd in one sweep. GREEN: only B per sweep."""
        prefix = b"pfx"
        n, b = 5, 2
        short = timedelta(milliseconds=100)
        long = timedelta(milliseconds=10_000)
        e1, e2 = 1100, 11000
        partition = MemoryStorePartition(
            changelog_producer=changelog_producer_mock, max_evictions_per_flush=b
        )
        _flip(partition, prefix)
        self._build_ghosts(partition, prefix, n, short, long)
        assert _index_expiries(partition).count(e1) == n
        assert _index_expiries(partition).count(e2) == n

        tx = partition.begin()
        tx.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=FAR)
        tx.prepare(processed_offsets={"topic": 4})
        tx.flush(changelog_offset=4)

        after = _index_expiries(partition)
        assert after.count(e1) == n - b
        assert after.count(e2) == n

        tx2 = partition.begin()
        tx2.set(key="advance", value="t", prefix=prefix, timestamp=5001, ttl=FAR)
        tx2.prepare(processed_offsets={"topic": 5})
        tx2.flush(changelog_offset=5)
        assert _index_expiries(partition).count(e1) == n - 2 * b

    def test_off_path_ghost_visits_count_against_budget(self, changelog_producer_mock):
        prefix = b"pfx"
        n, b = 5, 2
        short = timedelta(milliseconds=100)
        long = timedelta(milliseconds=10_000)
        e1, e2 = 1100, 11000
        partition = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
            max_evictions_per_flush=b,
            ttl_changelog_tombstones=False,
        )
        _flip(partition, prefix)
        self._build_ghosts(partition, prefix, n, short, long)
        assert _index_expiries(partition).count(e1) == n

        tx = partition.begin()
        tx.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=FAR)
        tx.prepare(processed_offsets={"topic": 4})
        tx.flush(changelog_offset=4)

        after = _index_expiries(partition)
        assert after.count(e1) == n - b
        assert after.count(e2) == n
