"""
State-TTL changelog reduction via sweep tombstones (spec ttl-changelog-tombstones
§5.1). With ``ttl_changelog_tombstones=True`` (default) a TTL eviction is staged
into the transaction cache at prepare-time, so the base ``_prepare`` produces a
changelog tombstone (``value=None``) for the evicted key and ``write()`` applies
the local delete — the changelog physically shrinks under compaction in step with
the store. Index-CF GC rides the same cache but is local-only (never produced).
The ``ttl_changelog_tombstones=False`` escape hatch restores the pre-change
local-only ``_run_sweep`` path.
"""

from datetime import timedelta

from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.ttl_codec import decode_index_key, decode_ttl_value

TTL = timedelta(milliseconds=100)
FAR = timedelta(days=3650)


def _index_expiries(partition):
    """Decoded expiry stamps of every ``__ttl_index__`` entry (with repeats)."""
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    return [decode_index_key(bytes(key))[0] for key, _ in cf.items()]


def _default_tombstones(mock):
    """Keys produced as default-CF tombstones (value=None) with the TTL header."""
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


def _produced_cfs(mock):
    return [
        (call.kwargs.get("headers") or {}).get(CHANGELOG_CF_MESSAGE_HEADER)
        for call in mock.produce.call_args_list
    ]


def _flip(partition, prefix=b"pfx"):
    """First ttl= write flips the (empty) partition into TTL mode. The seed uses a
    far-future ttl so it is never swept and does not perturb eviction assertions."""
    tx = partition.begin()
    tx.set(
        key="seed", value="s", prefix=prefix, timestamp=1000, ttl=timedelta(days=3650)
    )
    tx.prepare(processed_offsets={"topic": 1})
    tx.flush(changelog_offset=1)
    return partition


class TestSweepTombstones:
    def test_eviction_produces_tombstone(
        self, store_partition_factory, changelog_producer_mock
    ):
        prefix = b"pfx"
        with store_partition_factory() as partition:
            _flip(partition, prefix)

            # Write K expiring at 1100.
            tx1 = partition.begin()
            tx1.set(key="k", value="v", prefix=prefix, timestamp=1000, ttl=TTL)
            user_key = tx1._serialize_key(key="k", prefix=prefix)
            tx1.prepare(processed_offsets={"topic": 2})
            tx1.flush(changelog_offset=2)

            changelog_producer_mock.produce.reset_mock()
            # Advance high-water past 1100 with an unrelated future write → K is
            # swept and tombstoned at prepare-time.
            tx2 = partition.begin()
            tx2.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
            tx2.prepare(processed_offsets={"topic": 3})
            tx2.flush(changelog_offset=3)

            assert _default_tombstones(changelog_producer_mock) == [user_key]
            # And K is gone locally.
            main_cf = partition.get_or_create_column_family("default")
            assert main_cf.get(user_key, default=None) is None

    def test_rewritten_key_same_flush_not_tombstoned(
        self, store_partition_factory, changelog_producer_mock
    ):
        prefix = b"pfx"
        with store_partition_factory() as partition:
            _flip(partition, prefix)
            tx1 = partition.begin()
            tx1.set(key="k", value="v1", prefix=prefix, timestamp=1000, ttl=TTL)
            user_key = tx1._serialize_key(key="k", prefix=prefix)
            tx1.prepare(processed_offsets={"topic": 2})
            tx1.flush(changelog_offset=2)

            changelog_producer_mock.produce.reset_mock()
            # Same flush: advance past K's expiry AND re-write K with a fresh ttl.
            tx2 = partition.begin()
            tx2.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
            tx2.set(key="k", value="v2", prefix=prefix, timestamp=5000, ttl=TTL)
            tx2.prepare(processed_offsets={"topic": 3})
            tx2.flush(changelog_offset=3)

            # No tombstone for K (guard) and K reads back the fresh value.
            assert user_key not in _default_tombstones(changelog_producer_mock)
            raw = partition.get(user_key, "default")
            _, payload = decode_ttl_value(raw)
            assert payload == b'"v2"'

    def test_index_gc_never_hits_changelog(
        self, store_partition_factory, changelog_producer_mock
    ):
        prefix = b"pfx"
        with store_partition_factory() as partition:
            _flip(partition, prefix)
            tx1 = partition.begin()
            tx1.set(key="k", value="v", prefix=prefix, timestamp=1000, ttl=TTL)
            user_key = tx1._serialize_key(key="k", prefix=prefix)
            tx1.prepare(processed_offsets={"topic": 2})
            tx1.flush(changelog_offset=2)

            # User-deletes K → leaves a stale index entry (ghost).
            tx2 = partition.begin()
            tx2.delete(key="k", prefix=prefix)
            tx2.prepare(processed_offsets={"topic": 3})
            tx2.flush(changelog_offset=3)

            changelog_producer_mock.produce.reset_mock()
            # A later writing flush sweeps the ghost index entry (local-only GC).
            tx3 = partition.begin()
            tx3.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
            tx3.prepare(processed_offsets={"topic": 4})
            tx3.flush(changelog_offset=4)

            # No index-CF record ever produced; no new tombstone for the (already
            # deleted) K.
            assert TTL_INDEX_CF_NAME not in _produced_cfs(changelog_producer_mock)
            assert user_key not in _default_tombstones(changelog_producer_mock)

    def test_off_escape_hatch_no_tombstone(
        self, store_partition_factory, changelog_producer_mock
    ):
        prefix = b"pfx"
        with store_partition_factory(
            options=RocksDBOptions(ttl_changelog_tombstones=False)
        ) as partition:
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
            main_cf = partition.get_or_create_column_family("default")
            assert main_cf.get(user_key, default=None) is None  # still evicted locally

    def test_no_write_flush_does_not_sweep(
        self, store_partition_factory, changelog_producer_mock
    ):
        prefix = b"pfx"
        with store_partition_factory() as partition:
            _flip(partition, prefix)
            tx1 = partition.begin()
            tx1.set(key="k", value="v", prefix=prefix, timestamp=1000, ttl=TTL)
            user_key = tx1._serialize_key(key="k", prefix=prefix)
            tx1.prepare(processed_offsets={"topic": 2})
            tx1.flush(changelog_offset=2)

            changelog_producer_mock.produce.reset_mock()
            # Read-only tx advances high-water past K's expiry but writes nothing:
            # the prepare-scan is gated on a non-empty cache, so no sweep.
            tx2 = partition.begin()
            tx2.get("k", prefix=prefix, timestamp=5000)
            tx2.prepare(processed_offsets={"topic": 3})
            tx2.flush(changelog_offset=3)
            assert _default_tombstones(changelog_producer_mock) == []
            main_cf = partition.get_or_create_column_family("default")
            assert main_cf.get(user_key, default=None) is not None  # not yet swept

            # The next writing flush evicts + tombstones it.
            tx3 = partition.begin()
            tx3.set(key="advance", value="t", prefix=prefix, timestamp=5001, ttl=TTL)
            tx3.prepare(processed_offsets={"topic": 4})
            tx3.flush(changelog_offset=4)
            assert user_key in _default_tombstones(changelog_producer_mock)

    def test_budget_cap_on_on_path(
        self, store_partition_factory, changelog_producer_mock
    ):
        prefix = b"pfx"
        with store_partition_factory(
            options=RocksDBOptions(max_evictions_per_flush=2)
        ) as partition:
            _flip(partition, prefix)
            tx1 = partition.begin()
            for i in range(5):
                tx1.set(key=f"k{i}", value="v", prefix=prefix, timestamp=1000, ttl=TTL)
            tx1.prepare(processed_offsets={"topic": 2})
            tx1.flush(changelog_offset=2)

            changelog_producer_mock.produce.reset_mock()
            tx2 = partition.begin()
            tx2.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
            tx2.prepare(processed_offsets={"topic": 3})
            tx2.flush(changelog_offset=3)
            # At most `budget` (2) tombstones this flush.
            assert len(_default_tombstones(changelog_producer_mock)) == 2

    def test_compacted_rebuild_eviction_without_filter(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The tombstone (not the read-time Rule 4 filter) removes the key on
        rebuild: replay the captured stream into a fresh partition with a recovery
        wallclock BELOW the key's expiry, so Rule 4 would keep it — yet it is
        absent because the tombstone deleted it."""
        prefix = b"pfx"
        with store_partition_factory(name="src") as partition:
            _flip(partition, prefix)
            tx1 = partition.begin()
            tx1.set(key="k", value="v", prefix=prefix, timestamp=1000, ttl=TTL)
            user_key = tx1._serialize_key(key="k", prefix=prefix)
            tx1.prepare(processed_offsets={"topic": 2})
            tx1.flush(changelog_offset=2)
            tx2 = partition.begin()
            tx2.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=TTL)
            tx2.prepare(processed_offsets={"topic": 3})
            tx2.flush(changelog_offset=3)
            assert user_key in _default_tombstones(changelog_producer_mock)

        # Rebuild: replay every captured default-CF record (puts + the tombstone)
        # in offset order into a fresh partition, with the recovery clock pinned
        # BELOW K's expiry (1100) so the Rule 4 filter would NOT drop K.
        replay = []
        for call in changelog_producer_mock.produce.call_args_list:
            headers = call.kwargs.get("headers") or {}
            if headers.get(CHANGELOG_CF_MESSAGE_HEADER) == "default":
                replay.append(
                    (
                        call.kwargs.get("key"),
                        call.kwargs.get("value"),
                        bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER)),
                    )
                )

        with store_partition_factory(name="dst") as rebuilt:
            rebuilt._now_ms = lambda: 1050  # below K's expiry 1100
            offset = 0
            for key, value, ttl_stamped in replay:
                rebuilt.recover_from_changelog_message(
                    key=key,
                    value=value,
                    cf_name="default",
                    offset=offset,
                    ttl_stamped=ttl_stamped,
                )
                offset += 1
            main_cf = rebuilt.get_or_create_column_family("default")
            assert main_cf.get(user_key, default=None) is None


class TestSweepVisitBudget:
    """#7 (review batch 2): every index-entry visit (ghost or genuine) must
    count against the eviction budget, so a store dense with refresh-minted
    ghost index entries cannot pay more than ``budget`` main-CF point-gets per
    sweep."""

    def _build_ghosts(self, partition, prefix, n, short, long):
        """Refresh-mint ``n`` ghost index entries: stamp k0..k{n-1} at expiry E1
        (flush 1), then re-stamp them at a later expiry E2 (flush 2). The write
        path adds the E2 index entry but never removes the E1 one, so each old
        E1 entry becomes a ghost (main stamp E2 != idx stamp E1). Both flushes
        keep high_water at 1000 (< E1) so neither sweeps."""
        tx1 = partition.begin()
        for i in range(n):
            tx1.set(key=f"k{i}", value="v", prefix=prefix, timestamp=1000, ttl=short)
        tx1.prepare(processed_offsets={"topic": 2})
        tx1.flush(changelog_offset=2)

        tx2 = partition.begin()
        for i in range(n):
            tx2.set(key=f"k{i}", value="v", prefix=prefix, timestamp=1000, ttl=long)
        tx2.prepare(processed_offsets={"topic": 3})
        tx2.flush(changelog_offset=3)

    def test_ghost_visits_count_against_budget(
        self, store_partition_factory, changelog_producer_mock
    ):
        """RED (HEAD): all N ghost index entries are GC'd in one sweep (the loop
        counts only genuine evictions, so ghosts consume no budget → N unbudgeted
        point-gets).
        GREEN: exactly B index entries are visited/GC'd this sweep; N-B ghosts
        remain and are cleaned by later sweeps (convergent)."""
        prefix = b"pfx"
        n, b = 5, 2
        short = timedelta(milliseconds=100)  # E1 = 1100
        long = timedelta(milliseconds=10_000)  # E2 = 11000
        e1, e2 = 1100, 11000
        with store_partition_factory(
            options=RocksDBOptions(max_evictions_per_flush=b)
        ) as partition:
            _flip(partition, prefix)
            self._build_ghosts(partition, prefix, n, short, long)

            expiries = _index_expiries(partition)
            assert expiries.count(e1) == n, "precondition: N ghost index entries"
            assert expiries.count(e2) == n, "precondition: N genuine index entries"

            # One sweep at high_water=5000 (E1 <= 5000 < E2). A far-future advance
            # write triggers the sweep but is never itself evicted; no genuine
            # eviction happens (every E1 entry is a ghost).
            tx = partition.begin()
            tx.set(key="advance", value="t", prefix=prefix, timestamp=5000, ttl=FAR)
            tx.prepare(processed_offsets={"topic": 4})
            tx.flush(changelog_offset=4)

            after = _index_expiries(partition)
            assert after.count(e1) == n - b, "only B ghost visits allowed per sweep"
            assert after.count(e2) == n, "genuine (future) entries untouched"

            # Convergence: a second sweep GC's B more ghosts.
            tx2 = partition.begin()
            tx2.set(key="advance", value="t", prefix=prefix, timestamp=5001, ttl=FAR)
            tx2.prepare(processed_offsets={"topic": 5})
            tx2.flush(changelog_offset=5)
            assert _index_expiries(partition).count(e1) == n - 2 * b

    def test_off_path_ghost_visits_count_against_budget(
        self, store_partition_factory, changelog_producer_mock
    ):
        """Parity for the OFF path (``_run_sweep`` in ``write()``): same
        visit-budget bound."""
        prefix = b"pfx"
        n, b = 5, 2
        short = timedelta(milliseconds=100)
        long = timedelta(milliseconds=10_000)
        e1, e2 = 1100, 11000
        with store_partition_factory(
            options=RocksDBOptions(
                max_evictions_per_flush=b, ttl_changelog_tombstones=False
            )
        ) as partition:
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
