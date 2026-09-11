"""
#8 (review batch 3), both backends: on an already-flipped store, refreshing a key
with a new ``ttl=`` mints a new ``__ttl_index__`` entry at the new expiry. HEAD
never deletes the OLD index entry, so on a refresh-heavy store (dedup sliding
window) the below-cutoff index grows unboundedly. The fix inline-deletes the old
index entry on refresh (update-cache fast-path for within-batch repeats, one
point-get otherwise), bounding the index at ~= live-key count. The visit-budget
sweep is retained only as a backstop for pre-existing ghosts.
"""

from datetime import timedelta

import pytest

from quixstreams.state.manager import SUPPORTED_STORES
from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import decode_index_key, encode_index_key

DAY_MS = 86_400_000
BASE_TS = 1_000_000_000_000


def _index_user_keys(partition) -> list[bytes]:
    """Decoded user keys of every ``__ttl_index__`` entry (backend-agnostic)."""
    if isinstance(partition, MemoryStorePartition):
        raw_keys = list(partition._state.get(TTL_INDEX_CF_NAME, {}).keys())
    else:
        cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        raw_keys = [bytes(k) for k in cf.keys()]
    out = []
    for rk in raw_keys:
        try:
            _, user_key = decode_index_key(rk)
        except ValueError:
            continue
        out.append(user_key)
    return out


def _index_size(partition) -> int:
    return len(_index_user_keys(partition))


def _serialized(partition, key, prefix=b"pfx") -> bytes:
    return partition.begin()._serialize_key(key, prefix=prefix)


def _inject_index_entry(partition, stamp: int, user_key: bytes) -> None:
    """Write an ``__ttl_index__`` entry OUT-OF-BAND (as older code / a crash /
    recovery re-stamp would), so the inline-delete never touched it — exercising
    the sweep backstop."""
    index_key = encode_index_key(stamp, user_key)
    if isinstance(partition, MemoryStorePartition):
        partition._state.setdefault(TTL_INDEX_CF_NAME, {})[index_key] = b""
    else:
        from rocksdict import WriteBatch

        batch = WriteBatch(raw_mode=True)
        batch.put(
            index_key,
            b"",
            partition.get_column_family_handle(TTL_INDEX_CF_NAME),
        )
        partition._write(batch)


def _flip(partition, key="s", ts=BASE_TS, ttl=timedelta(days=1)):
    with partition.begin() as tx:
        tx.set(key=key, value="seed", prefix=b"seed", timestamp=ts, ttl=ttl)
    assert partition.uses_ttl_stamps is True


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestGhostIndexRefresh:
    def test_cross_flush_refresh_keeps_index_bounded(self, store_partition):
        # Refresh the SAME N keys over many flushes, each with a slightly later
        # event-time (distinct expiry / index key) but a far-future ttl so the
        # sweep never evicts. HEAD grows the index by N every flush; the fix keeps
        # it ~= N by inline-deleting the previous entry on each refresh.
        partition = store_partition
        n_keys = 8
        rounds = 15
        keys = [f"k{i}" for i in range(n_keys)]
        ttl = timedelta(days=365)
        for r in range(rounds):
            ts = BASE_TS + r * 1000
            with partition.begin() as tx:
                for k in keys:
                    tx.set(key=k, value=f"v{r}", prefix=b"pfx", timestamp=ts, ttl=ttl)

        size = _index_size(partition)
        assert size <= n_keys, (
            f"index must stay bounded at ~= live-key count ({n_keys}); refresh "
            f"ghosts grew it to {size} (HEAD leaks one entry per refresh)"
        )

    def test_within_batch_repeat_deletes_old_index_entry(self, store_partition):
        # Flip via an unrelated seed key, then write a NEW key "k" TWICE in one
        # transaction at different expiries. The first write mints index_S1; the
        # second must delete index_S1 (update-cache fast-path) and mint index_S2,
        # so exactly one index entry for "k" survives. HEAD leaves both.
        partition = store_partition
        _flip(partition)
        ks = _serialized(partition, "k")
        with partition.begin() as tx:
            tx.set(
                key="k",
                value="a",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
            tx.set(
                key="k",
                value="b",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=7),
            )

        k_entries = [uk for uk in _index_user_keys(partition) if uk == ks]
        assert len(k_entries) == 1, (
            f"a within-batch refresh must leave exactly one index entry for 'k', "
            f"got {len(k_entries)} (HEAD leaves the stale one as a ghost)"
        )
        assert partition.begin().get(key="k", prefix=b"pfx", timestamp=BASE_TS) == "b"

    def test_cross_flush_refresh_deletes_committed_old_entry(self, store_partition):
        # Flip via "k" itself, then refresh "k" in a LATER flush. The old committed
        # index entry must be deleted via a point-get of the committed value.
        partition = store_partition
        with partition.begin() as tx:
            tx.set(
                key="k",
                value="a",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
        ks = _serialized(partition, "k")
        with partition.begin() as tx:
            tx.set(
                key="k",
                value="b",
                prefix=b"pfx",
                timestamp=BASE_TS + 10,
                ttl=timedelta(days=7),
            )

        k_entries = [uk for uk in _index_user_keys(partition) if uk == ks]
        assert len(k_entries) == 1, (
            f"a cross-flush refresh must delete the committed old index entry, "
            f"got {len(k_entries)} entries for 'k'"
        )

    def test_sweep_backstop_still_gcs_injected_ghost(self, store_partition):
        # A ghost minted out-of-band (older code / crash / recovery re-stamp) that
        # the inline-delete never saw must still be GC'd by the visit-budget sweep.
        partition = store_partition
        with partition.begin() as tx:
            tx.set(
                key="g",
                value="v",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=30),
            )
        gk = _serialized(partition, "g")
        stale_stamp = BASE_TS - DAY_MS  # already past
        _inject_index_entry(partition, stale_stamp, gk)
        assert any(uk == gk for uk in _index_user_keys(partition))

        # Advance the high-water past the stale stamp; the sweep GCs the injected
        # ghost while "g" itself survives (its live stamp is far in the future).
        sweep_ts = BASE_TS + 1000
        with partition.begin() as tx:
            tx.set(
                key="other",
                value="x",
                prefix=b"pfx",
                timestamp=sweep_ts,
                ttl=timedelta(days=30),
            )

        assert partition.begin().get(key="g", prefix=b"pfx", timestamp=sweep_ts) == "v"
        # Only ONE index entry for "g" remains (the live far-future one); the
        # injected past-dated ghost was reclaimed by the sweep backstop.
        assert sum(1 for uk in _index_user_keys(partition) if uk == gk) == 1
