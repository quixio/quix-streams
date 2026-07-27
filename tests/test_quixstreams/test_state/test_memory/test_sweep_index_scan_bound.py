"""
Regression test for finding M5 (batch4 re-review, BEST-EFFORT): the memory
sweep full-sorts the index every flush.

``MemoryStorePartition._run_sweep`` / ``sweep_expired_into_cache`` do
``for index_key in sorted(index.keys())`` (~lines 1199, 1289) -- this fully
materializes and sorts the ENTIRE ``__ttl_index__`` dict on every flush,
regardless of ``max_evictions_per_flush``. A bounded-budget sweep should never
pay O(N log N) to examine only ``budget`` entries.

This test is deterministic (no timing): it monkeypatches the module-level
``sorted`` name (the only in-scope ``sorted`` a function defined in this
module resolves to) to record how many elements it is ever asked to sort
during a single flush, and asserts that count is bounded -- NOT proportional
to the full index size -- when the eviction budget is tiny and every index
entry is still in the future (so none would ever be evicted, and a
budget-bounded scan should barely look at the index at all).
"""

from datetime import timedelta

import quixstreams.state.memory.partition as memory_partition_module
from quixstreams.state.memory import MemoryStorePartition

PREFIX = b"pfx"
BASE_TS = 1_752_000_000_000
N_FUTURE_KEYS = 300
BUDGET = 1
# Generous slack over the tiny configured budget -- still far below
# N_FUTURE_KEYS, so the assertion only fails today's O(N) behavior, not a
# reasonable bounded-but-not-minimal implementation.
BOUND = 50


def test_sweep_does_not_sort_full_index_when_budget_is_small(monkeypatch):
    part = MemoryStorePartition(changelog_producer=None, max_evictions_per_flush=BUDGET)

    # Seed N_FUTURE_KEYS finite-ttl records, all expiring well in the FUTURE
    # relative to the high-water established by their own write timestamp, so
    # none of them is ever eligible for eviction (budget is irrelevant to
    # correctness here -- only to sweep cost).
    with part.begin() as tx:
        for i in range(N_FUTURE_KEYS):
            tx.set(
                key=f"k{i}",
                value=f"v{i}",
                prefix=PREFIX,
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
    assert part.uses_ttl_stamps is True

    sort_call_sizes = []
    real_sorted = sorted

    def counting_sorted(iterable, *args, **kwargs):
        items = list(iterable)
        sort_call_sizes.append(len(items))
        return real_sorted(items, *args, **kwargs)

    monkeypatch.setattr(
        memory_partition_module, "sorted", counting_sorted, raising=False
    )

    # A single additional write (non-empty cache) triggers the prepare-time
    # sweep (``sweep_expired_into_cache``) over the already-committed index.
    with part.begin() as tx:
        tx.set(key="trigger", value="x", prefix=PREFIX, timestamp=BASE_TS)

    assert sort_call_sizes, "sanity: the sweep path called sorted() at least once"
    max_sorted = max(sort_call_sizes)
    assert max_sorted <= BOUND, (
        "M5: the sweep must not fully sort the entire TTL index every flush "
        f"(budget={BUDGET}, but sorted() was asked to order {max_sorted} "
        f"elements out of {N_FUTURE_KEYS} -- an O(N) materialization "
        "regardless of the eviction budget)"
    )
