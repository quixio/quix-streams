"""
Regression test for finding M5 (batch4 re-review, BEST-EFFORT): the memory
sweep full-sorts the index every flush.

``MemoryStorePartition._run_sweep`` / ``sweep_expired_into_cache`` do
``for index_key in sorted(index.keys())`` (~lines 1199, 1289) -- this fully
materializes and sorts the ENTIRE ``__ttl_index__`` dict on every flush,
regardless of ``max_evictions_per_flush``. A bounded-budget sweep should never
pay O(N log N) to examine only ``budget`` entries.

This test is deterministic (no timing): the sweep now pulls only the
``budget`` smallest (oldest-expiry) index keys via ``heapq.nsmallest`` instead
of fully sorting the entire index (see
``MemoryStorePartition.sweep_expired_into_cache`` /
``MemoryStorePartition._run_sweep``, ~lines 1398/1501). This test monkeypatches
the module-level ``heapq.nsmallest`` to record the ``n`` (budget) argument it
is ever called with during a single flush, and asserts that count is bounded
-- NOT proportional to the full index size -- when the eviction budget is
tiny and every index entry is still in the future (so none would ever be
evicted, and a budget-bounded scan should barely look at the index at all).
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

    nsmallest_call_ns = []
    real_nsmallest = memory_partition_module.heapq.nsmallest

    def counting_nsmallest(n, iterable, *args, **kwargs):
        nsmallest_call_ns.append(n)
        return real_nsmallest(n, iterable, *args, **kwargs)

    monkeypatch.setattr(memory_partition_module.heapq, "nsmallest", counting_nsmallest)

    # A single additional write (non-empty cache) triggers the prepare-time
    # sweep (``sweep_expired_into_cache``) over the already-committed index.
    with part.begin() as tx:
        tx.set(key="trigger", value="x", prefix=PREFIX, timestamp=BASE_TS)

    assert (
        nsmallest_call_ns
    ), "sanity: the sweep path called heapq.nsmallest() at least once"
    max_n = max(nsmallest_call_ns)
    assert max_n <= BOUND, (
        "M5: the sweep must pull only `budget` (oldest-expiry) index entries "
        f"via heapq.nsmallest, not the full index (budget={BUDGET}, but "
        f"nsmallest was asked for {max_n} elements out of {N_FUTURE_KEYS} -- "
        "an O(N) materialization regardless of the eviction budget)"
    )
