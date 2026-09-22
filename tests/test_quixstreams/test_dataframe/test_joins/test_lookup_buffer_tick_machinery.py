"""
K7 and K10: the tick's per-tick bound, and the inertness of the shared
machinery it needed.

Spec: `dev-planning/lookup-deadline-tick/spec.md` §4.5, §6.2 and §8.

K7 is the only test in the suite that uses the **real** monotonic clock, because
the thing it measures is a wall-clock budget. The wall clock that decides what is
*due* stays fake, so which records come due is still deterministic; only the
budget is real.

K10 guards the two edits that reach outside the lookup buffer -
`BaseCheckpoint.empty()` and `PartitionTransaction.changed` - against the failure
mode that would hurt every Application in the SDK: an `empty()` that stops
skipping idle checkpoints and starts committing on every poll.
"""

import time

import pytest

from quixstreams.dataframe.joins.lookups import buffer_operator, buffer_tick
from quixstreams.dataframe.registry import DataFrameRegistry
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    FakeClock,
    make_buffer,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer_tick import (
    clock,
    tick_driver,
)

# Re-exported so pytest can resolve these fixtures by name in this module too.
__all__ = ["clock", "tick_driver"]

# Long enough that nothing expires while the records are being buffered: the
# 800 records below span 800 fake milliseconds of arrival time.
K7_GRACE_MS = 60_000
K7_KEYS = ("A", "B", "C", "D", "E", "F", "G", "H")
K7_PER_KEY = 100
K7_TOTAL = len(K7_KEYS) * K7_PER_KEY

# Real seconds slept per emitted record, standing in for an expensive downstream
# (a serializing `to_topic`, a sink's HTTP call). One `BufferSweeper` pass emits
# at most SWEEP_EMIT_BUDGET=256 records, so a pass costs ~0.26s - comfortably
# over TICK_TIME_BUDGET_S=0.2, which makes "one pass per tick" deterministic
# rather than a race with the machine's speed.
K7_DOWNSTREAM_DELAY_S = 0.001


class BudgetClock:
    """
    Fake wall clock, real monotonic.

    The tick reads `time()` to decide what is due and `monotonic()` to spend its
    budget. K7 needs the first faked and the second real, so it can drive a
    deterministic set of deadlines and still measure a genuine elapsed time.
    """

    def __init__(self, wall: FakeClock) -> None:
        self._wall = wall

    def time(self) -> float:
        return self._wall.time()

    def monotonic(self) -> float:
        return time.monotonic()


@pytest.fixture
def budget_clock(monkeypatch) -> FakeClock:
    wall = FakeClock()
    monkeypatch.setattr(buffer_operator, "time", wall)
    monkeypatch.setattr(buffer_tick, "time", BudgetClock(wall))
    return wall


class TestK7PerTickBound:
    """
    K7: one tick is bounded, and truncation loses nothing and duplicates
    nothing.

    Two deviations from the spec's wording, both deliberate:

    - **800 records, not 5000.** The record path's sweep re-reads a neighbouring
      key's withheld range whenever that key's conservative lower bound comes
      due, so buffering N records is quadratic in N. 800 records across eight
      keys truncates the tick three times over, including in the middle of a
      key, which is what the test is actually about; 5000 would buy nothing but
      minutes of runtime.
    - **Within-key order is asserted exactly; cross-key order is not.** The
      record path deliberately records a very conservative lower bound for a key
      that already holds records (`index.set_earliest(prefix, cutoff + 1)`), so
      the deadline queue's cross-key order is approximate by design. What the
      spec's ordering claim really rests on - a truncated prefix resumes exactly
      where it stopped - is within-key, and that is asserted.
    """

    def test_a_large_backlog_drains_over_several_bounded_ticks(
        self, budget_clock, tick_driver
    ):
        driver = tick_driver(
            buffer=make_buffer(grace_ms=K7_GRACE_MS, on_timeout="emit"),
            downstream_delay_s=K7_DOWNSTREAM_DELAY_S,
        )

        expected: list[tuple[str, int]] = []
        for index in range(K7_TOTAL):
            key = K7_KEYS[index % len(K7_KEYS)]
            driver.send(key, timestamp=index)
            expected.append((key, index))
            # Distinct arrival milliseconds, so the never-split-a-millisecond
            # rule in `_sweep_prefix` cannot quietly overrun the emit budget.
            budget_clock.advance_ms(1)
        assert driver.emitted == [], "none of these records resolve"

        budget_clock.advance_ms(K7_GRACE_MS + 10_000)

        started = time.monotonic()
        first = driver.tick()
        first_elapsed = time.monotonic() - started

        assert first, "the first tick must make progress"
        assert len(first) < K7_TOTAL, (
            "an unbounded drain is the max.poll.interval.ms hazard this budget "
            "exists to prevent"
        )
        assert first_elapsed < 2.0, (
            f"one tick took {first_elapsed:.2f}s against a "
            f"{buffer_tick.TICK_TIME_BUDGET_S}s budget; the budget is checked "
            f"between sweep passes, so the overrun is bounded by one pass, not "
            f"by the size of the backlog"
        )

        ticks = 1
        while driver.tick():
            ticks += 1
            assert ticks < 50, "the drain must terminate"

        assert ticks > 1, (
            "the backlog must have been truncated at least once, or this test "
            "is not exercising the budget at all"
        )

        emitted = [(key, timestamp) for _, key, timestamp, _ in driver.emitted]
        assert sorted(emitted) == sorted(expected), (
            "every buffered record must be emitted exactly once across the "
            "ticks: a duplicate means a truncated pass deleted less than it "
            "emitted, a missing one means it deleted more"
        )
        for key in K7_KEYS:
            timestamps = [
                timestamp for emitted_key, timestamp in emitted if emitted_key == key
            ]
            assert timestamps == sorted(timestamps), (
                f"within key {key!r} the order must stay arrival order across a "
                f"truncation boundary"
            )
        assert driver.pending_keys() == []


class TestK10SharedMachineryIsInert:
    """
    K10: nothing here changes for an Application without a `LookupBuffer`.

    The highest-risk edit in the change is `BaseCheckpoint.empty()`: it decides
    when an Application commits. If it were to start reporting idle checkpoints
    as non-empty, every Application in the SDK would commit on every poll.
    """

    def test_registry_without_tasks_is_a_no_op(self):
        registry = DataFrameRegistry()

        assert registry.run_periodic_tasks() is None
        assert registry.run_periodic_tasks() is None

    def test_join_lookup_without_a_buffer_registers_no_task(self, clock, tick_driver):
        driver = tick_driver(buffer=None)
        driver.lookup.configs["D"] = {"threshold": 7, "region": "us"}

        emitted = driver.send("D", timestamp=100)

        assert len(emitted) == 1, "the unbuffered path emits immediately"
        value, key, timestamp, _ = emitted[0]
        assert (key, timestamp) == ("D", 100)
        assert value["threshold"] == 7
        assert driver.tick() == [], "and the tick has nothing registered to run"

    def test_empty_checkpoint_stays_empty(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer())
        checkpoint = driver.sdf.processing_context.checkpoint

        assert checkpoint.empty() is True, "a fresh checkpoint has nothing to do"

        # An open-but-untouched store transaction is exactly what the tick
        # leaves behind when nothing was due. It must not make the checkpoint
        # commit, or an idle Application would produce broker traffic every
        # poll.
        transaction = driver.transaction()
        assert transaction.changed is False
        assert checkpoint.empty() is True

    def test_a_changed_store_transaction_alone_makes_a_checkpoint_committable(
        self, clock, tick_driver
    ):
        """
        The new clause, and the whole reason the tick's deletes survive: state
        changes count even with no consumer offsets.
        """
        driver = tick_driver(buffer=make_buffer())
        checkpoint = driver.sdf.processing_context.checkpoint
        transaction = driver.transaction()

        transaction.set(b"key", "value", prefix=b"prefix")

        assert transaction.changed is True
        assert checkpoint.empty() is False

    def test_a_delete_also_counts_as_a_change(self, clock, tick_driver):
        """The tick's work is deletes, so a delete-only transaction must count."""
        driver = tick_driver(buffer=make_buffer())
        checkpoint = driver.sdf.processing_context.checkpoint
        transaction = driver.transaction()

        transaction.delete(b"key", prefix=b"prefix")

        assert transaction.changed is True
        assert checkpoint.empty() is False

    def test_stored_offsets_still_make_a_checkpoint_non_empty(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer())
        checkpoint = driver.sdf.processing_context.checkpoint

        checkpoint.store_offset(topic=driver.topic.name, partition=0, offset=0)

        assert checkpoint.empty() is False
