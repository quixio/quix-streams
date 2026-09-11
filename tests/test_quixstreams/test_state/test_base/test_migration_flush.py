"""
Characterisation tests for the shared migration delivery-confirm helper
(``quixstreams.state.base.migration_flush.confirm_migration_delivery``),
driving its decision table DIRECTLY with fake producers.

Both state backends delegate to this helper
(``RocksDBStorePartition._flush_backfill_changelog`` with the module-level
slice constants, ``MemoryStorePartition._confirm_migration_delivery_or_raise``
with ``max_slices=1``), so the backend tests only exercise it indirectly. The
two rows that would catch the specific ways the extraction could have silently
changed behavior are pinned here explicitly:

- ``counters`` must be RE-INVOKED per slice (the acked counter is mutated by
  the delivery callbacks that ``flush()`` itself serves — freezing the first
  read would produce false NO_PROGRESS verdicts);
- ``max_slices == 1`` must reproduce the memory backend's single-shot shape
  exactly (one flush, positive outstanding -> SLICES_EXHAUSTED, no extra
  flush call).

Three further cases pin the ``MigrationDeliveryPhase`` contract, since every
production call site now passes a bound ``phase.counters``: that the bound
method satisfies the ``Callable[[], tuple[int, int]]`` parameter, that a failed
delivery is never counted as an ack, and that ``acked > produced`` is total
(``CONFIRMED``, no raise, no negative count) on BOTH branches — documenting that
the ``<= 0`` comparisons are totality rather than a guard against a state
per-phase attribution makes unreachable.
"""

from quixstreams.state.base.migration_flush import (
    MigrationDeliveryPhase,
    MigrationFlushVerdict,
    confirm_migration_delivery,
)


class _FakeProducer:
    """Minimal ``ChangelogProducer``-shaped double: ``flush`` pops scripted
    returns and records its calls; ``last_return`` lets counters lambdas
    mirror the shrinking backlog."""

    def __init__(self, flush_returns):
        self._returns = list(flush_returns)
        self.flush_calls: list = []
        self.last_return = None

    def flush(self, timeout=None, migration=False):
        self.flush_calls.append({"timeout": timeout, "migration": migration})
        self.last_return = self._returns.pop(0)
        return self.last_return


def _confirm(
    producer,
    *,
    counters=lambda: (0, 0),
    slice_timeout_s=25.0,
    max_slices=10,
    on_slice_progress=None,
):
    return confirm_migration_delivery(
        producer,
        counters=counters,
        slice_timeout_s=slice_timeout_s,
        max_slices=max_slices,
        on_slice_progress=on_slice_progress,
    )


class TestConfirmMigrationDelivery:
    def test_no_producer_is_confirmed_noop(self):
        outcome = _confirm(None)
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        assert outcome.outstanding == 0
        assert outcome.slices_used == 0

    def test_non_int_flush_return_is_indeterminate(self):
        # An unconfigured test double / producer with no delivery accounting:
        # do not block the caller's local commit.
        producer = _FakeProducer([object()])
        outcome = _confirm(producer)
        assert outcome.verdict is MigrationFlushVerdict.INDETERMINATE
        assert outcome.outstanding == 0
        assert len(producer.flush_calls) == 1

    def test_clean_confirm(self):
        producer = _FakeProducer([0])
        outcome = _confirm(producer, counters=lambda: (0, 0))
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        assert outcome.outstanding == 0
        # One flush, routed through the migration path with the given slice.
        assert producer.flush_calls == [{"timeout": 25.0, "migration": True}]

    def test_drained_but_unacked_is_failed_delivery(self):
        # Global queue drained (0) but this partition produced 1 and acked 0:
        # a FAILED delivery was drained without acking -> never CONFIRMED.
        producer = _FakeProducer([0])
        outcome = _confirm(producer, counters=lambda: (1, 0))
        assert outcome.verdict is MigrationFlushVerdict.DRAINED_UNACKED
        assert outcome.outstanding == 1
        assert len(producer.flush_calls) == 1

    def test_sibling_backlog_with_self_delivered_confirms(self):
        # The shared producer reports a GLOBAL backlog of 7 (a sibling
        # partition's wedged records), but THIS partition delivered all 3 of
        # its own -> CONFIRMED, not a false abort.
        producer = _FakeProducer([7])
        outcome = _confirm(producer, counters=lambda: (3, 3))
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        assert outcome.outstanding == 0
        assert len(producer.flush_calls) == 1

    def test_produced_zero_falls_back_to_global_return(self):
        # This partition produced nothing through the counter-tracked route
        # (produced == 0, the direct-call unit doubles): the decision falls
        # back to the producer's global int return.
        producer = _FakeProducer([4, 4])
        outcome = _confirm(producer, counters=lambda: (0, 0))
        assert outcome.verdict is MigrationFlushVerdict.NO_PROGRESS
        assert outcome.outstanding == 4
        assert len(producer.flush_calls) == 2

    def test_progressing_then_done(self):
        # 10 -> 6 -> 2 -> 0: each slice strictly decreases, so the loop keeps
        # going and confirms when the backlog hits 0.
        producer = _FakeProducer([10, 6, 2, 0])
        outcome = _confirm(
            producer,
            counters=lambda: (10, 10 - producer.last_return),
        )
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        assert outcome.outstanding == 0
        assert len(producer.flush_calls) == 4
        assert outcome.slices_used == 4

    def test_genuine_stall_is_no_progress(self):
        # Two full slices with identical outstanding -> zero progress.
        producer = _FakeProducer([10, 10])
        outcome = _confirm(producer, counters=lambda: (10, 0))
        assert outcome.verdict is MigrationFlushVerdict.NO_PROGRESS
        assert outcome.outstanding == 10
        assert len(producer.flush_calls) == 2

    def test_runaway_trickle_exhausts_slice_cap(self):
        # Ever-shrinking backlog that never reaches 0: terminates at the cap.
        producer = _FakeProducer([10, 9, 8, 7, 6])
        outcome = _confirm(
            producer,
            counters=lambda: (10, 10 - producer.last_return),
            max_slices=3,
        )
        assert outcome.verdict is MigrationFlushVerdict.SLICES_EXHAUSTED
        # The last (still-progressing) counters read, not the first.
        assert outcome.outstanding == 8
        assert len(producer.flush_calls) == 3
        assert outcome.slices_used == 3

    def test_counters_reread_per_slice_tracks_latest(self):
        # flush() returns an identical global backlog on both slices; only the
        # per-partition counters move (the delivery callbacks flush() serves
        # mutate the acked counter). If the helper froze the first read, both
        # slices would compute outstanding == 8 -> a false NO_PROGRESS verdict.
        producer = _FakeProducer([5, 5])
        reads = iter([(10, 2), (10, 4)])
        outcome = _confirm(producer, counters=lambda: next(reads), max_slices=2)
        assert outcome.verdict is MigrationFlushVerdict.SLICES_EXHAUSTED
        assert outcome.outstanding == 6  # the LATEST read (10 - 4), not 8
        assert len(producer.flush_calls) == 2

    def test_single_slice_memory_shape(self):
        # The memory backend's single-shot path: one flush, a positive
        # outstanding falls through to the runaway exit with the same count —
        # no second flush, no NO_PROGRESS (prev is None on the only slice).
        producer = _FakeProducer([5])
        outcome = _confirm(producer, counters=lambda: (5, 0), max_slices=1)
        assert outcome.verdict is MigrationFlushVerdict.SLICES_EXHAUSTED
        assert outcome.outstanding == 5
        assert len(producer.flush_calls) == 1
        assert outcome.slices_used == 1

    def test_bound_phase_counters_satisfy_the_contract(self):
        # A bound ``MigrationDeliveryPhase.counters`` is what every production
        # call site passes, so pin that it satisfies the helper's
        # ``Callable[[], tuple[int, int]]`` contract and reads live values.
        phase = MigrationDeliveryPhase()
        for _ in range(3):
            phase.record_produced()
        assert phase.counters() == (3, 0)
        for _ in range(3):
            phase.on_delivery(None)
        assert phase.counters() == (3, 3)
        assert (phase.produced, phase.acked) == (3, 3)

        producer = _FakeProducer([0])
        outcome = _confirm(producer, counters=phase.counters)
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        assert outcome.outstanding == 0
        assert len(producer.flush_calls) == 1

    def test_failed_delivery_is_not_acked_by_the_phase(self):
        # ``on_delivery`` counts SUCCESSFUL deliveries only, so a phase whose
        # record failed delivery stays at produced > acked and the drained
        # queue is adjudicated DRAINED_UNACKED (never a silent CONFIRMED).
        phase = MigrationDeliveryPhase()
        phase.record_produced()
        phase.on_delivery(RuntimeError("simulated delivery failure"))
        assert phase.counters() == (1, 0)

        producer = _FakeProducer([0])
        outcome = _confirm(producer, counters=phase.counters)
        assert outcome.verdict is MigrationFlushVerdict.DRAINED_UNACKED
        assert outcome.outstanding == 1

    def test_acked_exceeding_produced_is_total_and_confirms(self):
        # Totality, not a reachable state: per-phase attribution makes
        # ``acked <= produced`` structurally guaranteed (a phase's callback is
        # only ever handed to that phase's own produce calls). A caller that
        # nevertheless reports acked > produced must not raise, must not print a
        # negative count, and must fall straight to CONFIRMED on BOTH branches
        # (drained and positive-backlog).
        drained = _FakeProducer([0])
        outcome = _confirm(drained, counters=lambda: (0, 5))
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        assert outcome.outstanding == 0

        backlogged = _FakeProducer([7])
        outcome = _confirm(backlogged, counters=lambda: (2, 5))
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        assert outcome.outstanding == 0
        assert len(backlogged.flush_calls) == 1

    def test_progress_callback_called_per_progressing_slice(self):
        producer = _FakeProducer([10, 6, 2, 0])
        calls: list = []
        outcome = _confirm(
            producer,
            counters=lambda: (10, 10 - producer.last_return),
            on_slice_progress=lambda outstanding, slice_no: calls.append(
                (outstanding, slice_no)
            ),
        )
        assert outcome.verdict is MigrationFlushVerdict.CONFIRMED
        # Once per PROGRESSING slice with 1-based slice numbers; not called on
        # the final confirming slice.
        assert calls == [(10, 1), (6, 2), (2, 3)]
