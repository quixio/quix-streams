"""
Shared migration delivery-confirm loop for the legacy-TTL migration produce
sites, used by both state backends:

- ``RocksDBStorePartition._flush_backfill_changelog`` — chunked on-disk path,
  progress-sliced loop (``max_slices=10``).
- ``MemoryStorePartition._confirm_migration_delivery_or_raise`` — single-shot
  in-RAM path (``max_slices=1``).

Both backends must enforce the same changelog-first invariant: a stamped
chunk / marker has to be **confirmed delivered on the changelog BEFORE the
local commit finalizes the migration locally**, or a crash would leave the
local store ahead of the changelog and a peer rebuild would diverge. The two
methods used to hand-mirror this logic, and the same accounting bugs had to be
fixed twice (the memory side was missed first time round) — hence one shared
implementation. The *decision* lives here; each caller maps the returned
verdict onto its own error message wording, which is legitimately local
(RocksDB embeds ``path=...``; memory embeds a caller-supplied context phrase).

The accounting the decision consumes is **per produce phase**, not per
partition: :class:`MigrationDeliveryPhase` (below) is constructed as a local by
each phase function, hands its own bound ``on_delivery`` to every record that
phase produces, and hands its own bound ``counters`` to this helper. A phase is
therefore judged exclusively on the records it produced itself.

The shared decision table, per flush slice:

1. ``None`` producer -> ``CONFIRMED`` without touching the producer.
2. ``flush(timeout=slice_timeout_s, migration=True)``; a non-``int`` return
   (an unconfigured test double / a producer with no delivery accounting) ->
   ``INDETERMINATE`` — do not block the local commit (the pre-existing "flush
   and proceed" behavior).
3. ``remaining == 0``: the SHARED producer's global send queue is fully
   drained, so every delivery callback has been served and THIS PHASE's
   records are each either acked or FAILED. A record that FAILED delivery is
   ALSO removed from the queue (its callback fired with ``err != None``, which
   never increments the acked counter) — so drained-but-unacked on a phase
   that produced through the counter-tracked route means a failed delivery,
   not a success (e.g. a sibling partition drained the global queue while this
   phase's own record failed). Waiting is pointless (a failed delivery
   never acks): ``DRAINED_UNACKED``. With ``produced == 0`` (counter-less
   direct-call test doubles), the pre-existing "0 backlog -> ``CONFIRMED``"
   is preserved unchanged.
4. ``remaining > 0``: prefer THIS PHASE's own ``produced - acked`` when it
   produced through the counter-tracked migration route — NOT ``flush()``'s
   int return, which is the GLOBAL in-flight count across every
   topic/partition on the shared migration producer; a sibling partition's
   wedged records would otherwise hold that count static and falsely abort a
   phase that has fully delivered its own records. Fall back to the int
   return only when this phase produced nothing through the
   counter-tracked route. ``outstanding <= 0`` -> ``CONFIRMED``.
5. A full slice with zero progress (``outstanding >= prev``) ->
   ``NO_PROGRESS`` — the timeout measures *lack of progress*, not total time,
   so a large-but-progressing chunk keeps going while a wedged broker
   surfaces after ~2 slices.
6. ``max_slices`` exhausted -> ``SLICES_EXHAUSTED`` (runaway cap), so an
   ever-shrinking trickle still terminates within the consumer's poll budget.

``counters`` is a zero-argument callable returning ``(produced, acked)`` and
is re-invoked on every slice, after each ``flush()``: the acked counter is
mutated by delivery callbacks that ``flush()`` itself serves, so passing plain
ints once would freeze the accounting mid-loop. In production it is always a
bound :meth:`MigrationDeliveryPhase.counters`.

**Structural invariant: ``acked <= produced``.** A phase's ``on_delivery`` is
only ever handed to that phase's own ``produce()`` calls, and each of those
calls is paired with exactly one ``record_produced()``, so no ack from any
other phase (or any other partition) can be counted here. The ``<= 0``
comparisons below are therefore retained as cheap *totality* over the int
domain, not as a guard against a reachable state: a negative ``outstanding``
returns ``CONFIRMED`` before ``prev`` is ever assigned, and ``DRAINED_UNACKED``
is gated on ``produced > acked``, so the arithmetic stays sound even if a
caller passes an arbitrary ``counters`` callable (test doubles do).
"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from quixstreams.state.recovery import ChangelogProducer

__all__ = (
    "MigrationDeliveryPhase",
    "MigrationFlushOutcome",
    "MigrationFlushVerdict",
    "confirm_migration_delivery",
)


class MigrationDeliveryPhase:
    """
    Delivery accounting for ONE legacy-TTL migration produce phase.

    A "phase" is a single migration produce operation on one partition: a live
    populated-legacy backfill (all of its chunks), a recovery-completion
    re-stamp pass (all of its chunks), or one done-marker produce. Each phase
    function constructs **its own** instance as a local, passes
    :meth:`on_delivery` to every record it produces through the migration
    route, calls :meth:`record_produced` once per such produce, and passes
    :meth:`counters` to :func:`confirm_migration_delivery`. **One instance per
    phase — never shared between phases and never reused.**

    Why per phase and not per partition. A single shared ``(produced, acked)``
    pair on the partition credits a *late* ack from phase N to whichever phase
    is running when the callback is finally served — so a phase could be
    falsely CONFIRMED by a sibling phase's ack (skipping the changelog-first
    guarantee), or permanently wedged by a predecessor's unacked record (the
    swallowed empty-census done marker used to pin corroboration, and with it
    the TTL sweep, off for the life of the instance). Attributing the ack to
    the **producing** phase removes both directions structurally, with no
    counter reset to place and no per-site reachability argument to maintain.

    **The dead-phase object is the landing pad, not a leak.** The producer's
    callback queue keeps a finished phase's instance alive until it drops the
    callback. That is intended: the late ack of a failed-or-slow record from a
    finished phase must land *somewhere*, and landing on an otherwise
    unreferenced two-int object is exactly how it is made harmless. Do NOT
    "clean this up" by hoisting the instance onto the partition — that
    reintroduces the shared-counter bug this class exists to remove.
    """

    __slots__ = ("_acked", "_produced")

    def __init__(self) -> None:
        self._produced = 0
        self._acked = 0

    @property
    def produced(self) -> int:
        """Records this phase produced through the migration route."""
        return self._produced

    @property
    def acked(self) -> int:
        """Records of this phase whose delivery was confirmed successful."""
        return self._acked

    def record_produced(self) -> None:
        """
        Count one record produced through this phase's migration route. Call
        once immediately after each ``changelog_producer.produce(...,
        on_delivery=self.on_delivery)``.
        """
        self._produced += 1

    def on_delivery(self, err: Optional[object], msg: Optional[object] = None) -> None:
        """
        ``on_delivery``-compatible chained delivery callback for this phase's
        migration produce calls. Counts a SUCCESSFUL delivery so
        :func:`confirm_migration_delivery` can measure THIS PHASE's own
        outstanding records rather than the shared producer's global queue
        depth. Delivery errors are surfaced by the producer's own internal
        callback (``InternalProducer._on_delivery`` latches them and
        ``flush()`` re-raises) and by the stall detector (a wedged record
        simply never acks and trips the drained-but-unacked / no-progress
        abort), so only successful acks are counted here.
        """
        if err is None:
            self._acked += 1

    def counters(self) -> tuple[int, int]:
        """
        ``(produced, acked)`` snapshot for
        :func:`confirm_migration_delivery`'s ``counters=`` argument. Passed as
        a **bound method** so the helper re-reads live values per flush slice.
        """
        return self._produced, self._acked


class MigrationFlushVerdict(Enum):
    """Outcome classes of :func:`confirm_migration_delivery`."""

    # Nothing outstanding for this partition -> the caller may commit locally.
    CONFIRMED = "confirmed"
    # Non-int flush return (unconfigured test double) -> do not block; proceed.
    INDETERMINATE = "indeterminate"
    # Global queue drained but this partition is still unacked -> a FAILED
    # delivery was drained without acking; the caller must raise.
    DRAINED_UNACKED = "drained_unacked"
    # A full slice made zero delivery progress -> the caller must raise.
    NO_PROGRESS = "no_progress"
    # The runaway slice cap was hit while still progressing -> the caller must
    # raise.
    SLICES_EXHAUSTED = "slices_exhausted"


@dataclass(frozen=True)
class MigrationFlushOutcome:
    """Verdict plus the accounting the caller needs for its error message."""

    verdict: MigrationFlushVerdict
    # The undelivered record count backing the verdict; 0 for
    # CONFIRMED/INDETERMINATE. Two cases (decision-table rows 3-4):
    # - when this phase produced through the counter-tracked migration
    #   route (``produced > 0``), it is THIS PHASE's own
    #   ``produced - acked``;
    # - on the ``produced == 0`` fallback (counter-less direct-call test
    #   doubles), it is ``flush()``'s int return — the GLOBAL in-flight count
    #   across every topic/partition on the shared migration producer, not a
    #   per-partition figure. Callers embedding it in per-partition-scoped
    #   error text inherit that imprecision on the fallback path.
    outstanding: int
    slices_used: int


def confirm_migration_delivery(
    changelog_producer: Optional["ChangelogProducer"],
    *,
    counters: Callable[[], tuple[int, int]],
    slice_timeout_s: float,
    max_slices: int,
    on_slice_progress: Optional[Callable[[int, int], None]] = None,
) -> MigrationFlushOutcome:
    """
    Run the bounded, progress-based migration flush loop and return a verdict.

    See the module docstring for the full decision table and its rationale.
    This function never converts a verdict into a raise — mapping a
    non-CONFIRMED verdict onto
    :class:`quixstreams.state.exceptions.ChangelogFlushError` (with
    backend-specific message wording) is the caller's job. ``flush()`` itself
    may still raise
    :class:`quixstreams.kafka.exceptions.KafkaProducerDeliveryError` (a
    latched delivery error re-raised by ``InternalProducer._raise_for_error``),
    which propagates to the caller unhandled — intentionally: the critical
    migration paths let it fail the operation, and the sole best-effort site
    (the empty-census done-marker in ``complete_recovery``) catches it
    alongside ``ChangelogFlushError`` at the call site.

    :param changelog_producer: the partition's changelog producer, or ``None``
        (no changelog configured -> ``CONFIRMED`` no-op).
    :param counters: zero-argument callable returning the produce phase's
        ``(produced, acked)`` migration-delivery counters — in production the
        bound :meth:`MigrationDeliveryPhase.counters` of the phase whose
        records are being confirmed. Re-invoked after every ``flush()`` slice —
        the acked counter is mutated by the delivery callbacks that ``flush()``
        serves.
    :param slice_timeout_s: per-slice flush timeout in seconds.
    :param max_slices: runaway cap on the number of flush slices (>= 1 on all
        production paths; honoured verbatim, including ``max_slices == 1``).
    :param on_slice_progress: optional callback invoked once per *progressing*
        slice with ``(outstanding, slice_no)`` (1-based slice number), for
        per-slice debug logging.
    """
    if changelog_producer is None:
        return MigrationFlushOutcome(
            verdict=MigrationFlushVerdict.CONFIRMED, outstanding=0, slices_used=0
        )
    prev: Optional[int] = None
    slices_used = 0
    for slice_no in range(max_slices):
        remaining = changelog_producer.flush(timeout=slice_timeout_s, migration=True)
        slices_used = slice_no + 1
        if not isinstance(remaining, int):
            return MigrationFlushOutcome(
                verdict=MigrationFlushVerdict.INDETERMINATE,
                outstanding=0,
                slices_used=slices_used,
            )
        produced, acked = counters()
        if remaining == 0:
            if produced > acked:
                return MigrationFlushOutcome(
                    verdict=MigrationFlushVerdict.DRAINED_UNACKED,
                    outstanding=produced - acked,
                    slices_used=slices_used,
                )
            return MigrationFlushOutcome(
                verdict=MigrationFlushVerdict.CONFIRMED,
                outstanding=0,
                slices_used=slices_used,
            )
        outstanding = (produced - acked) if produced > 0 else remaining
        if outstanding <= 0:
            return MigrationFlushOutcome(
                verdict=MigrationFlushVerdict.CONFIRMED,
                outstanding=0,
                slices_used=slices_used,
            )
        if prev is not None and outstanding >= prev:
            return MigrationFlushOutcome(
                verdict=MigrationFlushVerdict.NO_PROGRESS,
                outstanding=outstanding,
                slices_used=slices_used,
            )
        if on_slice_progress is not None:
            on_slice_progress(outstanding, slice_no + 1)
        prev = outstanding
    # Loop exhausted while still progressing. ``prev`` is always set here when
    # ``max_slices >= 1`` (every completed slice either returned or set it);
    # the 0 fallback only covers the unreachable ``max_slices == 0`` input.
    return MigrationFlushOutcome(
        verdict=MigrationFlushVerdict.SLICES_EXHAUSTED,
        outstanding=prev if prev is not None else 0,
        slices_used=slices_used,
    )
