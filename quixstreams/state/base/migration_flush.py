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

The shared decision table, per flush slice:

1. ``None`` producer -> ``CONFIRMED`` without touching the producer.
2. ``flush(timeout=slice_timeout_s, migration=True)``; a non-``int`` return
   (an unconfigured test double / a producer with no delivery accounting) ->
   ``INDETERMINATE`` — do not block the local commit (the pre-existing "flush
   and proceed" behavior).
3. ``remaining == 0``: the SHARED producer's global send queue is fully
   drained, so every delivery callback has been served and THIS partition's
   records are each either acked or FAILED. A record that FAILED delivery is
   ALSO removed from the queue (its callback fired with ``err != None``, which
   never increments the acked counter) — so drained-but-unacked on a partition
   that produced through the counter-tracked route means a failed delivery,
   not a success (e.g. a sibling drained the global queue while this
   partition's own record failed). Waiting is pointless (a failed delivery
   never acks): ``DRAINED_UNACKED``. With ``produced == 0`` (counter-less
   direct-call test doubles), the pre-existing "0 backlog -> ``CONFIRMED``"
   is preserved unchanged.
4. ``remaining > 0``: prefer THIS partition's own ``produced - acked`` when it
   produced through the counter-tracked migration route — NOT ``flush()``'s
   int return, which is the GLOBAL in-flight count across every
   topic/partition on the shared migration producer; a sibling partition's
   wedged records would otherwise hold that count static and falsely abort a
   partition that has fully delivered its own records. Fall back to the int
   return only when this partition produced nothing through the
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
ints once would freeze the accounting mid-loop.
"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from quixstreams.state.recovery import ChangelogProducer

__all__ = (
    "MigrationFlushOutcome",
    "MigrationFlushVerdict",
    "confirm_migration_delivery",
)


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
    # - when this partition produced through the counter-tracked migration
    #   route (``produced > 0``), it is THIS partition's own
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
    :param counters: zero-argument callable returning this partition's
        ``(produced, acked)`` migration-delivery counters. Re-invoked after
        every ``flush()`` slice — the acked counter is mutated by the delivery
        callbacks that ``flush()`` serves.
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
