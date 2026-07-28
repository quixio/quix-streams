"""
Red-first test for a CONFIRMED review finding (round-3, ``fix/ttl-adopt-
tombstone-hardening``): ``_backfill_produced`` / ``_backfill_acked`` are set
only in ``RocksDBStorePartition.__init__`` (``partition.py`` ~297-309) and are
never reset afterwards. A swallowed done-marker delivery failure therefore
leaves a PERMANENT ``produced > acked`` skew on the instance: every LATER
migration-flush confirm on that same partition -- even one whose own marker
delivers successfully -- sees the stale skew and raises
``ChangelogFlushError``.

Reachable sequence this test drives (RocksDB backend only; the memory backend
is not affected, since ``MemoryStorePartition._adopt_provisional`` is only set
in a mutually exclusive branch):

1. A provisionally cold-adopted partition (``__ttl_adopt_pending__`` on disk,
   ``_adopt_provisional=True``) whose done-marker delivery already failed and
   was swallowed by ``complete_recovery``'s best-effort
   ``except (ChangelogFlushError, KafkaProducerDeliveryError)`` around the
   empty-census ``_produce_migration_done_marker()`` call (``partition.py``
   ~1028-1047). This leaves ``_backfill_produced=1, _backfill_acked=0`` on the
   live instance -- simulated directly here, mirroring the sibling regression
   test ``test_backfill_flush_delivery_error.py``'s pattern of poking the
   counters straight rather than re-driving the full swallow path.
2. A live ``state.set(..., ttl=...)`` write reaches
   ``PartitionTransaction._maybe_corroborate_adoption`` (``transaction.py``
   ~714-719), which calls ``RocksDBStorePartition.corroborate_adoption()``
   (``partition.py`` ~1824). That produces marker #2
   (``_backfill_produced`` -> 2) and its OWN delivery succeeds synchronously
   (``_backfill_acked`` -> 1).
3. ``migration_flush.py``'s shared confirm loop sees the shared producer's
   queue drained (``remaining == 0``) and compares THIS partition's
   ``produced - acked``: ``2 > 1`` -> ``DRAINED_UNACKED`` -> raises
   ``ChangelogFlushError``, even though marker #2's own delivery succeeded.
   The skew never closes, so corroboration can never complete on this
   partition instance -- the TTL sweep stays suppressed forever.

Expected (correct) behavior: a marker whose own delivery acks must not be
permanently blocked by a stale, already-resolved skew from an earlier
swallowed failure -- ``corroborate_adoption()`` should not raise here, and the
partition should leave provisional mode. This is RED on the current code
because ``corroborate_adoption()`` raises ``ChangelogFlushError`` instead.
"""

from typing import Optional

from quixstreams.state.metadata import METADATA_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import TTL_ADOPT_PENDING_KEY
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000


class _SelfAckingProducer:
    """A stub changelog producer whose OWN produced record always delivers
    successfully, synchronously, and whose ``flush()`` reports a 0 GLOBAL
    backlog -- mirroring a healthy broker with an otherwise-empty shared send
    queue (the exact condition under which ``migration_flush.py`` compares
    THIS partition's own ``produced - acked`` skew)."""

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers=None,
        migration: bool = False,
        on_delivery=None,
    ) -> None:
        if on_delivery is not None:
            on_delivery(None)

    def flush(self, timeout: Optional[float] = None, migration: bool = False) -> int:
        return 0


def _rocksdb_partition(tmp_path, name="db", changelog_producer=None):
    path = (tmp_path / name).as_posix()
    opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(
        path, options=opts, changelog_producer=changelog_producer
    )


def _v3240_msg(key_str, user_value, expiry_ms, prefix=b"pfx"):
    """One v3.24.0-style default-CF changelog message (already 8B-stamped)."""
    raw_key = prefix + b"|" + json_dumps(key_str)
    stamped = encode_ttl_value(expiry_ms, json_dumps(user_value))
    return (raw_key, stamped, False)


def _provision_partition(tmp_path, name, producer, n_keys=3):
    """Cold-provisionally-adopt a fresh partition via a 100%-stamped,
    not-all-past, full-coverage census replay (mirrors
    ``test_v3240_adoption_rollback_safety.py._provision_partition``)."""
    expiry_ms = NOW_MS + 7 * DAY_MS
    msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry_ms) for i in range(n_keys)]
    partition = _rocksdb_partition(tmp_path, name=name, changelog_producer=producer)
    partition._now_ms = lambda: NOW_MS  # noqa: E731
    offset = 0
    for key, value, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1
    partition.complete_recovery()

    assert partition.uses_ttl_stamps is True, "precondition: flipped"
    assert partition._adopt_provisional is True, "precondition: provisional"
    metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
    assert (
        metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
    ), "precondition: pending marker on disk"
    return partition


def test_stale_counter_skew_does_not_permanently_block_corroboration(tmp_path):
    """A produced>acked skew left behind by an earlier, already-swallowed
    done-marker delivery failure must not permanently wedge a LATER marker
    whose own delivery succeeds. Validates the changelog-first invariant's
    intent: it exists to stop the LOCAL store from getting ahead of the
    changelog, not to punish a partition forever for one already-resolved
    failure.
    """
    producer = _SelfAckingProducer()
    partition = _provision_partition(tmp_path, "db", producer)

    # Step 1 (already happened, simulated directly -- see sibling
    # ``test_backfill_flush_delivery_error.py`` for the same poke-the-
    # counters pattern): an earlier done-marker attempt on this partition's
    # migration route was produced but its delivery failed, and the failure
    # was swallowed by ``complete_recovery``'s best-effort except clause.
    # ``_backfill_produced`` / ``_backfill_acked`` are set only in
    # ``__init__`` and never reset, so this skew is now permanent on this
    # live instance.
    partition._backfill_produced = 1
    partition._backfill_acked = 0

    # Step 2: a live ``state.set(..., ttl=...)`` corroborates the adoption.
    # Marker #2 is produced through ``_SelfAckingProducer``, whose delivery
    # succeeds synchronously -- THIS marker is not the failure.
    #
    # Expected: no exception, and the provisional flag clears (adoption
    # corroborates). Currently raises ``ChangelogFlushError`` instead, because
    # ``confirm_migration_delivery`` compares the STALE cumulative
    # produced=2/acked=1 rather than resetting after the earlier failure was
    # already accounted for.
    partition.corroborate_adoption()

    assert partition._adopt_provisional is False, (
        "corroboration must clear the provisional flag once a marker with "
        "its own successful delivery is produced, regardless of an earlier "
        "already-resolved delivery failure on this partition"
    )
    metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
    assert (
        metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is None
    ), "the pending marker must be cleared once corroboration completes"
    partition.close()


def test_provisional_store_is_never_left_permanently_uncorroborable(tmp_path):
    """Same skew as above, but drives it through a SECOND corroboration
    attempt after the first raised -- proving the skew never closes on its
    own, i.e. the sweep stays suppressed forever (the finding's "wedges a
    partition permanently" claim), not just on the very next attempt.
    """
    producer = _SelfAckingProducer()
    partition = _provision_partition(tmp_path, "db", producer)
    partition._backfill_produced = 1
    partition._backfill_acked = 0

    first_raised = False
    try:
        partition.corroborate_adoption()
    except Exception:  # noqa: BLE001 -- capturing whatever ChangelogFlushError today
        first_raised = True

    if first_raised:
        # Retry, as a subsequent live ttl= write would on the next flush.
        # A permanently-wedged partition raises again here too, because
        # ``_backfill_produced``/``_backfill_acked`` were never reset by the
        # first (failed) attempt.
        partition.corroborate_adoption()

    assert partition._adopt_provisional is False, (
        "the provisional flag must eventually clear -- a partition must "
        "not be permanently uncorroborable once its own marker delivers"
    )
    partition.close()
