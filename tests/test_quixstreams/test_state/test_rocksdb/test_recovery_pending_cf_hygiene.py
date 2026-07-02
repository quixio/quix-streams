"""
Regression test for Bug 3: recovery censuses the ``__ttl_backfill_pending__`` CF
for pure non-TTL stores.

The ``recover_from_changelog_message`` method at partition.py:271 gates the
pending-CF bookkeeping on ``type(self).uses_ttl_stamps`` (the **class-level**
attribute, always ``True`` for ``RocksDBStorePartition``) rather than
``self.uses_ttl_stamps`` (the **instance-level** flag, ``False`` on a legacy
store). This means every replayed header-absent default-CF record inserts a key
into ``__ttl_backfill_pending__``, even when no TTL-stamped record was ever seen
and the partition never flips into TTL mode.

``complete_recovery()`` correctly short-circuits when ``_recovery_saw_stamped``
is False, so the leftover entries are never acted on — but they are never cleaned
up either, leaving a non-empty CF of orphan keys that waste space and could
confuse future logic.

Expected correct behavior: after replaying a purely legacy changelog and calling
``complete_recovery()``, the ``__ttl_backfill_pending__`` CF contains 0 keys.

Reference: shortcut 73191 review, recovery-census gate analysis.
See ``dev-planning/state-ttl-legacy-backfill/spec-incomplete-migration-recovery.md``
§8.8.
"""

from datetime import timedelta

from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions


def _replay_legacy(partition, msgs, now_ms=None):
    """Replay ``(key, value)`` pairs as header-absent (legacy) default-CF
    messages, optionally fixing the recovery wallclock."""
    if now_ms is not None:
        partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=False,  # all legacy, no TTL header
        )
        offset += 1


def _count_pending_keys(partition) -> int:
    """Count keys currently in the ``__ttl_backfill_pending__`` CF."""
    pending_cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    count = 0
    for _ in pending_cf.keys():
        count += 1
    return count


class TestRecoveryPendingCfHygiene:
    def test_pure_legacy_replay_leaves_pending_cf_empty(
        self, store_partition_factory, changelog_producer_mock
    ):
        """
        Bug 3 — recovery censuses the pending CF for pure non-TTL stores.

        Replays a purely legacy changelog (several default-CF records, NO
        ``__ttl_stamped__`` header on any of them), calls the recovery-completion
        hook, and asserts that the ``__ttl_backfill_pending__`` CF contains 0
        keys.

        On current code, every replayed record leaves an entry in the pending CF
        and nothing cleans it up — the test is expected to be RED.
        """
        now_ms = 1_780_000_000_000
        n = 5

        # Build a purely legacy changelog: no TTL stamps on any record.
        msgs = [(f"pfx|l{i}".encode(), f"legacy-{i}".encode()) for i in range(n)]

        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay_legacy(recovered, msgs, now_ms=now_ms)

        # Partition must NOT have flipped (all legacy).
        assert recovered.uses_ttl_stamps is False
        assert recovered._recovery_saw_stamped is False

        # Call the recovery-completion hook (mirrors recovery manager behavior).
        recovered.complete_recovery()

        # --- Assert: the pending CF must be empty ---
        # On current (buggy) code, every replayed legacy record was censused
        # into __ttl_backfill_pending__ and none were cleaned up.
        pending_count = _count_pending_keys(recovered)
        assert pending_count == 0, (
            f"Expected 0 keys in __ttl_backfill_pending__ after pure-legacy "
            f"replay + complete_recovery(), but found {pending_count}. "
            f"The recovery census gate checks type(self).uses_ttl_stamps "
            f"(class-level, always True) instead of self.uses_ttl_stamps "
            f"(instance-level, False for legacy stores)."
        )

        recovered.close()

    def test_pure_legacy_replay_no_options_leaves_pending_cf_empty(
        self, store_partition_factory, changelog_producer_mock
    ):
        """
        Same as above but WITHOUT ``legacy_records_ttl`` set in options — the
        most common deployment shape for a pure legacy store. Verifies the bug
        manifests regardless of the opt-in config.
        """
        now_ms = 1_780_000_000_000
        n = 3
        msgs = [(f"pfx|x{i}".encode(), f"payload-{i}".encode()) for i in range(n)]

        recovered = store_partition_factory(
            name="dst2",
            # No legacy_records_ttl — pure vanilla options.
            changelog_producer=changelog_producer_mock,
        )
        _replay_legacy(recovered, msgs, now_ms=now_ms)

        assert recovered.uses_ttl_stamps is False
        assert recovered._recovery_saw_stamped is False

        recovered.complete_recovery()

        pending_count = _count_pending_keys(recovered)
        assert pending_count == 0, (
            f"Expected 0 keys in __ttl_backfill_pending__ for a vanilla "
            f"(no TTL config) store after pure-legacy replay, but found "
            f"{pending_count}."
        )

        recovered.close()
