"""
Contract test for PR #1134 round-2 review Finding #3: the worst-case blocking
budget of ``_flush_backfill_changelog`` must not exceed a Kafka consumer's
default ``max.poll.interval.ms``, else a legacy-TTL migration flush -- called
INLINE from ``prepare()`` and the recovery-completion loop, both on the
consumer thread -- can block long enough to breach the poll budget and
trigger a rebalance mid-migration.

Today: ``_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES`` (40) x
``_BACKFILL_CHANGELOG_FLUSH_SLICE_S`` (25.0s) = 1000s (~16.7 min) -- more than
3x the 300s (``quixstreams.app._default_max_poll_interval_ms``) default poll
interval.

This is a CONTRACT test on the two module constants, mirroring the existing
whitebox precedent of reading/patching them directly (see
``test_backfill_flush_progress.py::test_runaway_cap_raises``, which patches
``_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES`` via
``quixstreams.state.rocksdb.partition``). Whatever mechanism ArchDev picks
(deriving the cap from the app's configured ``max.poll.interval.ms`` at
runtime, or simply shrinking the fixed default budget) must leave the
EFFECTIVE worst-case blocking time <= the default poll interval. If ArchDev
introduces a new attribute/helper to express a dynamically-derived budget
instead of shrinking these two constants, this assertion should be pointed at
that helper instead -- the CONTRACT (worst case <= max.poll.interval.ms) is
what must hold, not these particular names.
"""

import quixstreams.state.rocksdb.partition as partition_module
from quixstreams.app import _default_max_poll_interval_ms


class TestBackfillFlushPollIntervalBudget:
    def test_worst_case_flush_budget_bounded_by_default_poll_interval(self):
        max_slices = partition_module._BACKFILL_CHANGELOG_FLUSH_MAX_SLICES
        slice_seconds = partition_module._BACKFILL_CHANGELOG_FLUSH_SLICE_S
        worst_case_budget_s = max_slices * slice_seconds

        default_poll_interval_s = _default_max_poll_interval_ms / 1000

        assert worst_case_budget_s <= default_poll_interval_s, (
            f"_flush_backfill_changelog's worst-case blocking budget "
            f"({worst_case_budget_s}s = {max_slices} slices x "
            f"{slice_seconds}s) exceeds the default max.poll.interval.ms "
            f"({default_poll_interval_s}s). This method runs INLINE on the "
            f"consumer thread from prepare()/the recovery-completion loop, "
            f"so a flush this long breaches the poll budget and triggers a "
            f"rebalance mid-migration."
        )
