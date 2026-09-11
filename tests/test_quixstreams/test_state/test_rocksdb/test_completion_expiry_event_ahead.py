"""
Red-first test: recovery-completion stamps leftovers on the WALLCLOCK, but the
sweep expires them on the EVENT-TIME high-water.

``complete_recovery`` stamps each leftover legacy record at
``self._now_ms() + legacy_records_ttl`` (``rocksdb/partition.py``, the
"wallclock-at-rebuild + legacy_records_ttl" expiry). Both sweep paths, however,
compare against ``now_ms = self._high_water_ms`` -- the EVENT-TIME clock advanced
by ``advance_high_water`` on live TTL writes.

The two clocks are independent. ``_high_water_ms`` is ``None`` through recovery
(replay does not advance it), so completion has no event-time information and
commits a wallclock-derived expiry. When live processing then resumes on an
EVENT-AHEAD stream -- event timestamps beyond wallclock, e.g. future-skewed
producer clocks, or any stream whose event-time runs ahead of the rebuilding
consumer -- the first TTL write advances the high-water past
``wallclock + legacy_records_ttl`` and the very next sweep deletes every record
completion just wrote.

The defect is NON-UNIFORM COHORT EXPIRY. The replayed survivors and the legacy
leftovers are one cohort in one store, but completion gives the leftovers a
wallclock-derived expiry while the survivors keep their event-time-derived one.
Advance the event-time clock to any point BETWEEN the two and the leftovers are
swept while their cohort-mates live on -- completion destroying exactly the
records it exists to preserve.

The live write below sits deliberately between the two expiries. A write beyond
the SURVIVORS' expiry would legitimately expire the whole cohort, so it would
prove nothing; this test pins that the leftovers share their cohort's fate rather
than dying early.

RED (leftovers deleted, survivors kept) confirms the defect.
"""

from datetime import timedelta

from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.ttl_codec import decode_ttl_value

from .test_incomplete_migration_recovery import (
    DAY_MS,
    _default_cf,
    _mixed_changelog,
    _replay,
)


class TestCompletionExpiryVsSweepClock:
    def test_event_ahead_live_write_does_not_wipe_completed_leftovers(
        self, store_partition_factory, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000  # wallclock at rebuild
        legacy_ttl = timedelta(days=7)
        # Survivors carry an event-time-derived expiry well beyond wallclock --
        # evidence, available at completion time, that this store's event-time
        # runs ahead of the rebuilding consumer's wallclock.
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(4, 6, stamp_expiry)

        p = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=legacy_ttl),
            changelog_producer=changelog_producer_mock,
        )
        _replay(p, msgs, now_ms=now_ms)
        assert p.uses_ttl_stamps is True
        # Recovery establishes no event-time clock at all.
        assert p._high_water_ms is None

        p.complete_recovery()

        # All six leftovers are on disk, stamped by completion.
        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(p).items()}
        assert all(key in decoded for key in legacy_values)

        # Live processing resumes on an EVENT-AHEAD stream. This event-time is
        # past the leftovers' wallclock expiry (now+7d) but well short of the
        # survivors' (now+30d), so a correct store keeps BOTH.
        with p.begin() as tx:
            tx.set(
                key="live",
                value="v",
                prefix=b"pfx",
                timestamp=now_ms + 10 * DAY_MS,
                ttl=legacy_ttl,
            )

        after = {k: decode_ttl_value(v) for k, v in _default_cf(p).items()}
        # The survivors are untouched at this event-time -- that is what makes
        # the leftovers' deletion an asymmetry rather than a cohort expiry.
        survivor_keys = [f"pfx|s{i}".encode() for i in range(4)]
        assert all(key in after for key in survivor_keys), (
            "survivors must outlive this event-time; if they did not, the live "
            "write was too far ahead and this test proves nothing"
        )

        # The leftovers must share their cohort's fate, not die early.
        survived = [key for key in legacy_values if key in after]
        assert len(survived) == len(legacy_values), (
            f"leftovers swept while all 4 survivors live on: only {len(survived)} "
            f"of {len(legacy_values)} survived"
        )
        # And each still carries its original payload.
        for key, raw in legacy_values.items():
            assert after[key][1] == raw
        p.close()
