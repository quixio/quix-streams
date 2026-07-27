"""
#1 (review batch 3, HIGH): a ``state.set(..., ttl=...)`` write on a POPULATED
legacy in-memory store must MIGRATE the store live in RAM (flip + re-stamp the
pre-existing records + produce changelog re-stamps + flag-last done-marker),
instead of the old warn-and-defer no-op that could never complete (the deferred
changelog rebuild classified the store pure-legacy and no-op'd forever).
"""

from datetime import timedelta

import pytest

from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import SENTINEL_NEVER, decode_ttl_value
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
BASE_TS = 1_000_000_000_000

# Reuse the shared blackbox helper for a spec_set ChangelogProducer mock.
from ..test_ttl_blackbox_contract import (  # noqa: E402
    _make_changelog_producer_mock,
    _replay_msgs,
)


def _seed_legacy(partition, n):
    """Replay ``n`` header-absent legacy records (no flip)."""
    msgs = [(f"pfx|l{i}".encode(), f'"legacy-{i}"'.encode(), False) for i in range(n)]
    _replay_msgs(partition, msgs)
    assert partition.uses_ttl_stamps is False


def _default_restamps(producer):
    """(key, value, ttl_stamped, processed_offsets_header) for produced default-CF
    records, in call order."""
    out = []
    for call in producer.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        if headers.get(CHANGELOG_CF_MESSAGE_HEADER) == "default":
            out.append(
                (
                    call.kwargs["key"],
                    call.kwargs["value"],
                    bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER)),
                    headers.get(CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER),
                )
            )
    return out


def _marker_produce_index(producer):
    for i, call in enumerate(producer.produce.call_args_list):
        headers = call.kwargs.get("headers") or {}
        if (
            headers.get(CHANGELOG_CF_MESSAGE_HEADER) == TTL_SYSTEM_CF_NAME
            and call.kwargs.get("key") == TTL_MIGRATION_DONE_KEY
        ):
            return i
    return None


class TestPopulatedLegacyLiveBackfill:
    @pytest.mark.parametrize("with_config", [True, False])
    def test_live_migration_on_first_ttl_write(self, with_config):
        producer = _make_changelog_producer_mock()
        kwargs = {"changelog_producer": producer}
        if with_config:
            kwargs["legacy_records_ttl"] = timedelta(days=5)
        partition = MemoryStorePartition(**kwargs)

        n = 4
        _seed_legacy(partition, n)

        ts = BASE_TS
        write_ttl = timedelta(days=2)
        with partition.begin() as tx:
            tx.set(key="new", value="vnew", prefix=b"pfx", timestamp=ts, ttl=write_ttl)

        # (b) flipped.
        assert partition.uses_ttl_stamps is True

        # (a) pre-existing records carry the uniform expiry.
        expected_expiry = ts + (5 * DAY_MS if with_config else 2 * DAY_MS)
        for i in range(n):
            stored = partition._state["default"][f"pfx|l{i}".encode()]
            stamp, payload = decode_ttl_value(stored)
            assert stamp == expected_expiry
            assert payload == f'"legacy-{i}"'.encode()

        # (a) readable before the window, expired after. Replayed keys are raw
        # changelog keys (b"pfx|l0"), so read with a bytes key so _serialize_key
        # reproduces the same key rather than JSON-encoding "l0".
        assert (
            partition.begin().get(key=b"l0", prefix=b"pfx", timestamp=ts) == "legacy-0"
        )
        after = expected_expiry + 1
        assert partition.begin().get(key=b"l0", prefix=b"pfx", timestamp=after) is None

        # (c) N changelog re-stamps for the pre-existing keys, header-true, null
        # processed-offsets (always-apply).
        restamps = _default_restamps(producer)
        legacy_restamps = [r for r in restamps if r[0] != b'pfx|"new"']
        assert len(legacy_restamps) == n
        for key, value, ttl_stamped, po in legacy_restamps:
            assert ttl_stamped is True
            assert po == json_dumps(None)
            stamp, _ = decode_ttl_value(value)
            assert stamp == expected_expiry

        # (d) done-marker produced flag-last (after every pre-existing re-stamp).
        marker_idx = _marker_produce_index(producer)
        assert marker_idx is not None, "done-marker must be produced"
        legacy_keys = {f"pfx|l{i}".encode() for i in range(n)}
        last_legacy_idx = max(
            i
            for i, call in enumerate(producer.produce.call_args_list)
            if call.kwargs.get("key") in legacy_keys
        )
        assert marker_idx > last_legacy_idx, "done-marker must be produced flag-last"

    def test_cold_rebuild_does_not_re_migrate(self):
        # Run the live migration, capture its changelog, replay into a FRESH
        # partition, and assert the rebuild flips WITHOUT re-running the migration
        # (the flag-last marker latches the done-branch).
        producer = _make_changelog_producer_mock()
        partition = MemoryStorePartition(changelog_producer=producer)
        _seed_legacy(partition, 3)
        with partition.begin() as tx:
            tx.set(
                key="new",
                value="v",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=2),
            )

        # Build the replay changelog from the produced records (default-CF stamped
        # records) + the flag-last marker.
        replay = []
        for key, value, ttl_stamped, _po in _default_restamps(producer):
            replay.append((key, value, ttl_stamped))

        rebuilt_producer = _make_changelog_producer_mock()
        rebuilt = MemoryStorePartition(changelog_producer=rebuilt_producer)
        _replay_msgs(rebuilt, replay)
        # Replay the done-marker (system CF) then finalize.
        rebuilt.recover_from_changelog_message(
            key=TTL_MIGRATION_DONE_KEY,
            value=b"\x01",
            cf_name=TTL_SYSTEM_CF_NAME,
            offset=len(replay),
            ttl_stamped=False,
        )
        rebuilt.complete_recovery()

        assert rebuilt.uses_ttl_stamps is True
        assert rebuilt._state.get(TTL_BACKFILL_PENDING_CF_NAME, {}) == {}
        # No completion / re-migration production happened on the rebuild.
        assert rebuilt._backfill_produced == 0

    def test_no_config_no_ttl_writes_stays_legacy(self):
        # Guard: a plain (no ttl=) write on a populated legacy store must NOT flip.
        producer = _make_changelog_producer_mock()
        partition = MemoryStorePartition(changelog_producer=producer)
        _seed_legacy(partition, 2)
        with partition.begin() as tx:
            tx.set(key="plain", value="v", prefix=b"pfx")
        assert partition.uses_ttl_stamps is False
        assert _marker_produce_index(producer) is None

    def test_sentinel_fallback_unreachable_marker(self):
        # Defensive: the SENTINEL fallback in the expiry chain is unreachable on
        # the live path (a ttl= write always sets _max_batch_ttl_ms), so a real
        # migration never stamps SENTINEL. Confirm the resolved expiry is finite.
        producer = _make_changelog_producer_mock()
        partition = MemoryStorePartition(changelog_producer=producer)
        _seed_legacy(partition, 1)
        with partition.begin() as tx:
            tx.set(
                key="new",
                value="v",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
        stored = partition._state["default"][b"pfx|l0"]
        stamp, _ = decode_ttl_value(stored)
        assert stamp != SENTINEL_NEVER

    def test_live_migration_raises_on_undelivered_changelog(self):
        """Finding 1: the live memory backfill must raise ChangelogFlushError
        when the changelog flush leaves messages undelivered, mirroring the
        RocksDB backfill's confirm-or-raise discipline (see
        RocksDBStorePartition._flush_backfill_changelog).

        Validates spec: memory/RocksDB parity -- ChangelogFlushError on
        stalled flush during populated-legacy live backfill.
        """
        producer = _make_changelog_producer_mock()
        # Simulate a stuck producer: flush reports 5 undelivered, and
        # the on_delivery callbacks are never called (produced > acked).
        producer.flush.return_value = 5
        partition = MemoryStorePartition(changelog_producer=producer)
        _seed_legacy(partition, 3)

        with pytest.raises(ChangelogFlushError):
            with partition.begin() as tx:
                tx.set(
                    key="new",
                    value="vnew",
                    prefix=b"pfx",
                    timestamp=BASE_TS,
                    ttl=timedelta(days=2),
                )

    def test_additive_backfill_stamp_clamped_when_exceeds_plausible(self):
        """Finding 4: a ``legacy_records_ttl`` whose magnitude is individually
        below ``_MAX_PLAUSIBLE_STAMP_MS`` but whose SUM with the enable-time
        high-water ``>= _MAX_PLAUSIBLE_STAMP_MS`` must be clamped so the
        backfilled record remains readable (not stranded by
        ``_safe_decode_stamp``).

        Validates spec: additive backfill stamp is clamped to
        ``_MAX_PLAUSIBLE_STAMP_MS`` / ``SENTINEL_NEVER``.
        """
        # enable_time (set by the triggering write's timestamp) + ttl > 10**15.
        # The ttl alone is < 10**15, so it passes any magnitude validation.
        ts = 1_700_000_000_000  # realistic epoch-ms (~year 2023)
        large_ttl = timedelta(milliseconds=999_000_000_000_000)  # < 10**15 ms

        producer = _make_changelog_producer_mock()
        partition = MemoryStorePartition(
            changelog_producer=producer,
            legacy_records_ttl=large_ttl,
        )
        _seed_legacy(partition, 1)

        with partition.begin() as tx:
            tx.set(
                key="new",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        # The backfilled legacy record must be readable -- get must return
        # the original value, not raise StateSerializationError or return
        # a corrupted prefix||value blob.
        result = partition.begin().get(key=b"l0", prefix=b"pfx", timestamp=ts)
        assert result == "legacy-0", (
            "backfilled record with additive stamp >= _MAX_PLAUSIBLE_STAMP_MS "
            "must still be readable (stamp should be clamped); got "
            f"{result!r}"
        )
