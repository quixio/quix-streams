"""
Unit tests for §8.8 of the State TTL legacy-backfill spec (shortcut 73191):
**completing an interrupted legacy-TTL migration on cold restore of a MIXED
changelog** (OP-4).

Scenario: a legacy-TTL backfill started (producing stamped, ``__ttl_stamped__``-
header records to the changelog) but was interrupted before completion. Log
compaction leaves the changelog MIXED: the stamped keys carry header-true
records, the un-backfilled keys remain as their original header-absent legacy
records. A later fresh-volume cold restore replays both shapes.

The §8.7 header fix alone routes the records correctly (stamped → stamped path,
legacy → verbatim) but does NOT complete the migration: the partition flips on
the first stamped record, so the first live ``ttl=`` write sees an already-
flipped partition and the backfill gate short-circuits — stranding the leftover
legacy keys as never-expiring forever.

§8.8 fix (this module): during replay census the header-absent leftover keys into
the local-only ``__ttl_backfill_pending__`` CF (delete on stamped supersession);
at end of recovery, if a stamped record was seen AND pending is non-empty, run a
chunked completion backfill that stamps exactly the leftover keys at a uniform
expiry and produces header-bearing stamped records, leaving the pending CF empty.
Config-present → ``wallclock_now + legacy_records_ttl`` (Rule 4 wallclock-at-
rebuild). Config-absent → auto-finish at the §15.2 survivor-derived default (the
max surviving future stamp; the reject was removed by the 2026-07-02 revision).
All-legacy / all-stamped changelogs do NOT enter completion.

See ``dev-planning/state-ttl-legacy-backfill/spec-incomplete-migration-recovery.md``.
"""

from datetime import timedelta

import pytest

from quixstreams.state.metadata import (
    CHANGELOG_TTL_STAMPED_HEADER,
    TTL_BACKFILL_PENDING_CF_NAME,
)
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000


def _replay(partition, msgs, now_ms=None):
    """Replay ``(key, value, ttl_stamped)`` default-CF messages, optionally
    fixing the recovery wallclock so the completion expiry is deterministic."""
    if now_ms is not None:
        partition._now_ms = lambda: now_ms  # noqa: E731
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


def _mixed_changelog(n_stamped, n_legacy, stamp_expiry):
    """Build a MIXED changelog over DISJOINT keys: ``n_stamped`` header-true
    stamped records (keys ``s0..``) and ``n_legacy`` header-absent legacy records
    (keys ``l0..``). Returns the message list and the legacy raw value map."""
    msgs = []
    legacy_values = {}
    for i in range(n_stamped):
        key = f"pfx|s{i}".encode()
        value = encode_ttl_value(stamp_expiry, f"stamped-{i}".encode())
        msgs.append((key, value, True))
    for i in range(n_legacy):
        key = f"pfx|l{i}".encode()
        raw = f"legacy-payload-{i}".encode()
        legacy_values[key] = raw
        msgs.append((key, raw, False))
    return msgs, legacy_values


def _default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: value for key, value in cf.items()}


def _index_cf(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _captured_stamped_produces(changelog_producer_mock):
    """Return ``{key: value}`` of header-true default-CF records produced."""
    out = {}
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        if headers.get(CHANGELOG_TTL_STAMPED_HEADER):
            out[call.kwargs["key"]] = call.kwargs["value"]
    return out


class TestMixedChangelogCompletion:
    def test_leftover_legacy_completed_at_wallclock_expiry(
        self, store_partition_factory, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000
        legacy_ttl = timedelta(days=7)
        # Stamped records far in the future so Rule 4 keeps them.
        stamp_expiry = now_ms + 30 * DAY_MS
        n_stamped, n_legacy = 4, 6
        msgs, legacy_values = _mixed_changelog(n_stamped, n_legacy, stamp_expiry)

        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=legacy_ttl),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        # Flipped, and the leftover legacy keys are censused as pending.
        assert recovered.uses_ttl_stamps is True
        assert recovered._recovery_saw_stamped is True
        assert _pending_keys(recovered) == set(legacy_values.keys())

        changelog_producer_mock.produce.reset_mock()
        recovered.complete_recovery()

        expected_expiry = now_ms + 7 * DAY_MS
        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(recovered).items()}

        # N2 leftovers now stamped at wallclock + ttl, with index entries.
        index = _index_cf(recovered)
        for key, raw in legacy_values.items():
            assert decoded[key] == (expected_expiry, raw)
            assert index[key] == expected_expiry

        # N1 stamped survivors are byte-unchanged (not re-stamped / double-wrapped).
        for i in range(n_stamped):
            key = f"pfx|s{i}".encode()
            assert decoded[key] == (stamp_expiry, f"stamped-{i}".encode())

        # Pending CF drained; no never-expire (sentinel) legacy survivors.
        assert _pending_keys(recovered) == set()
        assert all(exp != SENTINEL_NEVER for exp, _ in decoded.values())

        # Completion produced exactly the N2 leftovers as stamped + header-bearing.
        produced = _captured_stamped_produces(changelog_producer_mock)
        assert set(produced.keys()) == set(legacy_values.keys())
        for key, raw in legacy_values.items():
            assert produced[key] == encode_ttl_value(expected_expiry, raw)

        recovered.close()

    def test_config_absent_auto_completes(
        self, store_partition_factory, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000
        # Stamped survivors far in the future → Rule 4 keeps them → they become
        # the §15.2 survivor-derived completion expiry when config is absent.
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(2, 3, stamp_expiry)

        recovered = store_partition_factory(
            name="dst",  # no legacy_records_ttl in options
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)
        assert recovered.uses_ttl_stamps is True
        assert _pending_keys(recovered) == set(legacy_values.keys())

        changelog_producer_mock.produce.reset_mock()
        # §15 revision: no raise — auto-finish the migration at the survivor-
        # derived default (the max surviving future stamp).
        recovered.complete_recovery()

        expected_expiry = stamp_expiry  # survivor-derived (max future stamp)
        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(recovered).items()}
        index = _index_cf(recovered)
        # Leftovers stamp-encoded at the survivor expiry, with index entries and
        # full raw payload preserved.
        for key, raw in legacy_values.items():
            assert decoded[key] == (expected_expiry, raw)
            assert index[key] == expected_expiry

        # Stamped survivors byte-unchanged (not re-stamped).
        for i in range(2):
            key = f"pfx|s{i}".encode()
            assert decoded[key] == (stamp_expiry, f"stamped-{i}".encode())

        # Pending drained; no never-expire survivors; leftovers produced stamped.
        assert _pending_keys(recovered) == set()
        assert all(exp != SENTINEL_NEVER for exp, _ in decoded.values())
        produced = _captured_stamped_produces(changelog_producer_mock)
        assert set(produced.keys()) == set(legacy_values.keys())
        for key, raw in legacy_values.items():
            assert produced[key] == encode_ttl_value(expected_expiry, raw)
        recovered.close()

    def test_interrupt_then_converges_no_double_stamp(
        self, store_partition_factory, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000
        legacy_ttl = timedelta(days=7)
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(1, 5, stamp_expiry)
        expected_expiry = now_ms + 7 * DAY_MS

        # Pass 1: interrupt completion after the first chunk (chunk_size=2) by
        # capping the loop via a small chunk and stopping after one commit.
        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(
                legacy_records_ttl=legacy_ttl, legacy_backfill_chunk_size=2
            ),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)
        pending_before = _pending_keys(recovered)
        assert pending_before == set(legacy_values.keys())

        # Stamp only the first chunk, then "crash": call the private chunk loop
        # once by monkeypatching the writer to raise after the first commit.
        original_write = recovered._write
        commits = {"n": 0}

        def write_once_then_raise(batch):
            original_write(batch)
            commits["n"] += 1
            if commits["n"] >= 1:
                raise RuntimeError("simulated interrupt after first chunk")

        recovered._write = write_once_then_raise
        with pytest.raises(RuntimeError):
            recovered.complete_recovery()
        recovered._write = original_write

        pending_after_interrupt = _pending_keys(recovered)
        # Strictly shrunk: at least one key (the committed chunk) drained.
        assert pending_after_interrupt < pending_before
        assert len(pending_after_interrupt) > 0

        # Pass 2: resume completion to convergence.
        completed = recovered.complete_recovery()  # noqa: F841 (returns None)
        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(recovered).items()}

        # Every leftover stamped exactly once at the wallclock expiry; no double
        # wrap (decode would fail / expiry would differ on a double-wrap).
        for key, raw in legacy_values.items():
            assert decoded[key] == (expected_expiry, raw)
        assert _pending_keys(recovered) == set()
        recovered.close()


class TestUnchangedPaths:
    def test_all_legacy_does_not_complete(
        self, store_partition_factory, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000
        msgs = [(f"pfx|l{i}".encode(), f"legacy-{i}".encode(), False) for i in range(5)]
        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        # Never flipped → completion is a no-op even though pending is non-empty.
        assert recovered.uses_ttl_stamps is False
        assert recovered._recovery_saw_stamped is False
        changelog_producer_mock.produce.reset_mock()
        recovered.complete_recovery()
        # No stamping happened; records remain raw legacy and reachable for the
        # live first-ttl=-write backfill (§8.6).
        on_disk = _default_cf(recovered)
        assert len(on_disk) == 5
        for i in range(5):
            assert on_disk[f"pfx|l{i}".encode()] == f"legacy-{i}".encode()
        assert _captured_stamped_produces(changelog_producer_mock) == {}
        recovered.close()

    def test_all_stamped_does_not_complete(
        self, store_partition_factory, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs = [
            (
                f"pfx|s{i}".encode(),
                encode_ttl_value(stamp_expiry, f"stamped-{i}".encode()),
                True,
            )
            for i in range(5)
        ]
        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        assert recovered.uses_ttl_stamps is True
        assert recovered._recovery_saw_stamped is True
        # No header-absent records → pending empty → completion no-op.
        assert _pending_keys(recovered) == set()
        changelog_producer_mock.produce.reset_mock()
        recovered.complete_recovery()
        assert _captured_stamped_produces(changelog_producer_mock) == {}

        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(recovered).items()}
        for i in range(5):
            key = f"pfx|s{i}".encode()
            assert decoded[key] == (stamp_expiry, f"stamped-{i}".encode())
        recovered.close()

    def test_supersession_removes_key_from_pending(
        self, store_partition_factory, changelog_producer_mock
    ):
        # A key that arrives header-absent (legacy) then later header-true
        # (stamped) — compaction can only keep the latest, but during replay both
        # may appear; the stamped supersedes and the key must NOT remain pending.
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        key = b"pfx|k0"
        msgs = [
            (key, b"legacy-raw", False),
            (key, encode_ttl_value(stamp_expiry, b"stamped-raw"), True),
        ]
        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        assert recovered.uses_ttl_stamps is True
        assert _pending_keys(recovered) == set()  # superseded
        recovered.complete_recovery()  # no-op
        decoded = decode_ttl_value(_default_cf(recovered)[key])
        assert decoded == (stamp_expiry, b"stamped-raw")
        recovered.close()
