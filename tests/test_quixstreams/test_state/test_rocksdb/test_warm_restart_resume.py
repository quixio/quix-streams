"""
C1 (shortcut 73191 review) — warm-restart resume of an interrupted live legacy
TTL backfill.

An in-place live backfill (:meth:`RocksDBStorePartition.backfill_legacy_records`)
re-stamps a populated legacy store one chunk at a time, flag-last. If the process
warm-restarts after some chunks committed but before the final flip flush,
recovery replays the already-produced stamped chunk records and durably flips the
store into TTL mode — but the un-stamped legacy leftovers (below the replayed
offset range) are never censused and never stamped. On HEAD the migration is then
permanently stranded and the resume ledger is deleted on the next open.

C1 fixes this so an interrupted live backfill ALWAYS resumes and completes after a
warm restart, with no new user-facing config:

- Part A: ``_cleanup_completed_backfill_bookkeeping`` drops the resume ledger only
  once the durable done-marker is present (an interrupted, flipped-but-unfinished
  migration keeps its ledger for the resume).
- Part B: ``complete_recovery`` detects the interrupted-live-backfill signature
  (flipped + ``__ttl_backfill_stamped__`` ledger non-empty + no done-marker) and
  resumes by re-invoking ``backfill_legacy_records`` over the ledger complement,
  then produces the done-marker and cleans up.

See ``dev-planning/state-ttl-c1-warm-restart-resume/spec.md``.
"""

from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.state.metadata import (
    METADATA_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import (
    TTL_BACKFILL_PROGRESS_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import decode_index_key, decode_ttl_value

DAY_MS = 86_400_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _producer():
    """A fresh changelog-producer mock. ``flush`` returns a MagicMock (non-int),
    so ``_flush_backfill_changelog`` treats it as indeterminate and proceeds."""
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _raise_on_nth_produce(producer, n, message="simulated crash mid-backfill"):
    """Make ``producer.produce`` raise ``RuntimeError`` on its ``n``-th call
    while still recording every call in ``call_args_list``."""
    calls = {"n": 0}

    def side(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == n:
            raise RuntimeError(message)

    producer.produce.side_effect = side


def _seed_legacy_records(partition, records, prefix=b"pfx"):
    """Write plain (un-stamped) records to a legacy partition."""
    with partition.begin() as tx:
        for key, value in records:
            tx.set(key=key, value=value, prefix=prefix)


def _capture_default_cf_changelog(producer):
    """Return ``(key, value, ttl_stamped)`` for the default-CF changelog
    messages the producer recorded, carrying the ``__ttl_stamped__`` header."""
    from quixstreams.state.metadata import (
        CHANGELOG_CF_MESSAGE_HEADER,
        CHANGELOG_TTL_STAMPED_HEADER,
    )

    msgs = []
    for call in producer.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        if headers.get(CHANGELOG_CF_MESSAGE_HEADER) == "default":
            ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))
            msgs.append((call.kwargs["key"], call.kwargs["value"], ttl_stamped))
    return msgs


def _replay_default(partition, msgs, now_ms):
    """Replay default-CF ``(key, value, ttl_stamped)`` messages with an injected
    wallclock ``now_ms`` (test clock seam), exactly as the recovery manager does."""
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


def _decode_default_cf(partition):
    """Return ``{raw_key: (expires_at, payload)}`` for the default CF."""
    cf = partition.get_or_create_column_family("default")
    return {key: decode_ttl_value(value) for key, value in cf.items()}


def _index_cf(partition):
    """Return ``{user_key: expires_at}`` for the ``__ttl_index__`` CF."""
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _ledger_keys(partition):
    """Direct scan of the live-backfill ``__ttl_backfill_stamped__`` ledger CF
    (works on HEAD and after the fix — does not use the new production probe)."""
    cf = partition.get_or_create_column_family(TTL_BACKFILL_STAMPED_CF_NAME)
    return set(cf.keys())


def _progress_counter(partition):
    meta = partition.get_or_create_column_family(METADATA_CF_NAME)
    return meta.get(TTL_BACKFILL_PROGRESS_KEY, default=None)


def _done_marker_present(partition):
    cf = partition.get_or_create_column_family(TTL_SYSTEM_CF_NAME)
    return cf.get(TTL_MIGRATION_DONE_KEY, default=None) is not None


def _interrupt_live_backfill_after_first_chunk(
    store_partition_factory, name, ttl, ts, n=5, chunk=2
):
    """Seed ``n`` legacy records and run a live backfill that crashes on the
    first ``produce`` call of the SECOND chunk (i.e. after ``chunk`` records were
    stamped + ledgered + produced + committed), leaving the store un-flipped
    (flag-last never landed). Returns ``(open_partition, producer, captured_msgs)``.
    The caller owns closing ``open_partition``.
    """
    seed = store_partition_factory(name=name)
    _seed_legacy_records(seed, [(f"k{i}", f"legacy-value-{i}") for i in range(n)])
    seed.close()

    producer = _producer()
    # Chunk 1 = ``chunk`` produce calls; the (chunk+1)-th produce is chunk 2's
    # first record — raise there so exactly one chunk commits.
    _raise_on_nth_produce(producer, chunk + 1)

    partition = store_partition_factory(
        name=name,
        options=RocksDBOptions(
            legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk
        ),
        changelog_producer=producer,
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)

    # Flag-last never landed; exactly one chunk's worth was produced + committed.
    assert partition.uses_ttl_stamps is False
    # Only the committed chunk's records are truly durable on the changelog: a
    # real crash on the failing produce call never enqueues that record (the mock
    # still records the raising call, so filter it out by ledger membership — a
    # key is in the ledger iff its chunk committed locally, which happens only
    # after its records were produced + flush-confirmed).
    ledger = _ledger_keys(partition)
    captured = [m for m in _capture_default_cf_changelog(producer) if m[0] in ledger]
    assert len(captured) == chunk
    assert all(stamped for _, _, stamped in captured)
    return partition, producer, captured


# ---------------------------------------------------------------------------
# C1 warm-restart-resume suite
# ---------------------------------------------------------------------------


class TestWarmRestartResume:
    # (i) partial backfill → crash before flip → warm reopen w/ tail replay →
    # resume completes.
    def test_partial_backfill_warm_replay_resumes_to_completion(
        self, store_partition_factory
    ):
        ttl = timedelta(days=7)
        ts = 1_000_000_000_000
        uniform_expiry = ts + 7 * DAY_MS

        p1, _producer1, captured = _interrupt_live_backfill_after_first_chunk(
            store_partition_factory, name="db", ttl=ttl, ts=ts, n=5, chunk=2
        )
        # The interrupted run left a resume ledger of the stamped chunk.
        assert len(_ledger_keys(p1)) == 2
        p1.close()

        # Warm reopen (same on-disk store). It opens legacy — the flip flag never
        # landed — so the open-time cleanup is skipped and the ledger survives.
        producer2 = _producer()
        p2 = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=2
            ),
            changelog_producer=producer2,
        )
        assert p2.uses_ttl_stamps is False
        assert len(_ledger_keys(p2)) == 2

        # Recovery replays the already-produced stamped chunk records (flips the
        # store) with a wallclock strictly before every stamp.
        _replay_default(p2, captured, now_ms=ts)
        assert p2.uses_ttl_stamps is True
        assert p2._recovery_saw_stamped is True

        p2.complete_recovery()

        # GREEN: every default record is stamped with the cohort's uniform expiry.
        decoded = _decode_default_cf(p2)
        assert len(decoded) == 5
        for _key, (expires_at, payload) in decoded.items():
            assert expires_at == uniform_expiry
            # No double-wrap: the payload is the original serialized legacy value.
            assert payload.startswith(b'"legacy-value-')
        # Index rebuilt for every (non-sentinel) key.
        index = _index_cf(p2)
        assert len(index) == 5
        for user_key in decoded:
            assert index[user_key] == uniform_expiry
        # Done-marker present; resume bookkeeping cleaned.
        assert _done_marker_present(p2) is True
        assert _ledger_keys(p2) == set()
        assert _progress_counter(p2) is None
        p2.close()

    # (ii) cleanup keeps the ledger when the done-marker is absent, still drops it
    # when present.
    def test_cleanup_keeps_ledger_without_done_marker(self, store_partition_factory):
        ttl = timedelta(days=7)
        ts = 1_000_000_000_000

        # Build a durably-flipped store with a non-empty ledger + progress but NO
        # done-marker: interrupt a live backfill, then stamp the flip metadata (as
        # a mid-replay flip would) without ever completing the migration.
        p1, _producer1, _captured = _interrupt_live_backfill_after_first_chunk(
            store_partition_factory, name="nomarker", ttl=ttl, ts=ts, n=5, chunk=2
        )
        assert len(_ledger_keys(p1)) == 2
        p1._stamp_flip_metadata()  # durable flip, still no done-marker
        assert _done_marker_present(p1) is False
        p1.close()

        # Reopen: the store opens flipped, so the open-time cleanup runs.
        reopened = store_partition_factory(
            name="nomarker",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=2
            ),
        )
        assert reopened.uses_ttl_stamps is True
        # GREEN: no done-marker → the ledger + progress SURVIVE for the resume.
        assert len(_ledger_keys(reopened)) == 2
        assert _progress_counter(reopened) is not None
        reopened.close()

    def test_cleanup_drops_ledger_with_done_marker(self, store_partition_factory):
        # Sibling of (ii): the same flipped store WITH a done-marker present must
        # still have its ledger + progress dropped on open (no regression to the
        # completed-migration cleanup).
        ttl = timedelta(days=7)
        ts = 1_000_000_000_000

        p1, producer1, _captured = _interrupt_live_backfill_after_first_chunk(
            store_partition_factory, name="withmarker", ttl=ttl, ts=ts, n=5, chunk=2
        )
        assert len(_ledger_keys(p1)) == 2
        p1._stamp_flip_metadata()  # durable flip
        producer1.produce.side_effect = None  # marker production must not raise
        p1._produce_migration_done_marker()  # durable done-marker
        assert _done_marker_present(p1) is True
        p1.close()

        reopened = store_partition_factory(
            name="withmarker",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=2
            ),
        )
        assert reopened.uses_ttl_stamps is True
        # Marker present → cleanup drops the dead bookkeeping.
        assert _ledger_keys(reopened) == set()
        assert _progress_counter(reopened) is None
        reopened.close()

    # (iii) crash mid-RESUME → second restart completes (convergent, no
    # double-wrap). The second restart is offset-caught-up (no new changelog
    # tail): the ledger arm of has_incomplete_ttl_migration + the C1 resume
    # branch complete it with NO replay this session (§7.2 / §7.4 fallback).
    def test_crash_mid_resume_second_restart_converges(self, store_partition_factory):
        ttl = timedelta(days=7)
        ts = 1_000_000_000_000
        uniform_expiry = ts + 7 * DAY_MS

        p1, _producer1, captured = _interrupt_live_backfill_after_first_chunk(
            store_partition_factory, name="db", ttl=ttl, ts=ts, n=5, chunk=2
        )
        p1.close()

        # Warm reopen + replay → flips + arms the resume.
        producer2 = _producer()
        # Interrupt the RESUME itself: chunk 1 of the resume produces 2 records
        # (the first 2 leftovers) and commits; raise on the 3rd produce (chunk 2's
        # first record) so chunk 2 never commits.
        _raise_on_nth_produce(producer2, 3, message="simulated crash mid-resume")
        p2 = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=2
            ),
            changelog_producer=producer2,
        )
        _replay_default(p2, captured, now_ms=ts)
        assert p2.uses_ttl_stamps is True

        # The resume runs inside complete_recovery; on the fixed code it is
        # interrupted after its first chunk. On HEAD (no resume) nothing raises.
        try:
            p2.complete_recovery()
        except RuntimeError:
            pass
        p2.close()

        # Second restart, offset-caught-up: reopen (flipped on disk, ledger
        # non-empty from the partial resume, no done-marker) and complete again
        # with NO replay → the ledger arm forces the pass and the resume drains
        # the now-smaller complement.
        producer3 = _producer()
        p3 = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=2
            ),
            changelog_producer=producer3,
        )
        p3._now_ms = lambda: ts  # noqa: E731
        p3.complete_recovery()

        # GREEN: convergent — all five stamped once at the uniform expiry, no
        # double-wrap; done-marker present; bookkeeping cleaned.
        decoded = _decode_default_cf(p3)
        assert len(decoded) == 5
        for _key, (expires_at, payload) in decoded.items():
            assert expires_at == uniform_expiry
            assert payload.startswith(b'"legacy-value-')
        assert _done_marker_present(p3) is True
        assert _ledger_keys(p3) == set()
        p3.close()

    # (iv) normal completed migration and empty-store flip are unaffected (no
    # spurious resume).
    def test_completed_migration_reopen_no_spurious_resume(
        self, store_partition_factory, caplog
    ):
        import logging

        ttl = timedelta(days=7)
        ts = 1_000_000_000_000

        seed = store_partition_factory(name="done")
        _seed_legacy_records(seed, [(f"k{i}", f"legacy-value-{i}") for i in range(3)])
        seed.close()

        producer = _producer()
        p1 = store_partition_factory(
            name="done",
            options=RocksDBOptions(legacy_records_ttl=ttl),
            changelog_producer=producer,
        )
        tx = p1.begin()
        tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)  # durable flip + done-marker
        assert p1.uses_ttl_stamps is True
        assert _done_marker_present(p1) is True
        p1.close()

        reopened = store_partition_factory(
            name="done",
            options=RocksDBOptions(legacy_records_ttl=ttl),
        )
        # Marker present → the ledger + progress are dropped on open.
        assert _ledger_keys(reopened) == set()
        assert _progress_counter(reopened) is None
        with caplog.at_level(logging.INFO):
            reopened.complete_recovery()
        # No spurious resume fired.
        assert not any(
            "RESUME STARTED" in r.message for r in caplog.records
        ), "A completed migration must not fire the warm-restart resume"
        # Records intact and still stamped.
        decoded = _decode_default_cf(reopened)
        legacy_keys = [k for k in decoded if b"knew" not in k]
        assert len(legacy_keys) == 3
        reopened.close()

    def test_empty_store_flip_reopen_no_resume(self, store_partition_factory, caplog):
        import logging

        ttl = timedelta(days=7)
        ts = 5_000

        producer = _producer()
        p1 = store_partition_factory(
            name="empty",
            options=RocksDBOptions(legacy_records_ttl=ttl),
            changelog_producer=producer,
        )
        tx = p1.begin()
        tx.set(key="k1", value="v1", prefix=b"pfx", timestamp=ts, ttl=timedelta(days=1))
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)  # empty-store flip + done-marker, no ledger
        assert p1.uses_ttl_stamps is True
        assert _ledger_keys(p1) == set()
        p1.close()

        reopened = store_partition_factory(
            name="empty",
            options=RocksDBOptions(legacy_records_ttl=ttl),
        )
        with caplog.at_level(logging.INFO):
            reopened.complete_recovery()
        assert not any("RESUME STARTED" in r.message for r in caplog.records)
        assert _ledger_keys(reopened) == set()
        reopened.close()
