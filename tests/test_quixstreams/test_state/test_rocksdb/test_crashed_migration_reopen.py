"""
Unit tests for reopening a store whose legacy-TTL migration never finished
recording itself: sc-74843, live crash loop in Quix Cloud on 2026-09-04.

MECHANISM (two sentences). A store that carries this-branch migration
bookkeeping (``__ttl_backfill_pending__`` / ``__ttl_backfill_stamped__`` /
``__ttl_system__`` / the progress markers) but NOT the ``__ttl_enabled__`` flip
flag was owned by no code path: ``_has_warm_ttl_artifacts`` deliberately vetoes
itself on exactly that bookkeeping and deferred to ``complete_recovery``, while
``complete_recovery`` only runs for a partition the ``RecoveryManager`` flags,
and the incomplete-migration term of that check
(``has_incomplete_ttl_migration``) short-circuits on ``uses_ttl_stamps`` -- the
very flag that is missing. So an offset-caught-up store opened in LEGACY mode
over TTL-stamped values, no recovery pass ever ran ("Beginning recovery check"
was absent from the logs), and the first ``state.get()`` handed ``8B||json`` to
the value deserializer: ``StateSerializationError`` (orjson "surrogates not
allowed") one second after "Assigned store partition", on every restart.

The fix under test:

* open-time repair (``_repair_unflagged_stamped_store``) flips + persists such a
  store when a bounded sample of its default CF still carries a LIVE stamp,
  which also makes ``has_incomplete_ttl_migration`` True and therefore forces
  the recovery pass that finishes the migration with the data partitions paused;
* the ``ttl_force_flip`` lever (``RocksDBOptions`` field or
  ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1``) as the operator escape hatch for a
  store whose bookkeeping is gone, so there is no evidence left to repair on;
* a legacy-read guard that raises the actionable ``StateMigrationError`` instead
  of letting a stamped value reach the deserializer.

Non-goal, asserted here so it stays deliberate: a repaired store does NOT
wrap-once its census. A repaired flip is inferred evidence ("these bytes are
stamped"), never proof of a legacy->TTL migration, so ``complete_recovery``
re-judges the census from its own bytes -- an all-stamped census is adopted
VERBATIM (never double-wrapped into ``8B||8B||json``, which would recreate the
crash) and a mixed one is discarded, leaving its un-stamped members readable and
never-expiring. Wrapping a mixed census uniformly would double-wrap its stamped
members, and per-record byte routing is banned in this codebase.
"""

import dataclasses
import logging
import struct
import uuid
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from confluent_kafka import TopicPartition as ConfluentPartition
from rocksdict import WriteBatch

from quixstreams.kafka import Consumer
from quixstreams.state import exceptions as state_exceptions
from quixstreams.state.base import StorePartition
from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.metadata import (
    METADATA_CF_NAME,
    TTL_ADOPT_BACKUP_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb import metadata as rocksdb_metadata
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION_KEY,
    TTL_ADOPT_PENDING_KEY,
    TTL_BACKFILL_IN_PROGRESS_KEY,
    TTL_BACKFILL_PROGRESS_KEY,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
    TTL_ROLLBACK_ENV_VAR,
)
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.utils.json import dumps as json_dumps


class _StateMigrationErrorNotImplemented(Exception):
    """Stand-in for ``StateMigrationError`` on a build that predates it.

    It is never raised, so a ``pytest.raises`` against it fails with whatever the
    unfixed code really did -- which for the sc-74843 shapes is the
    ``StateSerializationError`` these tests exist to eliminate.
    """


# The two symbols this fix ADDS are resolved dynamically instead of imported.
# A module-level ``from ... import StateMigrationError`` (or
# ``TTL_FORCE_FLIP_ENV_VAR``) turns this whole file into ONE collection
# ``ImportError`` when it is run against the pre-fix tree, which is how the RED
# proof is taken: the per-test verdicts vanish and the tests that pin
# fix-INDEPENDENT behaviour (``TestMigrationBlocksLiveProcessing``) cannot be
# shown green pre-fix at all. Resolving here keeps collection working on both
# trees so every test passes or fails on its own merits (Tester round 1,
# bug 1.2, 2026-09-04).
StateMigrationError = getattr(
    state_exceptions, "StateMigrationError", _StateMigrationErrorNotImplemented
)
TTL_FORCE_FLIP_ENV_VAR = getattr(
    rocksdb_metadata, "TTL_FORCE_FLIP_ENV_VAR", "QUIXSTREAMS_STATE_TTL_FORCE_FLIP"
)

PREFIX = b"pfx"

# A far-future stamp (~year 2096) rather than the ``NOW_MS + delta`` idiom of the
# sibling suites. The open-time evidence gate compares stamps against the REAL
# wallclock: it runs inside ``__init__``, before a test can patch ``_now_ms``, so
# a stamp derived from a hardcoded "now" would silently become past-dated and
# turn these tests red on a calendar date instead of on a code change.
LIVE_STAMP_MS = 4_000_000_000_000

# Past-dated 8-byte big-endian epoch-ms: the legacy ``set_bytes()`` dedup value
# shape. It decodes as a plausible stamp but is not one, and is the false
# positive that the evidence gate and the read guard must both refuse to act on.
PAST_DEDUP_MS = 1_700_000_000_000

# ``legacy_records_ttl`` is set exactly as the failing deployment had it: it is
# what routes a census into the wrap-once completion, so it must be present for
# the double-wrap guard in
# ``test_repaired_v3240_census_is_adopted_verbatim_not_double_wrapped`` to mean
# anything.
LEGACY_TTL_OPTIONS = RocksDBOptions(
    legacy_records_ttl=timedelta(hours=1),
    open_max_retries=0,
    open_retry_backoff=3.0,
)


def _raw_key(key_str: str, prefix: bytes = PREFIX) -> bytes:
    """The on-changelog / on-disk key for ``state.get(key_str)`` under ``prefix``."""
    return prefix + b"|" + json_dumps(key_str)


def _replay(partition, msgs) -> None:
    """Replay ``(key, value, ttl_stamped)`` default-CF changelog messages."""
    for offset, (key, value, ttl_stamped) in enumerate(msgs):
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )


def _delete_flip_flag(partition) -> None:
    """Delete ONLY ``__ttl_enabled__``, leaving every other marker in place --
    the disk shape of a store whose values are stamped but whose flip was either
    never recorded or recorded and later removed."""
    batch = WriteBatch(raw_mode=True)
    batch.delete(TTL_ENABLED_KEY, partition.get_column_family_handle(METADATA_CF_NAME))
    partition._write(batch)


def _strip_ttl_evidence(partition) -> None:
    """Erase every TTL trace from a store while leaving its (still stamped)
    values untouched: no flip flag, no format / high-water markers, no expiry
    index, no migration bookkeeping.

    This is the "no evidence left" shape -- a rebuilt state directory, or a
    cleanup that removed the markers -- in which the automatic open-time repair
    has nothing to identify the store by, so only the operator lever can fix it.
    """
    for cf_name in (
        TTL_BACKFILL_PENDING_CF_NAME,
        TTL_BACKFILL_STAMPED_CF_NAME,
        TTL_SYSTEM_CF_NAME,
        TTL_INDEX_CF_NAME,
        TTL_ADOPT_BACKUP_CF_NAME,
    ):
        partition._drop_local_cf_if_exists(cf_name)
    batch = WriteBatch(raw_mode=True)
    handle = partition.get_column_family_handle(METADATA_CF_NAME)
    for key in (
        TTL_ENABLED_KEY,
        STATE_FORMAT_VERSION_KEY,
        TTL_HIGH_WATER_KEY,
        TTL_BACKFILL_PROGRESS_KEY,
        TTL_BACKFILL_IN_PROGRESS_KEY,
        TTL_ADOPT_PENDING_KEY,
    ):
        batch.delete(key, handle)
    partition._write(batch)
    # All three open-time classifiers must now see nothing: no warm-preview
    # signal (which would flip via case 2), no migration bookkeeping (case 3),
    # and no v3.24.0-adoption bookkeeping (which would arm the legacy-read guard
    # even though nothing flips).
    assert partition._has_warm_ttl_artifacts() is False
    assert partition._migration_artifacts_at_open() == ""
    assert partition._v3240_adopt_artifacts_at_open() == ""


def _flip_flag(partition):
    return partition.get_or_create_column_family(METADATA_CF_NAME).get(
        TTL_ENABLED_KEY, default=None
    )


def _pending_keys(partition) -> set:
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _default_cf(partition) -> dict:
    cf = partition.get_or_create_column_family("default")
    return {bytes(key): bytes(value) for key, value in cf.items()}


def _read(partition, key_str: str):
    """The user read path: ``state.get(key)``."""
    return partition.begin().get(key=key_str, prefix=PREFIX)


def _read_bytes(partition, key_str: str):
    """The user read path for raw values: ``state.get_bytes(key)``."""
    return partition.begin().get_bytes(key=key_str, prefix=PREFIX, default=None)


def _messages(caplog):
    return [record.getMessage() for record in caplog.records]


def _seed_mixed_crashed_migration(
    store_partition_factory, changelog_producer_mock, name="mixed"
):
    """Replay a MIXED changelog (header-true stamped + header-absent legacy),
    then delete the flip flag and close: a migration whose stamped records are on
    disk but whose flip is not recorded.

    Returns ``(stamped_payloads, legacy_values)``.
    """
    partition = store_partition_factory(
        name=name,
        options=LEGACY_TTL_OPTIONS,
        changelog_producer=changelog_producer_mock,
    )
    stamped_payloads = {"s0": "stamped-0", "s1": "stamped-1"}
    legacy_values = {_raw_key("l0"): json_dumps("legacy-0")}
    msgs = [
        (_raw_key(key), encode_ttl_value(LIVE_STAMP_MS, json_dumps(payload)), True)
        for key, payload in stamped_payloads.items()
    ]
    msgs += [(key, value, False) for key, value in legacy_values.items()]
    _replay(partition, msgs)

    # Sanity: the replay itself flipped and censused, as it does in production.
    assert partition.uses_ttl_stamps is True
    assert _pending_keys(partition) == set(legacy_values)

    _delete_flip_flag(partition)
    partition.close()
    return stamped_payloads, legacy_values


def _seed_v3240_cold_replay(
    store_partition_factory,
    changelog_producer_mock,
    name="v3240",
    n=3,
    adopt=False,
):
    """Replay an all-header-absent changelog of v3.24.0-STAMPED values and close.

    With ``adopt=False`` (the incident's own shape) ``complete_recovery`` is NOT
    called: the process died at the recovery-finalize seam, so the census exists,
    the values are stamped, the changelog offset is caught up, and the flip flag
    was never written at all -- v3.24.0 never wrote it and this build never
    flipped. With ``adopt=True`` the recovery pass completes, which provisionally
    (reversibly) adopts the stamps and leaves the store in the state the rollback
    lever exists to undo.

    Returns ``{raw_key: verbatim on-disk value}``.
    """
    partition = store_partition_factory(
        name=name,
        options=LEGACY_TTL_OPTIONS,
        changelog_producer=changelog_producer_mock,
    )
    values = {
        _raw_key(f"k{i}"): encode_ttl_value(LIVE_STAMP_MS + i, json_dumps(f"v{i}"))
        for i in range(n)
    }
    _replay(partition, [(key, value, False) for key, value in values.items()])

    # No header-true record, so nothing flipped and nothing was flagged.
    assert partition.uses_ttl_stamps is False
    assert _flip_flag(partition) is None
    assert _pending_keys(partition) == set(values)

    if adopt:
        partition.complete_recovery()
        assert partition.uses_ttl_stamps is True
        assert partition._adopt_provisional is True

    partition.close()
    return values


class TestCrashedMigrationReopen:
    def test_reopen_after_crashed_migration_reads_stamped_value_without_crash(
        self, store_partition_factory, changelog_producer_mock
    ):
        """sc-74843 / 2026-09-04: reopening a store that holds TTL-stamped values
        with migration bookkeeping but no ``__ttl_enabled__`` flag must repair the
        flag at open and read its stamped values normally.

        RED on the unfixed code: the reopen leaves ``uses_ttl_stamps`` False (the
        bookkeeping vetoes the warm-artifact flip), so ``state.get()`` hands
        ``8B||json`` to the deserializer and raises ``StateSerializationError`` --
        the live crash loop, reproduced.
        """
        stamped_payloads, legacy_values = _seed_mixed_crashed_migration(
            store_partition_factory, changelog_producer_mock
        )

        reopened = store_partition_factory(
            name="mixed",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )

        # Repaired at open, and the repair is DURABLE: the next open loads the
        # flag and never re-enters the repair.
        assert reopened.uses_ttl_stamps is True
        assert _flip_flag(reopened) is not None

        # The crash, gone: the stamped values read back as their payloads.
        for key, payload in stamped_payloads.items():
            assert _read(reopened, key) == payload

        # The repair hands the leftover migration back to the recovery pass:
        # ``RecoveryPartition.needs_recovery_check`` consults this, which makes
        # the RecoveryManager pause every assigned partition and run
        # ``complete_recovery`` before any data partition resumes.
        assert reopened.has_incomplete_ttl_migration() is True

        reopened.complete_recovery()

        # A MIXED census on a REPAIRED store is discarded, not wrapped: wrapping
        # it uniformly would double-wrap its stamped members, and per-record byte
        # routing is banned. The leftover stays readable (the fail-safe read path
        # returns an un-stamped value raw) and never-expiring.
        assert _pending_keys(reopened) == set()
        assert _read(reopened, "l0") == "legacy-0"
        assert _default_cf(reopened)[_raw_key("l0")] == legacy_values[_raw_key("l0")]

        # Still readable, still not re-stamped, after the recovery pass.
        for key, payload in stamped_payloads.items():
            assert _read(reopened, key) == payload

        reopened.close()

    def test_repaired_v3240_census_is_adopted_verbatim_not_double_wrapped(
        self, store_partition_factory, changelog_producer_mock
    ):
        """sc-74843 / 2026-09-04, the incident's exact shape: a v3.24.0 store
        cold-replayed to its changelog highwater whose process died before
        ``complete_recovery`` could adopt it. The values are stamped, the census
        exists, the flag was never written, and the store is offset-caught-up, so
        no later recovery pass ever runs.

        RED on the unfixed code: the reopen stays legacy and the first read
        raises ``StateSerializationError``. This also guards the corruption the
        fix must not introduce -- with ``legacy_records_ttl`` configured, routing
        this census into the wrap-once completion would stamp already-stamped
        values a SECOND time (``8B||8B||json``) and recreate the same
        unreadable-value crash, so the values must come out byte-identical.
        """
        values = _seed_v3240_cold_replay(
            store_partition_factory, changelog_producer_mock
        )

        reopened = store_partition_factory(
            name="v3240",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        assert reopened.uses_ttl_stamps is True
        assert _flip_flag(reopened) is not None
        assert _read(reopened, "k0") == "v0"

        reopened.complete_recovery()

        # Adopted VERBATIM: not one byte changed, census drained, reads work.
        assert _default_cf(reopened) == values
        assert _pending_keys(reopened) == set()
        for i in range(len(values)):
            assert _read(reopened, f"k{i}") == f"v{i}"

        # The expiry index was rebuilt from the adopted stamps so the records can
        # still expire (the index is local-only and does not survive a rebuild).
        index_cf = reopened.get_or_create_column_family(TTL_INDEX_CF_NAME)
        assert len(list(index_cf.keys())) == len(values)

        reopened.close()

    def test_pure_legacy_store_is_not_falsely_flipped(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """A legacy ``set_bytes()`` dedup store -- 8-byte big-endian PAST epoch-ms
        values, which decode as plausible stamps -- must stay legacy across a
        reopen even though its replay left a pending census behind.

        This is the false-flip class from ``test_first_enablement_cold_restore``,
        re-asserted against the new open-time repair: bookkeeping alone must never
        be enough to flip, because flipping here would strip 8 bytes off every
        read AND hand the sweep a store in which every record is already expired.
        The read guard must stay silent too -- these values are not live stamps.
        """
        partition = store_partition_factory(
            name="legacy",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        values = {
            _raw_key(f"k{i}"): struct.pack(">Q", PAST_DEDUP_MS + i) for i in range(5)
        }
        _replay(partition, [(key, value, False) for key, value in values.items()])
        assert partition.uses_ttl_stamps is False
        assert _pending_keys(partition) == set(values)
        partition.close()

        with caplog.at_level(logging.WARNING):
            reopened = store_partition_factory(
                name="legacy",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )

        # Refused: still legacy, still unflagged, still byte-identical.
        assert reopened.uses_ttl_stamps is False
        assert _flip_flag(reopened) is None
        assert _default_cf(reopened) == values

        # And the read path does not raise on them either.
        for i in range(5):
            assert _read_bytes(reopened, f"k{i}") == values[_raw_key(f"k{i}")]

        # The refusal is observable and names the override.
        assert any(
            TTL_FORCE_FLIP_ENV_VAR in message for message in _messages(caplog)
        ), _messages(caplog)

        reopened.close()

    def test_legacy_read_path_never_hands_stamped_bytes_to_json(
        self, store_partition_factory, changelog_producer_mock, monkeypatch, caplog
    ):
        """CHOICE: fail loud, not auto-strip.

        A store flagged legacy while holding live TTL-stamped values is
        unreconciled, and neither degradation is safe to pick silently: returning
        the value raw is the sc-74843 crash (``StateSerializationError`` out of
        the deserializer), while stripping eight bytes would corrupt a genuine
        legacy value whose prefix merely happens to decode -- the documented
        residual of ``_safe_decode_stamp`` -- and would also hide the mis-flag
        while the partition keeps writing UN-stamped values. A store-wide flip IS
        decidable at open, where the whole default CF can be sampled and the
        decision persisted, so a read that still gets here is one the repair
        already looked at and declined: it raises ``StateMigrationError`` naming
        the override lever instead.

        The rollback lever is what suppresses the repair here. It is an explicit
        "keep this store legacy" instruction, so the repair must not silently
        undo it on the next restart.

        RED on the unfixed code: the read raises ``StateSerializationError`` from
        ``orjson`` instead, with a traceback pointing at the serializer rather
        than at the state store.
        """
        _seed_mixed_crashed_migration(
            store_partition_factory, changelog_producer_mock, name="unreconciled"
        )
        monkeypatch.setenv(TTL_ROLLBACK_ENV_VAR, "1")

        reopened = store_partition_factory(
            name="unreconciled",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        # Rollback intent respected: no repair, no flip.
        assert reopened.uses_ttl_stamps is False
        assert _flip_flag(reopened) is None

        with caplog.at_level(logging.WARNING):
            with pytest.raises(StateMigrationError) as first:
                _read(reopened, "s0")
            with pytest.raises(StateMigrationError):
                _read(reopened, "s1")

        message = str(first.value)
        assert TTL_FORCE_FLIP_ENV_VAR in message
        assert TTL_ROLLBACK_ENV_VAR in message
        # The payload is never echoed -- only the length and the stamp prefix.
        assert "stamped-0" not in message
        assert not isinstance(first.value, StateSerializationError)

        # The WARNING is rate-limited to once per partition; the raise is not.
        warnings = [
            message
            for message in _messages(caplog)
            if "Unreconciled TTL state" in message
        ]
        assert len(warnings) == 1, warnings

        reopened.close()


class TestForceFlipLever:
    def test_force_flip_flag_repairs_unflagged_stamped_store(
        self, store_partition_factory, changelog_producer_mock, monkeypatch, caplog
    ):
        """``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1`` repairs a store the automatic
        path cannot: stamped values, no flip flag, and no bookkeeping left to
        identify it by (a rebuilt state directory, or bookkeeping dropped by a
        cleanup). Without the lever such a store has no evidence to act on, so it
        stays legacy and still crashes on read; with it, the flip is persisted and
        the reads work. A later open without the lever is a no-op.

        RED on the unfixed code: no lever exists, so the flag is never persisted
        and the read keeps raising ``StateSerializationError``.
        """
        values = _seed_v3240_cold_replay(
            store_partition_factory, changelog_producer_mock, name="noevidence"
        )

        # This open REPAIRS the store (bookkeeping + live stamps), so strip every
        # trace afterwards: that leaves the automatic repair nothing to key on
        # and the read guard unarmed, which is what makes the lever necessary.
        stripped = store_partition_factory(
            name="noevidence",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        assert stripped.uses_ttl_stamps is True
        _strip_ttl_evidence(stripped)
        stripped.close()

        # Without the lever: no evidence, no repair -- and the original crash.
        unrepaired = store_partition_factory(
            name="noevidence",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        assert unrepaired.uses_ttl_stamps is False
        with pytest.raises(StateSerializationError):
            _read(unrepaired, "k0")
        unrepaired.close()

        # With the lever: flipped, persisted, readable, values untouched.
        monkeypatch.setenv(TTL_FORCE_FLIP_ENV_VAR, "1")
        with caplog.at_level(logging.INFO):
            forced = store_partition_factory(
                name="noevidence",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        assert forced.uses_ttl_stamps is True
        assert _flip_flag(forced) is not None
        assert _read(forced, "k0") == "v0"
        assert _default_cf(forced) == values
        assert any("Forced TTL mode" in message for message in _messages(caplog))
        assert any(
            "TTL force-flip lever: ON (source=env)" in message
            for message in _messages(caplog)
        )
        forced.close()

        # Idempotent: with the lever gone the store is still flipped (the flag is
        # on disk now) and the repair does not run again.
        monkeypatch.delenv(TTL_FORCE_FLIP_ENV_VAR)
        caplog.clear()
        with caplog.at_level(logging.INFO):
            reopened = store_partition_factory(
                name="noevidence",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        assert reopened.uses_ttl_stamps is True
        assert _read(reopened, "k0") == "v0"
        assert not any("Forced TTL mode" in message for message in _messages(caplog))
        reopened.close()

    def test_force_flip_lever_via_options_with_env_unset(
        self, store_partition_factory, changelog_producer_mock, monkeypatch, caplog
    ):
        """The lever must work as an IN-CODE option with no environment variable
        set: a Quix Cloud deployment silently drops env vars that the app's
        ``app.yaml`` does not declare, which is how the env-only lever went
        missing while sc-74843 was being diagnosed.
        """
        monkeypatch.delenv(TTL_FORCE_FLIP_ENV_VAR, raising=False)
        monkeypatch.delenv(TTL_ROLLBACK_ENV_VAR, raising=False)
        _seed_v3240_cold_replay(
            store_partition_factory, changelog_producer_mock, name="opt-force"
        )
        stripped = store_partition_factory(
            name="opt-force",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        _strip_ttl_evidence(stripped)
        stripped.close()

        with caplog.at_level(logging.INFO):
            forced = store_partition_factory(
                name="opt-force",
                options=dataclasses.replace(LEGACY_TTL_OPTIONS, ttl_force_flip=True),
                changelog_producer=changelog_producer_mock,
            )

        assert forced.uses_ttl_stamps is True
        assert _flip_flag(forced) is not None
        assert _read(forced, "k0") == "v0"
        assert any(
            "TTL force-flip lever: ON (source=options)" in message
            for message in _messages(caplog)
        ), _messages(caplog)
        forced.close()

    def test_force_flip_and_rollback_together_refuse_to_open(
        self, store_partition_factory, changelog_producer_mock, monkeypatch
    ):
        """The two levers are contradictory, so they are rejected: as options at
        construction, and on the RESOLVED values at open (which is the only place
        an option/env-var cross-combination can be seen)."""
        with pytest.raises(ValueError) as options_error:
            RocksDBOptions(ttl_rollback=True, ttl_force_flip=True)
        assert "mutually exclusive" in str(options_error.value)

        monkeypatch.setenv(TTL_FORCE_FLIP_ENV_VAR, "1")
        with pytest.raises(StateMigrationError) as open_error:
            store_partition_factory(
                name="both-levers",
                options=dataclasses.replace(LEGACY_TTL_OPTIONS, ttl_rollback=True),
                changelog_producer=changelog_producer_mock,
            )
        message = str(open_error.value)
        assert "rollback source=options" in message
        assert "force-flip source=env" in message


class TestRollbackLeverSurfaces:
    """The rollback lever must be settable in code, not only through the
    environment -- same reason as the force-flip lever. Both surfaces must reach
    the same code path and say which one won."""

    def test_rollback_lever_via_options_with_env_unset(
        self, store_partition_factory, changelog_producer_mock, monkeypatch, caplog
    ):
        monkeypatch.delenv(TTL_ROLLBACK_ENV_VAR, raising=False)
        monkeypatch.delenv(TTL_FORCE_FLIP_ENV_VAR, raising=False)
        values = _seed_v3240_cold_replay(
            store_partition_factory,
            changelog_producer_mock,
            name="opt-rollback",
            adopt=True,
        )

        with caplog.at_level(logging.INFO):
            rolled_back = store_partition_factory(
                name="opt-rollback",
                options=dataclasses.replace(LEGACY_TTL_OPTIONS, ttl_rollback=True),
                changelog_producer=changelog_producer_mock,
            )

        # The provisional adoption is reverted: legacy again, flag cleared, and
        # every untouched value byte-identical (the rollback contract).
        assert rolled_back.uses_ttl_stamps is False
        assert _flip_flag(rolled_back) is None
        assert _default_cf(rolled_back) == values
        assert any(
            "TTL rollback lever: ON (source=options)" in message
            for message in _messages(caplog)
        ), _messages(caplog)

        # Consequence worth pinning: rolling back a GENUINE v3.24.0 store leaves
        # live stamps under a legacy flag, so the read path now refuses with the
        # named migration error instead of dying inside the deserializer. The
        # operator's way out is in that message.
        with pytest.raises(StateMigrationError):
            _read(rolled_back, "k0")

        rolled_back.close()

    def test_rollback_lever_via_env_with_option_default(
        self, store_partition_factory, changelog_producer_mock, monkeypatch, caplog
    ):
        values = _seed_v3240_cold_replay(
            store_partition_factory,
            changelog_producer_mock,
            name="env-rollback",
            adopt=True,
        )
        monkeypatch.setenv(TTL_ROLLBACK_ENV_VAR, "1")

        with caplog.at_level(logging.INFO):
            rolled_back = store_partition_factory(
                name="env-rollback",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )

        assert rolled_back.uses_ttl_stamps is False
        assert _default_cf(rolled_back) == values
        assert any(
            "TTL rollback lever: ON (source=env)" in message
            for message in _messages(caplog)
        ), _messages(caplog)
        rolled_back.close()


class TestMigrationBlocksLiveProcessing:
    """The repair only works because the recovery machinery already blocks live
    processing until ``complete_recovery`` has returned. This pins that ordering,
    which is the second half of the sc-74843 fix: flipping the flag at open makes
    ``has_incomplete_ttl_migration`` True, and THAT is what gets the partition
    paused and its migration finished before any record is processed.

    Verified in ``quixstreams/state/recovery.py``: ``assign_partition`` registers
    the partition when ``needs_recovery_check`` is True and pauses the WHOLE
    assignment (``_pause_for_recovery``, line 742); ``do_recovery`` runs
    ``_recovery_loop`` (line 559) and only then resumes the non-changelog
    partitions (line 570); and ``_update_recovery_status`` calls
    ``rp.complete_recovery()`` (line 803) inside that loop, before the partition
    is revoked and therefore before the loop can end.
    """

    def test_complete_recovery_runs_before_data_partitions_resume(
        self, recovery_manager_factory, topic_manager_factory
    ):
        topic_name = str(uuid.uuid4())
        store_name = "default"
        lowwater, highwater = 0, 10

        topic_manager = topic_manager_factory()
        data_topic = topic_manager.topic(topic_name)
        changelog_topic = topic_manager.changelog_topic(
            stream_id=topic_name,
            store_name=store_name,
            config=data_topic.broker_config,
        )
        data_tp = ConfluentPartition(topic=data_topic.name, partition=0)
        changelog_tp = ConfluentPartition(topic=changelog_topic.name, partition=0)

        events = []

        consumer = MagicMock(spec_set=Consumer)
        consumer.assignment.return_value = [data_tp, changelog_tp]
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        # One empty poll is enough: the position below already reports the
        # highwater, so the partition finishes its recovery check immediately.
        consumer.poll.side_effect = [None]
        consumer.position.side_effect = lambda partitions: [
            ConfluentPartition(changelog_topic.name, 0, highwater)
        ]
        consumer.pause.side_effect = lambda tps: events.append(
            ("pause", tuple(tp.topic for tp in tps))
        )
        consumer.resume.side_effect = lambda tps: events.append(
            ("resume", tuple(tp.topic for tp in tps))
        )

        # A store partition that is OFFSET-CAUGHT-UP (offset == highwater - 1, so
        # the behind-the-changelog term of ``needs_recovery_check`` is False) but
        # still has an unfinished migration -- precisely the state the open-time
        # repair produces, and the one that used to be skipped entirely.
        store_partition = MagicMock(spec_set=StorePartition)
        store_partition.get_changelog_offset.return_value = highwater - 1
        store_partition.has_incomplete_ttl_migration.return_value = True
        store_partition.complete_recovery.side_effect = lambda: events.append(
            ("complete_recovery", ())
        )

        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )

        # Registered for recovery on the incomplete-migration term alone, and the
        # data partition is paused before anything is processed.
        assert recovery_manager.has_assignments is True
        assert ("pause", (data_topic.name, changelog_topic.name)) in events

        recovery_manager.do_recovery()

        store_partition.complete_recovery.assert_called_once()
        kinds = [name for name, _ in events]
        completed_at = kinds.index("complete_recovery")
        data_resumes = [
            index
            for index, (name, topics) in enumerate(events)
            if name == "resume" and data_topic.name in topics
        ]
        assert data_resumes, events
        assert min(data_resumes) > completed_at, events
