"""
A store whose default-CF values are ALL v3.24.0 TTL stamps must flip into TTL
mode automatically even when every stamp has already EXPIRED, while a genuine
legacy store must still never be flipped.

The defect these tests were written against: both migration decisions gated on
LIVENESS instead of on what the bytes ARE. The open-time repair
(:meth:`RocksDBStorePartition._repair_unflagged_stamped_store`) required at
least one still-live stamp in its bounded sample, and the cold-restore adoption
(``complete_recovery`` BRANCH B,
:meth:`RocksDBStorePartition._survey_backfill_pending`) refused an ``all_past``
census outright. A store that IS fully TTL-migrated but whose short-TTL records
have all expired by the time of a cold restart (a short TTL plus a restart
longer than one TTL window) is indistinguishable under that gate from a store
that never carried TTL bookkeeping -- so it stayed legacy, every read of a
stamped value crashed in the value deserializer, and the force-flip lever
became load-bearing for an entirely ordinary shape.

The evidence rule under test: flip when the sample is UNANIMOUS -- every sampled
value decodes as a stamp whose expiry is the sentinel or lies inside
``[946684800000 (2000-01-01T00:00Z), now + 100 years]`` AND carries a NON-EMPTY
payload after the 8-byte prefix. An empty CF makes no decision. Liveness is
IRRELEVANT. Refuse (stay legacy, guard armed, WARN) when any sampled value
fails: undecodable, out-of-window, or an EMPTY payload -- the last being a
legacy store holding bare 8-byte epoch-ms values (a ``set_bytes()`` dedup
store's "last seen" timestamps). That bare shape is what the retired liveness
gate was really protecting, and it must never flip.

Coverage: the open-time repair; the cold-restore census; a force-flipped store,
whose own leftover census must be adopted by BRANCH A rather than re-judged as
an ambiguous cold census; and the negative controls -- legacy JSON values with
bookkeeping present, a MIXED sample with no interrupted-migration signal, and
bare 8-byte values whose integers happen to look future-dated.
"""

import dataclasses
import logging
import time
from datetime import timedelta

from rocksdict import WriteBatch

from quixstreams.state.metadata import METADATA_CF_NAME, TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import (
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.state.serialization import int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

PREFIX = b"pfx"
HOUR_MS = 3_600_000
# A fixed constant rather than ``now +/- delta``: the open-time evidence gate
# compares stamps against the REAL wallclock inside ``__init__``, before a
# test can patch ``_now_ms``, so a stamp derived from a hardcoded "now" would
# drift into the wrong regime on a calendar date instead of on a code change.
NOW_MS = 1_780_000_000_000
PAST_STAMP_MS = NOW_MS - HOUR_MS
# The bare-8-byte test's REOPEN runs the real open-time gate, which compares
# against the REAL wallclock (``_now_ms`` is never patched on that partition
# object) -- so this must be future relative to ``time.time()`` at import
# time, not merely future relative to the fixed ``NOW_MS`` constant above.
FUTURE_BARE_STAMP_MS = int(time.time() * 1000) + 30 * 86_400_000

KEY_COUNT = 320
CONTROL_KEY_COUNT = 40

PLAIN_OPTIONS = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)


def _raw_key(key_str: str, prefix: bytes = PREFIX) -> bytes:
    """The on-changelog / on-disk key for ``state.get(key_str)`` under ``prefix``."""
    return prefix + b"|" + json_dumps(key_str)


def _replay(partition, msgs) -> None:
    """Replay ``(raw_key, value, ttl_stamped)`` default-CF changelog messages."""
    for offset, (key, value, ttl_stamped) in enumerate(msgs):
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )


def _flip_flag(partition):
    return partition.get_or_create_column_family(METADATA_CF_NAME).get(
        TTL_ENABLED_KEY, default=None
    )


def _pending_keys(partition) -> set:
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _index_count(partition) -> int:
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    return sum(1 for _ in cf.keys())


def _read(partition, key_str: str):
    """The user read path: ``state.get(key)``."""
    return partition.begin().get(key=key_str, prefix=PREFIX)


def _messages(caplog):
    return [record.getMessage() for record in caplog.records]


def _seed_case5_candidate(
    store_partition_factory,
    changelog_producer,
    name,
    value_for_index,
    key_count=KEY_COUNT,
):
    """Build the exact on-disk shape of open-time case 5 (``repair_candidate``):
    a default CF of ``key_count`` values (``value_for_index(i)`` supplies the
    raw bytes for key ``i``), a matching ``__ttl_backfill_pending__`` census
    (the interrupted-migration bookkeeping the live WARNING named), NO
    ``__ttl_enabled__`` flag, and a persisted high-water so an expired stamp
    reads back as MISSING (``None``) rather than raw once flipped -- the same
    invariant a real store establishes through live processing before a
    restart. Everything is written with a raw batch, bypassing replay, so the
    seeded partition's own ``__init__`` never sees the bookkeeping and never
    resolves it -- only the REOPEN below does. Closes the seed partition and
    returns the list of key strings written.
    """
    partition = store_partition_factory(
        name=name,
        options=PLAIN_OPTIONS,
        changelog_producer=changelog_producer,
    )
    batch = WriteBatch(raw_mode=True)
    default_handle = partition.get_column_family_handle("default")
    pending_handle = partition.get_column_family_handle(TTL_BACKFILL_PENDING_CF_NAME)
    metadata_handle = partition.get_column_family_handle(METADATA_CF_NAME)
    keys = [f"k{i}" for i in range(key_count)]
    for i, key in enumerate(keys):
        raw_key = _raw_key(key)
        batch.put(raw_key, value_for_index(i), default_handle)
        batch.put(raw_key, b"", pending_handle)
    batch.put(TTL_HIGH_WATER_KEY, int_to_bytes(NOW_MS), metadata_handle)
    partition._write(batch)
    # Released before returning: a leaked CF handle keeps the RocksDB alive
    # across the reopen below and fails on the Windows LOCK file.
    del batch, default_handle, pending_handle, metadata_handle

    assert partition._migration_artifacts_at_open() == TTL_BACKFILL_PENDING_CF_NAME
    assert partition.uses_ttl_stamps is False
    partition.close()
    return keys


class TestOpenTimeRepairIgnoresLiveness:
    def test_open_time_repair_flips_when_all_sampled_stamps_are_expired(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """THE LIVE SHAPE VERBATIM, 2026-09-07 09:08-09:11Z: a store carrying
        ``__ttl_backfill_pending__`` bookkeeping, no ``__ttl_enabled__`` flag,
        and a default CF where every sampled value decodes as a stamp -- but
        every one of them has already expired (short TTL, restart after more
        than one TTL). No lever set.

        RED on the unfixed code: the open-time repair's gate is
        ``evidence.future_or_sentinel == 0``, which is 0 here (every stamp is
        past), so it refuses and logs "NOT flipping it" -- the exact WARNING
        quoted at the top of this file.
        """
        keys = _seed_case5_candidate(
            store_partition_factory,
            changelog_producer_mock,
            name="all-expired-unanimous",
            value_for_index=lambda i: encode_ttl_value(
                PAST_STAMP_MS, json_dumps(f"payload-{i}")
            ),
        )
        with caplog.at_level(logging.INFO):
            reopened = store_partition_factory(
                name="all-expired-unanimous",
                options=PLAIN_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        try:
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
            assert _index_count(reopened) > 0
            for key in keys[:5]:
                assert _read(reopened, key) is None

            messages = _messages(caplog)
            refused = [m for m in messages if "NOT flipping it" in m]
            assert refused == [], refused
        finally:
            reopened.close()

    def test_cold_restore_adopts_header_absent_changelog_when_all_stamps_expired(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """The cold-restore sibling of the open-time defect: a FRESH partition
        replaying a header-ABSENT (v3.24.0-style) changelog whose stamps have
        all already expired, via ``complete_recovery`` BRANCH B
        (:meth:`RocksDBStorePartition._survey_backfill_pending`). No lever.

        LIVE EVIDENCE, 2026-09-07 (fix build 6d0eabef, single replica,
        ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1`` on an EMPTY store, ~600k
        header-absent replayed records per partition, all expired by the time
        recovery completed):

            Refused auto-adopt at path=...: all 588325 censused stamp(s) are
            already in the past (legacy dedup shape); the store stays legacy,
            byte-identical, and the census is preserved (quarantined). If
            this really is a v3.24.0 store, re-seed the state from source.

        The target rule applies here exactly as to the open-time repair: a
        100% quorum (plausibility window + non-empty payload) must adopt
        regardless of ``all_past``; ``all_past`` may still WARN, but must
        never refuse outright.

        RED on the unfixed code: ``uses_ttl_stamps`` stays False after
        ``complete_recovery()``.
        """
        partition = store_partition_factory(
            name="cold-restore-expired",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        partition._now_ms = lambda: NOW_MS
        msgs = [
            (
                _raw_key(f"k{i}"),
                encode_ttl_value(PAST_STAMP_MS, json_dumps(f"v{i}")),
                False,
            )
            for i in range(KEY_COUNT)
        ]
        try:
            _replay(partition, msgs)
            with caplog.at_level(logging.WARNING):
                partition.complete_recovery()
            messages = _messages(caplog)
            # Documents the live evidence quoted above; not the assertion
            # under test (the target rule may keep this as a non-refusing
            # WARN).
            assert any("legacy dedup shape" in m for m in messages), messages

            assert partition.uses_ttl_stamps is True
            assert _pending_keys(partition) == set()
            assert _index_count(partition) > 0
            for i in range(5):
                assert _read(partition, f"k{i}") == f"v{i}"
        finally:
            partition.close()

    def test_forced_flip_at_open_is_honoured_by_cold_census_completion(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """LIVE ADDENDUM, 2026-09-07 (fix build 6d0eabef, single replica,
        ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1``, cold restore of a
        header-absent v3.24.0 changelog whose stamps are all expired): the
        lever flips the EMPTY partition at open ("Forced TTL mode ... 0 of 0
        sampled") and persists the flag; replay then lands ~600k header-absent
        records + census; ``complete_recovery`` STILL routes into BRANCH B
        (which assumes an unflipped store) and quarantines the census on the
        ``all_past`` refusal -- even though the store is unambiguously
        TTL-mode by explicit operator instruction. Consequence: 2.4M expired,
        UNINDEXED keys stuck in RocksDB forever (quarantine never rebuilds the
        index, so the sweep never touches them).

        ROOT CAUSE, pinned directly below: ``_persisted_flipped_at_open`` is
        computed as ``uses_ttl_stamps and not _ttl_flag_repaired_at_open``. A
        forced flip at open (case 3) sets BOTH ``uses_ttl_stamps = True`` AND
        ``_ttl_flag_repaired_at_open = True`` (and persists the flag
        synchronously via ``_stamp_flip_metadata`` before ``__init__``
        returns) -- so ``_persisted_flipped_at_open`` comes out False for a
        flag that IS durably on disk, and ``complete_recovery`` treats the
        replay as an ambiguous cold census instead of an interrupted
        completion on an already-flipped store.

        RED on the unfixed code.
        """
        options = dataclasses.replace(PLAIN_OPTIONS, ttl_force_flip=True)
        partition = store_partition_factory(
            name="forced-cold-census",
            options=options,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert partition.uses_ttl_stamps is True
            assert _flip_flag(partition) is not None
            assert partition._persisted_flipped_at_open is True, (
                "a forced flip at open synchronously persists __ttl_enabled__ "
                "and must not be treated as a mere unpersisted inference"
            )

            partition._now_ms = lambda: NOW_MS
            msgs = [
                (
                    _raw_key(f"k{i}"),
                    encode_ttl_value(PAST_STAMP_MS, json_dumps(f"v{i}")),
                    False,
                )
                for i in range(CONTROL_KEY_COUNT)
            ]
            _replay(partition, msgs)
            assert _pending_keys(partition) == {
                _raw_key(f"k{i}") for i in range(CONTROL_KEY_COUNT)
            }

            with caplog.at_level(logging.WARNING):
                partition.complete_recovery()

            messages = _messages(caplog)
            quarantined = [m for m in messages if "legacy dedup shape" in m]
            assert quarantined == [], messages

            assert partition.uses_ttl_stamps is True
            assert _pending_keys(partition) == set()
            assert _index_count(partition) == CONTROL_KEY_COUNT
        finally:
            partition.close()


class TestGenuineLegacyStoresStillRefused:
    def test_legacy_json_values_with_bookkeeping_are_never_flipped(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The same bookkeeping (interrupted-migration census) but the default
        CF holds plain JSON, never stamped -- a genuine legacy store that
        happened to accumulate migration bookkeeping (e.g. a discarded
        census). Must stay legacy and byte-identical.

        GREEN control, before and after the fix: no sampled value decodes as
        a stamp at all, so it fails "unanimous" under both the old and the
        new gate.
        """
        keys = _seed_case5_candidate(
            store_partition_factory,
            changelog_producer_mock,
            name="genuine-legacy",
            value_for_index=lambda i: json_dumps(f"legacy-{i}"),
            key_count=CONTROL_KEY_COUNT,
        )
        reopened = store_partition_factory(
            name="genuine-legacy",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is False
            assert _flip_flag(reopened) is None
            for key in keys:
                assert _read(reopened, key) == f"legacy-{key[1:]}"
        finally:
            reopened.close()

    def test_mixed_sample_is_refused(
        self, store_partition_factory, changelog_producer_mock
    ):
        """Half the sample decodes as an expired stamp, half is plain JSON --
        not unanimous, so the store must stay legacy either way.

        GREEN control, before and after the fix.
        """

        def _value_for_index(i):
            if i % 2 == 0:
                return encode_ttl_value(PAST_STAMP_MS, json_dumps(f"stamped-{i}"))
            return json_dumps(f"legacy-{i}")

        _seed_case5_candidate(
            store_partition_factory,
            changelog_producer_mock,
            name="mixed-sample",
            value_for_index=_value_for_index,
            key_count=CONTROL_KEY_COUNT,
        )
        reopened = store_partition_factory(
            name="mixed-sample",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is False
            assert _flip_flag(reopened) is None
        finally:
            reopened.close()

    def test_bare_8_byte_values_are_not_stamps(
        self, store_partition_factory, changelog_producer_mock
    ):
        """A genuine legacy ``set_bytes()`` dedup store: every value is EXACTLY
        8 bytes (a "last seen" epoch-ms integer, no payload). Chosen
        FUTURE-dated (relative to the REAL wallclock, not the fixed ``NOW_MS``
        constant -- the reopen below never patches ``_now_ms``) on purpose:
        the current sampler's gate (``future_or_sentinel == 0``) is fooled by
        this shape into treating it as LIVE stamp evidence, because
        ``_safe_decode_stamp`` never checks for a non-empty payload -- only
        the target rule's explicit non-empty-payload requirement catches it.

        CONFIRMED RED on the unfixed code: the reopen logs "Repaired an
        interrupted legacy-TTL migration ... 40 of 40 sampled default-CF
        value(s) carrying a live TTL stamp" and flips a store that is, in
        fact, a bare-8-byte legacy dedup shape.
        """
        keys = _seed_case5_candidate(
            store_partition_factory,
            changelog_producer_mock,
            name="bare-8-byte",
            value_for_index=lambda i: encode_ttl_value(FUTURE_BARE_STAMP_MS, b""),
            key_count=CONTROL_KEY_COUNT,
        )
        reopened = store_partition_factory(
            name="bare-8-byte",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is False, (
                "bare 8-byte legacy values (empty payload) must never be "
                "treated as TTL stamp evidence, regardless of liveness"
            )
            assert _flip_flag(reopened) is None
            for key in keys:
                raw = reopened.begin().get_bytes(key=key, prefix=PREFIX, default=None)
                assert raw == encode_ttl_value(FUTURE_BARE_STAMP_MS, b"")
        finally:
            reopened.close()


class TestBackfillNeverDoubleWrapsAndReplaySelfHeals:
    """
    Round-5 regression tests requested in ArchDev's hand-off (bugs 5.6/5.7),
    written AFTER the fix (this class is validation-mode, not TDD): the
    ``skip_already_stamped=True`` don't-double-stamp guard on the live legacy
    backfill (bug 5.6) and the double-wrap self-heal at changelog replay / read
    (bug 5.7). See ``spec-5.2-amendment-round5.md`` sections "Live legacy
    backfill" and "Double-stamp self-heal".
    """

    def test_quarantined_census_is_never_legacy_backfilled(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """Bug 5.6: a mixed census -- mostly already-stamped, expired v3.24.0
        values plus a minority of bare 8-byte legacy dedup values -- is
        correctly REFUSED (quarantined: ``bare_payloads > 0``) at open, per
        bugs 5.4/5.5's non-empty-payload rule
        (``test_v3240_auto_adopt.py::TestAllPastAdoptsAndBareValuesRefused``).

        Live incident 2026-09-07 09:36-09:39Z: once such a census was
        quarantined (pre-5.2-fix, on an ALL-past census), the store stayed
        legacy holding already-stamped values, and the FIRST live ``ttl=``
        write drove ``backfill_legacy_records`` over the entire default CF and
        re-wrapped every one of them a SECOND time (``8B||8B||json``),
        corrupting the changelog and crash-looping the next restart with the
        same ``StateSerializationError`` that opened sc-74843. With 5.2 fixed,
        a 100%-stamped census no longer reaches quarantine -- but a MIXED
        census (stamped + bare) still legitimately does, and it carries the
        exact same already-stamped cargo. The ``skip_already_stamped=True``
        default (bug 5.6) must leave every already-stamped value byte-identical
        when that cargo is later handed to the live backfill.

        The already-stamped cohort is deliberately given a STILL-LIVE
        (future) own expiry, not an expired one: an expired already-stamped
        value is correctly indexed-then-reclaimed by the very next TTL sweep
        (the flip flush runs one), which would delete it and make a
        byte-identity comparison meaningless. A live own stamp isolates the
        assertion this test exists for -- no re-wrap -- from that unrelated,
        equally-correct sweep behavior.
        """
        STAMPED_COUNT = 80
        BARE_COUNT = 20
        LIVE_OWN_STAMP_MS = NOW_MS + HOUR_MS

        def _value_for_index(i):
            if i < STAMPED_COUNT:
                return encode_ttl_value(LIVE_OWN_STAMP_MS, json_dumps(f"payload-{i}"))
            return encode_ttl_value(FUTURE_BARE_STAMP_MS, b"")

        keys = _seed_case5_candidate(
            store_partition_factory,
            changelog_producer_mock,
            name="quarantine-then-backfill",
            value_for_index=_value_for_index,
            key_count=STAMPED_COUNT + BARE_COUNT,
        )
        options = dataclasses.replace(
            PLAIN_OPTIONS, legacy_records_ttl=timedelta(hours=2)
        )
        reopened = store_partition_factory(
            name="quarantine-then-backfill",
            options=options,
            changelog_producer=changelog_producer_mock,
        )
        try:
            # Preconditions: the mixed sample is refused (bare payload
            # present), the store stays legacy, the census is preserved.
            assert reopened.uses_ttl_stamps is False
            assert _flip_flag(reopened) is None
            assert _pending_keys(reopened) == {_raw_key(k) for k in keys}

            stamped_keys = keys[:STAMPED_COUNT]
            # Raw on-disk bytes via the partition-level CF handle, NOT
            # ``tx.get_bytes`` -- once flipped, the transaction's TTL-aware
            # ``get_bytes`` returns the STRIPPED payload (post-unwrap), not
            # the literal on-disk bytes, so it cannot detect a re-wrap.
            default_cf = reopened.get_or_create_column_family("default")
            before = {
                key: default_cf.get(_raw_key(key), default=None) for key in stamped_keys
            }
            assert all(v is not None for v in before.values())

            with caplog.at_level(logging.INFO):
                with reopened.begin() as tx:
                    tx.set(
                        key="trigger",
                        value="trigger-value",
                        prefix=PREFIX,
                        timestamp=NOW_MS,
                        ttl=timedelta(hours=1),
                    )

            # The trigger write completed the migration: the store is flipped
            # and every pre-existing stamped value stayed SINGLE-stamped.
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None

            after = {
                key: default_cf.get(_raw_key(key), default=None) for key in stamped_keys
            }
            assert after == before, "already-stamped values must stay byte-identical"

            messages = _messages(caplog)
            started = [m for m in messages if "TTL legacy backfill STARTED" in m]
            assert started, messages
            finished = [m for m in messages if "TTL legacy backfill FINISHED" in m]
            assert finished, messages
            # Only the BARE (genuinely legacy) values were re-stamped; every
            # already-stamped value was skipped and left byte-identical.
            assert f"{BARE_COUNT} records re-stamped" in finished[0], finished[0]
            assert f"{STAMPED_COUNT} already" in finished[0], finished[0]
            preserved_warn = [m for m in messages if "already TTL-stamped" in m]
            assert preserved_warn, messages

            # The already-stamped values read back as their ORIGINAL payload
            # (single-stamped, still live) -- never a StateSerializationError
            # and never the double-wrap corruption's mangled bytes.
            for i, key in enumerate(stamped_keys[:5]):
                assert _read(reopened, key) == f"payload-{i}"
        finally:
            reopened.close()

    def test_double_wrapped_record_is_unwrapped_at_replay_and_read(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """Bug 5.7, live addendum 09:43Z: a poisoned changelog record --
        ``outer_stamp(expired) || inner_stamp(expired) || json``, the exact
        shape a pre-fix bug-5.6 backfill produced -- must self-heal at replay:
        ``_normalize_replay_value`` strips the OUTER stamp on ingest and stores
        the value SINGLE-wrapped (the inner stamp/payload verbatim), so a
        subsequent read judges the record's real (inner) expiry instead of
        handing ``inner_stamp||json`` to the value deserializer (the
        sc-74843 crash, reproduced live when the lever replayed such a record
        into a fresh store: "the first message processed ... read one and
        crashed with the same StateSerializationError").
        """
        inner_stamp = PAST_STAMP_MS - HOUR_MS
        outer_stamp = PAST_STAMP_MS
        inner_value = encode_ttl_value(inner_stamp, json_dumps("poisoned-payload"))
        double_wrapped = encode_ttl_value(outer_stamp, inner_value)

        partition = store_partition_factory(
            name="fresh-double-wrap-replay",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert partition.uses_ttl_stamps is False

            with caplog.at_level(logging.INFO):
                partition.recover_from_changelog_message(
                    key=_raw_key("poisoned"),
                    value=double_wrapped,
                    cf_name="default",
                    offset=0,
                    ttl_stamped=True,
                )
                # A single header-true record flips a fresh partition into TTL
                # mode for the rest of recovery (flip-discovery); the
                # aggregate double-wrap WARNING is emitted here.
                partition.complete_recovery()

            assert partition.uses_ttl_stamps is True

            raw_on_disk = partition.get_or_create_column_family("default").get(
                _raw_key("poisoned"), default=None
            )
            assert raw_on_disk == inner_value, (
                "the record must be stored SINGLE-wrapped after replay -- the "
                "outer stamp stripped, the inner stamp/payload untouched"
            )

            messages = _messages(caplog)
            assert any("DOUBLE-STAMPED record" in m for m in messages), messages

            # Simulate a live event advancing the high-water past the
            # (already-expired) inner stamp, matching the production sequence
            # where the poisoned record was read well after ingestion.
            partition._high_water_ms = NOW_MS

            assert _read(partition, "poisoned") is None
        finally:
            partition.close()

    def test_double_wrapped_record_already_on_disk_self_heals_on_read_warn_once(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """Warm variant of the same repair: a double-wrapped value already
        sits on disk (as if written by a pre-fix bug-5.6 backfill) on an
        already-flipped store -- not landing via replay this session. The
        READ path (:meth:`RocksDBPartitionTransaction._decode_double_wrap`)
        must unwrap it on the fly, honour the inner expiry, and warn exactly
        ONCE per partition -- not once per read.
        """
        seed = store_partition_factory(
            name="warm-double-wrap-read",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        with seed.begin() as tx:
            tx.set(
                key="seed",
                value="v0",
                prefix=PREFIX,
                timestamp=NOW_MS,
                ttl=timedelta(days=365),
            )
        assert seed.uses_ttl_stamps is True
        seed.close()

        inner_stamp = PAST_STAMP_MS - HOUR_MS
        outer_stamp = PAST_STAMP_MS
        inner_value = encode_ttl_value(inner_stamp, json_dumps("poisoned-payload"))
        double_wrapped = encode_ttl_value(outer_stamp, inner_value)

        injector = store_partition_factory(
            name="warm-double-wrap-read",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        assert injector.uses_ttl_stamps is True
        batch = WriteBatch(raw_mode=True)
        default_handle = injector.get_column_family_handle("default")
        metadata_handle = injector.get_column_family_handle(METADATA_CF_NAME)
        batch.put(_raw_key("poisoned"), double_wrapped, default_handle)
        batch.put(TTL_HIGH_WATER_KEY, int_to_bytes(NOW_MS), metadata_handle)
        injector._write(batch)
        # Released before closing: a leaked CF handle keeps the RocksDB alive
        # across the reopen below and fails on the Windows LOCK file.
        del batch, default_handle, metadata_handle
        injector.close()

        reopened = store_partition_factory(
            name="warm-double-wrap-read",
            options=PLAIN_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is True
            raw_before = reopened.get_or_create_column_family("default").get(
                _raw_key("poisoned"), default=None
            )
            assert raw_before == double_wrapped  # untouched by open, still on disk

            with caplog.at_level(logging.WARNING):
                first = _read(reopened, "poisoned")
                second = _read(reopened, "poisoned")
            assert first is None
            assert second is None

            warnings_seen = [
                m for m in _messages(caplog) if "Double-stamped TTL value" in m
            ]
            assert len(warnings_seen) == 1, warnings_seen
        finally:
            reopened.close()
