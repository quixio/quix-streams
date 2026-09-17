"""
Red-first reproduction tests for the round-4 external review of PR 1110
(`feature/sc-72821` - buffer late-config records instead of blocking the
partition, `6632b46c`).

Each test attempts to reproduce one specific blocker from the review. Per the
brief: the existing 29-test suite (`test_lookup_buffer.py`) drives the
operator through the prefix-scoped in-process update cache, where writes
never reach RocksDB, so it cannot see a bug that only shows up on a real
flush. Every test in this file therefore forces a **real flush to RocksDB**
(via the transaction's `__exit__`, which calls `prepare()` + `flush()` -
`quixstreams/state/base/transaction.py:722-728`) and reads back through a
**fresh** transaction or a different prefix.

Traces to `dev-planning/lookup-late-config-buffering/spec.md` (revision 8):

- Blocker 1 (`TestSiblingKeyPrefixCollision*`): §7.5 `delete_interval`, §12
  "Also required before merge" - "a neighbouring prefix sharing a byte
  sequence ... is not touched".
- Blocker 2 (`TestBytesValuedFieldCrashesTheApplication`): §9.3
  construction-time validation - a field that cannot survive buffering must
  be rejected at build time, not crash the record path.
- Blocker 3 (`TestPendingIndexScaling`): §10 "Bounds and observability" - the
  pending index's cost model, `max_buffered_per_key` bounds one key, not the
  number of keys.
- Lower priority (`TestChangelogNotOptionalClaimIsNotEnforced`,
  `TestBookkeepingLogsGrowUnbounded`): §5 durability and §10 observability.

One later section traces elsewhere: `TestEmptyPrefixScansStayOutOfTheIndexNamespaces`
and `TestEmptyMessageKeyIsBufferedAndSettledLikeAnyOther` attempt the leak
described in `dev-planning/lookup-deadline-tick/open-points.md` §1 - the empty
message key and the two scans bounded at `MAX_RECEIVE_MS`.
"""

import logging
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Union

import pytest

from quixstreams.dataframe.joins.lookups.base import BaseLookup
from quixstreams.dataframe.joins.lookups.buffer_bookkeeping import BufferBookkeeping
from quixstreams.dataframe.joins.lookups.buffer_envelope import encode_envelope
from quixstreams.dataframe.joins.lookups.buffer_state import (
    INDEX_PREFIX,
    QUEUE_PREFIX,
    PendingIndex,
    encode_prefix,
    prefix_for_key,
)
from quixstreams.dataframe.joins.lookups.buffer_sweep import (
    MAX_RECEIVE_MS,
    SWEEP_BUDGET,
    BufferSweeper,
)
from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.rocksdb.timestamped import (
    TimestampedPartitionTransaction,
    TimestampedStore,
)
from quixstreams.state.serialization import int_to_bytes
from quixstreams.utils.json import dumps as orjson_dumps
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    GRACE_MS,
    STORE_NAME,
    UNRESOLVED,
    ConfigField,
    FakeLookup,
    make_buffer,
    make_fields,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    buffered as buffered,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    clock as clock,
)

# ---------------------------------------------------------------------------
# Blocker 1: sibling-key prefix collision
# ---------------------------------------------------------------------------
#
# The store's on-disk key format is `prefix + SEPARATOR + <encoded
# timestamp/counter>` with `SEPARATOR = b"|"` (`timestamped.py:_serialize_key`).
# The review claims a key that is another key's prefix followed by `|` - e.g.
# `dev1|sensor2` is such a "sibling" of `dev1` - sorts inside the shorter key's
# `get_interval`/`delete_interval` scan range, so releasing or timing out `dev1`
# would also read and delete `dev1|sensor2`'s buffered records.
#
# The claim is correct, and it is a defect of the shared `TimestampedStore`:
# `join_asof` and `join_interval` use the same transaction class
# (`dataframe/joins/base.py:99-101`). Fixing it at the store changes the on-disk
# key layout for those released features and needs a migration, so it is tracked
# as issue #1148 and left alone by this PR. The two state-level tests below
# therefore assert something the store does not do yet and are `xfail(strict)`:
# they will start failing loudly the day #1148 lands, which is exactly when they
# should be un-xfailed.
#
# The buffer defends itself in the meantime: `prefix_for_key()`
# (`buffer_state.py`) escapes the SEPARATOR out of the prefix before it reaches
# the store, so the operator-level test below passes for real.


@pytest.fixture
def store_type():
    # Overrides the module-scoped default (`RocksDBStore`) from
    # `tests/test_quixstreams/test_state/fixtures.py` for every test in this
    # file, the same way `test_timestamped.py` does.
    return TimestampedStore


@pytest.fixture
def transaction(store: TimestampedStore):
    @contextmanager
    def _transaction(
        grace_ms: Union[int, timedelta] = timedelta(days=7),
        keep_duplicates: bool = True,
    ):
        store._grace_ms = ensure_milliseconds(grace_ms)
        store._keep_duplicates = keep_duplicates
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            yield tx

    return _transaction


class TestSiblingKeyPrefixCollisionStateLevel:
    """
    Blocker 1, direct repro against `TimestampedPartitionTransaction`.

    Both tests pass raw sibling prefixes to the store, so they assert the
    *store* is safe against the collision. It is not, and making it so is
    issue #1148 - see the note above this class.
    """

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "TimestampedPartitionTransaction does not isolate a prefix that is "
            "another prefix plus the key SEPARATOR; fixing it changes the "
            "on-disk layout for join_asof/join_interval and needs a migration "
            "(issue #1148). The lookup buffer escapes its own prefixes instead."
        ),
    )
    def test_get_interval_does_not_return_a_sibling_keys_records(
        self, transaction: Any
    ) -> None:
        """
        Write under `b"dev1"` and `b"dev1|sensor2"`, flush to real RocksDB,
        and read `b"dev1"` back from a fresh transaction. If the review's
        claim reproduces, `sensor2-val` leaks into this list.
        """
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=10, value="dev1-val", prefix=b"dev1")
            tx.set_for_timestamp(
                timestamp=10, value="sensor2-val", prefix=b"dev1|sensor2"
            )

        with transaction() as tx:
            got = tx.get_interval(start=0, end=MAX_RECEIVE_MS, prefix=b"dev1")

        assert got == [
            "dev1-val"
        ], f"Sibling key 'dev1|sensor2' leaked into 'dev1' scan range: {got}"

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "TimestampedPartitionTransaction does not isolate a prefix that is "
            "another prefix plus the key SEPARATOR; fixing it changes the "
            "on-disk layout for join_asof/join_interval and needs a migration "
            "(issue #1148). The lookup buffer escapes its own prefixes instead."
        ),
    )
    def test_delete_interval_does_not_delete_a_sibling_keys_records(
        self, transaction: Any
    ) -> None:
        """The destructive half: deleting `dev1`'s range must not touch `dev1|sensor2`."""
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=10, value="dev1-val", prefix=b"dev1")
            tx.set_for_timestamp(
                timestamp=10, value="sensor2-val", prefix=b"dev1|sensor2"
            )

        with transaction() as tx:
            tx.delete_interval(start=0, end=MAX_RECEIVE_MS, prefix=b"dev1")

        with transaction() as tx:
            survivor = tx.get_interval(
                start=0, end=MAX_RECEIVE_MS, prefix=b"dev1|sensor2"
            )

        assert survivor == [
            "sensor2-val"
        ], f"'dev1'.delete_interval() destroyed 'dev1|sensor2' too: {survivor}"


def _force_real_flush(driver: Any) -> None:
    """
    Flush the driver's live transaction to real RocksDB and evict it from the
    checkpoint's memo (`checkpoint.py:190-213`), so the *next* record the
    driver sends opens a genuinely fresh `PartitionTransaction` that reads
    from disk instead of the in-process update cache.

    Required here because the update cache groups writes by **exact** prefix
    (`get_updates().get(prefix, {})` - a dict lookup, `transaction.py:915`),
    which cannot exhibit a cross-prefix byte collision by construction. Only
    the on-disk RocksDB scan (`iter_items` with `lower_bound`/`upper_bound`,
    a real byte-range iteration) can - which is exactly why the existing
    29-test in-process suite never saw Blocker 1: it never forces this flush.
    """
    transaction = driver.transaction()
    transaction.prepare()
    transaction.flush()
    checkpoint = driver.sdf.processing_context.checkpoint
    checkpoint._store_transactions.pop((driver.sdf.stream_id, 0, STORE_NAME), None)


class TestSiblingKeyPrefixCollisionOperatorLevel:
    """Blocker 1, same claim through the real `BufferOperator` record path."""

    def test_releasing_dev1_does_not_leak_or_delete_the_sibling_keys_record(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = buffered(buffer=make_buffer())

        # Neither key has a configuration yet - both buffer.
        assert driver.send("dev1", timestamp=1) == []
        clock.advance_ms(10)
        assert driver.send("dev1|sensor2", timestamp=2) == []

        # Force both writes to real RocksDB and drop the memoised transaction,
        # so the release below reads from disk - see `_force_real_flush`.
        _force_real_flush(driver)

        # Configure and release only "dev1".
        driver.lookup.configs["dev1"] = {"threshold": 42, "region": "eu"}
        clock.advance_ms(10)
        result = driver.send("dev1", timestamp=3)

        emitted_keys = [key for _, key, _, _ in result]
        assert emitted_keys == [
            "dev1",
            "dev1",
        ], f"Releasing 'dev1' emitted under unexpected keys: {emitted_keys}"
        assert (
            driver.stored("dev1|sensor2") != []
        ), "'dev1's release deleted 'dev1|sensor2's still-buffered record"


# ---------------------------------------------------------------------------
# Blocker 2: a `bytes`-valued field crashes buffering instead of being
# rejected at build time
# ---------------------------------------------------------------------------
#
# `validate_fields` only rejects fields whose `default` is `RAISE_ON_MISSING`;
# it has no notion of `BytesField`. The envelope lifts `bytes` out of band, so
# a `bytes`-valued field round-trips rather than failing the store write.


class BytesFieldLookup(BaseLookup):
    """
    Mimics the shape `QuixConfigurationService.join()` produces
    (`lookup.py:466-478`) when one configuration `type` resolves and another
    does not: a `BytesField` (`models.py:131-142`, real binary content, e.g.
    `lookup.bytes_field(...)`) resolves normally while a JSON-typed field of a
    different `type` has no configuration at all, leaving the overall record
    unresolved.
    """

    def __init__(self, bytes_field_name: str, json_field_name: str) -> None:
        self._bytes_field_name = bytes_field_name
        self._json_field_name = json_field_name

    def join(self, fields, on, value, key, timestamp, headers) -> None:
        # The "cert" type always resolves, with real binary content.
        value[self._bytes_field_name] = b"-----BEGIN CERTIFICATE-----binary"
        # The "device" type never resolves.
        value[self._json_field_name] = None
        value[UNRESOLVED] = ["device"]


class TestBytesValuedFieldSurvivesBuffering:
    def test_validate_fields_does_not_reject_a_bytes_defaulted_field(self) -> None:
        """
        Sanity check for the claim: `join_lookup(..., buffer=...)` calls
        `buffer.validate_fields(fields)` at build time
        (`dataframe.py:1988-1993`). A field whose `default` is real `bytes`
        (the shape `lookup.bytes_field(..., default=b"...")` produces) is not
        `RAISE_ON_MISSING`, so it must sail through today.
        """
        buffer = make_buffer()
        fields = {
            "cert": ConfigField(type="cert", default=b"fallback-bytes", source="cert"),
            **make_fields(),
        }

        buffer.validate_fields(fields)  # must not raise - that is the bug

    def test_a_bytes_valued_field_is_buffered_not_refused(
        self,
        create_sdf: Any,
        assign_partition: Any,
        publish: Any,
        topic_manager_topic_factory: Any,
    ) -> None:
        """
        The envelope lifts `bytes` out of band, so a record carrying a
        resolved bytes field and an unresolved one is withheld like any other.
        """
        fields = {
            "cert": ConfigField(type="cert", default=b"fallback-bytes", source="cert"),
            "threshold": ConfigField(type="device", default=None, source="threshold"),
        }
        buffer = make_buffer()

        topic = topic_manager_topic_factory()
        sdf = create_sdf(topic)
        lookup = BytesFieldLookup(bytes_field_name="cert", json_field_name="threshold")
        sdf = sdf.join_lookup(lookup, fields, buffer=buffer)
        assign_partition(sdf)

        assert (
            publish(sdf, topic, {}, "D", 100, None) == []
        ), "the record is unresolved, so it is withheld rather than emitted"


# ---------------------------------------------------------------------------
# Blocker 3: the pending index's per-record cost and changelog message size
# used to scale with the total number of distinct waiting keys
# ---------------------------------------------------------------------------
#
# Round 4 caught this as "the pending index is one JSON blob per partition":
# `PendingIndex` was parsed whole on first read per transaction and
# re-serialized whole on `flush()`, produced as a single changelog message per
# checkpoint - measured at 43 bytes/key, crossing a default broker
# `message.max.bytes` (1 MB) at ~24,385 distinct waiting keys on one partition.
#
# ArchDev's fix (architecture.md §3.3) replaced the single blob with two
# namespaces: a per-prefix marker (point get/set/delete) and a deadline queue
# read only as far as one sweep pass can settle (`PendingIndex.due()`). Round 5
# re-derived the round-4 measurement against the current code and confirmed
# it is now MISLEADING, not merely stale: `PendingIndex.entries()` still
# materialises the whole index and still costs ~43 bytes/key when serialized
# (`test_pending_index_size_grows_linearly_with_distinct_keys` below still
# passes on that basis) - but `entries()` is explicitly documented and used as
# an *introspection-only* view (buffer_state.py:339-346), never on the record
# path and never what one checkpoint produces as a changelog message. Measuring
# `entries()` size therefore no longer measures either of the two properties
# blocker 3 was actually about, so the original assertion
# (`test_pending_index_crosses_1mb_within_a_realistic_key_count`) is retired
# and replaced with direct measurements of the two properties that do matter,
# taken independently rather than accepted on ArchDev's description:
#
# 1. `PendingIndex.due()` (the sweep's range read, the only per-record query
#    whose cost could plausibly grow with key count), measured against the
#    number of prefixes that are actually **overdue**.
# 2. The size of each *individual* update-cache entry `PendingIndex.flush()`
#    produces - i.e. exactly what `TimestampedPartitionTransaction._prepare()`
#    (`state/base/transaction.py:657-664`) turns into one changelog message
#    each - against a 200x range of distinct waiting keys.
#
# Round 5's version of (1) varied the *not-yet-due* population instead, and a
# second external review was right that it proved nothing: `due()` excludes
# those by its `end = cutoff + 1` bound, so the measurement was flat before it
# was taken. The cost was on the other axis and it was real - 0.9 ms per record
# at 200 overdue prefixes, 26.9 ms at 2,000, ~37 rec/s on one partition - which
# is the shape of a cold start, the case this feature exists to serve. That
# measurement is kept in `dev-planning/lookup-deadline-tick/bugs-round3.md`;
# what stands here is the property that replaces it, asserted as a bound on
# what a pass reads rather than as a wall-clock ratio.
#
# The original round-4 43-bytes/key, ~24,385-key-crossover measurement is
# preserved verbatim in `dev-planning/lookup-late-config-buffering/bugs-round5.md`;
# it is real and still reproducible, it is simply no longer a description of
# anything on the record path or the changelog.


class TestPendingIndexScaling:
    BASE_MS = 1_700_000_000_000

    @staticmethod
    def _index_size(transaction: TimestampedPartitionTransaction, count: int) -> int:
        index = PendingIndex(transaction)
        for i in range(count):
            prefix = f"device-{i:08d}".encode()
            index.ensure(prefix, f"device-{i:08d}", receive_ms=1_700_000_000_000 + i)
        index.flush()
        return len(orjson_dumps(index.entries()))

    def test_pending_index_size_grows_linearly_with_distinct_keys(
        self, transaction: Any
    ) -> None:
        """
        `entries()` (introspection only, not on the record path or the
        changelog - buffer_state.py:339-346) still materialises the whole
        index, so this baseline from round 4 is still accurate on its own
        terms. It is kept to make plain that the fix is architectural (moving
        the hot paths off this view), not a change to what `entries()` costs.
        """
        with transaction() as tx:
            small = self._index_size(tx, 100)
        with transaction() as tx:
            large = self._index_size(tx, 1_000)

        ratio = large / small
        assert 9.5 <= ratio <= 10.5, (
            f"Expected near-exactly-linear growth for 10x the keys, got "
            f"{ratio:.2f}x ({small} -> {large} bytes)."
        )

    @classmethod
    def _withhold_overdue(
        cls, transaction: TimestampedPartitionTransaction, tag: str, count: int
    ) -> None:
        """
        Withhold one record for each of `count` distinct prefixes, all arriving
        in the same millisecond, and index them. Every one of them is overdue
        for a sweep at `cutoff = BASE_MS`.
        """
        index = PendingIndex(transaction)
        for i in range(count):
            key = f"{tag}-{i:08d}"
            prefix = key.encode()
            transaction.set_for_timestamp(
                timestamp=cls.BASE_MS,
                value=encode_envelope(
                    value={"v": i},
                    timestamp=cls.BASE_MS,
                    receive_ms=cls.BASE_MS,
                    headers=None,
                ),
                prefix=prefix,
            )
            index.ensure(prefix, key, receive_ms=cls.BASE_MS)
        index.flush()

    @staticmethod
    def _count_queue_reads(monkeypatch: Any) -> list[int]:
        """
        Record the size of every range read the buffer makes over the deadline
        queue, whatever arguments it passes.
        """
        reads: list[int] = []
        original = TimestampedPartitionTransaction.get_interval

        def counting(self, start, end, prefix, *args, **kwargs):
            result = original(self, start, end, prefix, *args, **kwargs)
            if prefix == QUEUE_PREFIX:
                reads.append(len(result))
            return result

        monkeypatch.setattr(TimestampedPartitionTransaction, "get_interval", counting)
        return reads

    def test_one_sweep_pass_reads_a_bounded_slice_however_many_are_overdue(
        self, transaction: Any, monkeypatch: Any
    ) -> None:
        """
        Spec §10 "Bounds and observability": `max_buffered_per_key` bounds one
        key's queue depth, and the review's concern was that *nothing* bounded
        the per-record cost of key cardinality.

        `PendingIndex.due()` is the query every record's sweep step makes, and
        a pass settles at most `SWEEP_BUDGET` prefixes however many are due. So
        the entries it reads must be bounded by that budget - not by the
        backlog, which on a cold start is every key on the partition at once.

        Asserted as a bound on what is read, not as a wall-clock ratio: the
        cost is linear in the entries returned (each is a RocksDB iteration
        step plus one orjson parse), and a count is not noisy.
        """
        overdue = 0
        results = {}
        for added in (10, 190, 1_800):
            with transaction() as tx:
                self._withhold_overdue(tx, f"overdue-{added}", added)
            overdue += added

            reads = self._count_queue_reads(monkeypatch)
            sweeper = BufferSweeper(
                emit_on_timeout=True, bookkeeping=BufferBookkeeping()
            )
            with transaction() as tx:
                index = PendingIndex(tx)
                result = sweeper.sweep(tx, index, 0, self.BASE_MS, skip=None)
                index.flush()
            monkeypatch.undo()

            assert result.swept == SWEEP_BUDGET, (
                f"test setup assumption failed: a pass over {overdue} overdue "
                f"prefixes should settle a full budget, settled {result.swept}"
            )
            assert reads, "test setup assumption failed: no queue read observed"
            results[overdue] = max(reads)

        for population, entries_read in results.items():
            assert entries_read <= SWEEP_BUDGET + 1, (
                f"One sweep pass read {entries_read} deadline-queue entries "
                f"with {population} prefixes overdue, but can settle at most "
                f"{SWEEP_BUDGET} of them (+1 for the record path's own prefix, "
                f"which it skips). The per-record cost of the sweep therefore "
                f"grows with the backlog: {results}."
            )

    def test_earliest_reads_one_queue_entry_not_the_whole_queue(
        self, transaction: Any, monkeypatch: Any
    ) -> None:
        """
        `PendingIndex.earliest()` is the deadline tick's "when do I next need to
        look at this partition" read (`buffer_tick.py:343`). The queue is
        deadline-ordered, so the answer is its first entry; reading the rest is
        pure waste, paid once per partition per settled tick.
        """
        with transaction() as tx:
            self._withhold_overdue(tx, "waiting", 2_000)

        reads = self._count_queue_reads(monkeypatch)
        with transaction() as tx:
            earliest = PendingIndex(tx).earliest()
        monkeypatch.undo()

        assert earliest == self.BASE_MS
        assert reads == [1], (
            f"PendingIndex.earliest() read {reads} deadline-queue entries to "
            f"return the first one, with 2,000 prefixes waiting."
        )

    def test_changelog_message_size_does_not_scale_with_waiting_key_count(
        self, transaction: Any
    ) -> None:
        """
        Spec §10 "Bounds and observability", refined by the round-4 finding:
        the original defect was one changelog message per checkpoint whose
        *size* grew with distinct waiting keys, risking a broker's
        `message.max.bytes`. `PendingIndex.flush()` now writes one small
        update-cache entry per changed prefix
        (`TimestampedPartitionTransaction._prepare()`,
        `state/base/transaction.py:657-664`, turns each into its own
        `changelog_producer.produce()` call) - so no single produced message
        should grow with the total number of waiting keys.
        """
        results = {}
        for n in (100, 1_000, 20_000):
            with transaction() as tx:
                index = PendingIndex(tx)
                for i in range(n):
                    prefix = f"device-{i:08d}".encode()
                    index.ensure(
                        prefix, f"device-{i:08d}", receive_ms=1_700_000_000_000 + i
                    )
                index.flush()

                sizes = [
                    len(value)
                    for cf_name in tx._update_cache.get_column_families()
                    for prefix_bytes, kv in tx._update_cache.get_updates(
                        cf_name=cf_name
                    ).items()
                    if prefix_bytes in (INDEX_PREFIX, QUEUE_PREFIX)
                    for value in kv.values()
                ]
            results[n] = max(sizes)

        # The old one-blob design's single message grew ~43 bytes per key: at
        # 20,000 keys that would be a message roughly 200x the size of the one
        # at 100 keys. The fixed design's messages are constant-sized (one
        # marker or queue entry each), so this ratio should stay flat.
        ratio = results[20_000] / results[100]
        assert ratio < 3, (
            f"A single changelog message's size still scales with total "
            f"waiting key count: {ratio:.2f}x larger at 20,000 keys than at "
            f"100 ({results[100]} -> {results[20_000]} bytes)."
        )


# ---------------------------------------------------------------------------
# Lower priority: `use_changelog_topics=False` silently drops the
# "not optional" durability guarantee
# ---------------------------------------------------------------------------


class TestChangelogNotOptionalClaimIsNotEnforced:
    """
    `LookupBuffer.register_store`'s docstring (buffer.py:221-229) states
    changelog backing "is not optional" for durability. Nothing in
    `register_store` or `join_lookup` verifies one will actually be produced.
    `StateStoreManager._setup_changelogs` (`state/manager.py:157-168`)
    silently returns `None` whenever there is no `RecoveryManager` - the
    condition `Application(use_changelog_topics=False)` produces - and
    `register_timestamped_store` accepts that with no error and no log line.
    """

    def test_register_store_succeeds_silently_with_no_changelog_producer(
        self,
        topic_manager_topic_factory: Any,
        dataframe_factory: Any,
        state_manager: Any,
        caplog: Any,
    ) -> None:
        # The shared `state_manager` fixture is built via
        # `state_manager_factory()` with `recovery_manager=None`
        # (`tests/test_quixstreams/fixtures.py:349-365`) - the same condition
        # `use_changelog_topics=False` produces at the `Application` level
        # (`StateStoreManager.using_changelogs`, `state/manager.py:109-116`).
        assert state_manager.using_changelogs is False

        sdf = dataframe_factory(
            topic_manager_topic_factory(), state_manager=state_manager
        )
        buffer = make_buffer()

        with caplog.at_level(logging.WARNING):
            sdf.join_lookup(FakeLookup(), make_fields(), buffer=buffer)

        store = state_manager.get_store(stream_id=sdf.stream_id, store_name=STORE_NAME)
        assert (
            store._changelog_producer_factory is None
        ), "test setup assumption failed: expected no changelog producer"

        durability_warnings = [
            record
            for record in caplog.records
            if "changelog" in record.message.lower()
            or "durab" in record.message.lower()
        ]
        assert durability_warnings, (
            "LookupBuffer's docstring claims changelog backing 'is not "
            "optional' for durability, but register_store() neither raises "
            "nor logs anything when no changelog producer will actually be "
            "created for this store. A buffered record would be silently "
            "lost on the next restart or rebalance, exactly like the "
            "in-memory buffer this design was built to avoid (spec §5) - "
            "with no trace in the logs."
        )


# ---------------------------------------------------------------------------
# Lower priority: `_drop_log` / `_overflow_log` grow one entry per distinct
# key forever, never evicted
# ---------------------------------------------------------------------------


class TestBookkeepingLogsGrowUnbounded:
    """
    `BufferBookkeeping._drop_log` / `_overflow_log` (buffer_bookkeeping.py:
    38-39) gain one entry per distinct prefix that has ever dropped or
    overflowed. Nothing in the module ever pops an entry - not on release,
    not on sweep, not ever - so a long-running deployment that cycles through
    many distinct unconfigured keys over its lifetime (e.g. per-device IDs
    that come and go) grows these dicts without bound, for the life of the
    process.
    """

    def test_drop_log_does_not_grow_unboundedly_across_many_one_off_keys(
        self,
    ) -> None:
        bookkeeping = BufferBookkeeping()
        num_keys = 10_000

        # Each of these 10,000 distinct one-off keys logs its own immediate
        # first-drop warning (`_rate_limited` reports on the first event for
        # a prefix) - silence it, it is not what this test is about.
        logging.disable(logging.CRITICAL)
        try:
            for i in range(num_keys):
                prefix = f"device-{i}".encode()
                bookkeeping.log_dropped(prefix, f"device-{i}", dropped=1)
        finally:
            logging.disable(logging.NOTSET)

        assert len(bookkeeping._drop_log) < num_keys, (
            f"_drop_log holds {len(bookkeeping._drop_log)} entries after "
            f"{num_keys} distinct one-off keys that will never be seen "
            f"again. Nothing in BufferBookkeeping evicts a rate-limiter "
            f"entry, so this dict grows without bound for the life of the "
            f"process."
        )


# ---------------------------------------------------------------------------
# Open point 1: the empty message key and the scans bounded at MAX_RECEIVE_MS
# ---------------------------------------------------------------------------
#
# `prefix_for_key("")` returns `b""`, and
# `dev-planning/lookup-deadline-tick/open-points.md` §1 suspected that this
# leaves every range scan for that key unqualified: the base
# `PartitionTransaction._serialize_key` (`state/base/transaction.py:297-300`)
# drops the SEPARATOR when the prefix is empty, so the bounds would be bare
# big-endian timestamps. A scan bounded at `MAX_RECEIVE_MS` (`0x7f ff ...`)
# would then sort above the `__` (`0x5f 0x5f`) of `__lookup_buffer_index__` and
# `__lookup_buffer_queue__` and return the buffer's own index entries as though
# they were envelopes - `BufferSweeper._reindex` would evaluate
# `remaining[0][ENVELOPE_RECEIVED]`, a string subscript on a list.
#
# The buffer's store does not use that method. `TimestampedPartitionTransaction`
# overrides it with an unconditional `prefix + SEPARATOR + key`
# (`state/rocksdb/timestamped.py:263-264`), and the scan bounds are built by
# `append_integer` (`state/serialization.py:82-92`), which also appends the
# SEPARATOR unconditionally. The empty prefix's keys and both of its bounds
# therefore start with `0x7c`, strictly above every key in the two reserved
# namespaces, whatever the upper bound is.
#
# These tests are the arbiter of that reasoning rather than a decoration on it:
# each attempts the leak for real, from disk rather than from the exact-prefix
# update cache (see `_force_real_flush`), on each path that carries a
# `MAX_RECEIVE_MS` bound - `BufferSweeper._reindex` and
# `BufferOperator._release`.


class TestEmptyPrefixScansStayOutOfTheIndexNamespaces:
    """Open point 1, direct repro against `TimestampedPartitionTransaction`."""

    def test_the_timestamped_store_qualifies_the_empty_prefix(
        self, transaction: Any
    ) -> None:
        """
        The one line the whole property rests on. The day this fails - because
        the store's key layout changed, which is what issue #1148 proposes to do
        - the empty message key needs a non-empty prefix of its own before the
        buffer is safe again.
        """
        with transaction() as tx:
            assert tx._serialize_key(b"x", b"") == SEPARATOR + b"x"

    def test_a_max_bounded_scan_of_the_empty_prefix_returns_only_its_records(
        self, transaction: Any
    ) -> None:
        """
        Write one envelope under the empty prefix and one entry in each of the
        buffer's own namespaces, flush to real RocksDB, then scan the empty
        prefix from a fresh transaction with both bound shapes the buffer uses.
        If the open point's claim reproduces, the two lists come back as
        records.
        """
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=10, value={"envelope": True}, prefix=b"")
            # The shapes `PendingIndex.flush()` writes: one marker per prefix
            # and one deadline queue entry per prefix, both lists.
            tx.set(b"k", [10, "s"], prefix=INDEX_PREFIX)
            tx.set(int_to_bytes(10) + SEPARATOR, ["", 10], prefix=QUEUE_PREFIX)

        with transaction() as tx:
            everything = tx.get_interval(start=0, end=MAX_RECEIVE_MS, prefix=b"")
            # `BufferSweeper._reindex`'s exact shape: `start=cutoff + 1`, above
            # everything the sweep has just settled.
            past_the_record = tx.get_interval(start=11, end=MAX_RECEIVE_MS, prefix=b"")

        assert everything == [
            {"envelope": True}
        ], f"The empty prefix's own scan is wrong or leaks the index: {everything}"
        assert past_the_record == [], (
            f"A scan of the empty prefix bounded at MAX_RECEIVE_MS read the "
            f"buffer's own index namespaces back: {past_the_record}"
        )


class TestEmptyMessageKeyIsBufferedAndSettledLikeAnyOther:
    """Open point 1, the same claim through the real `BufferOperator`."""

    def test_a_sweep_settles_the_empty_key_without_reading_the_index_back(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        `BufferSweeper._reindex` is the `MAX_RECEIVE_MS`-bounded path a sweep
        reaches once it has emptied a prefix's timed-out range. Both keys are
        due here and both index namespaces are on disk, so a scan the empty
        prefix failed to qualify would hand `_reindex` an index entry as its
        first "envelope".
        """
        driver = buffered(buffer=make_buffer())

        assert driver.send("", timestamp=1) == []
        assert driver.send("other", timestamp=2) == []
        # Both envelopes and both index namespaces to real RocksDB - only an
        # on-disk scan can exhibit a byte-range collision, see
        # `_force_real_flush`.
        _force_real_flush(driver)

        clock.advance_ms(GRACE_MS + 1)
        # An unrelated record drives the sweep; its own prefix is the one the
        # record path skips, so both due prefixes are settled by the sweeper.
        emitted = driver.send("later", timestamp=3)

        keys = [key for _, key, _, _ in emitted]
        assert sorted(keys) == ["", "other"], f"Swept under unexpected keys: {keys}"
        value, _, timestamp, _ = emitted[keys.index("")]
        assert timestamp == 1, "the empty key's record keeps its own event timestamp"
        assert value["region"] == "unknown", "emitted with its declared defaults"
        assert driver.stored("") == [], "the empty key's buffer was not drained"
        assert driver.pending_keys() == [encode_prefix(prefix_for_key("later"))], (
            "after the sweep only the record that has just been withheld should "
            "still be indexed"
        )

    def test_a_release_for_the_empty_key_does_not_destroy_the_index(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        `BufferOperator._release` carries the other `MAX_RECEIVE_MS` bound, and
        it is the destructive one: it deletes everything it read. An unqualified
        scan would emit the index entries as records and then delete both
        namespaces, taking every other waiting key's marker with them.
        """
        driver = buffered(buffer=make_buffer())

        assert driver.send("", timestamp=1) == []
        assert driver.send("other", timestamp=2) == []
        _force_real_flush(driver)

        # Configure only the empty key, then release it well inside its grace
        # window so the survivor read is the one doing the work.
        driver.lookup.configs[""] = {"threshold": 42, "region": "eu"}
        clock.advance_ms(10)
        emitted = driver.send("", timestamp=3)

        keys = [key for _, key, _, _ in emitted]
        assert keys == ["", ""], f"Released under unexpected keys: {keys}"
        thresholds = [value["threshold"] for value, _, _, _ in emitted]
        assert thresholds == [42, 42], "both records are enriched by the release"
        assert (
            driver.stored("other") != []
        ), "the empty key's release deleted 'other's still-buffered record"
        assert driver.pending_keys() == [encode_prefix(prefix_for_key("other"))], (
            "the empty key's release destroyed another key's index entry, so "
            "'other' would never be swept again"
        )
