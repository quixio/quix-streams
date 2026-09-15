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
"""

import logging
import time
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Union

import pytest

from quixstreams.dataframe.joins.lookups.base import BaseLookup
from quixstreams.dataframe.joins.lookups.buffer_bookkeeping import BufferBookkeeping
from quixstreams.dataframe.joins.lookups.buffer_state import (
    INDEX_PREFIX,
    QUEUE_PREFIX,
    PendingIndex,
)
from quixstreams.dataframe.joins.lookups.buffer_sweep import MAX_RECEIVE_MS
from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.state.rocksdb.timestamped import (
    TimestampedPartitionTransaction,
    TimestampedStore,
)
from quixstreams.utils.json import dumps as orjson_dumps
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
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
# `validate_fields` (buffer.py:191-219) only rejects fields whose `default`
# is `RAISE_ON_MISSING`; it has no notion of `BytesField`. The
# `isinstance(value, bytes)` guard in `_buffer()` (buffer_operator.py:385-392)
# tests the whole record *value*, which is always a `dict` by the time
# `_buffer` sees it - `lookup.join()` already wrote into it in-place a few
# lines earlier - so it can never fire for a `bytes`-valued *field* inside
# that dict. `orjson` cannot serialize `bytes`
# (`quixstreams/state/rocksdb/options.py:10`), so the first record carrying a
# resolved `BytesField` alongside an unresolved field of another type should
# crash `_buffer()` -> `transaction.set_for_timestamp()` -> `set()`
# (`state/base/transaction.py:435-462`, which serializes synchronously and
# raises immediately, before the offset is committed).


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


class TestBytesValuedFieldCrashesTheApplication:
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

    def test_bytes_field_crashes_buffering_instead_of_being_rejected_at_build(
        self,
        create_sdf: Any,
        assign_partition: Any,
        publish: Any,
        topic_manager_topic_factory: Any,
    ) -> None:
        """
        `join_lookup(..., buffer=...)` accepts this field mapping (previous
        test), so the failure - if any - must surface on the record path
        instead, as a crash rather than a clean `ValueError` at build time.
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

        # Expected (per the review): raises before the offset would be
        # committed, crash-looping the application on every unresolvable
        # record that also carries a resolved bytes field.
        publish(sdf, topic, {}, "D", 100, None)


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
# scanned only up to what is actually overdue (`PendingIndex.due()`). Round 5
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
#    whose cost could plausibly grow with key count) timed with a fixed
#    handful of genuinely overdue prefixes against a 500x range of coexisting
#    *not-yet-due* prefixes on the same partition.
# 2. The size of each *individual* update-cache entry `PendingIndex.flush()`
#    produces - i.e. exactly what `TimestampedPartitionTransaction._prepare()`
#    (`state/base/transaction.py:657-664`) turns into one changelog message
#    each - against a 200x range of distinct waiting keys.
#
# The original 43-bytes/key, ~24,385-key-crossover measurement is preserved
# verbatim in `dev-planning/lookup-late-config-buffering/bugs-round5.md` for
# the record; it is real and still reproducible, it is simply no longer a
# description of anything on the record path or the changelog.


class TestPendingIndexScaling:
    ONE_MB = 1_048_576

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

    def test_due_lookup_cost_does_not_scale_with_not_yet_due_key_count(
        self, transaction: Any
    ) -> None:
        """
        Spec §10 "Bounds and observability": `max_buffered_per_key` bounds one
        key's queue depth, and the review's concern was that *nothing* bounded
        the per-record cost of key cardinality. `PendingIndex.due()` is the
        query every record's sweep step makes; it must cost what is overdue,
        not what is merely waiting.
        """
        base_ms = 1_700_000_000_000
        results = {}
        for n_not_due in (100, 5_000, 50_000):
            with transaction() as tx:
                index = PendingIndex(tx)
                for i in range(n_not_due):
                    prefix = f"waiting-{i:08d}".encode()
                    index.ensure(
                        prefix, f"waiting-{i:08d}", receive_ms=base_ms + 10_000_000
                    )
                for i in range(5):
                    prefix = f"overdue-{i:08d}".encode()
                    index.ensure(
                        prefix, f"overdue-{i:08d}", receive_ms=base_ms - 10_000_000
                    )
                index.flush()

            best = float("inf")
            for _ in range(10):
                with transaction() as tx:
                    index = PendingIndex(tx)
                    started = time.perf_counter()
                    due = index.due(base_ms)
                    elapsed = time.perf_counter() - started
                best = min(best, elapsed)
            assert len(due) == 5, "test setup assumption failed: expected 5 overdue"
            results[n_not_due] = best

        ratio = results[50_000] / results[100]
        # A range read bounded to what is overdue should not measurably slow
        # down as the coexisting not-due population grows 500x. This is a
        # generous ratio precisely because wall-clock timing is noisy; the
        # property under test is "does not scale with N", not a tight bound.
        assert ratio < 20, (
            f"PendingIndex.due() appears to scale with total waiting key "
            f"count, not overdue count: {ratio:.2f}x slower at 50,000 "
            f"not-yet-due keys than at 100 ({results[100] * 1000:.3f}ms -> "
            f"{results[50_000] * 1000:.3f}ms)."
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
