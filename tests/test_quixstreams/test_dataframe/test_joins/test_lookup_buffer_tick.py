"""
In-process tests for the lookup buffer's deadline tick.

Spec: `dev-planning/lookup-deadline-tick/spec.md`, tests K1-K6, K11 and K12.
K7 and K10 live in `test_lookup_buffer_tick_machinery.py`; K8 and K9 need a real
broker and live in `test_lookup_buffer_tick_broker.py`.

The property under test throughout is the one the change exists for: **every
behaviour `LookupBuffer` documents holds with zero input messages**. So every
test here buffers records through the record path and then drives
`DataFrameRegistry.run_periodic_tasks()` - the exact call the Application's run
loop makes on every iteration - without sending anything else.

Wall-clock time is faked in both modules that read it: `buffer_operator`, which
stamps arrival times, and `buffer_tick`, which decides what is due. Freezing
`monotonic` along with it also freezes the tick's wall-clock budget, which is
what lets these tests treat one tick as "settle everything due". The budget
itself is tested against the real monotonic clock in
`test_lookup_buffer_tick_machinery.py`.
"""

import dataclasses
import logging
import time
from typing import Any, Optional

import pytest

from quixstreams.context import copy_context, message_context, set_message_context
from quixstreams.dataframe.joins.lookups import (
    LookupBuffer,
    buffer_operator,
    buffer_tick,
)
from quixstreams.dataframe.joins.lookups.buffer_state import (
    MAX_RECEIVE_MS,
    PendingIndex,
    prefix_for_key,
)
from quixstreams.dataframe.registry import DataFrameRegistry
from quixstreams.models.messagecontext import MessageContext
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    GRACE_MS,
    STORE_NAME,
    FakeClock,
    FakeLookup,
    is_resolved,
    make_buffer,
    make_fields,
)


@dataclasses.dataclass
class TickDriver:
    """
    One buffered `StreamingDataFrame`, composed once so the tick's downstream
    executor stays bound across sends and ticks.

    Composing once is the point: `sdf.test()` re-composes per call and would
    rebind the operator to a fresh collector each time, which is exactly what a
    tick must not depend on.
    """

    sdf: Any
    topic: Any
    lookup: FakeLookup
    registry: DataFrameRegistry
    state_manager: Any
    executor: Any
    emitted: list
    contexts: list

    def send(
        self,
        key,
        timestamp: int = 0,
        value: Optional[dict] = None,
        headers=None,
        partition: int = 0,
        offset: int = 0,
    ) -> list:
        """Run one record through the operator and return what it emitted."""
        before = len(self.emitted)
        context = copy_context()
        context.run(
            set_message_context,
            MessageContext(
                topic=self.topic.name,
                partition=partition,
                offset=offset,
                size=0,
            ),
        )
        context.run(
            self.executor, dict(value) if value else {}, key, timestamp, headers
        )
        return self.emitted[before:]

    def tick(self) -> list:
        """Run one Application-loop tick and return what it emitted."""
        before = len(self.emitted)
        self.registry.run_periodic_tasks()
        return self.emitted[before:]

    def transaction(self, partition: int = 0):
        return self.sdf.processing_context.checkpoint.get_store_transaction(
            stream_id=self.sdf.stream_id,
            partition=partition,
            store_name=STORE_NAME,
        )

    def store(self):
        return self.state_manager.get_store(
            stream_id=self.sdf.stream_id, store_name=STORE_NAME
        )

    def stored(self, key, partition: int = 0) -> list:
        return self.transaction(partition).get_interval(
            start=0, end=MAX_RECEIVE_MS, prefix=prefix_for_key(key)
        )

    def pending_keys(self, partition: int = 0) -> list:
        return list(PendingIndex(self.transaction(partition)).entries())


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(buffer_operator, "time", fake)
    monkeypatch.setattr(buffer_tick, "time", fake)
    return fake


@pytest.fixture
def tick_driver(topic_manager_topic_factory, dataframe_factory, state_manager):
    """Build a composed, partition-assigned `StreamingDataFrame` with a buffer."""

    def _factory(
        buffer: Optional[LookupBuffer] = None,
        fields: Optional[dict] = None,
        on=None,
        partitions: tuple = (0,),
        downstream_delay_s: float = 0.0,
    ) -> TickDriver:
        registry = DataFrameRegistry()
        topic = topic_manager_topic_factory()
        sdf = dataframe_factory(
            topic=topic, state_manager=state_manager, registry=registry
        )
        lookup = FakeLookup()
        sdf = sdf.join_lookup(
            lookup,
            fields if fields is not None else make_fields(),
            on=on,
            buffer=buffer,
        )

        contexts: list = []

        def capture_context(value):
            # Stands in for any downstream operator that reads the context:
            # `to_topic`, `sdf.sink`, or a stateful operator keying its
            # transaction on the partition. `downstream_delay_s` makes it stand
            # in for an expensive one, which is what the tick's wall-clock
            # budget exists to bound.
            contexts.append(message_context())
            if downstream_delay_s:
                time.sleep(downstream_delay_s)
            return value

        sdf = sdf.apply(capture_context)

        for partition in partitions:
            state_manager.on_partition_assign(
                stream_id=sdf.stream_id,
                partition=partition,
                committed_offsets={},
            )

        emitted: list = []
        executors = sdf.compose(
            sink=lambda value, key, timestamp, headers: emitted.append(
                (value, key, timestamp, headers)
            )
        )
        return TickDriver(
            sdf=sdf,
            topic=topic,
            lookup=lookup,
            registry=registry,
            state_manager=state_manager,
            executor=executors[topic.name],
            emitted=emitted,
            contexts=contexts,
        )

    return _factory


def _spy_on_transactions(monkeypatch, store) -> list:
    """Record every partition a store opens a transaction for."""
    opened: list = []
    original = store.start_partition_transaction

    def spy(partition):
        opened.append(partition)
        return original(partition)

    monkeypatch.setattr(store, "start_partition_transaction", spy)
    return opened


class TestK1TickEmitsAtTheDeadline:
    """K1: records resolve at their deadline with zero traffic. The headline."""

    def test_buffered_records_are_emitted_by_the_tick_alone(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        for timestamp in (1, 2, 3):
            assert driver.send("D", timestamp=timestamp) == []
        assert len(driver.stored("D")) == 3

        # Not a single further record of any kind.
        clock.advance_ms(GRACE_MS + 1)
        emitted = driver.tick()

        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 2, 3], (
            "all three records must go downstream, in arrival order, driven by "
            "nothing but the clock"
        )
        assert all(key == "D" for _, key, _, _ in emitted)
        assert all(
            value["region"] == "unknown" for value, *_ in emitted
        ), "a timed-out record carries its fields' declared defaults"
        assert driver.stored("D") == [], "the store entries must be gone"
        assert driver.pending_keys() == [], "and the prefix must leave the index"

    def test_a_second_tick_emits_nothing_more(self, clock, tick_driver):
        """The tick is not a re-emitter: once settled, always settled."""
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        driver.send("D", timestamp=1)
        clock.advance_ms(GRACE_MS + 1)
        assert len(driver.tick()) == 1

        assert driver.tick() == []
        assert driver.tick() == []


class TestK2TickHonoursOnTimeout:
    """K2: the tick respects `on_timeout="drop"` rather than always emitting."""

    def test_drop_mode_emits_nothing_and_empties_the_store(
        self, clock, tick_driver, caplog
    ):
        driver = tick_driver(buffer=make_buffer(on_timeout="drop"))
        for timestamp in (1, 2, 3):
            driver.send("D", timestamp=timestamp)
        assert len(driver.stored("D")) == 3

        clock.advance_ms(GRACE_MS + 1)
        with caplog.at_level(logging.WARNING):
            emitted = driver.tick()

        assert emitted == [], "nothing may be emitted under on_timeout='drop'"
        assert driver.stored("D") == []
        assert driver.pending_keys() == []
        dropped = [
            record for record in caplog.records if "dropped" in record.getMessage()
        ]
        assert dropped, (
            "a dropped key must leave a trace in the logs of a service nobody "
            "is watching"
        )
        assert any("D" in str(record.args) for record in dropped)


class TestK3NothingResolvesEarly:
    """K3: nothing is settled before its own deadline, on any path."""

    def test_ticking_inside_the_window_changes_nothing(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer())
        driver.send("D", timestamp=1)

        for _ in range(5):
            clock.advance_ms(GRACE_MS // 10)
            assert driver.tick() == []

        assert len(driver.stored("D")) == 1, "the record must still be in the store"
        assert driver.pending_keys() != [], "and still in the pending index"

    def test_a_survivor_is_still_releasable_enriched_after_ticks(
        self, clock, tick_driver
    ):
        """
        The tick must not consume a record that is still inside its window: it
        stays eligible for a real, enriched release.
        """
        driver = tick_driver(buffer=make_buffer())
        driver.send("D", timestamp=1)
        clock.advance_ms(GRACE_MS // 2)
        assert driver.tick() == []

        driver.lookup.configs["D"] = {"threshold": 42, "region": "eu"}
        emitted = driver.send("D", timestamp=2)

        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 2]
        assert all(
            value["threshold"] == 42 for value, *_ in emitted
        ), "the survivor must be enriched, not resolved to its defaults"


class TestK4RevokeIsNotADeadline:
    """K4: a partition this instance no longer owns is never touched."""

    def test_revoked_partition_is_not_swept(self, clock, tick_driver, monkeypatch):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        for timestamp in (1, 2, 3):
            driver.send("D", timestamp=timestamp)

        store = driver.store()
        driver.state_manager.on_partition_revoke(
            stream_id=driver.sdf.stream_id, partition=0
        )
        assert store.partitions == {}, "the revoke must have taken effect"

        opened = _spy_on_transactions(monkeypatch, store)

        clock.advance_ms(GRACE_MS + 1)
        for _ in range(5):
            assert driver.tick() == [], "a revoked partition must emit nothing"

        assert opened == [], (
            "the tick must iterate the store's own assignment map, so a revoked "
            "partition is simply absent - not remembered and then queried, "
            "which would either raise PartitionNotAssignedError or emit records "
            "the new owner is going to emit again from the changelog"
        )

    def test_reassigned_partition_takes_a_fresh_pass(
        self, clock, tick_driver, monkeypatch
    ):
        """
        The other half of the same coin. A rebalance can revoke and re-assign
        the same partition *number* inside one callback, so a cache keyed on
        numbers alone would never notice - and the re-assigned partition, whose
        buffer has just been recovered from the changelog, would inherit the
        previous instance's "nothing is due here" verdict and never be swept.
        """
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        driver.tick()  # First pass: nothing buffered, so the cache says "never".

        stream_id = driver.sdf.stream_id
        driver.state_manager.on_partition_revoke(stream_id=stream_id, partition=0)
        # A real revoke commits and re-inits the checkpoint, which is what drops
        # the transaction held for the old store partition.
        driver.sdf.processing_context.init_checkpoint()
        driver.state_manager.on_partition_assign(
            stream_id=stream_id, partition=0, committed_offsets={}
        )

        opened = _spy_on_transactions(monkeypatch, driver.store())
        clock.advance_ms(GRACE_MS + 1)
        driver.tick()

        assert opened == [0], (
            "a re-assigned partition must take one real pass: its store is a "
            "fresh object holding a freshly recovered buffer, and the previous "
            "instance's cached deadline says nothing about it"
        )


class TestK5EmptyBufferCostsNothing:
    """
    K5: an empty buffer costs no state access.

    Deviation from the spec's literal wording, recorded here so the next reader
    does not think it was overlooked. The spec asks for **zero** transactions
    across 100 ticks. That is unachievable together with correctness: a fresh
    process cannot know whether a changelog-recovered buffer holds anything
    without reading it once, so every newly assigned partition is seeded with a
    deadline of `0` and takes exactly one real pass. The property actually worth
    guarding - the one an idle deployment pays every second - is the steady
    state after that pass: no transaction, no RocksDB read.
    """

    def test_first_tick_takes_one_pass_and_the_rest_take_none(
        self, clock, tick_driver, monkeypatch
    ):
        driver = tick_driver(buffer=make_buffer())
        store = driver.store()
        store_partition = store.partitions[0]
        checkpoint = driver.sdf.processing_context.checkpoint

        opened = _spy_on_transactions(monkeypatch, store)
        reads = {"count": 0}
        original_iter = type(store_partition).iter_items

        def counting_iter_items(self, *args, **kwargs):
            reads["count"] += 1
            return original_iter(self, *args, **kwargs)

        monkeypatch.setattr(type(store_partition), "iter_items", counting_iter_items)

        driver.tick()
        assert opened == [0], (
            "the first tick after an assignment must take exactly one pass: the "
            "store may hold a changelog-recovered buffer and there is no way to "
            "find out without reading it"
        )
        assert reads["count"] > 0

        opened.clear()
        reads["count"] = 0
        clock.advance_ms(60_000)
        for _ in range(100):
            driver.tick()

        assert opened == [], (
            "with nothing buffered the tick must never open a transaction "
            "again: the gate is a cached watermark, not a query"
        )
        assert reads["count"] == 0, "and never touch the store"
        assert len(checkpoint._store_transactions) == 1, (
            "only the single first-pass transaction, which holds no changes and "
            "therefore still leaves the checkpoint empty"
        )


class TestK6FlushedRecordsCarryTheirOwnIdentity:
    """K6: a tick-emitted record is the record, not a synthetic stand-in."""

    def test_key_timestamp_headers_and_context_are_the_records_own(
        self, clock, tick_driver
    ):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        headers = [("source", b"sensor-7")]
        driver.send(
            "D",
            timestamp=987_654,
            value={"reading": 12},
            headers=headers,
            offset=4242,
        )
        # A later, unrelated record for another key, so the context reported for
        # D cannot accidentally be "the last one this operator happened to see".
        driver.send("E", timestamp=111, offset=9999)

        clock.advance_ms(GRACE_MS + 1)
        emitted = driver.tick()

        keys = [key for _, key, _, _ in emitted]
        assert sorted(keys) == ["D", "E"]
        value, _, timestamp, emitted_headers = emitted[keys.index("D")]
        assert timestamp == 987_654, "its own event timestamp, not the tick's"
        assert emitted_headers == headers
        assert value["reading"] == 12

        context = driver.contexts[keys.index("D")]
        assert context.topic == driver.topic.name, "the record's original topic"
        assert context.offset == 4242, "the record's original offset"
        assert context.partition == 0, "the partition being ticked"


class TestK11SkipSentinel:
    """
    K11: `skip=None` means "settle everything", and `b""` is a real prefix.

    `b""` is what the empty message key encodes to, so it cannot double as the
    "skip nothing" sentinel - which is why `BufferSweeper.sweep(skip=...)` is
    `Optional[bytes]` and the tick passes `None`.

    Scope note: this asserts the sentinel's meaning without buffering under an
    actually-empty key. Whether that key is safe to buffer at all is a property
    of the store rather than of the tick - `TimestampedPartitionTransaction`
    appends the key SEPARATOR unconditionally, so the empty prefix's scans are
    qualified like any other's. It is covered end to end, on both paths bounded
    at `MAX_RECEIVE_MS`, by `TestEmptyMessageKeyIsBufferedAndSettledLikeAnyOther`
    in `test_lookup_buffer_findings.py`; see
    `dev-planning/lookup-deadline-tick/open-points.md` §1.
    """

    def test_empty_string_key_encodes_to_the_empty_prefix(self):
        assert prefix_for_key("") == b"", (
            "b'' is a legal prefix, so it must not be used as the sweeper's "
            "'skip nothing' sentinel"
        )

    def test_tick_settles_every_due_prefix_including_the_last_record_seen(
        self, clock, tick_driver
    ):
        """
        The record path skips the arriving record's own prefix, because it
        settles that one itself in the same callback. The tick has no such
        record, so it must settle every due prefix - including the one the most
        recent arrival owned.
        """
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        driver.send("A", timestamp=1)
        driver.send("B", timestamp=2)
        # "C" is the most recent arrival: the prefix the record path would skip.
        driver.send("C", timestamp=3)
        assert len(driver.pending_keys()) == 3

        clock.advance_ms(GRACE_MS + 1)
        emitted = driver.tick()

        assert sorted(key for _, key, _, _ in emitted) == ["A", "B", "C"]
        assert driver.pending_keys() == []


class TestK12SubSecondGraceWarning:
    """K12: a `grace_ms` below the poll timeout warns at construction."""

    def test_sub_second_grace_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            LookupBuffer(grace_ms=200, is_resolved=is_resolved)

        messages = [record.getMessage() for record in caplog.records]
        assert any("consumer_poll_timeout" in message for message in messages), (
            "a user who picks a 200ms window deserves to learn why it resolves "
            "in 1.2s on an idle partition"
        )
        assert any("200" in message for message in messages)

    def test_a_second_of_grace_does_not_warn(self, caplog):
        with caplog.at_level(logging.WARNING):
            LookupBuffer(grace_ms=1_000, is_resolved=is_resolved)
            LookupBuffer(grace_ms=30_000, is_resolved=is_resolved)

        warned = [
            record
            for record in caplog.records
            if "consumer_poll_timeout" in record.getMessage()
        ]
        assert warned == []
