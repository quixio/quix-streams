"""
Broker-level tests for `join_lookup(..., buffer=LookupBuffer(...))`.

Spec: `dev-planning/lookup-late-config-buffering/spec.md` revision 8, section 12,
"Also required before merge": "Broker-level test with a real config topic and a
real store... There is currently no broker-level coverage of this area at all."
This file is that coverage. Every other assertion about the buffer's durability
lives in `test_lookup_buffer.py` (in-process, fake clock) -- this file is only
for the properties that require a real changelog topic, a real committed
consumer offset, and a real `Application.run()` loop.

Two mechanisms are used to approximate the two durability claims, and neither
is a literal two-consumer live Kafka group rebalance -- see the module-level
note under `TestRebalanceDurability` for why that is not attempted here.

- **Rebalance (approximated):** `app1` stops, `app2` starts in the *same*
  consumer group, *same* `state_dir` (so its local RocksDB files are intact --
  the "reassigned back to the same node" case). This drives a real
  `_on_revoke` / `_on_assign` cycle through `Application.run()`, exercising
  `store.revoke_partition()` / `StateStoreManager.on_partition_assign()`
  against a real changelog-backed store, without forcing changelog replay.
- **Restart (literal):** `app1` stops, `app2` starts in the same consumer
  group with a **fresh** `state_dir`, forcing `state_manager.recovery_required`
  / `do_recovery()` to replay the buffer's changelog topic from scratch. This
  is the mechanism section 5 of the spec argues for; here it is proven.
"""

import time
import uuid
from json import dumps
from typing import Any

from confluent_kafka import TopicPartition

from quixstreams.dataframe.joins.lookups import LookupBuffer
from quixstreams.models import TopicConfig
from quixstreams.models.serializers import JSONDeserializer
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    FakeLookup,
    is_resolved,
    make_fields,
)

# The in-process suite's `GRACE_MS` (1s) is sized for a fake clock advanced by
# hand; it is far too small here. A real `app_factory` round trip -- topic
# creation, consumer group join, RocksDB open -- routinely costs several real
# wall-clock seconds per `Application.run()` call (see the captured logs from
# this file's first draft: ~13s between a topic being created and its first
# message being processed), and this module's `time.time()` inside the buffer
# operator is never faked. A short `grace_ms` would time out records the test
# means to keep alive purely because the harness was slow to start, which is
# a test bug, not a buffer bug. Ten minutes comfortably outlasts every test in
# this file while still being trivially distinguishable from "never expires".
BROKER_GRACE_MS = 10 * 60 * 1000


def _make_topic(app, name: str, num_partitions: int = 1):
    return app.topic(
        name,
        key_deserializer="str",
        value_deserializer=JSONDeserializer(),
        config=TopicConfig(num_partitions=num_partitions, replication_factor=1),
    )


def _attach_buffer(
    app, topic, lookup: FakeLookup, buffer: LookupBuffer, collected: list
):
    sdf = app.dataframe(topic)
    sdf = sdf.join_lookup(lookup, make_fields(), buffer=buffer)
    sdf.update(
        lambda value, key, timestamp, headers: collected.append(
            (dict(value), key, timestamp, headers)
        ),
        metadata=True,
    )
    return sdf


def _produce(app, topic, records: list[dict[str, Any]]):
    with app.get_producer() as producer:
        for record in records:
            producer.produce(
                topic=topic.name,
                key=record["key"],
                value=dumps(record.get("value", {})).encode(),
                timestamp=record["timestamp"],
                partition=record.get("partition", 0),
            )


class TestRebalanceDurability:
    """
    Scenario 1 (T9-adjacent, broker-level): a partition is revoked and
    reassigned, and every buffered record is still accounted for afterwards --
    neither lost nor duplicated.

    **Scope note, stated plainly per the brief:** a literal two-consumer live
    Kafka rebalance (start `app1`, join `app2` to the same group while `app1`
    keeps running, let the broker's group coordinator revoke/reassign
    partitions between them) is not attempted here. Grepping this repo's own
    `test_app.py` and `test_recovery_rebalance_mid_recovery.py` turns up zero
    instances of two `Application.run()` calls executing concurrently against
    a live broker in the same consumer group -- every existing "rebalance"
    test either mocks `RecoveryManager`/`Consumer` directly or runs `app1` and
    `app2` **sequentially** (stop, then start). Building a genuinely
    concurrent two-consumer harness from scratch, with correct-but-unknown
    partition-to-consumer assignment after the rebalance settles, is exactly
    the kind of timing-dependent construction the brief warns produces a
    flaky test that is worse than none. The sequential proxy below instead
    drives a **real** `_on_revoke` / `_on_assign` cycle through a real
    `Application.run()` against a real changelog-backed store -- the same
    code path a live rebalance would hit -- with the local RocksDB files
    intact (no changelog replay forced; that is `TestRestartDurability`).
    """

    def test_buffered_records_survive_revoke_and_reassign(self, app_factory, tmp_path):
        """
        Validates spec §5 ("Buffered records survive rebalance, restart and
        replica replacement") for the revoke/reassign half of that claim, and
        spec T9 ("Buffer survives restart/rebalance... all 4 emit with their
        original timestamps").
        """
        consumer_group = str(uuid.uuid4())
        state_dir = tmp_path / "state"
        topic_name = str(uuid.uuid4())

        processed = {"count": 0}

        def stop_after_two(_topic, _partition, _offset):
            processed["count"] += 1
            if processed["count"] >= 2:
                app1.stop()

        app1 = app_factory(
            consumer_group=consumer_group,
            state_dir=state_dir,
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_after_two,
        )
        topic1 = _make_topic(app1, topic_name)
        lookup1 = FakeLookup()
        collected1: list = []
        buffer1 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf1 = _attach_buffer(app1, topic1, lookup1, buffer1, collected1)

        _produce(
            app1,
            topic1,
            [
                {"key": "D", "timestamp": 1},
                {"key": "D", "timestamp": 2},
            ],
        )

        app1.run(sdf1, timeout=30)

        assert processed["count"] == 2, (
            "both D records must have been consumed (and their offsets "
            "committed) before the revoke -- otherwise this is not testing "
            "the property in question"
        )
        assert collected1 == [], (
            "neither D record resolves, so nothing should have been emitted "
            "before the revoke"
        )

        # `app1` has already returned from `run()` (stop() closes its stores,
        # which is the revoke). `app2` now takes over the same partition with
        # the same on-disk state -- the "reassigned back to the same node"
        # case -- and must see exactly the two buffered records, not zero,
        # not four.
        processed2 = {"count": 0}

        def stop_after_one(_topic, _partition, _offset):
            processed2["count"] += 1
            if processed2["count"] >= 1:
                app2.stop()

        app2 = app_factory(
            consumer_group=consumer_group,
            state_dir=state_dir,
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_after_one,
        )
        topic2 = _make_topic(app2, topic_name)
        lookup2 = FakeLookup()
        lookup2.configs["D"] = {"threshold": 42, "region": "eu"}
        collected2: list = []
        buffer2 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf2 = _attach_buffer(app2, topic2, lookup2, buffer2, collected2)

        _produce(app2, topic2, [{"key": "D", "timestamp": 3}])
        app2.run(sdf2, timeout=30)

        assert [timestamp for _, _, timestamp, _ in collected2] == [1, 2, 3], (
            "all three D records (the two survivors plus the releasing "
            "record) must emit exactly once, in arrival order, after the "
            "reassignment -- neither lost nor duplicated. If app1's two "
            "buffered writes had not survived the revoke, only [3] would "
            "appear here"
        )
        assert all(value["threshold"] == 42 for value, *_ in collected2)


class TestRestartDurability:
    """
    Scenario 2: the application stops and a fresh instance recovers the
    buffer from the changelog topic, not from memory (which no longer
    exists). This is the literal claim spec §5 makes: "an in-memory buffer
    ... loses every held record on rebalance or restart".
    """

    def test_buffered_records_survive_restart_via_changelog_recovery(
        self, app_factory, tmp_path
    ):
        """
        Validates spec §5's durability argument end-to-end: `app2` never
        shares a process, a RocksDB directory or any in-memory state with
        `app1`. The only channel between them is the changelog topic and the
        committed consumer offset, both on the real broker.
        """
        consumer_group = str(uuid.uuid4())
        topic_name = str(uuid.uuid4())

        processed = {"count": 0}

        def stop_after_three(_topic, _partition, _offset):
            processed["count"] += 1
            if processed["count"] >= 3:
                app1.stop()

        app1 = app_factory(
            consumer_group=consumer_group,
            state_dir=tmp_path / "state-app1",
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_after_three,
        )
        topic1 = _make_topic(app1, topic_name)
        lookup1 = FakeLookup()
        collected1: list = []
        buffer1 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf1 = _attach_buffer(app1, topic1, lookup1, buffer1, collected1)

        _produce(
            app1,
            topic1,
            [
                {"key": "D", "timestamp": 1},
                {"key": "D", "timestamp": 2},
                {"key": "D", "timestamp": 3},
            ],
        )

        app1.run(sdf1, timeout=30)

        assert processed["count"] == 3
        assert collected1 == [], "none of D1..D3 resolve, so nothing emits yet"

        # `app1`'s process-local RocksDB directory is simply never reused.
        # `app2` starts cold: its only source of truth is the changelog topic
        # on the broker.
        processed2 = {"count": 0}

        def stop_after_one(_topic, _partition, _offset):
            processed2["count"] += 1
            if processed2["count"] >= 1:
                app2.stop()

        app2 = app_factory(
            consumer_group=consumer_group,
            state_dir=tmp_path / "state-app2-fresh",
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_after_one,
        )
        topic2 = _make_topic(app2, topic_name)
        lookup2 = FakeLookup()
        lookup2.configs["D"] = {"threshold": 7, "region": "us"}
        collected2: list = []
        buffer2 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf2 = _attach_buffer(app2, topic2, lookup2, buffer2, collected2)

        _produce(app2, topic2, [{"key": "D", "timestamp": 4}])
        app2.run(sdf2, timeout=30)

        assert [timestamp for _, _, timestamp, _ in collected2] == [1, 2, 3, 4], (
            "all four records -- three recovered survivors plus the "
            "releasing record -- must emit exactly once, in arrival order, "
            "after a cold restart with no shared memory or disk state. If "
            "changelog recovery had lost app1's writes, only [4] would "
            "appear here; if it had replayed them twice, some of [1, 2, 3] "
            "would repeat"
        )
        assert all(value["threshold"] == 7 for value, *_ in collected2)


class TestNoDuplicateEmission:
    """
    Scenario 3: a survivor released once must not be released again after a
    rebalance mid-flight.

    Spec §7.2 / architecture §6 deviation 5 drop the replay-dedupe mechanism
    the spec sketched: "Buffered records are at-least-once, the same as every
    other record in the SDK." So this test states what is actually observed
    on the *reachable* path -- a clean revoke/reassign or restart, where the
    release only ever happens once because the releasing record itself is
    only ever consumed once -- and does not claim protection against the
    unreachable-in-this-harness case (a crash strictly between the changelog
    produce and the offset commit, replaying the releasing record itself).
    That case is argued, not tested, in architecture.md §6 deviation 5, and
    ArchDev's own text calls it "imperfect by design, and imperfect in the
    harmless direction" for the *buffering* write -- there is no equivalent
    dedupe for the *release*, so a replayed *releasing* record would legally
    re-emit its survivors a second time. Flagging this precisely rather than
    asserting a guarantee the design does not make.
    """

    def test_release_emits_each_survivor_exactly_once_across_a_restart(
        self, app_factory, tmp_path
    ):
        """
        Validates the reachable claim: across a full stop/cold-restart cycle
        (the harness in `TestRestartDurability`), the total count of times
        each buffered record is emitted, summed across both application
        instances, is exactly one -- not zero, not two.
        """
        consumer_group = str(uuid.uuid4())
        topic_name = str(uuid.uuid4())

        processed = {"count": 0}

        def stop_after_two(_topic, _partition, _offset):
            processed["count"] += 1
            if processed["count"] >= 2:
                app1.stop()

        app1 = app_factory(
            consumer_group=consumer_group,
            state_dir=tmp_path / "state-app1",
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_after_two,
        )
        topic1 = _make_topic(app1, topic_name)
        lookup1 = FakeLookup()
        collected1: list = []
        buffer1 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf1 = _attach_buffer(app1, topic1, lookup1, buffer1, collected1)
        _produce(
            app1,
            topic1,
            [{"key": "D", "timestamp": 1}, {"key": "D", "timestamp": 2}],
        )
        app1.run(sdf1, timeout=30)

        processed2 = {"count": 0}

        def stop_after_one(_topic, _partition, _offset):
            processed2["count"] += 1
            if processed2["count"] >= 1:
                app2.stop()

        app2 = app_factory(
            consumer_group=consumer_group,
            state_dir=tmp_path / "state-app2",
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_after_one,
        )
        topic2 = _make_topic(app2, topic_name)
        lookup2 = FakeLookup()
        lookup2.configs["D"] = {"threshold": 1, "region": "eu"}
        collected2: list = []
        buffer2 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf2 = _attach_buffer(app2, topic2, lookup2, buffer2, collected2)
        _produce(app2, topic2, [{"key": "D", "timestamp": 3}])
        app2.run(sdf2, timeout=30)

        all_emitted_timestamps = [t for _, _, t, _ in collected1] + [
            t for _, _, t, _ in collected2
        ]
        assert sorted(all_emitted_timestamps) == [1, 2, 3], (
            "each record must be emitted exactly once across the whole run: "
            "duplicates would show a repeated timestamp, loss would show a "
            "missing one"
        )


class TestOffsetBufferAtomicity:
    """
    Scenario 4: no record whose offset has been committed has lost its buffer
    entry -- the property the checkpoint ordering in checkpointing/checkpoint.py
    (flush sinks -> produce changelogs -> flush producer -> commit offsets ->
    flush store partitions) exists to guarantee.
    """

    def test_committed_records_are_all_present_in_the_recovered_store(
        self, app_factory, internal_consumer_factory, tmp_path
    ):
        """
        Validates spec §5 point 1-2 directly: buffers several D records
        across more than one commit (`commit_interval=0` commits after every
        message, so this exercises several checkpoint cycles, not one), then
        confirms the committed offset for the source topic and the recovered
        buffer count agree exactly -- i.e. every commit's changelog write
        landed before that commit's offset did, for every one of them, not
        just the last.
        """
        consumer_group = str(uuid.uuid4())
        topic_name = str(uuid.uuid4())
        num_records = 5

        processed = {"count": 0}

        def stop_when_done(_topic, _partition, _offset):
            processed["count"] += 1
            if processed["count"] >= num_records:
                app1.stop()

        app1 = app_factory(
            consumer_group=consumer_group,
            state_dir=tmp_path / "state-app1",
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_when_done,
        )
        topic1 = _make_topic(app1, topic_name)
        lookup1 = FakeLookup()
        collected1: list = []
        buffer1 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf1 = _attach_buffer(app1, topic1, lookup1, buffer1, collected1)

        _produce(
            app1,
            topic1,
            [{"key": "D", "timestamp": i} for i in range(1, num_records + 1)],
        )
        app1.run(sdf1, timeout=30)

        assert processed["count"] == num_records

        with internal_consumer_factory(
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
        ) as consumer:
            committed_offset = consumer.committed([TopicPartition(topic1.name, 0)])[
                0
            ].offset
        assert committed_offset == num_records, (
            "every one of the five buffered records' offsets must be "
            "committed -- otherwise this test is not exercising the "
            "atomicity property at all"
        )

        # A completely fresh replica recovers from the changelog and must
        # account for every one of the five records whose offset was just
        # confirmed committed above. Proven through emission, not a direct
        # store read: `Application.run()` tears its `StateStoreManager` down
        # in its own `exit_stack` on return, so there is no supported way to
        # inspect the store from outside a running `Application` -- the
        # existing suite's own `_validate_state` helper in test_app.py works
        # around this by building a second, throwaway `StateStoreManager`,
        # which cannot drive real changelog recovery without also
        # reimplementing `RecoveryManager` wiring by hand. Emission is the
        # black-box-safe way to observe the same property: if any of the
        # five commits had lost its changelog write, that record's timestamp
        # would simply be missing from the release.
        processed2 = {"count": 0}

        def stop_after_one(_topic, _partition, _offset):
            processed2["count"] += 1
            if processed2["count"] >= 1:
                app2.stop()

        app2 = app_factory(
            consumer_group=consumer_group,
            state_dir=tmp_path / "state-app2-fresh",
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=stop_after_one,
        )
        topic2 = _make_topic(app2, topic_name)
        lookup2 = FakeLookup()
        lookup2.configs["D"] = {"threshold": 9, "region": "eu"}
        collected2: list = []
        buffer2 = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf2 = _attach_buffer(app2, topic2, lookup2, buffer2, collected2)

        _produce(app2, topic2, [{"key": "D", "timestamp": num_records + 1}])
        app2.run(sdf2, timeout=30)

        expected_timestamps = list(range(1, num_records + 2))
        assert [t for _, _, t, _ in collected2] == expected_timestamps, (
            f"every one of the {num_records} records whose offset app1 "
            f"committed must reappear here plus the releasing record, in "
            f"order -- got {[t for _, _, t, _ in collected2]}. A gap means "
            f"a committed record's buffer write did not survive, breaking "
            f"the atomicity the checkpoint ordering promises"
        )


class TestAcceptanceScenario:
    """
    Scenario 5, spec §3 / T2: 2 partitions, 4 keys A/B/C/D. Configs exist for
    A, B and C; D has none and never will. A/B/C must keep processing at full
    rate -- the whole reason this design exists instead of the branch's
    blocking `grace_ms` wait.
    """

    PER_KEY_COUNT = 20
    TOLERANCE_SECONDS = 3.0
    """
    Deliberately generous, and derived rather than eyeballed: this run is
    dominated by real Docker + Kafka + RocksDB round-trip cost that has
    nothing to do with the property under test, so a tight ratio between the
    two runs would be noisy in exactly the range that matters (per the
    brief's warning that a flaky timing assertion is worse than none). The
    rejected blocking design sleeps a full `grace_ms` **per unresolved D
    record processed on the shared thread** -- even at `BROKER_GRACE_MS`
    (chosen large on purpose so a real Docker/Kafka round trip never times a
    record out mid-test, see the module-level comment), a single D record
    would blow this 3-second budget by two orders of magnitude, so the
    tolerance does not depend on how large `grace_ms` happens to be set. This
    design's added cost per D record is a dict membership test and a bounded
    sweep slice (spec §3) -- microseconds, not seconds, regardless of
    `grace_ms`. 3 seconds distinguishes "a real stall happened" from
    "ordinary CI jitter" without being tight enough to flake on jitter alone.
    """

    def _run(self, app_factory, tmp_path, run_name: str, include_d: bool):
        consumer_group = str(uuid.uuid4())
        topic_name = f"{run_name}-{uuid.uuid4()}"

        records = []
        for i in range(self.PER_KEY_COUNT):
            records.append({"key": "A", "timestamp": i, "partition": 0})
            records.append({"key": "C", "timestamp": i, "partition": 0})
            records.append({"key": "B", "timestamp": i, "partition": 1})
            if include_d:
                # D shares a partition with B, so a stall on D would be
                # directly observable as a delay on B's messages -- not just
                # a different partition that a single-threaded poll loop
                # might happen to visit less often.
                records.append({"key": "D", "timestamp": i, "partition": 1})
        total = len(records)

        processed = {"count": 0}
        finished_at = {}

        def on_processed(_topic, _partition, _offset):
            processed["count"] += 1
            if processed["count"] >= total:
                finished_at["t"] = time.monotonic()
                app.stop()

        app = app_factory(
            consumer_group=consumer_group,
            state_dir=tmp_path / f"state-{run_name}",
            auto_offset_reset="earliest",
            commit_interval=0,
            on_message_processed=on_processed,
        )
        topic = _make_topic(app, topic_name, num_partitions=2)
        lookup = FakeLookup()
        lookup.configs["A"] = {"threshold": 1, "region": "eu"}
        lookup.configs["B"] = {"threshold": 2, "region": "eu"}
        lookup.configs["C"] = {"threshold": 3, "region": "eu"}
        collected: list = []
        buffer = LookupBuffer(grace_ms=BROKER_GRACE_MS, is_resolved=is_resolved)
        sdf = _attach_buffer(app, topic, lookup, buffer, collected)

        started_at = time.monotonic()
        _produce(app, topic, records)
        app.run(sdf, timeout=60)

        return collected, finished_at.get("t", time.monotonic()) - started_at

    def test_configured_keys_are_not_degraded_by_an_unconfigured_key(
        self, app_factory, tmp_path
    ):
        """
        Validates spec §3 (the acceptance scenario) and T2: "A/B/C emit
        immediately; total run time within a tolerance derived from a
        control run with D absent."
        """
        control_collected, control_time = self._run(
            app_factory, tmp_path, "control", include_d=False
        )
        test_collected, test_time = self._run(
            app_factory, tmp_path, "test", include_d=True
        )

        expected_count = self.PER_KEY_COUNT * 3
        assert len(control_collected) == expected_count
        # D's grace window (`BROKER_GRACE_MS`) comfortably outlasts this run, and D
        # is never configured, so it is legitimately absent from the output
        # -- only A/B/C, each resolved on the first attempt, should appear.
        assert len(test_collected) == expected_count, (
            "every A/B/C record must still emit -- and, since none of them "
            "were ever buffered, immediately -- with an unconfigured D "
            "interleaved on the same partition as B"
        )
        assert {key for _, key, _, _ in test_collected} == {"A", "B", "C"}

        for value, key, _, _ in test_collected:
            assert value["threshold"] in (1, 2, 3), (
                f"key {key!r} emitted with a non-configured value; it must "
                f"have resolved on the first attempt instead of having been "
                f"buffered and then timed out"
            )

        assert test_time - control_time <= self.TOLERANCE_SECONDS, (
            f"the run with D present took {test_time:.2f}s versus "
            f"{control_time:.2f}s for the control run without D -- a "
            f"difference over {self.TOLERANCE_SECONDS}s is the stall the "
            f"acceptance scenario (spec §3) forbids"
        )
