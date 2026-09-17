"""
Red-first reproduction tests for round 4: a release must not cut a sibling's
`grace_ms` short.

The buffer stores withheld records under the **message** key while
resolvability is decided by the **lookup** key, and with an `on=` that derives
the lookup key from the value - the DCM enrichment shape - one message key
carries several of them. Until this round a release was all-or-nothing per
message key: a record whose lookup resolved emitted *everything* queued under
that message key, so a record whose own configuration had not arrived left with
its declared defaults while its own grace window was still open. `grace_ms`
promises a record waits up to that long for its own configuration; in that
topology it did not.

`BufferOperator._release()` now classifies three ways - resolved, past its
deadline (settled earlier, by `_take_timed_out()`), and unresolved-but-inside-
its-window - and the third stays buffered. `buffer_release.py` carries that into
the store without rewriting the records that stay, so their arrival times,
duplicate counters and order are the ones they already had.

Every test in the first, second, third and fourth class below fails on the
unfixed code, apart from the two explicitly labelled regression guards.
`TestReleasePlanStoreSurgery` and `TestTheSnapshotBeatsTheReadingOfTheEnvelope`
are unit tests of code this round introduces, so they cannot be red before it -
they exist to pin the two properties the reproductions can only observe
indirectly: that a retained record is not rewritten unless a neighbour's delete
forces it, and that what is written back is the arrival-time envelope.

Traces to `dev-planning/lookup-deadline-tick/open-points.md` §5 and
`dev-planning/lookup-deadline-tick/architecture.md` §10.
"""

from typing import Any

import pytest

from quixstreams.dataframe.joins.lookups import buffer_operator, buffer_tick
from quixstreams.dataframe.joins.lookups.buffer_envelope import (
    ENVELOPE_RECEIVED,
    ENVELOPE_TIMESTAMP,
    encode_envelope,
    envelope_value,
)
from quixstreams.dataframe.joins.lookups.buffer_release import (
    ReleasePlan,
    Withheld,
    crowded_milliseconds,
    snapshot_for_rewrite,
)
from quixstreams.dataframe.joins.lookups.buffer_state import (
    PendingIndex,
    encode_prefix,
    prefix_for_key,
)
from quixstreams.utils.json import dumps as orjson_dumps
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    GRACE_MS,
    STORE_NAME,
    FakeClock,
    make_buffer,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    buffered as buffered,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    clock as clock,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer_tick import (
    tick_driver as tick_driver,
)

CONFIG_ONE = {"threshold": 1, "region": "eu"}
CONFIG_TWO = {"threshold": 2, "region": "us"}
CONFIG_THREE = {"threshold": 3, "region": "de"}


@pytest.fixture
def tick_clock(monkeypatch) -> FakeClock:
    """
    A fake clock for both modules that read wall time, for the tick tests.

    Declared here rather than re-exported from `test_lookup_buffer_tick`: this
    file also uses `test_lookup_buffer`'s `clock`, which patches the operator
    only, and two fixtures cannot share a name.
    """
    fake = FakeClock()
    monkeypatch.setattr(buffer_operator, "time", fake)
    monkeypatch.setattr(buffer_tick, "time", fake)
    return fake


def _now_ms(fake_clock: Any) -> int:
    """The arrival time the operator would stamp on a record sent right now."""
    return int(fake_clock.time() * 1000)


def _flush_to_disk(driver: Any) -> None:
    """
    Commit the driver's live transaction to real RocksDB and evict it from the
    checkpoint's memo, so the next store access reads from disk.

    Used where the assertion is about what the *store* now holds rather than
    about what the update cache remembers: a selective delete followed by a
    rewrite of the same millisecond is exactly the shape an in-process cache
    could paper over.
    """
    transaction = driver.transaction()
    transaction.prepare()
    transaction.flush()
    checkpoint = driver.sdf.processing_context.checkpoint
    checkpoint._store_transactions.pop((driver.sdf.stream_id, 0, STORE_NAME), None)


class TestASiblingResolutionLeavesTheOthersBuffered:
    """
    The headline, through the real record path. Red on the unfixed code, where
    every one of these releases emitted the whole buffer.
    """

    def test_an_unresolved_sibling_stays_buffered(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = buffered(buffer=make_buffer(), on="device")
        arrival = _now_ms(clock)
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["two"] = CONFIG_TWO
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=2, value={"device": "two"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [2], (
            "'two' resolved its own configuration and nothing else did, so it "
            "is the only record that may leave"
        )
        stored = driver.stored(b"D")
        assert [envelope[ENVELOPE_TIMESTAMP] for envelope in stored] == [1]
        assert [envelope[ENVELOPE_RECEIVED] for envelope in stored] == [
            arrival
        ], "'one' must still be waiting, at the arrival time it came in with"

    def test_a_record_left_behind_is_enriched_when_its_own_config_arrives(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        The payoff. On the unfixed code 'one' was emitted with `threshold=None`
        at the sibling's release, 10 ms into a 1000 ms window and 10 ms before
        its own configuration landed.
        """
        driver = buffered(buffer=make_buffer(), on="device")
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["two"] = CONFIG_TWO
        clock.advance_ms(10)
        assert driver.send(b"D", timestamp=2, value={"device": "two"}) != []

        driver.lookup.configs["one"] = CONFIG_ONE
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=3, value={"device": "one"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 3]
        assert [value["threshold"] for value, *_ in emitted] == [1, 1], (
            "the record that waited is enriched from its own configuration, "
            "and records sharing a lookup key keep their arrival order"
        )

    def test_the_kept_records_deadline_is_not_restarted(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        Staying in the buffer must not buy a record a second window. 'one'
        arrives at T, is passed over by a release at T+10, and must time out at
        T + `grace_ms` - not at T + 10 + `grace_ms`.
        """
        driver = buffered(buffer=make_buffer(), on="device")
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["two"] = CONFIG_TWO
        clock.advance_ms(10)
        assert driver.send(b"D", timestamp=2, value={"device": "two"}) != []

        # Exactly `grace_ms` after 'one' arrived, and not a millisecond later.
        clock.advance_ms(GRACE_MS - 10)
        emitted = driver.send(b"other", timestamp=3, value={"device": "three"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [1], (
            "'one' is due now, measured from its own arrival; a restarted "
            "deadline would still have 10ms to run"
        )
        assert [value["region"] for value, *_ in emitted] == ["unknown"]
        assert driver.stored(b"D") == []

    def test_a_kept_record_is_dropped_at_its_own_deadline_not_at_the_release(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        `on_timeout="drop"` is not the release's business either: the record
        stays until its own deadline, then goes without being emitted.
        """
        driver = buffered(buffer=make_buffer(on_timeout="drop"), on="device")
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["two"] = CONFIG_TWO
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=2, value={"device": "two"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [2]
        assert driver.stored(b"D") != [], "'one' is still inside its window"

        clock.advance_ms(GRACE_MS)
        assert driver.send(b"other", timestamp=3, value={"device": "three"}) == []
        assert driver.stored(b"D") == [], "'one' is dropped at its own deadline"


class TestTheIndexFollowsWhatIsLeft:
    """
    The pending index is what the sweep and the deadline tick read. A release
    that leaves records behind and drops the prefix's marker strands them until
    the key's next record - the stranding the tick exists to remove.
    """

    def test_the_index_points_at_the_earliest_record_still_waiting(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        The oldest record is the one that resolves, so the marker has to move
        forward onto the second. On the unfixed code it was deleted outright.
        """
        driver = buffered(buffer=make_buffer(), on="device")
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []
        clock.advance_ms(5)
        second_arrival = _now_ms(clock)
        assert driver.send(b"D", timestamp=2, value={"device": "two"}) == []

        driver.lookup.configs["one"] = CONFIG_ONE
        clock.advance_ms(5)
        emitted = driver.send(b"D", timestamp=3, value={"device": "one"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 3]
        marker = PendingIndex(driver.transaction()).get(prefix_for_key(b"D"))
        assert marker is not None, (
            "the prefix still holds a withheld record, so it must still be "
            "indexed - otherwise nothing will ever sweep it"
        )
        assert marker[0] == second_arrival
        assert driver.pending_keys() == [
            encode_prefix(prefix_for_key(b"D"))
        ], "and its deadline-queue entry has to move with the marker"

    def test_the_prefix_still_leaves_the_index_when_everything_resolves(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        A regression guard, green before and after: the all-resolve release is
        the common case and must still empty the prefix and the index.
        """
        driver = buffered(buffer=make_buffer(), on="device")
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["one"] = CONFIG_ONE
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=2, value={"device": "one"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 2]
        assert driver.stored(b"D") == []
        assert driver.pending_keys() == []


class TestACrowdedMillisecond:
    """
    Two records for one message key can arrive inside the same millisecond, and
    `delete_interval()` addresses whole milliseconds - so a released record
    cannot leave without taking a retained neighbour with it. Those neighbours,
    and only those, are written back.
    """

    def test_a_crowded_millisecond_keeps_only_the_unresolved_record(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = buffered(buffer=make_buffer(), on="device")
        arrival = _now_ms(clock)
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []
        # No clock advance: the same arrival millisecond, two store entries,
        # told apart only by the duplicate counter.
        assert driver.send(b"D", timestamp=2, value={"device": "two"}) == []

        driver.lookup.configs["one"] = CONFIG_ONE
        driver.lookup.configs["three"] = CONFIG_THREE
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=3, value={"device": "three"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 3]

        _flush_to_disk(driver)
        stored = driver.stored(b"D")
        assert [envelope[ENVELOPE_TIMESTAMP] for envelope in stored] == [2]
        assert [envelope[ENVELOPE_RECEIVED] for envelope in stored] == [arrival], (
            "'two' goes back at its own arrival time, so its deadline is "
            "unchanged by having been caught in its neighbour's delete"
        )

    def test_records_rewritten_out_of_a_crowded_millisecond_keep_their_order(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        Two records of one lookup key share the millisecond a third is released
        from. They get fresh duplicate counters, which must ascend in the order
        they were read or their arrival order is lost.
        """
        driver = buffered(buffer=make_buffer(), on="device")
        for timestamp, device in ((1, "one"), (2, "two"), (3, "two")):
            value = {"device": device}
            assert driver.send(b"D", timestamp=timestamp, value=value) == []

        driver.lookup.configs["one"] = CONFIG_ONE
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=4, value={"device": "one"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 4]
        _flush_to_disk(driver)
        stored = driver.stored(b"D")
        assert [envelope[ENVELOPE_TIMESTAMP] for envelope in stored] == [2, 3]

        driver.lookup.configs["two"] = CONFIG_TWO
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=5, value={"device": "two"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [2, 3, 5]
        assert [value["threshold"] for value, *_ in emitted] == [2, 2, 2]


class TestTheTickHonoursARecordLeftBehind:
    """
    The deadline tick does not share `_release()` - it drives
    `BufferSweeper.sweep()`, which never enriches anything. What connects the
    two is the pending index, so these are the tests that prove the index a
    release leaves behind is one the tick can act on.
    """

    def test_the_tick_settles_a_record_left_behind_by_a_release(
        self,
        tick_clock: FakeClock,
        tick_driver: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"), on="device")
        assert driver.send("D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["two"] = CONFIG_TWO
        tick_clock.advance_ms(10)
        assert driver.send("D", timestamp=2, value={"device": "two"}) != []

        # Not one further record of any kind: the clock alone settles it.
        tick_clock.advance_ms(GRACE_MS - 10)
        emitted = driver.tick()

        assert [timestamp for _, _, timestamp, _ in emitted] == [1]
        assert [value["region"] for value, *_ in emitted] == ["unknown"]
        assert driver.stored("D") == []
        assert driver.pending_keys() == []

    def test_the_tick_does_not_advance_past_a_record_left_behind(
        self,
        tick_clock: FakeClock,
        tick_driver: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        The tick caches a per-partition deadline and skips the partition until
        it passes. A release that dropped the prefix's marker made that cache
        read `NO_DEADLINE`, so the tick never looked again - the second tick
        below is the one that catches it.
        """
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"), on="device")
        assert driver.send("D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["two"] = CONFIG_TWO
        tick_clock.advance_ms(10)
        driver.send("D", timestamp=2, value={"device": "two"})

        assert driver.tick() == [], "'one' is 990ms away from its deadline"

        tick_clock.advance_ms(GRACE_MS - 10)
        assert [timestamp for _, _, timestamp, _ in driver.tick()] == [1], (
            "the first tick must have recorded the kept record's deadline, "
            "not concluded the partition holds nothing"
        )


class _RecordingTransaction:
    """The two store calls `ReleasePlan.apply()` can make, captured verbatim."""

    def __init__(self) -> None:
        self.deleted: list[tuple[int, int]] = []
        self.written: list[tuple[int, Any]] = []

    def delete_interval(self, start: int, end: int, prefix: bytes) -> int:
        self.deleted.append((start, end))
        return 0

    def set_for_timestamp(self, timestamp: int, value: Any, prefix: bytes) -> None:
        self.written.append((timestamp, value))


class TestReleasePlanStoreSurgery:
    """
    The property the reproductions can only observe indirectly: a retained
    record keeps its store entry unless a neighbour's delete forces a rewrite.
    """

    def test_a_release_that_frees_nothing_writes_nothing(self) -> None:
        plan = ReleasePlan()
        plan.keep(Withheld(10, None))
        plan.keep(Withheld(20, None))
        transaction = _RecordingTransaction()

        plan.apply(transaction, b"D")

        assert transaction.deleted == []
        assert transaction.written == []
        assert plan.earliest_retained == 10
        assert plan.retained_count == 2

    def test_neighbouring_released_milliseconds_cost_one_delete(self) -> None:
        plan = ReleasePlan()
        for receive_ms in (10, 20, 30):
            plan.release(receive_ms)
        transaction = _RecordingTransaction()

        plan.apply(transaction, b"D")

        assert transaction.deleted == [(10, 31)]
        assert plan.earliest_retained is None
        assert plan.retained_count == 0

    def test_a_retained_millisecond_ends_the_delete_run(self) -> None:
        plan = ReleasePlan()
        plan.release(10)
        plan.keep(Withheld(20, None))
        plan.release(30)
        transaction = _RecordingTransaction()

        plan.apply(transaction, b"D")

        assert transaction.deleted == [(10, 11), (30, 31)], (
            "the retained millisecond must fall in neither range, or the "
            "record in it is deleted without ever being emitted"
        )
        assert transaction.written == []
        assert plan.earliest_retained == 20

    def test_only_a_crowded_retained_record_is_written_back(self) -> None:
        envelope = {"stand-in": "for the stored envelope"}
        plan = ReleasePlan()
        plan.release(10)
        plan.keep(Withheld(10, envelope))
        plan.keep(Withheld(20, None))
        transaction = _RecordingTransaction()

        plan.apply(transaction, b"D")

        assert transaction.deleted == [(10, 11)]
        assert transaction.written == [(10, envelope)], (
            "written back at its own arrival time, so its deadline is the one "
            "it already had"
        )

    def test_crowded_milliseconds_names_only_shared_arrivals(self) -> None:
        survivors = [
            {ENVELOPE_RECEIVED: 10},
            {ENVELOPE_RECEIVED: 20},
            {ENVELOPE_RECEIVED: 20},
            {ENVELOPE_RECEIVED: 30},
        ]

        assert crowded_milliseconds(survivors) == {20}
        assert crowded_milliseconds([]) == set()


class TestTheSnapshotBeatsTheReadingOfTheEnvelope:
    """
    `envelope_value()` restores an envelope's binary leaves **in place**, so by
    the time a record is classified its envelope is no longer storable. The
    snapshot has to be taken first, and only where a rewrite is possible.
    """

    def test_a_snapshot_survives_the_restore_that_ruins_the_original(self) -> None:
        envelope = encode_envelope(
            value={"cert": b"x"}, timestamp=1, receive_ms=7, headers=None
        )
        keepsake = snapshot_for_rewrite(envelope, {7})

        assert envelope_value(envelope) == {"cert": b"x"}

        with pytest.raises(TypeError):
            # The mutated envelope is no longer orjson-encodable, which is
            # why the snapshot is taken before `envelope_value()` runs.
            orjson_dumps(envelope)
        assert orjson_dumps(keepsake), "the snapshot is still what the store holds"
        assert envelope_value(keepsake) == {"cert": b"x"}

    def test_no_snapshot_is_taken_for_a_millisecond_of_its_own(self) -> None:
        envelope = encode_envelope(
            value={"cert": b"x"}, timestamp=1, receive_ms=7, headers=None
        )

        assert snapshot_for_rewrite(envelope, set()) is None
        assert snapshot_for_rewrite(envelope, {8}) is None
