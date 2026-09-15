"""
Red-first reproduction tests for round 5: a record the buffer cannot serialize
must not crash-loop the application.

Withholding a record means writing it to a JSON-backed state store, and some
record values cannot go in one - a dict with non-`str` keys (ordinary output of
an upstream `apply()` that counts by id), an arbitrary object, an integer
outside 64 bits, a reference cycle. Round 3 widened the envelope to lift every
shape an encoding *can* rescue; these are the residue, and until this round they
raised `StateSerializationError` from inside `set_for_timestamp()`. That is the
worst place for it: `PartitionTransaction.set()` marks the transaction FAILED
and re-raises, so the checkpoint is lost, the offset is never committed, and
redelivery reproduces the crash on the same record forever - on precisely the
path the buffer exists to serve.

`BufferOperator._buffer()` now builds the envelope and offers it to the store
transaction's own value serializer *before* the write. A record that survives
both is buffered exactly as before. A record that does not is never written: it
is settled immediately by `on_timeout`, the same answer a record that ran out of
grace gets, with a rate-limited warning naming the key and the offending path.

Every test in the first four classes raises `StateSerializationError` (or, for
the cycle, `RecursionError`) out of `driver.send()` on the unfixed code, which
is the defect itself. `TestDescribeUnstorable` and `TestTheLimiterStaysBounded`
are unit tests of code this round introduces and cannot be red before it.

Traces to `dev-planning/lookup-deadline-tick/open-points.md` §4.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import pytest

from quixstreams.dataframe.joins.lookups.buffer_bookkeeping import (
    MAX_RATE_LIMITED_KEYS,
    BufferBookkeeping,
)
from quixstreams.dataframe.joins.lookups.buffer_envelope import (
    ENVELOPE_TIMESTAMP,
    ENVELOPE_VALUE,
    describe_unstorable,
    encode_envelope,
)
from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.serialization import serialize
from quixstreams.utils.json import dumps as orjson_dumps
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    buffered as buffered,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    clock as clock,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    make_buffer,
)

# The shapes no encoding in `buffer_envelope.py` can rescue, one per row, as a
# user would write them into a record value.
NON_STR_KEYS = {"counts": {1: 5}}
ARBITRARY_OBJECT = {"seen_at": datetime(2026, 9, 15, tzinfo=timezone.utc)}
HUGE_INTEGER = {"nonce": 2**64}

BUFFER_LOGGER = "quixstreams.dataframe.joins.lookups.buffer_bookkeeping"


def probe(value: Any) -> bytes:
    """Serialize the way a default-configured state store would."""
    return serialize(value, dumps=orjson_dumps)


def cyclic_value() -> dict:
    """A record value holding a reference cycle."""
    loop: dict = {"name": "loop"}
    loop["self"] = loop
    return {"nested": loop}


def unstorable_warnings(caplog: Any) -> list:
    """The `log_unstorable()` lines captured so far."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == BUFFER_LOGGER and "could not store" in record.getMessage()
    ]


class TestARecordTheStoreCannotHoldIsSettledNotRaised:
    """
    The headline, through the real record path. Every test here raises out of
    `driver.send()` on the unfixed code.
    """

    @pytest.mark.parametrize(
        "value",
        [NON_STR_KEYS, ARBITRARY_OBJECT, HUGE_INTEGER],
        ids=["non-str-dict-key", "arbitrary-object", "huge-integer"],
    )
    def test_it_is_emitted_with_its_defaults_under_on_timeout_emit(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
        value: dict,
    ) -> None:
        driver = buffered(buffer=make_buffer())

        emitted = driver.send("D", timestamp=1, value=value)

        assert [timestamp for _, _, timestamp, _ in emitted] == [1], (
            "the record cannot be held, so it is settled now rather than at a "
            "deadline it will never reach"
        )
        settled = emitted[0][0]
        assert settled["threshold"] is None
        assert settled["region"] == "unknown", (
            "settled means resolved through every field's `missing()`, exactly "
            "as a timed-out record is - `lookup.join()` already wrote them"
        )
        assert driver.stored("D") == []
        assert (
            driver.pending_keys() == []
        ), "nothing was withheld, so nothing may be waiting in the index"

    @pytest.mark.parametrize(
        "value",
        [NON_STR_KEYS, ARBITRARY_OBJECT, HUGE_INTEGER],
        ids=["non-str-dict-key", "arbitrary-object", "huge-integer"],
    )
    def test_it_is_discarded_under_on_timeout_drop(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
        value: dict,
    ) -> None:
        driver = buffered(buffer=make_buffer(on_timeout="drop"))

        assert driver.send("D", timestamp=1, value=value) == []
        assert driver.stored("D") == []
        assert driver.pending_keys() == []

    def test_a_reference_cycle_is_settled_too(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        A cycle never reaches the serializer: `encode_envelope()` walks the
        value without cycle detection and exhausts the stack first. Same record,
        same crash-loop, so the guard covers `RecursionError` as well.
        """
        driver = buffered(buffer=make_buffer())

        emitted = driver.send("D", timestamp=1, value=cyclic_value())

        assert [timestamp for _, _, timestamp, _ in emitted] == [1]
        assert driver.stored("D") == []

    def test_an_unstorable_header_value_settles_the_record(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        Headers ride inside the envelope too. Binary values are base64'd, but a
        header holding an arbitrary object is refused like any other.
        """
        driver = buffered(buffer=make_buffer())

        emitted = driver.send(
            "D",
            timestamp=1,
            value={"device": "one"},
            headers=[("seen", datetime(2026, 9, 15, tzinfo=timezone.utc))],
        )

        assert [timestamp for _, _, timestamp, _ in emitted] == [1]
        assert driver.stored("D") == []


class TestTheTransactionSurvives:
    """
    The point of pre-validating rather than catching: the checkpoint has to be
    committable afterwards, or the offset is still never committed and the
    crash-loop is merely quieter.
    """

    def test_the_transaction_is_not_poisoned(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = buffered(buffer=make_buffer())

        driver.send("D", timestamp=1, value=NON_STR_KEYS)

        assert not driver.transaction().failed

    def test_later_records_still_buffer_and_reach_disk(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = buffered(buffer=make_buffer())
        driver.send("D", timestamp=1, value=NON_STR_KEYS)

        assert driver.send("E", timestamp=2, value={"device": "two"}) == []

        flushed = driver.flushed("E")
        assert [envelope[ENVELOPE_TIMESTAMP] for envelope in flushed] == [2], (
            "the refused record left the transaction usable, so the next "
            "record is withheld and the checkpoint commits"
        )


class TestARefusedRecordDoesNotJoinAQueue:
    """
    The second call site: a key that is already holding records. The refused
    record cannot queue behind them, so it overtakes them - the ordering trade
    this fix makes deliberately, in exchange for not crash-looping.
    """

    def test_it_leaves_while_its_sibling_keeps_waiting(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = buffered(buffer=make_buffer())
        assert driver.send("D", timestamp=1, value={"device": "one"}) == []

        clock.advance_ms(10)
        emitted = driver.send("D", timestamp=2, value=NON_STR_KEYS)

        assert [timestamp for _, _, timestamp, _ in emitted] == [2]
        stored = driver.stored("D")
        assert [envelope[ENVELOPE_TIMESTAMP] for envelope in stored] == [1], (
            "the record that can be held keeps its place and its own deadline; "
            "only the one that cannot leaves early"
        )


class TestTheGuardAsksTheStoresOwnSerializer:
    """
    The check must be the store's, not a hardcoded `orjson.dumps`. A store built
    with its own `dumps` would otherwise disagree with the guard, and the
    crash-loop would be back for exactly the values that store refuses.
    """

    def test_a_value_only_this_store_refuses_is_settled(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
        monkeypatch: Any,
    ) -> None:
        driver = buffered(buffer=make_buffer())
        transaction = driver.transaction()
        accepted = transaction._serialize_value

        def refusing(value: Any) -> bytes:
            # Perfectly good JSON; this store simply will not take it.
            if "REFUSED" in repr(value):
                raise StateSerializationError("this store's dumps refuses it")
            return accepted(value)

        monkeypatch.setattr(transaction, "_serialize_value", refusing)

        emitted = driver.send("D", timestamp=1, value={"shape": "REFUSED"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [1], (
            "a guard hardcoded to orjson would have buffered this record and "
            "then failed the transaction on the write"
        )
        assert driver.stored("D") == []


class TestTheWarning:
    """
    Silent data degradation otherwise: the record went downstream unenriched, or
    vanished, and nothing in the value says why.
    """

    def test_it_names_the_key_and_the_path(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
        caplog: Any,
    ) -> None:
        driver = buffered(buffer=make_buffer())

        with caplog.at_level(logging.WARNING, logger=BUFFER_LOGGER):
            driver.send("D", timestamp=1, value=NON_STR_KEYS)

        warnings = unstorable_warnings(caplog)
        assert len(warnings) == 1
        assert "'D'" in warnings[0]
        assert "value['counts']" in warnings[0]
        assert "not strings" in warnings[0]
        assert "1 (int)" in warnings[0]

    def test_it_is_rate_limited_per_key(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
        caplog: Any,
    ) -> None:
        driver = buffered(buffer=make_buffer())

        with caplog.at_level(logging.WARNING, logger=BUFFER_LOGGER):
            for timestamp in range(3):
                driver.send("D", timestamp=timestamp, value=NON_STR_KEYS)
            driver.send("E", timestamp=9, value=NON_STR_KEYS)

        assert len(unstorable_warnings(caplog)) == 2, (
            "the first event for a key reports immediately and the rest are "
            "folded into its window - one line per key, not per record"
        )


class TestTheLimiterStaysBounded:
    """
    The first review's unbounded-dict defect must not come back through the new
    limiter: a deployment cycling through per-device keys would grow it forever.
    """

    def test_the_unstorable_limiter_is_capped(self) -> None:
        bookkeeping = BufferBookkeeping()

        for index in range(MAX_RATE_LIMITED_KEYS + 50):
            bookkeeping.log_unstorable(f"k{index}".encode(), index, lambda: "shape")

        assert len(bookkeeping._unstorable_log) == MAX_RATE_LIMITED_KEYS

    def test_the_description_is_not_produced_while_suppressed(self) -> None:
        bookkeeping = BufferBookkeeping()
        produced = []

        def describe() -> str:
            produced.append(1)
            return "shape"

        bookkeeping.log_unstorable(b"k", "k", describe)
        bookkeeping.log_unstorable(b"k", "k", describe)

        assert produced == [1], (
            "producing the description re-serializes the value, so it must not "
            "be paid on the records the limiter suppresses"
        )


class TestDescribeUnstorable:
    """
    The diagnosis. It asks the same serializer that refused the write, so it
    cannot name a shape that store would actually have accepted.
    """

    def envelope_for(self, value: Any, headers: Any = None) -> dict:
        return encode_envelope(value=value, timestamp=1, receive_ms=2, headers=headers)

    def test_it_walks_to_the_deepest_refused_node(self) -> None:
        envelope = self.envelope_for({"a": {"b": {1: 5}}})

        assert describe_unstorable(envelope, probe) == (
            "value['a']['b']: a dict whose keys are not strings: 1 (int)"
        )

    def test_it_names_a_list_index(self) -> None:
        envelope = self.envelope_for({"rows": [{"ok": 1}, {2: 3}]})

        assert describe_unstorable(envelope, probe) == (
            "value['rows'][1]: a dict whose keys are not strings: 2 (int)"
        )

    def test_an_arbitrary_object_is_named_by_its_type(self) -> None:
        envelope = self.envelope_for(ARBITRARY_OBJECT)

        assert describe_unstorable(envelope, probe) == (
            "value['seen_at']: a value of type datetime"
        )

    def test_an_integer_outside_64_bits_says_so(self) -> None:
        envelope = self.envelope_for(HUGE_INTEGER)

        assert describe_unstorable(envelope, probe) == (
            "value['nonce']: an integer outside the 64-bit range"
        )

    def test_a_header_value_is_named_under_headers(self) -> None:
        envelope = self.envelope_for(
            {"device": "one"},
            headers=[("seen", datetime(2026, 9, 15, tzinfo=timezone.utc))],
        )

        assert describe_unstorable(envelope, probe) == (
            "headers[0][2]: a value of type datetime"
        )

    def test_a_reference_cycle_is_reported_as_one(self) -> None:
        """
        The stand-in mapping `_buffer()` builds when `encode_envelope()` itself
        could not finish, which is what a cycle does to it.
        """
        loop: dict = {"name": "loop"}
        loop["self"] = loop

        assert describe_unstorable({ENVELOPE_VALUE: loop}, probe) == (
            "value['self']: a reference cycle"
        )

    def test_the_root_is_named_when_nothing_below_it_is_refused(self) -> None:
        assert describe_unstorable({ENVELOPE_VALUE: 1, 7: "x"}, probe) == (
            "the envelope itself: a dict whose keys are not strings: 7 (int)"
        )
