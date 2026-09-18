"""
Red-first reproduction tests for the second external review of PR 1110, on top
of the deadline-tick branch.

Three blockers, two of them reproduced here:

- **A** - `PendingIndex.due()` read every overdue prefix on every record, so the
  per-record cost grew with the backlog rather than with what a sweep pass can
  settle. Its replacement test lives in `test_lookup_buffer_findings.py`
  (`TestPendingIndexScaling`), where the round-5 test it retires lived.
- **B** - `TestBinaryAndContainerValuesSurviveTheEnvelope` below. The earlier
  fix for a `bytes`-valued field (`f6f8e76c`, defect 2 of five) only walked
  `dict` and `list`, so a `bytes` inside a `tuple`, a `bytearray` or a
  `memoryview` still reached orjson and raised `StateSerializationError` from
  the record path - before the offset is committed, hence forever.
- **C** - `TestAnUnkeyedTopicIsBufferable` below. `prefix_for_key(None)` raised,
  and the default key deserializer returns `None` for an unkeyed topic, so every
  record on such a topic died on arrival - even with an `on=` that resolves the
  lookup key from the value perfectly well.

`TestMessageKeyGroupingIsNotAnEnrichmentBug` answers the question the review
raised alongside C: the storage prefix is the *message* key while resolvability
is decided by the *lookup* key, so records that share a message key are examined
together whatever their lookup keys are. Round 4 changed what "examined
together" costs them - see the class docstring.

Traces to `dev-planning/lookup-deadline-tick/bugs-round3.md`.
"""

from typing import Any

import pytest

from quixstreams.dataframe.joins.lookups.buffer_envelope import (
    ENVELOPE_VALUE,
    decode_headers,
    encode_envelope,
    envelope_value,
)
from quixstreams.dataframe.joins.lookups.buffer_state import (
    INDEX_PREFIX,
    NULL_KEY_PREFIX,
    QUEUE_PREFIX,
    key_from_prefix,
    key_kind,
    prefix_for_key,
)
from quixstreams.utils.json import dumps as orjson_dumps
from quixstreams.utils.json import loads as orjson_loads
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    GRACE_MS,
    ConfigField,
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
# Blocker B: a `bytes` inside a tuple, bytearray or memoryview crash-loops
# ---------------------------------------------------------------------------
#
# The envelope lifts binary leaves out of band before the store sees them.
# orjson encodes a tuple as an array and cannot encode `bytes`, `bytearray` or
# `memoryview`, so each is lifted and restored by path, with a kind tag so a
# tuple does not come back as a list.
#
# The fix lifts binary leaves *and* the container types orjson cannot round-trip
# faithfully, both addressed by path (`buffer_envelope.py`). A tuple needs a tag
# of its own and not merely an escape from the crash: orjson would give it back
# as a `list`, silently changing the type of a value a user put there.


def _round_trip(value: Any) -> Any:
    """
    Put a value through the envelope and the store's real serializer.

    `orjson_dumps` is exactly what `PartitionTransaction.set()` calls, so a
    value that cannot survive this cannot be buffered at all.
    """
    envelope = encode_envelope(value=value, timestamp=10, receive_ms=20, headers=None)
    try:
        stored = orjson_dumps(envelope)
    except TypeError as exc:
        pytest.fail(
            f"The envelope for {value!r} is not serializable by the store's "
            f"own dumps: {exc}. On the record path this is a "
            f"StateSerializationError raised before the offset is committed, "
            f"i.e. a crash-loop on redelivery."
        )
    return envelope_value(orjson_loads(stored))


class TestBinaryAndContainerValuesSurviveTheEnvelope:
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param({"certs": (b"a", b"b")}, id="tuple-of-bytes"),
            pytest.param({"certs": [b"a", b"b"]}, id="list-of-bytes"),
            pytest.param({"certs": {"inner": b"a"}}, id="dict-of-bytes"),
            pytest.param({"certs": ({"inner": (b"a",)},)}, id="nested-tuple"),
            pytest.param({"pairs": [(1, b"a"), (2, b"b")]}, id="list-of-tuples"),
            pytest.param({"tags": {"x", "y"}}, id="set"),
            pytest.param({"tags": frozenset({"x", "y"})}, id="frozenset"),
            pytest.param({"tags": {b"x"}}, id="set-of-bytes"),
            pytest.param({"empty": ()}, id="empty-tuple"),
            pytest.param({"plain": {"a": [1, "2", None, True]}}, id="already-json"),
        ],
    )
    def test_types_survive_the_store_round_trip(self, value: Any) -> None:
        """
        Equality is the assertion, and it is enough: a `tuple` that came back a
        `list` is not equal to the tuple, and a lost `bytes` leaf is not equal
        to the `None` left in its place.
        """
        assert _round_trip(value) == value

    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param({"cert": bytearray(b"a")}, b"a", id="bytearray"),
            pytest.param({"cert": memoryview(b"a")}, b"a", id="memoryview"),
        ],
    )
    def test_a_binary_buffer_becomes_bytes(self, value: Any, expected: Any) -> None:
        """
        `bytearray` and `memoryview` are coerced rather than tagged, so they come
        back as `bytes`. Deliberate and documented (`buffer_envelope.py`'s module
        docstring); the alternative is a tag per binary flavour for no gain.
        `bytearray(b"a") == b"a"`, so the type has to be asserted explicitly.
        """
        restored = _round_trip(value)["cert"]
        assert restored == expected
        assert type(restored) is bytes

    def test_a_value_that_needs_no_lift_is_not_copied(self) -> None:
        """
        The fast path the common case depends on: no bytes, no non-list
        container, so the walk is read-only and the record's own dict is stored.
        """
        value = {"a": [1, {"b": "c"}]}
        envelope = encode_envelope(
            value=value, timestamp=10, receive_ms=20, headers=None
        )

        assert envelope[ENVELOPE_VALUE] is value

    def test_headers_carrying_a_binary_buffer_survive(self) -> None:
        """
        `HeadersValue` is `str | bytes`, but nothing enforces that on a header an
        `sdf.apply()` has just written, and the header encoder's `else` branch
        stores the object verbatim for orjson to reject.
        """
        envelope = encode_envelope(
            value={},
            timestamp=10,
            receive_ms=20,
            headers=[("a", bytearray(b"x")), ("b", "plain")],
        )

        restored = decode_headers(orjson_loads(orjson_dumps(envelope)))
        assert restored == [("a", b"x"), ("b", "plain")]


class TestATupleValuedFieldReachesTheStore:
    """
    Blocker B through the real record path, which is where it crash-loops.

    A `lookup.bytes_field()` resolving to several certificates is the realistic
    shape: the field's `default` is what `missing()` returns for an unresolved
    record, so it is written into the value *before* the record is withheld.
    """

    def test_a_tuple_of_bytes_is_buffered_and_emitted_with_its_type(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        fields = {
            **make_fields(),
            "certs": ConfigField(
                type="cert", default=(b"first", b"second"), source="certs"
            ),
        }
        driver = buffered(buffer=make_buffer(), fields=fields)

        # Unresolvable, so it is withheld - and withholding it serializes it.
        assert driver.send("D", timestamp=1) == []

        # Nothing configures it, so it leaves through the timeout path, which
        # emits the stored value verbatim. Another key's record drives the sweep.
        clock.advance_ms(GRACE_MS + 1)
        emitted = driver.send("other", timestamp=2)

        assert len(emitted) == 1, f"expected the timed-out record only: {emitted}"
        value, key, timestamp, _ = emitted[0]
        assert key == "D"
        assert timestamp == 1
        assert value["certs"] == (b"first", b"second")


# ---------------------------------------------------------------------------
# Blocker C: a null message key crash-looped on the first record
# ---------------------------------------------------------------------------
#
# `prefix_for_key(None)` raised `ValueError`, and an unkeyed topic deserializes
# every key to `None` (`models/topics/topic.py`'s default key deserializer). So
# the buffer killed the application on record one of an unkeyed topic, before
# the lookup ran - including the entirely ordinary topology where `on=` reads
# the lookup key out of the value and the message key is irrelevant to the join.
#
# The fix gives the null key a reserved prefix. The proof that it cannot collide
# is the escaping's own invariant: every 0x7F in an escaped prefix starts an
# escape sequence whose second byte is 0x01 or 0x02, so no real key can produce
# `7f 00`.


class TestTheNullKeySentinel:
    def test_the_null_key_maps_to_the_reserved_prefix(self) -> None:
        assert prefix_for_key(None) == NULL_KEY_PREFIX

    @pytest.mark.parametrize(
        "key",
        [
            b"\x7f\x00",
            b"\x7f",
            b"\x00",
            b"",
            b"|",
            "",
            "\x7f\x00",
            "key",
            INDEX_PREFIX + b"x",
            QUEUE_PREFIX + b"x",
        ],
    )
    def test_no_real_key_can_produce_the_sentinel(self, key: Any) -> None:
        """
        The adversarial list includes the sentinel's own bytes: they escape to
        `7f 02 00`, because the escape byte is escaped first.
        """
        assert prefix_for_key(key) != NULL_KEY_PREFIX

    def test_the_sentinel_is_clear_of_the_reserved_namespaces(self) -> None:
        assert NULL_KEY_PREFIX not in (INDEX_PREFIX, QUEUE_PREFIX)
        # Neither namespace's keys can sort inside a scan of the sentinel, and
        # vice versa: `0x5f` < `0x7f`, and neither prefix contains the SEPARATOR.
        assert NULL_KEY_PREFIX[:1] > INDEX_PREFIX[:1]
        assert NULL_KEY_PREFIX[:1] > QUEUE_PREFIX[:1]

    def test_the_null_key_round_trips_through_the_index(self) -> None:
        """
        The sweep path rebuilds the key from the prefix and the recorded kind.
        The sentinel is a perfectly good `bytes` key as far as the prefix alone
        goes, so only the kind can say the key was absent.
        """
        assert key_from_prefix(prefix_for_key(None), key_kind(None)) is None

    def test_a_key_of_an_unsupported_type_is_still_rejected(self) -> None:
        with pytest.raises(ValueError, match="int"):
            prefix_for_key(7)


class TestAnUnkeyedTopicIsBufferable:
    """
    Blocker C through the record path, in the topology the review named: the
    lookup key comes from the value, so the join works and only the storage
    prefix ever needed the message key.
    """

    def test_an_unkeyed_record_is_withheld_and_released(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        driver = buffered(buffer=make_buffer(), on="device")

        assert driver.send(None, timestamp=1, value={"device": "one"}) == []
        assert driver.stored(None) != [], "the record was not withheld anywhere"

        driver.lookup.configs["one"] = {"threshold": 1, "region": "eu"}
        clock.advance_ms(10)
        emitted = driver.send(None, timestamp=2, value={"device": "one"})

        assert [key for _, key, _, _ in emitted] == [None, None]
        assert [value["threshold"] for value, *_ in emitted] == [1, 1]
        assert [timestamp for _, _, timestamp, _ in emitted] == [1, 2]

    def test_an_unkeyed_record_times_out_under_the_null_key(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        The sweep path, which has only the prefix and the recorded kind to work
        from. A key rebuilt as `b"\\x7f\\x00"` instead of `None` would repartition
        the record downstream.
        """
        driver = buffered(buffer=make_buffer(), on="device")

        assert driver.send(None, timestamp=1, value={"device": "one"}) == []

        clock.advance_ms(GRACE_MS + 1)
        emitted = driver.send("keyed", timestamp=2, value={"device": "two"})

        assert len(emitted) == 1, f"expected the timed-out record only: {emitted}"
        value, key, timestamp, _ = emitted[0]
        assert key is None
        assert timestamp == 1
        assert value["region"] == "unknown", "emitted with its declared defaults"


# ---------------------------------------------------------------------------
# The question the review raised alongside C
# ---------------------------------------------------------------------------


class TestMessageKeyGroupingIsNotAnEnrichmentBug:
    """
    The evidence for the answer to the review's question about grouping.

    The storage prefix is the message key; resolvability is decided by the lookup
    key. So records sharing a message key are examined together whatever their
    lookup keys are, and records sharing a lookup key are split if their message
    keys differ. No record can ever be enriched with another key's configuration:
    the arriving record is joined under its own lookup key before the index is
    consulted, and every survivor is re-joined under its own on release
    (`buffer_operator.py`'s `__call__` and `_release`).

    Round 4 changed what "examined together" costs a record. Grouping used to
    decide *when* a record's wait ended as well: a release was all-or-nothing
    per message key, so a resolving record ended the wait of everything queued
    under that message key, including one whose own lookup key had not resolved
    and whose grace window was still open. That is the behaviour the first test
    below used to characterize, and it is now gone - the release is per record
    (`buffer_release.py`, `open-points.md` §5, architecture §10). What survives
    of the grouping property is the read: a release deserializes every record
    withheld under the message key, and emits the ones that resolve.

    The second test is unchanged from round 3 and still green: splitting by
    message key was never the problem.
    """

    def test_a_sibling_resolution_does_not_end_a_records_wait(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        Round 3 asserted the opposite of this: `one` used to leave 990 ms before
        its own deadline because `two`, which merely shares its message key,
        found a configuration. Now only `two` leaves.
        """
        driver = buffered(buffer=make_buffer(), on="device")

        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["two"] = {"threshold": 2, "region": "us"}
        clock.advance_ms(10)
        emitted = driver.send(b"D", timestamp=2, value={"device": "two"})

        assert [timestamp for _, _, timestamp, _ in emitted] == [2], (
            "only the record whose own lookup key resolved may be emitted; "
            "'one' still has 990ms of its grace window left"
        )
        assert [value["threshold"] for value, *_ in emitted] == [2]
        stored = driver.stored(b"D")
        assert [envelope["t"] for envelope in stored] == [1], (
            "'one' must still be withheld, waiting for its own configuration "
            "or its own deadline"
        )

    def test_two_message_keys_sharing_a_lookup_key_are_settled_apart(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        """
        The other half: the same lookup key under two message keys is two
        independent buffers. Releasing one leaves the other waiting, and both
        are enriched from the one configuration when their turn comes.
        """
        driver = buffered(buffer=make_buffer(), on="device")

        assert driver.send(b"A", timestamp=1, value={"device": "one"}) == []
        assert driver.send(b"B", timestamp=2, value={"device": "one"}) == []

        driver.lookup.configs["one"] = {"threshold": 1, "region": "eu"}
        clock.advance_ms(10)
        emitted = driver.send(b"A", timestamp=3, value={"device": "one"})

        assert [key for _, key, _, _ in emitted] == [b"A", b"A"]
        assert [value["threshold"] for value, *_ in emitted] == [1, 1]
        assert driver.stored(b"B") != [], (
            "b'B' shares the lookup key but not the message key, so b'A's "
            "release must not touch it"
        )
