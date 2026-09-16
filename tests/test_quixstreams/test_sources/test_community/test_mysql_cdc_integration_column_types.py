"""
Column-type integration tests for `MySqlCdcSource` against a real MySQL server.

These cover the column types whose *binlog* decoding depends on table metadata the
server only emits when `binlog_row_metadata=FULL`, or on how MySQL renders the value
to text. The snapshot path reads them through pymysql, so every test here compares the
two paths against each other rather than against a hand-written expectation.

Requires Docker.
"""

import json
from typing import Any, Dict

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")
pytest.importorskip("testcontainers.mysql")

from tests.test_quixstreams.test_sources.test_community.test_mysql_cdc_integration import (  # noqa: E501
    DATABASE,
    RunningSource,
    count_kinds,
    execute,
    make_source,
    of_kind,
    oldkeys_of,
    open_connection,
    row_of,
    start_mysql_server,
)


@pytest.fixture(scope="module")
def mysql_server():
    yield from start_mysql_server()


@pytest.fixture()
def mysql(mysql_server):
    yield from open_connection(mysql_server)


def dump(messages: Any) -> str:
    return json.dumps(messages, indent=2, default=str)


def test_enum_and_set_values_survive_the_binlog_path(mysql, mysql_server):
    """
    ENUM and SET values must be the real strings on both paths.

    `pymysqlreplication` populates `Column.enum_values`/`set_values` only from the
    optional table metadata a server emits when `binlog_row_metadata=FULL`, which this
    source raises at start-up. An event written while it was below FULL carries neither
    dictionary, and the `use_column_name_cache` fallback recovers column *names* from
    INFORMATION_SCHEMA, not the value lists - so such an event is refused rather than
    published, and this test's events are all written after the repair.
    """
    execute(mysql, "DROP TABLE IF EXISTS enumset")
    execute(
        mysql,
        "CREATE TABLE enumset (id INT PRIMARY KEY, "
        "status ENUM('new','paid','shipped'), tags SET('a','b'))",
    )
    execute(mysql, "INSERT INTO enumset VALUES (1, 'paid', 'a,b')")

    source = make_source(mysql_server, "enumset", {}, initial_snapshot=True)
    with RunningSource(source) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=1), "the snapshot row"
        )
        execute(mysql, "UPDATE enumset SET status = 'shipped' WHERE id = 1")
        execute(mysql, "INSERT INTO enumset VALUES (2, 'new', 'b')")
        messages = running.wait_for(
            lambda m: count_kinds(m, update=1, insert=1),
            "the update and insert change events",
        )

    snapshot = of_kind(messages, "snapshot_insert")[0]
    updated = of_kind(messages, "update")[0]
    inserted = of_kind(messages, "insert")[0]

    # The snapshot path is the reference: it carries the real values.
    assert row_of(snapshot) == {"id": 1, "status": "paid", "tags": "a,b"}, dump(
        messages
    )

    # The binlog path must agree.
    assert row_of(updated) == {"id": 1, "status": "shipped", "tags": "a,b"}, dump(
        messages
    )
    assert oldkeys_of(updated) == {"id": 1, "status": "paid", "tags": "a,b"}, dump(
        messages
    )
    assert row_of(inserted) == {"id": 2, "status": "new", "tags": "b"}, dump(messages)


def test_binary_column_does_not_crash_the_binlog_reader(mysql, mysql_server):
    """
    A VARBINARY/BLOB column holding non-UTF-8 bytes must reach the topic base64-encoded.

    `RowsEvent.__read_string` decodes with strict errors and only knows the column's
    character set from the metadata a FULL server emits; without it the bytes are read
    as UTF-8 and the source dies on the row. Both paths here run against the repaired
    server, and must agree byte for byte.
    """
    execute(mysql, "DROP TABLE IF EXISTS binary_payload")
    execute(
        mysql,
        "CREATE TABLE binary_payload (id INT PRIMARY KEY, payload VARBINARY(255))",
    )
    execute(mysql, "INSERT INTO binary_payload VALUES (1, %s)", (b"\xff\xfe\x00",))

    source = make_source(mysql_server, "binary_payload", {}, initial_snapshot=True)
    with RunningSource(source) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=1), "the snapshot row"
        )
        execute(mysql, "INSERT INTO binary_payload VALUES (2, %s)", (b"\xff\xfe\x01",))
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1), "the live insert of binary data"
        )

    # The snapshot path base64-encodes the bytes (see `serialize_value`).
    assert row_of(of_kind(messages, "snapshot_insert")[0]) == {
        "id": 1,
        "payload": "//4A",
    }, dump(messages)
    assert row_of(of_kind(messages, "insert")[0]) == {
        "id": 2,
        "payload": "//4B",
    }, dump(messages)


def test_json_column_is_parseable_on_both_paths(mysql, mysql_server):
    """
    A JSON column must reach the topic as JSON on both paths.

    The snapshot emits the document text pymysql returns; the binlog decoder returns a
    Python `dict` whose keys and string values are `bytes`. Both are rendered as
    canonical JSON text, so both must parse.
    """
    execute(mysql, "DROP TABLE IF EXISTS json_col")
    execute(mysql, "CREATE TABLE json_col (id INT PRIMARY KEY, doc JSON)")
    execute(mysql, """INSERT INTO json_col VALUES (1, '{"a": 1, "b": "x"}')""")

    source = make_source(mysql_server, "json_col", {}, initial_snapshot=True)
    with RunningSource(source) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=1), "the snapshot row"
        )
        execute(mysql, """INSERT INTO json_col VALUES (2, '{"a": 2, "b": "y"}')""")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1), "the live insert"
        )

    def document(message: Dict[str, Any]) -> Any:
        value = row_of(message)["doc"]
        try:
            return json.loads(value)
        except (TypeError, ValueError) as exc:
            raise AssertionError(
                f"{message['kind']} carried an unparseable JSON column {value!r}: {exc}"
            ) from exc

    assert document(of_kind(messages, "snapshot_insert")[0]) == {"a": 1, "b": "x"}
    assert document(of_kind(messages, "insert")[0]) == {"a": 2, "b": "y"}
    assert of_kind(messages, "insert")[0]["schema"] == DATABASE


def test_float_and_double_columns_agree_on_both_paths(mysql, mysql_server):
    """
    A FLOAT and a DOUBLE must carry one value, whichever path the row arrives on.

    The binlog carries the stored 4- or 8-byte value; `SELECT` renders a FLOAT with six
    significant digits and a DOUBLE shortest-round-trip, and the snapshot path ships
    what `SELECT` returns. This is the comparison, made by the server rather than
    asserted from memory: the same row read both ways.
    """
    execute(mysql, "DROP TABLE IF EXISTS floats")
    execute(
        mysql,
        "CREATE TABLE floats (id INT PRIMARY KEY, f FLOAT, d DOUBLE, n FLOAT)",
    )
    insert = "INSERT INTO floats VALUES (%s, %s, %s, NULL)"
    values = [
        ("a third", 1 / 3),
        ("money", 19.99),
        ("close to the float maximum", 3.402823466e38),
        ("negative", -7.7),
    ]
    for row_id, (_, value) in enumerate(values, start=1):
        execute(mysql, insert, (row_id, value, value))

    source = make_source(mysql_server, "floats", {}, initial_snapshot=True)
    with RunningSource(source) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=len(values)), "the snapshot rows"
        )
        for row_id, (_, value) in enumerate(values, start=1):
            execute(mysql, insert, (row_id + 100, value, value))
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=len(values)), "the live inserts"
        )

    snapshots = {
        row_of(m)["id"]: row_of(m) for m in of_kind(messages, "snapshot_insert")
    }
    inserts = {row_of(m)["id"]: row_of(m) for m in of_kind(messages, "insert")}

    for row_id, (label, _) in enumerate(values, start=1):
        from_snapshot = snapshots[row_id]
        from_binlog = inserts[row_id + 100]
        assert from_binlog["f"] == from_snapshot["f"], (
            f"FLOAT {label} diverged: binlog {from_binlog['f']!r} vs snapshot "
            f"{from_snapshot['f']!r}\n{dump(messages)}"
        )
        assert from_binlog["d"] == from_snapshot["d"], (
            f"DOUBLE {label} diverged: binlog {from_binlog['d']!r} vs snapshot "
            f"{from_snapshot['d']!r}\n{dump(messages)}"
        )
        assert from_binlog["n"] is None and from_snapshot["n"] is None
