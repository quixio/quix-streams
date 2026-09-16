"""
Column-type integration tests for `MySqlCdcSource` against a real MySQL server.

These cover the column types whose *binlog* decoding depends on table metadata the
server only emits when `binlog_row_metadata=FULL`. The snapshot path reads them
through pymysql and is unaffected, so every test here compares the two paths.

They are kept apart from `test_mysql_cdc_integration.py` because, unlike that file,
they currently FAIL: each one documents a live defect. See the module-level notes on
each test for the mechanism.

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
    optional table metadata a server emits when `binlog_row_metadata=FULL`. On the
    MySQL 8.x default (MINIMAL) they stay `None`, and `row_event.py` reads the ENUM
    (and SET) bytes off the wire and then returns `None` for the value. The
    `use_column_name_cache` fallback recovers column *names* from INFORMATION_SCHEMA,
    not the ENUM/SET value dictionaries, so the names are right and the values are
    silently gone.
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
    A VARBINARY/BLOB column holding non-UTF-8 bytes must not kill the source.

    Under non-FULL row metadata `Column.character_set_name` is `None`, so
    `RowsEvent.__read_string` falls through to `bytes.decode()` with strict errors -
    a UTF-8 decode of raw binary. `MySqlCdcSource` never sets the library's
    `ignore_decode_errors` flag, so the `UnicodeDecodeError` propagates out of
    `read_changes`. It is not a connection error, so `_stream_changes` re-raises it
    immediately and the process dies; on restart it resumes at the same committed
    position and hits the same row again.
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

    The snapshot emits the document text pymysql returns. The binlog decoder returns a
    Python `dict` whose keys and string values are `bytes`, which `serialize_value`
    cannot recognise and falls back to `str()` on - producing `"{b'a': 2, b'b': b'y'}"`,
    a Python repr no JSON parser accepts.
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
