"""
Live-MySQL tests for `MySqlCdcLiteSource`.

Kafka is stubbed - the producer is a list and the state store is a dict. MySQL is a
real container configured the way `docs/connectors/sources/mysql-cdc-lite-source.md`
tells an operator to configure it: `binlog_format=ROW`, `binlog_row_image=FULL`,
`binlog_row_metadata=FULL`, and the documented grants, and nothing more.

Four of these tests exist to keep that page honest and will fail if MySQL stops
behaving the way it records.

Requires Docker.
"""

import json
import threading
import time
from collections import Counter
from typing import Any, Callable, Dict, List, Optional

import pymysql
import pytest
from testcontainers.mysql import MySqlContainer

from quixstreams.sources.community.mysql_cdc_lite import (
    MySqlCdcLiteError,
    MySqlCdcLiteSource,
)
from tests.utils import ConfluentKafkaMessageStub

MYSQL_IMAGE = "mysql:8.0"
DATABASE = "test_db"
CDC_USER = "cdc"
CDC_PASSWORD = "cdc_password"
ROOT_PASSWORD = "root_password"

# What the docs page tells the operator to set. Nothing here is checked by the source
# except binlog_format.
MYSQL_COMMAND = (
    "--server-id=1 --log-bin=mysql-bin --binlog-format=ROW "
    "--binlog-row-image=FULL --binlog-row-metadata=FULL"
)


class _DictState:
    """A `State` stand-in backed by a plain dict."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value


class _CapturingProducer:
    """An `InternalProducer` stand-in that keeps the produced `(key, value)` pairs."""

    def __init__(self):
        self._lock = threading.Lock()
        self._messages: List[Any] = []

    def produce(self, topic: str, value=None, key=None, **kwargs: Any) -> None:
        with self._lock:
            self._messages.append((key, value))

    def flush(self, timeout: Optional[float] = None) -> int:
        return 0

    def snapshot(self) -> List[Any]:
        with self._lock:
            return list(self._messages)


class LiteHarness(MySqlCdcLiteSource):
    """`MySqlCdcLiteSource` with only the Kafka side replaced."""

    def __init__(self, state_data: Dict[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._state = _DictState(state_data)
        self._capturing_producer = _CapturingProducer()
        self.polled = threading.Event()
        self.buffered = threading.Event()
        self.configure(topic=self.default_topic(), producer=self._capturing_producer)

    @property
    def state(self) -> _DictState:  # type: ignore[override]
        return self._state

    def flush(self, timeout: Optional[float] = None) -> None:
        if self.producer.flush(timeout) > 0:
            raise AssertionError("stub producer failed to flush")

    def _read_changes(self) -> None:
        super()._read_changes()
        self.polled.set()
        if self._buffer:
            self.buffered.set()

    def received(self) -> List[Dict[str, Any]]:
        """Produced messages, decoded from the topic's JSON serializer."""
        return [
            dict(json.loads(value), _key=key)
            for key, value in self._capturing_producer.snapshot()
        ]


class RunningSource:
    """Runs a source in a thread, stops it on exit, and surfaces what it raised."""

    def __init__(self, source: LiteHarness, timeout: float = 30.0):
        self.source = source
        self._timeout = timeout
        self._error: Optional[BaseException] = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            self.source.start()
        except BaseException as exc:
            self._error = exc

    def __enter__(self) -> "RunningSource":
        self._thread.start()
        # stop() before start() sets `running` would be swallowed, so wait for the
        # source to be marked running before the body can ask it to stop.
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            if self.source.running or self._error is not None:
                break
            time.sleep(0.01)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.source.stop()
        self._thread.join(timeout=self._timeout)
        if exc_type is None and self._error is not None:
            raise self._error
        return False

    def wait_for(
        self,
        predicate: Callable[[List[Dict[str, Any]]], bool],
        description: str,
        timeout: float = 30.0,
    ) -> List[Dict[str, Any]]:
        """:return: the produced messages once `predicate` accepts them."""
        deadline = time.monotonic() + timeout
        while True:
            messages = self.source.received()
            if predicate(messages):
                return messages
            if self._error is not None:
                raise AssertionError(
                    f"the source died while waiting for {description}: {self._error}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"timed out waiting for {description}; got {messages}"
                )
            time.sleep(0.1)


def execute(connection, statement: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(statement)
    connection.commit()


def of_kind(messages: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    return [message for message in messages if message["kind"] == kind]


def count_kinds(messages: List[Dict[str, Any]], **expected: int) -> bool:
    counts = Counter(message["kind"] for message in messages)
    return all(counts[kind] == count for kind, count in expected.items())


def row_of(message: Dict[str, Any]) -> Dict[str, Any]:
    return dict(zip(message["columnnames"], message["columnvalues"]))


def oldkeys_of(message: Dict[str, Any]) -> Dict[str, Any]:
    oldkeys = message["oldkeys"]
    return dict(zip(oldkeys["keynames"], oldkeys["keyvalues"]))


@pytest.fixture(scope="module")
def mysql_server():
    """A MySQL container configured exactly as the docs page requires."""
    container = MySqlContainer(
        MYSQL_IMAGE,
        username=CDC_USER,
        password=CDC_PASSWORD,
        dbname=DATABASE,
        root_password=ROOT_PASSWORD,
    ).with_command(MYSQL_COMMAND)
    with container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(3306))
        root = pymysql.connect(
            host=host, port=port, user="root", password=ROOT_PASSWORD, database=DATABASE
        )
        try:
            with root.cursor() as cursor:
                cursor.execute(
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT "
                    f"ON *.* TO '{CDC_USER}'@'%'"
                )
                cursor.execute("FLUSH PRIVILEGES")
            root.commit()
        finally:
            root.close()
        yield {"host": host, "port": port}


@pytest.fixture()
def mysql(mysql_server):
    connection = pymysql.connect(
        host=mysql_server["host"],
        port=mysql_server["port"],
        user=CDC_USER,
        password=CDC_PASSWORD,
        database=DATABASE,
        autocommit=True,
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture()
def root_connection(mysql_server):
    conn = pymysql.connect(
        host=mysql_server["host"],
        port=mysql_server["port"],
        user="root",
        password=ROOT_PASSWORD,
        database=DATABASE,
        autocommit=True,
    )
    try:
        yield conn
    finally:
        with conn.cursor() as cursor:
            cursor.execute("SET GLOBAL binlog_row_metadata = FULL")
            cursor.execute("SET GLOBAL binlog_row_image = FULL")
        conn.close()


def make_source(mysql_server, table: str, state: Dict[str, Any], **kwargs: Any):
    params: Dict[str, Any] = {
        "host": mysql_server["host"],
        "port": mysql_server["port"],
        "user": CDC_USER,
        "password": CDC_PASSWORD,
        "database": DATABASE,
        "table": table,
        "commit_interval": 0.3,
        "shutdown_timeout": 5,
    }
    params.update(kwargs)
    return LiteHarness(state_data=state, **params)


def test_default_topic_round_trips_a_string_key():
    """The `"<database>.<table>"` key reaches a consumer as `str`, not `bytes`."""
    source = MySqlCdcLiteSource(
        host="localhost",
        user="cdc",
        password="pw",
        database="mydb",
        table="mytable",
    )
    topic = source.default_topic()
    assert topic.name == "mysql_cdc_lite_mydb_mytable"

    serialized = topic.serialize(key="mydb.mytable", value={"kind": "insert"})
    assert serialized.key == b"mydb.mytable"

    deserialized = topic.deserialize(
        ConfluentKafkaMessageStub(
            topic=topic.name, key=serialized.key, value=serialized.value
        )
    )
    assert deserialized.key == "mydb.mytable"
    assert deserialized.value == {"kind": "insert"}


def test_insert_update_delete_stream_with_correct_values(mysql, mysql_server):
    """The three row events arrive with real column names and real values."""
    table = "lite_dml"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(
        mysql,
        f"CREATE TABLE {table} "
        "(id INT PRIMARY KEY, customer VARCHAR(50), amount INT)",
    )
    # Written before the source starts: streaming-only, so it must NOT appear.
    execute(mysql, f"INSERT INTO {table} VALUES (99, 'pre-existing', 0)")

    source = make_source(mysql_server, table, {})
    with RunningSource(source) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        execute(mysql, f"INSERT INTO {table} VALUES (1, 'ada', 100)")
        execute(mysql, f"UPDATE {table} SET amount = 250 WHERE id = 1")
        execute(mysql, f"DELETE FROM {table} WHERE id = 1")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1, update=1, delete=1),
            "one change event of each kind",
        )

    assert [m["kind"] for m in messages] == ["insert", "update", "delete"]

    envelope = {"kind", "schema", "table", "columnnames", "columnvalues", "oldkeys"}
    for message in messages:
        assert set(message) - {"_key"} == envelope
        assert message["schema"] == DATABASE
        assert message["table"] == table
        assert message["_key"] == f"{DATABASE}.{table}".encode()

    inserted = of_kind(messages, "insert")[0]
    assert row_of(inserted) == {"id": 1, "customer": "ada", "amount": 100}
    assert inserted["oldkeys"] == {}

    updated = of_kind(messages, "update")[0]
    assert row_of(updated) == {"id": 1, "customer": "ada", "amount": 250}
    assert oldkeys_of(updated) == {"id": 1, "customer": "ada", "amount": 100}

    deleted = of_kind(messages, "delete")[0]
    assert deleted["columnnames"] == []
    assert deleted["columnvalues"] == []
    assert oldkeys_of(deleted) == {"id": 1, "customer": "ada", "amount": 250}

    # Nothing from before the source started, and no positional placeholder anywhere.
    assert not [m for m in messages if 99 in m["columnvalues"]]
    for message in messages:
        names = message["columnnames"] + message["oldkeys"].get("keynames", [])
        assert names and not [n for n in names if n.startswith("UNKNOWN_COL")]


def test_every_column_type_encodes_json_safely(mysql, mysql_server):
    """The encoder is the one thing kept from the hardened connector: prove it."""
    table = "lite_types"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(
        mysql,
        f"CREATE TABLE {table} ("
        "id INT PRIMARY KEY, flag BOOLEAN, small_unsigned TINYINT UNSIGNED, "
        "amount DECIMAL(10,2), payload JSON, tags SET('a','b','c'), "
        "status ENUM('new','done'), blob_col VARBINARY(16), when_at DATETIME, "
        "dur TIME, ratio DOUBLE, empty_tags SET('x','y'))",
    )

    payload = '{"a": [1, 2], "b": "text"}'
    source = make_source(mysql_server, table, {})
    with RunningSource(source) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        execute(
            mysql,
            f"INSERT INTO {table} VALUES (1, TRUE, 200, 12.34, '{payload}', "
            "'c,a', 'done', 0x0001FF, '2024-05-06 07:08:09', '10:20:30', 1.5, '')",
        )
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1), "the typed insert"
        )

    row = row_of(of_kind(messages, "insert")[0])
    assert row["id"] == 1
    assert row["flag"] == 1
    assert row["small_unsigned"] == 200, "UNSIGNED needs binlog_row_metadata=FULL"
    assert row["amount"] == "12.34"
    assert row["payload"] == {"a": [1, 2], "b": "text"}
    assert row["tags"] == "a,c"
    assert row["status"] == "done"
    assert row["blob_col"] == "AAH/"
    assert row["when_at"] == "2024-05-06T07:08:09"
    assert row["dur"] == "10:20:30"
    assert row["ratio"] == 1.5
    # An empty SET decodes to None, which this source does not distinguish from NULL.
    assert row["empty_tags"] is None

    # The whole change dict really is JSON, not just JSON-ish (`_key` is the harness's
    # own addition: the serialized Kafka key, which is bytes by then).
    message = dict(of_kind(messages, "insert")[0])
    message.pop("_key")
    json.dumps(message)


def test_restart_resumes_and_delivers_the_downtime_window(mysql, mysql_server):
    """Changes made while the source is stopped arrive after it restarts."""
    table = "lite_downtime"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(50))")

    state: Dict[str, Any] = {}
    position_key = f"binlog_position_{DATABASE}_{table}"

    first = make_source(mysql_server, table, state)
    with RunningSource(first) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        execute(mysql, f"INSERT INTO {table} VALUES (1, 'while-running')")
        running.wait_for(lambda m: count_kinds(m, insert=1), "the live insert")

    committed = state.get(position_key)
    assert committed is not None, "the position must survive in state, not on disk"

    # Downtime: the source is stopped, the table keeps changing.
    execute(mysql, f"INSERT INTO {table} VALUES (2, 'during-downtime')")
    execute(mysql, f"UPDATE {table} SET note = 'edited' WHERE id = 1")
    execute(mysql, f"DELETE FROM {table} WHERE id = 2")

    second = make_source(mysql_server, table, state)
    with RunningSource(second) as running:
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1, update=1, delete=1),
            "the change events made during downtime",
        )

    assert [row_of(m) for m in of_kind(messages, "insert")] == [
        {"id": 2, "note": "during-downtime"}
    ]
    updated = of_kind(messages, "update")[0]
    assert row_of(updated) == {"id": 1, "note": "edited"}
    assert oldkeys_of(updated) == {"id": 1, "note": "while-running"}
    assert [oldkeys_of(m) for m in of_kind(messages, "delete")] == [
        {"id": 2, "note": "during-downtime"}
    ]

    # The restart moved the position on rather than re-reading from the old one.
    assert state[position_key] != committed


def test_stop_drains_the_buffer(mysql, mysql_server):
    """A stop() with changes still buffered produces them instead of dropping them."""
    table = "lite_drain"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(50))")

    # Long enough that a read lands in the buffer and sits there until stop().
    source = make_source(mysql_server, table, {}, commit_interval=5.0)
    running = RunningSource(source)
    with running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        for i in range(3):
            execute(mysql, f"INSERT INTO {table} VALUES ({i}, 'buffered-{i}')")

        assert source.buffered.wait(timeout=30.0), "the source never buffered the rows"
        assert not source.received(), "the rows were produced before stop() was asked"

    messages = source.received()
    assert count_kinds(messages, insert=3), messages
    assert [row_of(m) for m in of_kind(messages, "insert")] == [
        {"id": i, "note": f"buffered-{i}"} for i in range(3)
    ]


def test_minimal_row_metadata_is_silently_wrong(mysql, mysql_server, root_connection):
    """
    What the operator gets when the docs page's `binlog_row_metadata=FULL` is not met.

    This is evidence for the docs page, not a property of the source: the source does
    not check this setting, so the point is to record how the failure shows up.
    """
    table = "lite_minimal"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(
        mysql,
        f"CREATE TABLE {table} (id INT PRIMARY KEY, small_unsigned TINYINT UNSIGNED, "
        "tags SET('a','b'), status ENUM('new','done'))",
    )
    execute(root_connection, "SET GLOBAL binlog_row_metadata = MINIMAL")
    execute(root_connection, "FLUSH BINARY LOGS")

    source = make_source(mysql_server, table, {})
    with RunningSource(source) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        execute(mysql, f"INSERT INTO {table} VALUES (1, 200, 'a,b', 'done')")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1), "the insert under MINIMAL metadata"
        )

    observed = dict(of_kind(messages, "insert")[0])
    observed.pop("_key")
    print("\nMINIMAL binlog_row_metadata produces:", json.dumps(observed, indent=2))
    assert observed["columnnames"] == [
        "UNKNOWN_COL0",
        "UNKNOWN_COL1",
        "UNKNOWN_COL2",
        "UNKNOWN_COL3",
    ]
    assert observed["columnvalues"] == [1, -56, None, None]


def test_minimal_row_image_truncates_an_update(mysql_server, root_connection):
    """
    What the operator gets when the docs page's `binlog_row_image=FULL` is not met.

    Also evidence for the docs page. `binlog_row_image` has session scope, so the
    writer below is opened after the global is lowered.
    """
    table = "lite_partial"
    execute(root_connection, "SET GLOBAL binlog_row_image = MINIMAL")
    writer = pymysql.connect(
        host=mysql_server["host"],
        port=mysql_server["port"],
        user=CDC_USER,
        password=CDC_PASSWORD,
        database=DATABASE,
        autocommit=True,
    )
    try:
        execute(writer, f"DROP TABLE IF EXISTS {table}")
        execute(
            writer,
            f"CREATE TABLE {table} (id INT PRIMARY KEY, kept VARCHAR(50), "
            "changed VARCHAR(50))",
        )
        execute(writer, f"INSERT INTO {table} VALUES (1, 'keep-me', 'before')")

        source = make_source(mysql_server, table, {})
        with RunningSource(source) as running:
            assert running.source.polled.wait(timeout=30.0), "the stream never opened"
            execute(writer, f"UPDATE {table} SET changed = 'after' WHERE id = 1")
            messages = running.wait_for(
                lambda m: count_kinds(m, update=1), "the update under MINIMAL row image"
            )
    finally:
        writer.close()

    observed = dict(of_kind(messages, "update")[0])
    observed.pop("_key")
    print("\nMINIMAL binlog_row_image produces:", json.dumps(observed, indent=2))
    # The truncated row is shaped exactly like a full one: every column still named,
    # the untouched ones null, and the primary key gone from the after-image.
    assert observed["columnnames"] == ["id", "kept", "changed"]
    assert observed["columnvalues"] == [None, None, "after"]
    assert oldkeys_of(observed) == {"id": 1, "kept": None, "changed": None}


def test_the_documented_grant_set_is_exactly_what_is_needed(
    mysql_server, root_connection
):
    """
    Evidence for the docs page's grant line.

    REPLICATION SLAVE and REPLICATION CLIENT alone are refused, because `setup()`
    connects with the database as its default and neither is a schema privilege.
    Adding SELECT on the one table is enough - the stream itself reads no rows.
    """
    table = "lite_grants"
    execute(root_connection, f"DROP TABLE IF EXISTS {table}")
    execute(
        root_connection,
        f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(50))",
    )
    execute(root_connection, "DROP USER IF EXISTS 'cdc_min'@'%'")
    execute(root_connection, "CREATE USER 'cdc_min'@'%' IDENTIFIED BY 'pw'")
    execute(
        root_connection,
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_min'@'%'",
    )
    execute(root_connection, "FLUSH PRIVILEGES")

    without_select = make_source(mysql_server, table, {}, user="cdc_min", password="pw")
    with pytest.raises(pymysql.err.OperationalError, match="Access denied"):
        without_select.setup()

    execute(root_connection, f"GRANT SELECT ON {DATABASE}.{table} TO 'cdc_min'@'%'")
    execute(root_connection, "FLUSH PRIVILEGES")

    source = make_source(mysql_server, table, {}, user="cdc_min", password="pw")
    with RunningSource(source) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        execute(root_connection, f"INSERT INTO {table} VALUES (1, 'minimal-grants')")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1), "the insert under the documented grants"
        )

    assert row_of(of_kind(messages, "insert")[0]) == {"id": 1, "note": "minimal-grants"}


def test_a_purged_position_kills_the_source(mysql, mysql_server, root_connection):
    """
    Evidence for the docs page: what the operator sees when retention is too short.

    There is no snapshot to fall back on, so the source has nothing to do but exit
    with the position it cannot reach, and be restarted into the same failure.
    """
    table = "lite_purged"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(50))")

    state: Dict[str, Any] = {}
    first = make_source(mysql_server, table, state)
    with RunningSource(first) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        execute(mysql, f"INSERT INTO {table} VALUES (1, 'before-the-purge')")
        running.wait_for(lambda m: count_kinds(m, insert=1), "the live insert")

    committed = state[f"binlog_position_{DATABASE}_{table}"]

    execute(root_connection, "FLUSH BINARY LOGS")
    with root_connection.cursor() as cursor:
        cursor.execute("SHOW BINARY LOGS")
        newest = str(cursor.fetchall()[-1][0])
    execute(root_connection, f"PURGE BINARY LOGS TO '{newest}'")
    assert newest != committed["log_file"], "the committed file was not purged"

    second = make_source(mysql_server, table, state)
    with pytest.raises(AssertionError, match="no longer holds the binlog position"):
        with RunningSource(second) as running:
            running.wait_for(lambda m: False, "a change event that never comes", 45.0)


def test_a_missing_table_fails_at_setup(mysql_server):
    """A typo'd table name is a start-up error, not silence."""
    source = make_source(mysql_server, "no_such_table", {})
    with pytest.raises(MySqlCdcLiteError, match="has no table no_such_table"):
        source.setup()
