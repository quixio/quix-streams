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

import base64
import json
import struct
import threading
import time
from collections import Counter
from datetime import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

import pymysql
import pytest
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent, QueryEvent, XAPrepareEvent, XidEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    TableMapEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from testcontainers.mysql import MySqlContainer

from quixstreams.sources.community.mysql_cdc_lite import (
    MySqlCdcLiteError,
    MySqlCdcLiteSource,
    mysql_cdc_lite,
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

# No source is connected while the raw reader below is, so any id the server itself is
# not using will do.
_PROBE_SERVER_ID = 990077


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


class ReconnectingHarness(LiteHarness):
    """
    A source whose first read fails the way a dropped connection does.

    The read is held until the test releases it, so a change can be written after the
    source has resolved its start position and before it has read anything - and the
    failure lands before any position has been committed.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.stream_open = threading.Event()
        self.may_read = threading.Event()
        self.dropped = threading.Event()

    def _open_stream(self) -> Any:
        stream = super()._open_stream()
        self.stream_open.set()
        assert self.may_read.wait(timeout=30.0), "the test never released the read"
        return stream

    def _read_changes(self) -> None:
        super()._read_changes()
        if not self.dropped.is_set():
            self.dropped.set()
            raise ConnectionResetError("Lost connection to MySQL server during query")


class DyingHarness(LiteHarness):
    """
    A source whose process ends before its first commit.

    The read is held the way `ReconnectingHarness` holds it, so a change lands after the
    start position is resolved; `run()` then leaves through a `BaseException` the retry
    loop does not catch, which is a process being killed rather than stopped - no drain,
    no commit, and the state store is all the successor gets.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.stream_open = threading.Event()
        self.may_read = threading.Event()

    def _open_stream(self) -> Any:
        stream = super()._open_stream()
        self.stream_open.set()
        assert self.may_read.wait(timeout=30.0), "the test never released the read"
        return stream

    def _read_changes(self) -> None:
        super()._read_changes()
        raise SystemExit("killed before the first commit")


class OneCommitHarness(LiteHarness):
    """
    A source that stops itself as soon as it commits a batch that produced something.

    The successor then resumes from that one position, which is what makes a commit
    taken part-way through a statement observable.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.committed = threading.Event()

    def _commit_batch(self, timeout: Optional[float] = None) -> None:
        super()._commit_batch(timeout)
        if self._capturing_producer.snapshot():
            self.committed.set()
            self.stop()


class HoldingHarness(LiteHarness):
    """
    A source that holds itself once, right after a commit.

    A change written while it is held lands at the top of the next cycle, where the
    read that opens the cycle buffers it and the sleep that follows leaves it there.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.held = threading.Event()
        self.may_resume = threading.Event()

    def _commit_batch(self, timeout: Optional[float] = None) -> None:
        super()._commit_batch(timeout)
        if not self.held.is_set():
            self.held.set()
            assert self.may_resume.wait(timeout=30.0), "the test never resumed the read"


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


def wait_for_insert(source: LiteHarness, row_id: int, timeout: float) -> float:
    """:return: the monotonic time the message for `row_id` was first seen at."""
    deadline = time.monotonic() + timeout
    while True:
        if any(row_of(m)["id"] == row_id for m in of_kind(source.received(), "insert")):
            return time.monotonic()
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for the message for row {row_id}")
        time.sleep(0.01)


def statement_positions(
    mysql_server, table: str, position: Tuple[str, int]
) -> Tuple[int, List[int]]:
    """
    Read the raw event boundaries of the statement that follows `position`.

    :return: `(table_map_end, [row_event_end, ...])`, the `log_pos` each of the
        statement's events ends at. The last row event is the statement's end.
    """
    stream = BinLogStreamReader(
        connection_settings={
            "host": mysql_server["host"],
            "port": mysql_server["port"],
            "user": CDC_USER,
            "password": CDC_PASSWORD,
        },
        server_id=_PROBE_SERVER_ID,
        only_events=[TableMapEvent, WriteRowsEvent],
        only_schemas=[DATABASE],
        only_tables=[table],
        resume_stream=True,
        blocking=False,
        log_file=position[0],
        log_pos=position[1],
    )
    table_map_end: Optional[int] = None
    row_event_ends: List[int] = []
    try:
        for event in stream:
            if isinstance(event, TableMapEvent):
                if table_map_end is None:
                    table_map_end = stream.log_pos
            else:
                row_event_ends.append(stream.log_pos)
    finally:
        stream.close()
    assert table_map_end is not None, "the statement wrote no table map"
    return table_map_end, row_event_ends


class RawEvent(NamedTuple):
    """One binlog event as a reader that filters no table saw it."""

    name: str
    table: Optional[str]
    end: int
    flags: Optional[int]
    query: Optional[str]


_ROW_LAYOUT_EVENTS = [
    DeleteRowsEvent,
    TableMapEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    XidEvent,
]
# The row layout plus every event that marks the start or the end of a transaction.
_TRANSACTION_LAYOUT_EVENTS = _ROW_LAYOUT_EVENTS + [
    GtidEvent,
    QueryEvent,
    XAPrepareEvent,
]


def raw_events(
    mysql_server, position: Tuple[str, int], events: Optional[List[type]] = None
) -> List[RawEvent]:
    """
    Read every row event, table map and commit after `position`, for every table.

    :param events: event classes to read.
        Default - the row layout, without the transaction statements around it.
    :return: the events in binlog order. `table` and `flags` are `None` for an event
        that belongs to no table.
    """
    stream = BinLogStreamReader(
        connection_settings={
            "host": mysql_server["host"],
            "port": mysql_server["port"],
            "user": CDC_USER,
            "password": CDC_PASSWORD,
        },
        server_id=_PROBE_SERVER_ID,
        only_events=events or _ROW_LAYOUT_EVENTS,
        only_schemas=[DATABASE],
        resume_stream=True,
        blocking=False,
        log_file=position[0],
        log_pos=position[1],
    )
    try:
        return [
            RawEvent(
                type(event).__name__,
                getattr(event, "table", None),
                stream.log_pos,
                getattr(event, "flags", None),
                getattr(event, "query", None),
            )
            for event in stream
        ]
    finally:
        stream.close()


def with_audit_trigger(
    connection, table: str, audit: str, engine: str = "InnoDB"
) -> None:
    """
    Create `table` and an `audit` table that an AFTER INSERT trigger also writes.

    Needs the root connection: MySQL refuses CREATE TRIGGER to a user without SUPER
    while binary logging is on (error 1419).
    """
    execute(connection, f"DROP TABLE IF EXISTS {table}")
    execute(connection, f"DROP TABLE IF EXISTS {audit}")
    execute(
        connection,
        f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(20)) ENGINE={engine}",
    )
    execute(
        connection,
        f"CREATE TABLE {audit} (id INT PRIMARY KEY AUTO_INCREMENT, note VARCHAR(20)) "
        f"ENGINE={engine}",
    )
    execute(
        connection,
        f"CREATE TRIGGER {table}_to_{audit} AFTER INSERT ON {table} FOR EACH ROW "
        f"INSERT INTO {audit} (note) VALUES (NEW.note)",
    )


def read_once(
    source: LiteHarness, position: Tuple[str, int]
) -> Tuple[Optional[Tuple[str, int]], List[Dict[str, Any]]]:
    """
    Run one `_read_changes()` against a stream opened at `position`.

    :return: `(pending position, buffered changes)`, taken before `_drop_stream()`
        discards both.
    """
    source._running = True
    source._position = position
    source._stream = source._open_stream()
    try:
        source._read_changes()
        return source._pending, list(source._buffer)
    finally:
        source._drop_stream()


def trip_at(monkeypatch, log_pos: int) -> None:
    """Replace the shipped clock bound with one that trips at a chosen event."""

    class TripAt(mysql_cdc_lite._ScanBound):
        def __le__(self, at: object) -> bool:
            return isinstance(at, int) and at >= log_pos

    monkeypatch.setattr(mysql_cdc_lite, "_ScanBound", TripAt)


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


def make_source(
    mysql_server,
    table: str,
    state: Dict[str, Any],
    harness: type = LiteHarness,
    **kwargs: Any,
):
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
    return harness(state_data=state, **params)


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


def b64_of_hex(rendered: str) -> str:
    return base64.b64encode(bytes.fromhex(rendered)).decode("ascii")


def widened_float(rendered: str) -> float:
    """:return: the double MySQL holds for a `FLOAT`, from the digits it prints."""
    return struct.unpack("<f", struct.pack("<f", float(rendered)))[0]


def iso_datetime(rendered: str) -> str:
    return datetime.fromisoformat(rendered).isoformat()


def sorted_set(rendered: str) -> Optional[str]:
    return ",".join(sorted(rendered.split(","))) or None


class TypeCase(NamedTuple):
    """
    One column of the docs page's encoding table, with the oracle for its value.

    :param oracle: SQL rendering the documented form of the column on the server.
    :param encoded: the value this source must emit, from that rendering.
    """

    name: str
    ddl: str
    literal: str
    oracle: str
    encoded: Callable[[str], Any]


JSON_VALUE = {"b": "text", "a": [1, 2], "z": None, "n": 1.5, "e": {}, "u": "ünïcode"}

TYPE_CASES = [
    TypeCase("flag", "BOOLEAN", "TRUE", "CAST(flag AS CHAR)", int),
    TypeCase("small_uns", "TINYINT UNSIGNED", "200", "CAST(small_uns AS CHAR)", int),
    TypeCase("big_neg", "BIGINT", "-9223372036854775808", "CAST(big_neg AS CHAR)", int),
    TypeCase(
        "big_uns",
        "BIGINT UNSIGNED",
        "18446744073709551615",
        "CAST(big_uns AS CHAR)",
        int,
    ),
    TypeCase(
        "dec_tiny", "DECIMAL(20,10)", "'0.0000000001'", "CAST(dec_tiny AS CHAR)", str
    ),
    TypeCase("dec_neg", "DECIMAL(10,2)", "'-12.34'", "CAST(dec_neg AS CHAR)", str),
    TypeCase("dec_zero", "DECIMAL(10,2)", "'0'", "CAST(dec_zero AS CHAR)", str),
    TypeCase(
        "dec_wide",
        "DECIMAL(30,0)",
        "'999999999999999999999999999999'",
        "CAST(dec_wide AS CHAR)",
        str,
    ),
    TypeCase("f_one", "FLOAT", "1.1", "CAST(f_one AS CHAR)", widened_float),
    TypeCase("f_neg", "FLOAT", "-0.5", "CAST(f_neg AS CHAR)", widened_float),
    TypeCase("d_neg", "DOUBLE", "-0.1", "CAST(d_neg AS CHAR)", float),
    TypeCase("d_zero", "DOUBLE", "0", "CAST(d_zero AS CHAR)", float),
    TypeCase(
        "payload",
        "JSON",
        f"'{json.dumps(JSON_VALUE)}'",
        "CAST(payload AS CHAR)",
        json.loads,
    ),
    TypeCase("tags", "SET('c','b','a')", "'a,c'", "CAST(tags AS CHAR)", sorted_set),
    TypeCase("no_tags", "SET('x','y')", "''", "CAST(no_tags AS CHAR)", sorted_set),
    TypeCase("status", "ENUM('new','done')", "'done'", "CAST(status AS CHAR)", str),
    TypeCase(
        "status_1st", "ENUM('new','done')", "'new'", "CAST(status_1st AS CHAR)", str
    ),
    TypeCase("bin_pad", "BINARY(4)", "0x00000000", "HEX(bin_pad)", b64_of_hex),
    TypeCase("bin_tail", "BINARY(4)", "0x41000000", "HEX(bin_tail)", b64_of_hex),
    TypeCase("bin_full", "BINARY(4)", "0xFFFFFFFF", "HEX(bin_full)", b64_of_hex),
    TypeCase("vbin_zero", "VARBINARY(16)", "0x00000000", "HEX(vbin_zero)", b64_of_hex),
    TypeCase("vbin_empty", "VARBINARY(16)", "''", "HEX(vbin_empty)", b64_of_hex),
    TypeCase("blob_col", "BLOB", "0x0001FF00", "HEX(blob_col)", b64_of_hex),
    TypeCase("day", "DATE", "'2024-05-06'", "CAST(day AS CHAR)", str),
    TypeCase("day_min", "DATE", "'1000-01-01'", "CAST(day_min AS CHAR)", str),
    TypeCase(
        "dt0", "DATETIME", "'2024-05-06 07:08:09'", "CAST(dt0 AS CHAR)", iso_datetime
    ),
    TypeCase(
        "dt3",
        "DATETIME(3)",
        "'2024-05-06 07:08:09.123'",
        "CAST(dt3 AS CHAR)",
        iso_datetime,
    ),
    TypeCase(
        "dt6_zero",
        "DATETIME(6)",
        "'2024-05-06 07:08:09.000000'",
        "CAST(dt6_zero AS CHAR)",
        iso_datetime,
    ),
    TypeCase(
        "ts",
        "TIMESTAMP NULL",
        "'2024-05-06 07:08:09'",
        "CAST(ts AS CHAR)",
        iso_datetime,
    ),
    TypeCase(
        "ts_epoch",
        "TIMESTAMP NULL",
        "'1970-01-01 00:00:01'",
        "CAST(ts_epoch AS CHAR)",
        iso_datetime,
    ),
    TypeCase("t0", "TIME(0)", "'00:00:00'", "CAST(t0 AS CHAR)", str),
    TypeCase("t0_max", "TIME(0)", "'838:59:59'", "CAST(t0_max AS CHAR)", str),
    TypeCase("t3", "TIME(3)", "'01:02:03.456'", "CAST(t3 AS CHAR)", str),
    TypeCase("t3_neg", "TIME(3)", "'-00:00:00.5'", "CAST(t3_neg AS CHAR)", str),
    TypeCase("t6_zero", "TIME(6)", "'00:00:00'", "CAST(t6_zero AS CHAR)", str),
    TypeCase("t6_neg", "TIME(6)", "'-838:59:58.999999'", "CAST(t6_neg AS CHAR)", str),
    TypeCase("bit1", "BIT(1)", "b'1'", "LPAD(BIN(bit1), 1, '0')", str),
    TypeCase("bit8", "BIT(8)", "b'10000001'", "LPAD(BIN(bit8), 8, '0')", str),
    TypeCase("bit8_low", "BIT(8)", "b'1'", "LPAD(BIN(bit8_low), 8, '0')", str),
    TypeCase("bit8_zero", "BIT(8)", "b'0'", "LPAD(BIN(bit8_zero), 8, '0')", str),
    TypeCase("bit12", "BIT(12)", "b'100000000001'", "LPAD(BIN(bit12), 12, '0')", str),
]


def test_every_documented_type_matches_the_servers_own_rendering(mysql, mysql_server):
    """
    Every type on the docs page's encoding table, against the server's own rendering.

    Each case carries the SQL that renders its documented form - `HEX` for the binary
    types, `BIN` for `BIT`, `CAST(v AS CHAR)` for the ones the page documents as MySQL
    prints them - and the transform from that rendering to the value this source must
    emit. Three rows of the page are documented divergences and are canonicalised
    rather than compared raw: a `SET` is sorted, a `JSON` object is compared parsed
    rather than as MySQL's text, and a `FLOAT` is widened from the six digits MySQL
    prints to the double it holds.

    `TIMESTAMP` is written and read in UTC, which is the zone the decoder resolves it
    in.
    """
    table = "lite_types"
    execute(mysql, "SET time_zone = '+00:00'")
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    columns = ", ".join(f"{case.name} {case.ddl}" for case in TYPE_CASES)
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, {columns})")

    source = make_source(mysql_server, table, {})
    with RunningSource(source) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        literals = ", ".join(case.literal for case in TYPE_CASES)
        execute(mysql, f"INSERT INTO {table} VALUES (1, {literals})")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1), "the typed insert"
        )

    oracle = ", ".join(case.oracle for case in TYPE_CASES)
    with mysql.cursor() as cursor:
        cursor.execute(f"SELECT {oracle} FROM {table} WHERE id = 1")
        rendered = cursor.fetchone()

    emitted = row_of(of_kind(messages, "insert")[0])
    assert emitted["id"] == 1
    assert emitted["small_uns"] == 200, "UNSIGNED needs binlog_row_metadata=FULL"
    wrong = [
        (case, case.encoded(text), emitted[case.name])
        for case, text in zip(TYPE_CASES, rendered)
        if emitted[case.name] != case.encoded(text)
    ]
    assert not wrong, "\n".join(
        f"{case.name:>11} {case.ddl:<17} {case.literal:>34} -> MySQL renders "
        f"{want!r}, the source emits {got!r}"
        for case, want, got in wrong
    )

    # The whole change dict really is JSON, not just JSON-ish (`_key` is the harness's
    # own addition: the serialized Kafka key, which is bytes by then).
    message = dict(of_kind(messages, "insert")[0])
    message.pop("_key")
    json.dumps(message)


def test_a_negative_time_is_rendered_the_way_mysql_prints_it(mysql, mysql_server):
    """
    Every `TIME` must reach the topic as `CAST(v AS CHAR)` renders it on the server.

    MySQL stores a negative `TIME` with its fraction complemented, and
    `row_event.py:449-477` builds the magnitude from the whole-second bitfield and then
    multiplies by the sign, so the `timedelta` it hands over is a whole second away from
    the stored value whenever the fraction is not zero. The two positive-looking rows
    are controls: `01:02:03.456789` never took the negative path, and `-00:00:00.5` is
    its own complement, so a compensation that also moves them is wrong.
    """
    table = "lite_neg_time"
    written = [
        "-01:02:03.456789",
        "-00:00:01.250000",
        "-10:20:30.000001",
        "-838:59:58.999999",
        "01:02:03.456789",
        "-00:00:00.500000",
    ]
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, dur TIME(6))")

    source = make_source(mysql_server, table, {}, name="lite_neg_time_source")
    with RunningSource(source) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        values = ", ".join(f"({i}, '{v}')" for i, v in enumerate(written))
        execute(mysql, f"INSERT INTO {table} VALUES {values}")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=len(written)), "every TIME value"
        )

    with mysql.cursor() as cursor:
        cursor.execute(f"SELECT id, CAST(dur AS CHAR) FROM {table} ORDER BY id")
        printed = {int(row[0]): str(row[1]) for row in cursor.fetchall()}

    emitted = {row_of(m)["id"]: row_of(m)["dur"] for m in of_kind(messages, "insert")}
    assert emitted == printed, "\n".join(
        f"{written[i]:>18} MySQL prints {printed[i]:>18}, the source emits "
        f"{emitted[i]:>18}"
        for i in sorted(printed)
    )


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


def test_a_reconnect_before_the_first_commit_skips_nothing(mysql, mysql_server):
    """
    A dropped connection before the first commit must not lose the changes since start.

    The only position in state is the anchor written at start-up, so the only resume
    point is where this source started. A source that asks the server again instead
    resumes past what it had already seen, with no error and nothing on the topic to
    show it.

    `max_buffer_size=2` makes the retry commit as soon as both changes are read, rather
    than waiting out the long `commit_interval` that keeps the first commit from
    landing before the drop.
    """
    table = "lite_reconnect"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(50))")

    state: Dict[str, Any] = {}
    position_key = f"binlog_position_{DATABASE}_{table}"
    source = make_source(
        mysql_server,
        table,
        state,
        harness=ReconnectingHarness,
        commit_interval=10.0,
        max_buffer_size=2,
    )

    with RunningSource(source) as running:
        assert source.stream_open.wait(timeout=30.0), "the stream never opened"
        anchor = state[position_key]
        execute(mysql, f"INSERT INTO {table} VALUES (1, 'before-the-drop')")
        source.may_read.set()

        assert source.dropped.wait(timeout=30.0), "the stream never failed"
        assert state[position_key] == anchor, "a batch was committed before the drop"
        execute(mysql, f"INSERT INTO {table} VALUES (2, 'after-the-drop')")

        messages = running.wait_for(
            lambda m: count_kinds(m, insert=2),
            "both inserts to survive the reconnect",
        )

    assert [row_of(m) for m in of_kind(messages, "insert")] == [
        {"id": 1, "note": "before-the-drop"},
        {"id": 2, "note": "after-the-drop"},
    ]


def test_a_process_that_dies_before_its_first_commit_skips_nothing(mysql, mysql_server):
    """
    A deployment killed before its first commit must not cost its successor the window.

    Nothing was produced, so the successor may replay - but it must not start later than
    its predecessor did. `test_restart_resumes_and_delivers_the_downtime_window` is the
    control: same code path, same downtime, and the only difference is whether a commit
    ever landed.
    """
    table = "lite_early_death"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(50))")

    state: Dict[str, Any] = {}
    first = make_source(mysql_server, table, state, harness=DyingHarness)
    with pytest.raises(SystemExit):
        with RunningSource(first):
            assert first.stream_open.wait(timeout=30.0), "the stream never opened"
            execute(mysql, f"INSERT INTO {table} VALUES (1, 'before-the-kill')")
            first.may_read.set()

    assert not first.received(), "the first source produced something before it died"

    # Downtime: nothing is running, and the table keeps changing.
    execute(mysql, f"INSERT INTO {table} VALUES (2, 'during-downtime')")

    second = make_source(mysql_server, table, state)
    with RunningSource(second) as running:
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=2),
            "the changes since the dead process started",
        )

    assert [row_of(m) for m in of_kind(messages, "insert")] == [
        {"id": 1, "note": "before-the-kill"},
        {"id": 2, "note": "during-downtime"},
    ]


def test_a_statement_split_across_events_survives_a_restart(mysql, mysql_server):
    """
    A statement whose rows MySQL splits across several events must arrive whole.

    MySQL emits one TableMapEvent and then a row event per `binlog_row_event_max_size`
    chunk, so a statement changing more rows than `max_buffer_size` is committed
    part-way through it. A reader reopened there has no table map for the rows that
    follow and discards them without raising, which takes them off the topic with
    nothing in the log to say so.
    """
    table = "lite_bulk"
    rows = 5000
    sentinel = rows + 1
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(20))")

    state: Dict[str, Any] = {}
    first = make_source(mysql_server, table, state, harness=OneCommitHarness)
    with RunningSource(first) as running:
        assert running.source.polled.wait(timeout=30.0), "the stream never opened"
        values = ", ".join(f"({i}, 'bulk')" for i in range(1, rows + 1))
        execute(mysql, f"INSERT INTO {table} VALUES {values}")
        assert first.committed.wait(timeout=60.0), "the source committed no changes"

    execute(mysql, f"INSERT INTO {table} VALUES ({sentinel}, 'sentinel')")

    second = make_source(mysql_server, table, state)
    with RunningSource(second) as running:
        running.wait_for(
            lambda m: any(row_of(x)["id"] == sentinel for x in of_kind(m, "insert")),
            "the successor to read past the bulk statement",
            60.0,
        )

    delivered = Counter(
        row_of(message)["id"]
        for message in of_kind(first.received() + second.received(), "insert")
    )
    missing = [i for i in range(1, rows + 1) if not delivered[i]]
    assert not missing, (
        f"{len(missing)} of the statement's {rows} rows never reached the topic, "
        f"from id {missing[0]} to id {missing[-1]}"
    )
    assert delivered[sentinel] == 1
    assert max(delivered.values()) == 1, "a row was delivered more than once"


def test_a_scan_bound_landing_on_a_table_map_reads_the_statement_whole(
    mysql, mysql_server, monkeypatch
):
    """
    A scan bound that trips on a statement's TableMapEvent must not end the read there.

    `fetchone` checks the bound (`binlogstream.py:672`) before it filters the event out
    as one the caller did not ask for (`:723`), so a bound tripping on the TableMapEvent
    ends the read with nothing delivered and the position between the table map and the
    statement's first row event - which is where the previous fix left `STMT_END_F`
    unable to help, because that event never reaches the loop. A reader reopened there
    has no map for the rows that follow and discards them without raising.

    The shipped bound is triggered by the wall clock, which cannot be aimed at one
    event; the stand-in below is the shipped bound with a position for a trigger, so
    the trip lands on the table map every run instead of once in a while.
    """
    table = "lite_table_map"
    rows = 5000
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(20))")

    source = make_source(mysql_server, table, {})
    start = source._start_position()
    values = ", ".join(f"({i}, 'bulk')" for i in range(1, rows + 1))
    execute(mysql, f"INSERT INTO {table} VALUES {values}")

    table_map_end, row_event_ends = statement_positions(mysql_server, table, start)
    statement_end = row_event_ends[-1]
    assert len(row_event_ends) > 1, (
        f"{rows} rows came out as one row event: the statement must be split for this "
        "test to have a mid-statement position to land on"
    )

    class TripOnTableMap(mysql_cdc_lite._ScanBound):
        """The shipped bound, triggered by a position rather than by the clock."""

        def __le__(self, log_pos: object) -> bool:
            return isinstance(log_pos, int) and log_pos >= table_map_end

    monkeypatch.setattr(mysql_cdc_lite, "_ScanBound", TripOnTableMap)
    source._running = True
    source._position = start
    # The deadline really has passed: only *where* the reader notices is the stand-in's.
    source._last_commit_at = time.monotonic() - source._commit_interval
    source._stream = source._open_stream()
    try:
        source._read_changes()
        pending = source._pending
        read_first = list(source._buffer)
    finally:
        source._drop_stream()
    monkeypatch.undo()

    successor = make_source(mysql_server, table, {}, name=f"{table}_successor")
    successor._running = True
    successor._position = pending
    successor._last_commit_at = time.monotonic() + 3600.0
    successor._stream = successor._open_stream()
    try:
        successor._read_changes()
        read_second = list(successor._buffer)
    finally:
        successor._drop_stream()

    delivered = Counter(row_of(change)["id"] for change in read_first + read_second)
    missing = [i for i in range(1, rows + 1) if not delivered[i]]
    assert not missing, (
        f"{len(missing)} of the statement's {rows} rows were never read, from id "
        f"{missing[0]} to id {missing[-1]}: the read stopped at {pending}, and the "
        f"statement runs from its table map at {table_map_end} to {statement_end}"
    )
    assert pending is not None and pending[1] >= statement_end, (
        f"the read stopped at {pending}, inside the statement: its table map ends at "
        f"{table_map_end} and its last row event at {statement_end}"
    )
    assert max(delivered.values()) == 1, "a row was read more than once"


def test_a_bound_on_a_trigger_written_table_map_delivers_the_row(
    mysql, mysql_server, root_connection, monkeypatch
):
    """
    A statement a trigger spreads over two tables must survive a bound on the other map.

    MySQL writes both tables' maps before either table's rows, and the map this source
    did not ask for is dropped at packet level, so the loop never sees it. A bound
    tripping on it ends the read at a position between this table's map and this table's
    rows; a reader reopened there has no map for the rows that follow and discards them
    without raising.

    Resuming from `None` is not a hole: it means the read observed no boundary of its
    own, so the source keeps the position it already had - which is `start` here.
    """
    table = "lite_trigger_map"
    audit = "lite_trigger_map_audit"
    with_audit_trigger(root_connection, table, audit)

    source = make_source(mysql_server, table, {})
    start = source._start_position()
    execute(mysql, f"INSERT INTO {table} VALUES (1, 'triggered')")

    layout = raw_events(mysql_server, start)
    maps = [(e.name, e.table) for e in layout[:2]]
    assert maps == [("TableMapEvent", table), ("TableMapEvent", audit)], (
        "MySQL no longer writes both table maps before either table's rows: "
        f"{layout}"
    )

    trip_at(monkeypatch, layout[1].end)
    pending, read_first = read_once(source, start)
    monkeypatch.undo()

    successor = make_source(mysql_server, table, {}, name=f"{table}_successor")
    _, read_second = read_once(successor, pending or start)

    delivered = Counter(row_of(change)["id"] for change in read_first + read_second)
    assert delivered[1], (
        f"the row was never read: the read stopped at {pending}, inside a statement "
        f"whose events run {layout}"
    )


def test_a_bound_on_a_multi_table_updates_other_map_delivers_both_rows(
    mysql, mysql_server, monkeypatch
):
    """
    The same shape from an `UPDATE` touching two tables in one statement.

    No trigger involved: a multi-table `UPDATE` writes both maps up front too, so the
    same bound lands in the same place.
    """
    table = "lite_join"
    other = "lite_join_other"
    for name in (table, other):
        execute(mysql, f"DROP TABLE IF EXISTS {name}")
        execute(mysql, f"CREATE TABLE {name} (id INT PRIMARY KEY, note VARCHAR(20))")
        execute(mysql, f"INSERT INTO {name} VALUES (1, 'before'), (2, 'before')")

    source = make_source(mysql_server, table, {})
    start = source._start_position()
    execute(
        mysql,
        f"UPDATE {table} o JOIN {other} x ON o.id = x.id "
        "SET o.note = 'after', x.note = 'after'",
    )

    layout = raw_events(mysql_server, start)
    maps = [(e.name, e.table) for e in layout[:2]]
    assert maps == [("TableMapEvent", table), ("TableMapEvent", other)], (
        "MySQL no longer writes both table maps before either table's rows: "
        f"{layout}"
    )

    trip_at(monkeypatch, layout[1].end)
    pending, read_first = read_once(source, start)
    monkeypatch.undo()

    successor = make_source(mysql_server, table, {}, name=f"{table}_successor")
    _, read_second = read_once(successor, pending or start)

    delivered = {row_of(change)["id"] for change in read_first + read_second}
    assert delivered == {1, 2}, (
        f"the statement updated rows 1 and 2 of {table}; {sorted(delivered)} were "
        f"read. The read stopped at {pending}, and its events run {layout}"
    )


def test_the_bounds_are_honoured_on_a_trigger_shaped_statement(
    mysql, mysql_server, root_connection
):
    """
    `max_buffer_size` must bound a burst whose statements a trigger spreads over two
    tables.

    MySQL sets STMT_END_F on the last row event of a statement whichever table it
    belongs to, so here it is the audit table's rows that carry it and this source's
    never do. A loop that looks for the flag on its own events alone finds no boundary
    to stop at and reads the whole burst.
    """
    table = "lite_trigger_bounds"
    audit = "lite_trigger_bounds_audit"
    with_audit_trigger(root_connection, table, audit)

    source = make_source(
        mysql_server, table, {}, max_buffer_size=1, commit_interval=30.0
    )
    start = source._start_position()
    for i in range(1, 6):
        execute(mysql, f"INSERT INTO {table} VALUES ({i}, 'burst-{i}')")

    layout = raw_events(mysql_server, start)
    ours = [e for e in layout if e.name.endswith("RowsEvent") and e.table == table]
    assert ours and not ours[0].flags, (
        "MySQL now flags this source's own rows as the statement end, so this shape "
        f"no longer reproduces: {layout}"
    )
    first_commit = next(e for e in layout if e.name == "XidEvent")

    pending, buffered = read_once(source, start)

    assert len(buffered) == 1, (
        f"max_buffer_size=1, and the read buffered {len(buffered)} changes: it ran to "
        f"the end of the stream instead of stopping at the first boundary. Its events "
        f"run {layout}"
    )
    assert pending == (start[0], first_commit.end), (
        f"the read committed {pending}, not the first transaction boundary at "
        f"{first_commit.end}; its events run {layout}"
    )


def test_an_xa_transaction_advances_the_position(mysql, mysql_server, root_connection):
    """
    An XA transaction writes no `XidEvent`, and the position must still advance past it.

    MySQL ends one with `XA END`, an `XAPrepareEvent` and `XA COMMIT`. In the trigger
    shape this source's own rows carry no `STMT_END_F` either, so a loop whose only
    closer is `XidEvent` never leaves the statement group: it keeps no position at all,
    the committed one stays where it was, and every restart replays from there until
    the file ages past retention.
    """
    table = "lite_xa"
    audit = "lite_xa_audit"
    with_audit_trigger(root_connection, table, audit)

    source = make_source(mysql_server, table, {}, name="lite_xa_source")
    start = source._start_position()
    with mysql.cursor() as cursor:
        cursor.execute("XA START 'lite-xa'")
        cursor.execute(f"INSERT INTO {table} VALUES (1, 'xa')")
        cursor.execute("XA END 'lite-xa'")
        cursor.execute("XA PREPARE 'lite-xa'")
        cursor.execute("XA COMMIT 'lite-xa'")

    layout = raw_events(mysql_server, start, _TRANSACTION_LAYOUT_EVENTS)
    assert not [e for e in layout if e.name == "XidEvent"], (
        "an XA transaction now writes an XidEvent, so this shape no longer "
        f"reproduces: {layout}"
    )
    ours = [e for e in layout if e.name.endswith("RowsEvent") and e.table == table]
    assert ours and not ours[0].flags, (
        "MySQL now flags this source's own rows as the statement end, so this shape "
        f"no longer reproduces: {layout}"
    )

    pending, buffered = read_once(source, start)

    assert [row_of(change)["id"] for change in buffered] == [
        1
    ], f"the XA transaction's row never reached the buffer; its events run {layout}"
    assert pending is not None and pending[1] > start[1], (
        f"the read started at {start} and kept {pending}: nothing in an XA transaction "
        "closed the statement group, so the position never moved. Its events run "
        f"{layout}"
    )


def test_a_myisam_statement_advances_the_position(mysql, mysql_server, root_connection):
    """
    A non-transactional engine ends its statement with `COMMIT` and no `XidEvent`.

    Same freeze as the XA shape, from an ordinary autocommit `INSERT`: the position a
    read keeps never leaves the point it started from.
    """
    table = "lite_myisam"
    audit = "lite_myisam_audit"
    with_audit_trigger(root_connection, table, audit, engine="MyISAM")

    source = make_source(mysql_server, table, {}, name="lite_myisam_source")
    start = source._start_position()
    execute(mysql, f"INSERT INTO {table} VALUES (1, 'myisam')")

    layout = raw_events(mysql_server, start, _TRANSACTION_LAYOUT_EVENTS)
    assert not [e for e in layout if e.name == "XidEvent"], (
        "a MyISAM statement now writes an XidEvent, so this shape no longer "
        f"reproduces: {layout}"
    )
    ours = [e for e in layout if e.name.endswith("RowsEvent") and e.table == table]
    assert ours and not ours[0].flags, (
        "MySQL now flags this source's own rows as the statement end, so this shape "
        f"no longer reproduces: {layout}"
    )

    pending, buffered = read_once(source, start)

    assert [row_of(change)["id"] for change in buffered] == [
        1
    ], f"the MyISAM statement's row never reached the buffer; its events run {layout}"
    assert pending is not None and pending[1] > start[1], (
        f"the read started at {start} and kept {pending}: nothing in a MyISAM "
        "statement closed the statement group, so the position never moved. Its "
        f"events run {layout}"
    )


def test_max_buffer_size_is_honoured_across_a_myisam_backlog(
    mysql, mysql_server, root_connection
):
    """
    `max_buffer_size` must bound a backlog of statements that write no `XidEvent`.

    Both bounds are checked only where the read is outside a statement group, so a
    group that never closes disables them: one read swallows the whole backlog however
    large it is, which is the memory bound `max_buffer_size` exists to hold.
    """
    table = "lite_myisam_bounds"
    audit = "lite_myisam_bounds_audit"
    with_audit_trigger(root_connection, table, audit, engine="MyISAM")

    statements = 20
    bound = 5
    source = make_source(
        mysql_server,
        table,
        {},
        name="lite_myisam_bounds_source",
        max_buffer_size=bound,
        commit_interval=30.0,
    )
    start = source._start_position()
    for i in range(1, statements + 1):
        execute(mysql, f"INSERT INTO {table} VALUES ({i}, 'burst-{i}')")

    sizes: List[int] = []
    delivered: List[int] = []
    position = start
    for _ in range(statements // bound):
        pending, buffered = read_once(source, position)
        sizes.append(len(buffered))
        delivered.extend(row_of(change)["id"] for change in buffered)
        position = pending or position

    assert sizes == [bound] * (statements // bound), (
        f"max_buffer_size={bound} over a backlog of {statements} statements was read "
        f"as {sizes}: a read that never leaves the statement group never reaches the "
        "bound check"
    )
    assert delivered == list(range(1, statements + 1))


def test_the_position_advances_over_another_tables_traffic(
    mysql, mysql_server, root_connection
):
    """
    A quiet table's position must keep moving while the rest of the server is busy.

    The stream skips the other table's row events inside its own loop and hands none of
    them over, so the position a read keeps has to come from the stream rather than
    from the events it received - and a rotation is carried the same way, by a
    `RotateEvent` this source does not ask for. Without that, a quiet table's committed
    position would sit still in a file the server eventually purges.
    """
    table = "lite_quiet"
    noisy = "lite_quiet_noisy"
    for name in (table, noisy):
        execute(mysql, f"DROP TABLE IF EXISTS {name}")
        execute(mysql, f"CREATE TABLE {name} (id INT PRIMARY KEY, note VARCHAR(20))")

    source = make_source(mysql_server, table, {}, commit_interval=5.0)
    start = source._start_position()
    for i in range(5):
        execute(mysql, f"INSERT INTO {noisy} VALUES ({i}, 'noise-{i}')")

    pending, buffered = read_once(source, start)

    assert not buffered, f"the other table's rows reached this source: {buffered}"
    assert pending is not None and pending[1] > start[1], (
        f"the read started at {start} and kept {pending}: five statements on another "
        "table moved the position nowhere"
    )

    execute(root_connection, "FLUSH BINARY LOGS")
    rotated, _ = read_once(source, pending)

    assert rotated is not None and rotated[0] != pending[0], (
        f"the read started at {pending} and kept {rotated}: the source stayed in a "
        "file the server has rotated out of"
    )


def test_stop_drains_the_buffer(mysql, mysql_server):
    """A stop() with changes still buffered produces them instead of dropping them."""
    table = "lite_drain"
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(50))")

    # Long enough that a read lands in the buffer and sits there until stop().
    source = make_source(
        mysql_server, table, {}, harness=HoldingHarness, commit_interval=5.0
    )
    running = RunningSource(source)
    with running:
        assert source.held.wait(timeout=30.0), "the source never committed"
        for i in range(3):
            execute(mysql, f"INSERT INTO {table} VALUES ({i}, 'buffered-{i}')")
        source.may_resume.set()

        assert source.buffered.wait(timeout=30.0), "the source never buffered the rows"
        assert not source.received(), "the rows were produced before stop() was asked"

    messages = source.received()
    assert count_kinds(messages, insert=3), messages
    assert [row_of(m) for m in of_kind(messages, "insert")] == [
        {"id": i, "note": f"buffered-{i}"} for i in range(3)
    ]


def test_commit_interval_bounds_the_delay_before_a_message(mysql, mysql_server):
    """
    A change must reach the topic within one `commit_interval` of being written.

    That is what the interval is documented to bound. A cycle that reads once, at its
    start, cannot hold it: a change written just after that read waits out the rest of
    the cycle, is read at the top of the next one, and is committed at the end of that
    one - nearly two intervals for a change that missed a read by a millisecond.
    """
    table = "lite_latency"
    interval = 2.0
    execute(mysql, f"DROP TABLE IF EXISTS {table}")
    execute(mysql, f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(20))")

    source = make_source(mysql_server, table, {}, commit_interval=interval)
    with RunningSource(source):
        assert source.polled.wait(timeout=30.0), "the stream never opened"
        # A row written before the source anchors its position is never streamed, and
        # the commit that delivers this one starts the cycle measured against below.
        execute(mysql, f"INSERT INTO {table} VALUES (1, 'warm-up')")
        cycle_start = wait_for_insert(source, 1, timeout=4 * interval)

        offset = 0.05 * interval
        time.sleep(max(0.0, cycle_start + offset - time.monotonic()))
        execute(mysql, f"INSERT INTO {table} VALUES (2, 'measured')")
        written = time.monotonic()
        delay = wait_for_insert(source, 2, timeout=4 * interval) - written

    assert delay <= 1.3 * interval, (
        f"a change written {offset:.2f}s into the cycle took {delay:.2f}s "
        f"({delay / interval:.2f}x) to reach the topic, but commit_interval="
        f"{interval} is documented as the bound on that delay"
    )


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
