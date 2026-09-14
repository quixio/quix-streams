import datetime
import uuid
from typing import Generator

import psycopg2
import pytest
from psycopg2 import sql
from testcontainers.postgres import PostgresContainer

from quixstreams.models.topics import Topic
from quixstreams.sinks.base import BaseSink
from quixstreams.sinks.community.postgresql import (
    KEY_COLUMN_NAME,
    TIMESTAMP_COLUMN_NAME,
    PostgreSQLSink,
    PostgreSQLSinkException,
    PostgresSQLSinkInvalidPK,
    PostgresSQLSinkMissingExistingPK,
    PostgresSQLSinkPKNullValue,
    PrimaryKeyColumns,
    TableName,
)
from quixstreams.sources.base import Source

DEFAULT_TABLE_NAME = "test_table"


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    container = PostgresContainer("postgres:17.5")
    with container:
        yield container


@pytest.fixture()
def postgres_connection(postgres_container):
    with psycopg2.connect(
        dbname=postgres_container.dbname,
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(postgres_container.port),
        user=postgres_container.username,
        password=postgres_container.password,
        options=f"-c statement_timeout={20}s",
    ) as conn:
        yield conn


@pytest.fixture(autouse=True)
def refresh_table(postgres_connection):
    with postgres_connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {DEFAULT_TABLE_NAME}")
    postgres_connection.commit()


class ResourceEventSource(Source):
    def __init__(self, events: list[dict]):
        super().__init__(name=f"sink_resource_generator_{uuid.uuid4()}")
        self._events = iter(events)

    def _next_event(self):
        try:
            return next(self._events)
        except StopIteration:
            return None

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_deserializer="str",
            value_deserializer="json",
            key_serializer="str",
            value_serializer="json",
        )

    def run(self):
        """
        Each Source must have a `run` method.

        It will include the logic behind your source, contained within a
        "while self.running" block for exiting when its parent Application stops.

        There a few methods on a Source available for producing to Kafka, like
        `self.serialize` and `self.produce`.
        """
        # either break when the app is stopped, or data is exhausted
        while self.running and (event := self._next_event()):
            event_serialized = self.serialize(key=event["hostname"], value=event)
            self.produce(key=event_serialized.key, value=event_serialized.value)


@pytest.fixture()
def get_all_table_rows(postgres_connection):
    cols = [
        KEY_COLUMN_NAME,
        TIMESTAMP_COLUMN_NAME,
        "event_time",
        "hostname",
        "resource",
        "used_percent",
    ]

    def inner(table_name: str = DEFAULT_TABLE_NAME):
        query = sql.SQL("SELECT {columns} FROM {table}").format(
            columns=sql.SQL(", ").join(map(sql.Identifier, cols)),
            table=sql.Identifier(table_name),
        )
        with postgres_connection.cursor() as cursor:
            cursor.execute(query)
            data = [dict(zip(cols, row)) for row in cursor.fetchall()]
        postgres_connection.commit()
        return data

    return inner


@pytest.fixture
def postgres_sink_factory(postgres_connection) -> callable:
    def inner(
        table_name: TableName = DEFAULT_TABLE_NAME,
        primary_key_columns: PrimaryKeyColumns = (),
        upsert_on_primary_key: bool = False,
        on_conflict_do_nothing: bool = False,
        include_metadata: bool = True,
    ) -> PostgreSQLSink:
        info = postgres_connection.info
        return PostgreSQLSink(
            host=info.host,
            port=info.port,
            user=info.user,
            password=info.password,
            dbname=info.dbname,
            table_name=table_name,
            primary_key_columns=primary_key_columns,
            upsert_on_primary_key=upsert_on_primary_key,
            on_conflict_do_nothing=on_conflict_do_nothing,
            include_metadata=include_metadata,
        )

    return inner


@pytest.fixture()
def sink_app_factory(app_factory):
    def inner(source: Source, sink: BaseSink):
        app = app_factory(commit_every=10)
        app.dataframe(source=source).sink(sink)
        return app

    return inner


@pytest.fixture()
def resource_source_factory():
    def inner(data: list[dict]) -> ResourceEventSource:
        return ResourceEventSource(events=data)

    return inner


def test_sink(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Base functionality: each record is treated as an independent entity"""
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109873,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 37.03,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
        {
            "event_time": 1752158109877,
            "hostname": "host_1",
            "resource": "RAM",
            "used_percent": 80.01,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(),
    )
    result = app.run(count=len(data), metadata=True)

    for idx, r in enumerate(result):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()


def test_sink_primary_key(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
    ]
    primary_key_columns = ["hostname"]
    sink = postgres_sink_factory(primary_key_columns=primary_key_columns)
    app = sink_app_factory(
        resource_source_factory(data),
        sink,
    )
    result = app.run(count=len(data), metadata=True)

    for idx, r in enumerate(result):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()
    assert (
        sink.get_current_primary_key_columns(DEFAULT_TABLE_NAME) == primary_key_columns
    )


def test_sink_primary_key_collision(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """An error is raised when a primary key is repeated without upsert enabled"""
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 56.22,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(primary_key_columns=["hostname"]),
    )
    with pytest.raises(PostgreSQLSinkException) as exc_info:
        app.run()
    assert "Key (hostname)=(host_0) already exists" in (str(exc_info.value))


def test_sink_composite_primary_key(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Composite means primary key comprised of multiple columns"""
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
    ]
    primary_key_columns = ["hostname", "resource"]
    sink = postgres_sink_factory(primary_key_columns=primary_key_columns)
    app = sink_app_factory(
        resource_source_factory(data),
        sink,
    )
    result = app.run(count=len(data), metadata=True)

    for idx, r in enumerate(result):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()
    assert (
        sink.get_current_primary_key_columns(DEFAULT_TABLE_NAME) == primary_key_columns
    )


def test_sink_primary_key_null_value(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": None,
            "used_percent": 56.22,
        },
    ]
    primary_key_columns = ["hostname", "resource"]
    sink = postgres_sink_factory(primary_key_columns=primary_key_columns)
    app = sink_app_factory(
        resource_source_factory(data),
        sink,
    )
    with pytest.raises(PostgresSQLSinkPKNullValue) as exc_info:
        app.run(count=len(data), metadata=True)
    assert "resource" in str(exc_info.value)


def test_sink_primary_key_additional_key(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Primary keys can't be added once others have already been defined."""
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
    ]
    primary_key_columns = ["hostname"]
    sink = postgres_sink_factory(primary_key_columns=primary_key_columns)
    app = sink_app_factory(
        resource_source_factory(data),
        sink,
    )
    result = app.run(count=len(data), metadata=True)

    for idx, r in enumerate(result):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()
    assert (
        sink.get_current_primary_key_columns(DEFAULT_TABLE_NAME) == primary_key_columns
    )

    # Now attempt to add a new primary key, "resource"
    primary_key_columns = ["hostname", "resource"]
    sink = postgres_sink_factory(primary_key_columns=primary_key_columns)
    app = sink_app_factory(
        resource_source_factory(
            [{"event_time": 1, "hostname": "0", "resource": "0", "used_percent": 0.0}]
        ),
        sink,
    )
    with pytest.raises(PostgresSQLSinkInvalidPK):
        app.run()
    assert (
        sink.get_current_primary_key_columns(DEFAULT_TABLE_NAME) != primary_key_columns
    )


def test_sink_primary_key_missing_composite_key(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """
    When defining primary keys, you must specify all existing ones on the table.
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
    ]
    primary_key_columns = ["hostname", "resource"]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(primary_key_columns=primary_key_columns),
    )
    app.run(count=len(data))

    # run app again, but missing an existing primary key
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(primary_key_columns=["hostname"]),
    )

    with pytest.raises(PostgresSQLSinkMissingExistingPK) as exc_info:
        app.run(count=len(data))
    assert "resource" in str(exc_info.value)


def test_sink_primary_key_upsert_dedup(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """
    Upserting works with deduplication (repeat primary key in a given batch of data
    is consolidated to last received version of said message)
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
        {
            "event_time": 1752158109881,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 11.29,
        },
        {
            "event_time": 1752158109883,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 96.12,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname"], upsert_on_primary_key=True
        ),
    )
    result = app.run(count=len(data), metadata=True)

    data = data[2:]
    for idx, r in enumerate(result[2:]):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()


def test_sink_primary_key_upsert_split_transactions(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Upserting works as expected across separate transactions"""
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname"], upsert_on_primary_key=True
        ),
    )
    result = app.run(count=len(data), metadata=True)

    for idx, r in enumerate(result):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()

    # Send new messages for same keys in another run of the app
    data = [
        {
            "event_time": 1752158109881,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 11.29,
        },
        {
            "event_time": 1752158109883,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 96.12,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname"], upsert_on_primary_key=True
        ),
    )
    result = app.run(count=len(data), metadata=True)

    for idx, r in enumerate(result):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()


def test_sink_composite_primary_key_upsert(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """
    Upserting works with a composite primary key.
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109873,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 37.03,
        },
        {
            "event_time": 1752158109881,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 77.87,
        },
        {
            "event_time": 1752158109882,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 44.75,
        },
        # this also checks that these indeed remain independent
        {
            "event_time": 1752158109888,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
        {
            "event_time": 1752158109889,
            "hostname": "host_1",
            "resource": "RAM",
            "used_percent": 16.09,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname", "resource"], upsert_on_primary_key=True
        ),
    )
    result = app.run(count=len(data), metadata=True)

    data = data[2:]
    for idx, r in enumerate(result[2:]):
        data[idx][KEY_COLUMN_NAME] = r["_key"]
        data[idx][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
            r["_timestamp"] / 1000
        )
    assert data == get_all_table_rows()


def test_sink_without_metadata(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    postgres_connection,
):
    """Validates PR #1098: include_metadata=False omits __key and timestamp columns.

    When include_metadata=False, the written table must contain ONLY data columns
    (event_time, hostname, resource, used_percent) and must NOT contain the
    metadata columns (__key, timestamp).
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109873,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 37.03,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
        {
            "event_time": 1752158109877,
            "hostname": "host_1",
            "resource": "RAM",
            "used_percent": 80.01,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(include_metadata=False),
    )
    app.run(count=len(data))

    # -- Assert metadata columns are ABSENT from the table schema --
    data_columns = {"event_time", "hostname", "resource", "used_percent"}
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (DEFAULT_TABLE_NAME,),
        )
        actual_columns = {row[0] for row in cursor.fetchall()}
    postgres_connection.commit()

    assert KEY_COLUMN_NAME not in actual_columns, (
        f"Metadata column {KEY_COLUMN_NAME!r} should be absent when "
        f"include_metadata=False, but found columns: {actual_columns}"
    )
    assert TIMESTAMP_COLUMN_NAME not in actual_columns, (
        f"Metadata column {TIMESTAMP_COLUMN_NAME!r} should be absent when "
        f"include_metadata=False, but found columns: {actual_columns}"
    )
    for col in data_columns:
        assert col in actual_columns, (
            f"Data column {col!r} should be present, "
            f"but found columns: {actual_columns}"
        )

    # -- Assert the written row data matches the input data --
    data_col_list = sorted(data_columns)
    select_query = sql.SQL("SELECT {columns} FROM {table} ORDER BY {order}").format(
        columns=sql.SQL(", ").join(map(sql.Identifier, data_col_list)),
        table=sql.Identifier(DEFAULT_TABLE_NAME),
        order=sql.Identifier("event_time"),
    )
    with postgres_connection.cursor() as cursor:
        cursor.execute(select_query)
        rows = [dict(zip(data_col_list, row)) for row in cursor.fetchall()]
    postgres_connection.commit()

    expected = sorted(data, key=lambda r: r["event_time"])
    assert rows == expected


def test_sink_with_metadata_default(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    postgres_connection,
):
    """Validates PR #1098: default include_metadata=True still includes __key and
    timestamp columns, ensuring backward compatibility.
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(),  # default include_metadata=True
    )
    app.run(count=len(data))

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (DEFAULT_TABLE_NAME,),
        )
        actual_columns = {row[0] for row in cursor.fetchall()}
    postgres_connection.commit()

    assert KEY_COLUMN_NAME in actual_columns, (
        f"Metadata column {KEY_COLUMN_NAME!r} should be present by default, "
        f"but found columns: {actual_columns}"
    )
    assert TIMESTAMP_COLUMN_NAME in actual_columns, (
        f"Metadata column {TIMESTAMP_COLUMN_NAME!r} should be present by default, "
        f"but found columns: {actual_columns}"
    )


def test_sink_without_metadata_upsert_primary_key(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    postgres_connection,
):
    """Validates PR #1098: include_metadata=False combined with primary-key upsert.

    When include_metadata=False and upsert_on_primary_key=True, the dedup/upsert
    logic must still work correctly and the written table must contain only data
    columns (no __key, no timestamp). After dedup on hostname, only the last
    value per hostname should survive (data[2:]).
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 56.22,
        },
        {
            "event_time": 1752158109881,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 11.29,
        },
        {
            "event_time": 1752158109883,
            "hostname": "host_1",
            "resource": "CPU",
            "used_percent": 96.12,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname"],
            upsert_on_primary_key=True,
            include_metadata=False,
        ),
    )
    app.run(count=len(data))

    # -- Assert metadata columns are ABSENT from the table schema --
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (DEFAULT_TABLE_NAME,),
        )
        actual_columns = {row[0] for row in cursor.fetchall()}
    postgres_connection.commit()

    assert KEY_COLUMN_NAME not in actual_columns, (
        f"Metadata column {KEY_COLUMN_NAME!r} should be absent when "
        f"include_metadata=False, but found columns: {actual_columns}"
    )
    assert TIMESTAMP_COLUMN_NAME not in actual_columns, (
        f"Metadata column {TIMESTAMP_COLUMN_NAME!r} should be absent when "
        f"include_metadata=False, but found columns: {actual_columns}"
    )

    # -- Assert the surviving rows are the deduped last-per-hostname values --
    data_columns = sorted(["event_time", "hostname", "resource", "used_percent"])
    select_query = sql.SQL("SELECT {columns} FROM {table} ORDER BY {order}").format(
        columns=sql.SQL(", ").join(map(sql.Identifier, data_columns)),
        table=sql.Identifier(DEFAULT_TABLE_NAME),
        order=sql.Identifier("hostname"),
    )
    with postgres_connection.cursor() as cursor:
        cursor.execute(select_query)
        rows = [dict(zip(data_columns, row)) for row in cursor.fetchall()]
    postgres_connection.commit()

    expected = sorted(data[2:], key=lambda r: r["hostname"])
    assert rows == expected


def test_sink_on_conflict_do_nothing(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Validates PR #1099: on_conflict_do_nothing=True silently ignores duplicate
    primary-key inserts via ON CONFLICT DO NOTHING instead of raising an exception.
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 56.22,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname"],
            on_conflict_do_nothing=True,
        ),
    )
    # Without on_conflict_do_nothing this would raise PostgreSQLSinkException
    result = app.run(count=len(data), metadata=True)

    # Only the first row should survive (duplicate hostname silently dropped)
    expected = [data[0]]
    expected[0][KEY_COLUMN_NAME] = result[0]["_key"]
    expected[0][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
        result[0]["_timestamp"] / 1000
    )
    assert expected == get_all_table_rows()


def test_sink_on_conflict_do_nothing_default_absent(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Validates PR #1099: when on_conflict_do_nothing is not set (default False),
    a duplicate primary key raises PostgreSQLSinkException as before.
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 56.22,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(primary_key_columns=["hostname"]),
    )
    with pytest.raises(PostgreSQLSinkException) as exc_info:
        app.run()
    assert "Key (hostname)=(host_0) already exists" in str(exc_info.value)


def test_sink_upsert_and_on_conflict_do_nothing_raises(
    postgres_sink_factory,
):
    """Validates PR #1099: combining upsert_on_primary_key=True with
    on_conflict_do_nothing=True raises ValueError at construction time.
    """
    with pytest.raises(ValueError, match="Cannot use both"):
        postgres_sink_factory(
            primary_key_columns=["hostname"],
            upsert_on_primary_key=True,
            on_conflict_do_nothing=True,
        )


def test_sink_upsert_without_on_conflict_do_nothing(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Validates PR #1099: upsert_on_primary_key=True alone performs a proper upsert
    (ON CONFLICT ... DO UPDATE SET) and does NOT emit ON CONFLICT DO NOTHING.
    After dedup on hostname, only the last value per hostname should survive.
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 56.22,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname"],
            upsert_on_primary_key=True,
        ),
    )
    result = app.run(count=len(data), metadata=True)

    # Upsert: the second record overwrites the first (same hostname)
    expected = [data[1]]
    expected[0][KEY_COLUMN_NAME] = result[1]["_key"]
    expected[0][TIMESTAMP_COLUMN_NAME] = datetime.datetime.fromtimestamp(
        result[1]["_timestamp"] / 1000
    )
    assert expected == get_all_table_rows()


def test_sink_on_conflict_do_nothing_with_metadata(
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    postgres_connection,
):
    """Validates PR #1099 orthogonality: on_conflict_do_nothing=True combined with
    include_metadata=True (default) — metadata columns (__key, timestamp) are
    present AND duplicate primary keys are silently ignored.
    """
    data = [
        {
            "event_time": 1752158109872,
            "hostname": "host_0",
            "resource": "CPU",
            "used_percent": 91.61,
        },
        {
            "event_time": 1752158109876,
            "hostname": "host_0",
            "resource": "RAM",
            "used_percent": 56.22,
        },
    ]
    app = sink_app_factory(
        resource_source_factory(data),
        postgres_sink_factory(
            primary_key_columns=["hostname"],
            on_conflict_do_nothing=True,
            include_metadata=True,
        ),
    )
    # Should not raise — duplicate is silently ignored
    app.run(count=len(data))

    # Verify metadata columns are present
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (DEFAULT_TABLE_NAME,),
        )
        actual_columns = {row[0] for row in cursor.fetchall()}
    postgres_connection.commit()

    assert KEY_COLUMN_NAME in actual_columns, (
        f"Metadata column {KEY_COLUMN_NAME!r} should be present when "
        f"include_metadata=True, but found columns: {actual_columns}"
    )
    assert TIMESTAMP_COLUMN_NAME in actual_columns, (
        f"Metadata column {TIMESTAMP_COLUMN_NAME!r} should be present when "
        f"include_metadata=True, but found columns: {actual_columns}"
    )

    # Verify only one row survived (duplicate hostname silently dropped)
    count_query = sql.SQL("SELECT COUNT(*) FROM {table}").format(
        table=sql.Identifier(DEFAULT_TABLE_NAME),
    )
    with postgres_connection.cursor() as cursor:
        cursor.execute(count_query)
        row_count = cursor.fetchone()[0]
    postgres_connection.commit()

    assert (
        row_count == 1
    ), f"Expected 1 row (duplicate silently dropped), got {row_count}"
