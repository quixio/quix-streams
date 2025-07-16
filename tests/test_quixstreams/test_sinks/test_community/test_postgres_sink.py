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


@pytest.fixture()
def refresh_table(postgres_connection):
    def inner(table_name: str = DEFAULT_TABLE_NAME):
        with postgres_connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        postgres_connection.commit()

    return inner


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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Base functionality: each record is treated as an independent entity"""
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """An error is raised when a primary key is repeated without upsert enabled"""
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Composite means primary key comprised of multiple columns"""
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Primary keys can't be added once others have already been defined."""
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """
    When defining primary keys, you must specify all existing ones on the table.
    """
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """
    Upserting works with deduplication (repeat primary key in a given batch of data
    is consolidated to last received version of said message)
    """
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """Upserting works as expected across separate transactions"""
    refresh_table()
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
    refresh_table,
    sink_app_factory,
    postgres_sink_factory,
    resource_source_factory,
    get_all_table_rows,
):
    """
    Upserting works with a composite primary key.
    """
    refresh_table()
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
