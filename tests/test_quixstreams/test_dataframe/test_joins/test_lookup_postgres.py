import time
from typing import Generator, Iterable

import psycopg2
import pytest

# Skip these tests if testcontainers is unavailable in the environment
pytest.importorskip("testcontainers", reason="testcontainers not available")
from testcontainers.postgres import PostgresContainer

from quixstreams.dataframe.joins.lookups.postgresql import PostgresLookup


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


DEFAULT_TABLE_DATA = [
    {"id": "k1", "col1": "foo", "col2": 1},
    {"id": "k1", "col1": "foo0", "col2": 0},
    {"id": "k1", "col1": "foo5", "col2": 5},
    {"id": "k2", "col1": "bar", "col2": 2},
]


@pytest.fixture()
def init_table(postgres_connection):
    def inner(table_name: str, rows: Iterable[dict] = ()):
        rows = rows or DEFAULT_TABLE_DATA
        with postgres_connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"(pk SERIAL, id TEXT, col1 TEXT, col2 INTEGER)"
            )
            cursor.execute(f"DELETE FROM {table_name}")  # noqa: S608
            cursor.executemany(
                f"INSERT INTO {table_name} (id, col1, col2) VALUES (%s, %s, %s)",  # noqa: S608
                [tuple(r.values()) for r in rows],
            )
        postgres_connection.commit()

    return inner


@pytest.fixture
def postgres_lookup(postgres_connection) -> PostgresLookup:
    info = postgres_connection.info
    return PostgresLookup(
        host=info.host,
        port=info.port,
        user=info.user,
        password=info.password,
        dbname=info.dbname,
    )


def test_postgres_lookup_basic(init_table, postgres_lookup):
    init_table(table_name="test")
    fields = {
        "f": postgres_lookup.field(table="test", columns=["col1", "col2"], on="id")
    }

    value = {}
    postgres_lookup.join(
        fields, on="k1", value=value, key=None, timestamp=0, headers=None
    )
    assert value["f"] == {"col1": "foo", "col2": 1}

    value = {}
    postgres_lookup.join(
        fields, on="k2", value=value, key=None, timestamp=0, headers=None
    )
    assert value["f"] == {"col1": "bar", "col2": 2}


def test_postgres_lookup_cache(init_table, postgres_lookup):
    init_table(table_name="test")

    lookup = postgres_lookup
    join_value = "k1"
    fields = {"f": lookup.field(table="test", columns=["col1"], on="id", ttl=0.1)}

    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    info = lookup.cache_info()
    assert info == {"hits": 0, "misses": 1, "size": 1, "maxsize": 1000}

    # Update DB directly, setting col1 to baz for k1
    init_table(
        table_name="test",
        rows=[
            {"id": "k1", "col1": "baz", "col2": 1},
            {"id": "k1", "col1": "baz", "col2": 0},
            {"id": "k1", "col1": "baz", "col2": 5},
            {"id": "k2", "col1": "bar", "col2": 2},
        ],
    )

    # Should return cached value
    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    info = lookup.cache_info()
    assert info == {"hits": 1, "misses": 1, "size": 1, "maxsize": 1000}

    # Wait for cache to expire
    time.sleep(0.5)
    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "baz"}

    info = lookup.cache_info()
    assert info == {"hits": 1, "misses": 2, "size": 1, "maxsize": 1000}


def test_postgres_lookup_no_ttl(init_table, postgres_lookup):
    init_table(table_name="test")
    lookup = postgres_lookup
    join_value = "k1"
    fields = {"f": lookup.field(table="test", columns=["col1"], on="id", ttl=0)}

    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    # Cache isn't in use
    info = lookup.cache_info()
    assert info == {"hits": 0, "misses": 0, "size": 0, "maxsize": 1000}

    # Update DB directly, setting col1 to baz for k1
    init_table(
        table_name="test",
        rows=[
            {"id": "k1", "col1": "baz", "col2": 1},
            {"id": "k1", "col1": "baz", "col2": 0},
            {"id": "k1", "col1": "baz", "col2": 5},
            {"id": "k2", "col1": "bar", "col2": 2},
        ],
    )

    # Should return new value
    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "baz"}


def test_postgres_lookup_orderby(init_table, postgres_lookup):
    init_table(table_name="test")
    lookup = postgres_lookup
    join_value = "k1"
    fields = {
        "normal": lookup.field(table="test", columns=["col1"], on="id"),
        "orderby": lookup.field(
            table="test", columns=["col1"], on="id", order_by="col2"
        ),
        "asc": lookup.field(
            table="test",
            columns=["col1"],
            on="id",
            order_by="col2",
            order_by_direction="ASC",
        ),
        "desc": lookup.field(
            table="test",
            columns=["col1"],
            on="id",
            order_by="col2",
            order_by_direction="DESC",
        ),
    }

    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value == {
        "normal": {"col1": "foo"},
        "orderby": {"col1": "foo0"},
        "asc": {"col1": "foo0"},
        "desc": {"col1": "foo5"},
    }


def test_postgres_lookup_invalid_identifier(postgres_lookup):
    with pytest.raises(ValueError):
        postgres_lookup.field(table="test;DROP TABLE test", columns=["col1"], on="id")

    with pytest.raises(ValueError):
        postgres_lookup.field(table="test", columns=["col1;DROP"], on="id")


def test_postgres_lookup_default(init_table, postgres_lookup):
    init_table(table_name="test")
    lookup = postgres_lookup
    join_value = "missing"

    fields = {
        "f": lookup.field(table="test", columns=["col1"], on="id", default="notfound")
    }
    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == "notfound"


def test_postgres_lookup_query_field(init_table, postgres_lookup):
    init_table(table_name="test")
    lookup = postgres_lookup
    fields = {
        "f": lookup.query_field(
            query="SELECT col2 FROM test WHERE col1 = %(field1)s",
            first_match_only=True,
        )
    }

    value = {"field1": "foo"}
    lookup.join(fields, "key1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (1,)

    # Using the same target key will return the cached value
    value = {"field1": "bar"}
    lookup.join(fields, "key1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (1,)

    value = {"field1": "bar"}
    lookup.join(fields, "key2", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (2,)


def test_postgres_lookup_field_all_rows(init_table, postgres_lookup):
    init_table(table_name="test")
    lookup = postgres_lookup
    join_value = "k1"

    # first_match_only=False
    fields = {
        "f": lookup.field(
            table="test", columns=["col1", "col2"], on="id", first_match_only=False
        )
    }
    value = {}
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)

    # Should return all rows with id='k1'
    assert isinstance(value["f"], list)
    assert {"col1": "foo", "col2": 1} in value["f"]
    assert {"col1": "foo0", "col2": 0} in value["f"]
    assert {"col1": "foo5", "col2": 5} in value["f"]
    assert len(value["f"]) == 3


def test_postgres_lookup_query_field_all_rows(init_table, postgres_lookup):
    lookup = postgres_lookup

    # first_match_only=False
    fields = {
        "f": lookup.query_field(
            query="SELECT col1, col2 FROM test WHERE id = %(id)s",
            first_match_only=False,
        )
    }
    value = {"id": "k1"}
    # Should return all col1, col2 values for id='k1'
    query_result = {"f": [("foo", 1), ("foo0", 0), ("foo5", 5)]}
    lookup.join(fields, "cache_key_0", value, key=None, timestamp=0, headers=None)
    assert isinstance(value["f"], list)
    assert value == {"id": "k1", **query_result}
