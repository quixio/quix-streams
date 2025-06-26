import time
from unittest.mock import patch

import psycopg2
import pytest

from quixstreams.dataframe.joins.lookups.postgresql import (
    PostgresLookup,
    PostgresLookupField,
    PostgresLookupQueryField,
)


class MockPostgresConnect:
    def __init__(self):
        self._db_data = [
            {"id": "k1", "col1": "foo", "col2": 1},
            {"id": "k1", "col1": "foo0", "col2": 0},
            {"id": "k1", "col1": "foo5", "col2": 5},
            {"id": "k2", "col1": "bar", "col2": 2},
        ]
        self._query_result = [(1,)]  # 'SELECT 1' for connect placeholder
        self._fields_results = {}
        self._executors = []

    def update_data(self, data):
        self._db_data = data

    def _execute_lookup(self, columns, on, order_by, order_dir, on_val):
        """
        Since we are assigning what are essentially lambda functions in a
        loop, the `inner` pattern avoids "late binding" issue that comes from looping.
        """

        def inner():
            data = filter(lambda row: row[on] == on_val, self._db_data)
            if order_by:
                data = sorted(
                    data, key=lambda d: d[order_by], reverse=(order_dir != "ASC")
                )
            self._query_result = [tuple(row[col] for col in columns) for row in data]

        return inner

    def prepare_lookups(
        self, fields: dict[str, PostgresLookupField], on_val: str, error=False
    ):
        self._executors = []
        if error:

            def _raise():
                raise psycopg2.Error("mock connection error")

            self._executors.append(_raise)
            return
        for lookup in fields.values():
            self._executors.append(
                self._execute_lookup(
                    lookup.columns,
                    lookup.on,
                    lookup.order_by,
                    lookup.order_by_direction,
                    on_val,
                )
            )
        self._executors.reverse()

    def _execute_lookup_query(self, result):
        def inner():
            self._query_result = result

        return inner

    def prepare_lookup_queries(self, results: dict[str, list[tuple]], error=False):
        self._executors = []
        if error:

            def _raise():
                raise psycopg2.Error("mock connection error")

            self._executors.append(_raise)
            return
        for result in results.values():
            self._executors.append(self._execute_lookup_query(result))
        self._executors.reverse()

    def execute(self, *args, **kwargs):
        if self._executors:
            self._executors.pop()()

    def cursor(self):
        return self

    def fetchone(self):
        return self._query_result[0] if self._query_result else None

    def fetchall(self):
        return self._query_result

    def close(self):
        return

    def __iter__(self):
        return iter(self._query_result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


@pytest.fixture
def mock_postgres():
    with patch("psycopg2.connect") as conn:
        mock_connect = MockPostgresConnect()
        conn.return_value = mock_connect
        yield mock_connect


@pytest.fixture
def postgres_lookup(mock_postgres):
    return PostgresLookup(
        **{
            "host": "",
            "port": 0,
            "user": "",
            "password": "",
            "dbname": "",
        },
    )


def test_sqlite_lookup_basic(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    fields = {"f": PostgresLookupField(table="test", columns=["col1", "col2"], on="id")}

    value = {}
    join_value = "k1"
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo", "col2": 1}

    value = {}
    join_value = "k2"
    postgres_connection.prepare_lookups(fields=fields, on_val=join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "bar", "col2": 2}


def test_sqlite_lookup_cache(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    join_value = "k1"
    fields = {
        "f": PostgresLookupField(table="test", columns=["col1"], on="id", ttl=0.1)
    }

    value = {}
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    info = lookup.cache_info()
    assert info == {"hits": 0, "misses": 1, "size": 1, "maxsize": 1000}

    # Update DB directly, setting col1 to baz for k1
    postgres_connection.update_data(
        [
            {"id": "k1", "col1": "baz", "col2": 1},
            {"id": "k1", "col1": "baz", "col2": 0},
            {"id": "k1", "col1": "baz", "col2": 5},
            {"id": "k2", "col1": "bar", "col2": 2},
        ]
    )

    # Should return cached value
    value = {}
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    info = lookup.cache_info()
    assert info == {"hits": 1, "misses": 1, "size": 1, "maxsize": 1000}

    # Wait for cache to expire
    time.sleep(0.11)
    value = {}
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "baz"}

    info = lookup.cache_info()
    assert info == {"hits": 1, "misses": 2, "size": 1, "maxsize": 1000}


def test_sqlite_lookup_no_ttl(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    join_value = "k1"
    fields = {"f": PostgresLookupField(table="test", columns=["col1"], on="id", ttl=0)}

    value = {}
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    # Cache isn't in use
    info = lookup.cache_info()
    assert info == {"hits": 0, "misses": 0, "size": 0, "maxsize": 1000}

    # Update DB directly, setting col1 to baz for k1
    postgres_connection.update_data(
        [
            {"id": "k1", "col1": "baz", "col2": 1},
            {"id": "k1", "col1": "baz", "col2": 0},
            {"id": "k1", "col1": "baz", "col2": 5},
            {"id": "k2", "col1": "bar", "col2": 2},
        ]
    )

    # Should return new value
    value = {}
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "baz"}


def test_sqlite_lookup_orderby(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    join_value = "k1"
    fields = {
        "normal": PostgresLookupField(table="test", columns=["col1"], on="id"),
        "orderby": PostgresLookupField(
            table="test", columns=["col1"], on="id", order_by="col2"
        ),
        "asc": PostgresLookupField(
            table="test",
            columns=["col1"],
            on="id",
            order_by="col2",
            order_by_direction="ASC",
        ),
        "desc": PostgresLookupField(
            table="test",
            columns=["col1"],
            on="id",
            order_by="col2",
            order_by_direction="DESC",
        ),
    }

    value = {}
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value == {
        "normal": {"col1": "foo"},
        "orderby": {"col1": "foo0"},
        "asc": {"col1": "foo0"},
        "desc": {"col1": "foo5"},
    }


def test_sqlite_lookup_invalid_identifier():
    with pytest.raises(ValueError):
        PostgresLookupField(table="test;DROP TABLE test", columns=["col1"], on="id")

    with pytest.raises(ValueError):
        PostgresLookupField(table="test", columns=["col1;DROP"], on="id")


def test_sqlite_lookup_default(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    join_value = "missing"

    fields = {
        "f": PostgresLookupField(
            table="test", columns=["col1"], on="id", default="notfound"
        )
    }
    value = {}
    postgres_connection.prepare_lookups(fields, on_val=join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)
    assert value["f"] == "notfound"


def test_sqlite_lookup_error(mock_postgres, postgres_lookup):
    """
    Emulate a closed connection by raising an error during query
    """
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    join_value = "missing"

    fields = {
        "f": PostgresLookupField(
            table="test", columns=["col1"], on="id", default="notfound"
        )
    }

    with pytest.raises(psycopg2.Error):
        postgres_connection.prepare_lookups(fields, on_val=join_value, error=True)
        lookup.join(fields, join_value, {}, key=None, timestamp=0, headers=None)


def test_sqlite_lookup_query_field(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    fields = {
        "f": PostgresLookupQueryField(
            query="SELECT col2 FROM test WHERE col1 = %(field_1)s",
            first_match_only=True,
        )
    }

    value = {"field1": "foo"}
    postgres_connection.prepare_lookup_queries({"f": [(1,)]})
    lookup.join(fields, "key1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (1,)

    # Using the same target key will return the cached value
    value = {"field1": "bar"}
    lookup.join(fields, "key1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (1,)

    value = {"field1": "bar"}
    postgres_connection.prepare_lookup_queries({"f": [(2,)]})
    lookup.join(fields, "key2", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (2,)


def test_sqlite_lookup_field_all_rows(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup
    join_value = "k1"

    # first_match_only=False
    fields = {
        "f": PostgresLookupField(
            table="test", columns=["col1", "col2"], on="id", first_match_only=False
        )
    }
    value = {}
    postgres_connection.prepare_lookups(fields, join_value)
    lookup.join(fields, join_value, value, key=None, timestamp=0, headers=None)

    # Should return all rows with id='k1'
    assert isinstance(value["f"], list)
    assert {"col1": "foo", "col2": 1} in value["f"]
    assert {"col1": "foo0", "col2": 0} in value["f"]
    assert {"col1": "foo5", "col2": 5} in value["f"]
    assert len(value["f"]) == 3


def test_sqlite_lookup_query_field_all_rows(mock_postgres, postgres_lookup):
    postgres_connection = mock_postgres
    lookup = postgres_lookup

    # first_match_only=False
    fields = {
        "f": PostgresLookupQueryField(
            query="SELECT col1, col2 FROM test WHERE id = %(id)s",
            first_match_only=False,
        )
    }
    value = {"id": "k1"}
    # Should return all col1, col2 values for id='k1'
    query_result = {"f": [("foo", 1), ("foo0", 0), ("foo5", 5)]}
    postgres_connection.prepare_lookup_queries(query_result)
    lookup.join(fields, "cache_key_0", value, key=None, timestamp=0, headers=None)
    assert isinstance(value["f"], list)
    assert value == {"id": "k1", **query_result}
