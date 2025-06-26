import time
from collections import deque
from functools import wraps
from unittest.mock import patch

import psycopg2
import pytest

from quixstreams.dataframe.joins.lookups.postgresql import (
    PostgresLookup,
    PostgresLookupField,
    PostgresLookupQueryField,
)


class MockPostgresConnect:
    """
    This allows you to mock out a postgres connection with a single populated table.

    It queues up `MockPostgresCursor`s with mock results based on a given
    PostgresLookupFields or PostgresLookupQueryFields.

    Simply call `prepare_lookups` (PostgresLookupFields) or `prepare_lookup_queries`
    (PostgresLookupQueryFields) before each PostgresLookup.join().
    """

    def __init__(self):
        self._table_data = [
            {"id": "k1", "col1": "foo", "col2": 1},
            {"id": "k1", "col1": "foo0", "col2": 0},
            {"id": "k1", "col1": "foo5", "col2": 5},
            {"id": "k2", "col1": "bar", "col2": 2},
        ]
        self._pending_cursors = deque()
        self._add_cursor([(1,)])  # handles SELECT 1 (initial connect check)

    def update_data(self, data):
        self._table_data = data

    def _lookup_result(self, columns, on, order_by, order_dir, on_val):
        data = filter(lambda row: row[on] == on_val, self._table_data)
        if order_by:
            data = sorted(data, key=lambda d: d[order_by], reverse=(order_dir != "ASC"))
        return [tuple(row[col] for col in columns) for row in data]

    def prepare_lookups(
        self, fields: dict[str, PostgresLookupField], on_val: str, error=False
    ):
        """
        Here we can use the params of PostgresLookupField to translate a postgres query
        into equivalent python operations. We pack this result onto a mock cursor, which is
        popped when the PostgresLookup.join is called.
        """
        self._pending_cursors.clear()
        if error:
            self._add_cursor(psycopg2.Error("mock connection error"))
            return
        for lookup in fields.values():
            self._add_cursor(
                self._lookup_result(
                    lookup.columns,
                    lookup.on,
                    lookup.order_by,
                    lookup.order_by_direction,
                    on_val,
                )
            )

    def prepare_lookup_queries(self, results: dict[str, list[tuple]], error=False):
        """
        In reality this doesn't really mock much since it's originally supposed to be a
        user-provided query. Mostly here to confirm the cache works.
        """
        self._pending_cursors.clear()
        if error:
            self._add_cursor(psycopg2.Error("mock connection error"))
            return
        for result in results.values():
            self._add_cursor(result)

    def _add_cursor(self, result):
        self._pending_cursors.append(MockPostgresCursor(result))

    def cursor(self):
        return self._pending_cursors.popleft()


def _is_executed(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not getattr(self, "_executed"):
            raise RuntimeError("Must call .execute() first")
        return method(self, *args, **kwargs)

    return wrapper


class MockPostgresCursor:
    """
    A mock cursor, with the expected result already packed on it;
    `execute` just allows it to return the result.
    """

    def __init__(self, result):
        self._result = result
        self._executed = False

    def set_result(self, result):
        self._result = result

    def execute(self, *args, **kwargs):
        if isinstance(self._result, Exception):
            raise self._result
        self._executed = True

    def close(self):
        self._executed = False

    @_is_executed
    def fetchone(self):
        return self._result[0] if self._result else None

    @_is_executed
    def fetchall(self):
        return self._result

    @_is_executed
    def __iter__(self):
        return iter(self._result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


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


def test_postgres_lookup_basic(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_cache(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_no_ttl(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_orderby(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_invalid_identifier():
    with pytest.raises(ValueError):
        PostgresLookupField(table="test;DROP TABLE test", columns=["col1"], on="id")

    with pytest.raises(ValueError):
        PostgresLookupField(table="test", columns=["col1;DROP"], on="id")


def test_postgres_lookup_default(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_error(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_query_field(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_field_all_rows(mock_postgres, postgres_lookup):
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


def test_postgres_lookup_query_field_all_rows(mock_postgres, postgres_lookup):
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
