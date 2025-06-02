import abc
import copy
import dataclasses
import logging
import re
import sqlite3
import time
from collections import OrderedDict
from typing import Any, Literal, Mapping, Tuple, Union

from .base import BaseField, BaseLookup
from .utils import CacheInfo

logger = logging.getLogger(__name__)


MISSING = object()


# @dataclasses.dataclass(frozen=True, kw_only=True)  # TODO: uncomment python 3.10+
@dataclasses.dataclass(frozen=True)
class BaseSQLiteLookupField(BaseField, abc.ABC):
    # TODO: Uncomment python 3.10+
    # ttl: float = 60.0
    # default: Any = None
    # first_match_only: bool = True

    @abc.abstractmethod
    def build_query(
        self, on: str, value: dict[str, Any]
    ) -> Tuple[str, Union[dict[str, Any], Tuple[str, ...]]]:
        """
        Build the SQL query string for this field.

        :param on: The key to use in the WHERE clause for lookup.
        :param value: The message value, used to substitute parameters in the query.

        :returns: A tuple of the SQL query string and the parameters.
        """
        raise NotImplementedError("Subclasses must implement build_query method.")

    @abc.abstractmethod
    def result(self, cursor: sqlite3.Cursor) -> Union[dict[str, Any], list[Any]]:
        """
        Extract the result from the cursor based on the field definition.

        :param cursor: The SQLite cursor containing the query results.

        :returns: The extracted data, either a single row or a list of rows.
        """
        raise NotImplementedError("Subclasses must implement result method.")


@dataclasses.dataclass(frozen=True)
class SQLiteLookupField(BaseSQLiteLookupField):
    """
    Field definition for use with SQLiteLookup in lookup joins.

    Table and column names are sanitized to prevent SQL injection.
    Rows will be deserialized into a dictionary with column names as keys.

    Example:

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")

        # Select the value in `col1` from the table `my_table` where `col2` matches the `sdf.join_lookup` on parameter.
        fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col1", "col2"], on="col2")}

        # After the lookup the `my_field` column in the message will contains:
        # {"col1": <row1 col1 value>, "col2": <row1 col2 value>}
        sdf = sdf.join_lookup(lookup, fields)
    ```

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")

        # Select the value in `col1` from the table `my_table` where `col2` matches the `sdf.join_lookup` on parameter.
        fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col1", "col2"], on="col2", first_match_only=False)}

        # After the lookup the `my_field` column in the message will contains:
        # [
        #   {"col1": <row1 col1 value>, "col2": <row1 col2 value>},
        #   {"col1": <row2 col1 value>, "col2": <row2 col2 value>},
        #   ...
        #   {"col1": <rowN col1 value>, "col2": <rowN col2 value>,},
        # ]
        sdf = sdf.join_lookup(lookup, fields)
    ```

    :param table: Name of the table to query in the SQLite database.
    :param columns: List of columns to select from the table.
    :param on: The column name to use in the WHERE clause for matching against the target key.
    :param order_by: Optional ORDER BY clause to sort the results.
    :param order_by_direction: Direction of the ORDER BY clause, either "ASC" or "DESC". Default is "ASC".
    :param ttl: Time-to-live for cache in seconds. Default is 60.0.
    :param default: Default value if no result is found. Default is None.
    :param first_match_only: If True, only the first row is returned; otherwise, all rows are returned.
    """

    table: str
    columns: list[str]
    on: str
    order_by: str = ""
    order_by_direction: Literal["ASC", "DESC"] = "ASC"

    # TODO: remove on python 3.10+
    ttl: float = 60.0
    default: Any = None
    first_match_only: bool = True

    def __post_init__(self) -> None:
        if "*" in self.columns:
            raise ValueError(
                "Using '*' in columns is not allowed for SQLiteLookupField."
            )

        self._validate_identifier(self.table)
        self._validate_identifier(self.on)
        for column in self.columns:
            self._validate_identifier(column)

        if self.order_by:
            self._validate_identifier(self.order_by)
            self._validate_identifier(self.order_by_direction)

    def _validate_identifier(self, identifier: str) -> None:
        """
        Validate SQL identifiers (table/column names) to allow only alphanumeric and underscore.

        :param identifier: the identifier to validate (table or column name).
        """
        if not re.match(r"^\w+$", identifier):
            raise ValueError(f"Invalid SQL identifier: {identifier}")

    def build_query(
        self, on: str, value: dict[str, Any]
    ) -> Tuple[str, Tuple[str, ...]]:
        """
        Build the SQL query string for this field.

        :param on: The key to use in the WHERE clause for lookup.
        :param value: The message value, used to substitute parameters in the query.

        :returns: A tuple of the SQL query string and the parameters.
        """

        # SQLite doesn't support parameters for colums and table names, we validate them to ensure they are safe.
        query = (
            f"SELECT {', '.join(self.columns)} FROM {self.table} WHERE {self.on} = ?"  # noqa: S608
        )
        if self.order_by:
            query += f" ORDER BY {self.order_by} {self.order_by_direction}"
        if self.first_match_only:
            query += " LIMIT 1"
        return query, (on,)

    def result(
        self, cursor: sqlite3.Cursor
    ) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """
        Extract the result from the cursor based on the field definition.

        :param cursor: The SQLite cursor containing the query results.

        :returns: The extracted data, either a single row or a list of rows.
        """
        if self.first_match_only:
            row = cursor.fetchone()
            return {k: v for k, v in zip(self.columns, row)} if row else self.default
        else:
            return [{k: v for k, v in zip(self.columns, row)} for row in cursor]


@dataclasses.dataclass(frozen=True)
class SQLiteLookupQueryField(BaseSQLiteLookupField):
    """
    Field definition for use with SQLiteLookup in lookup joins.

    Enables advanced SQL queries with support for parameter substitution from message columns, allowing dynamic lookups.

    The `sdf.join_lookup` `on` parameter is not used in the query itself, but is important for cache management. When caching is enabled, the query is executed once per TTL for each unique target key.

    Query results are returned as tuples of values, without additional deserialization.

    Example:

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")

        # Select all columns from the first row of `my_table` where `col2` matches the value of `field1` in the message.
        fields = {"my_field": SQLiteLookupQueryField("SELECT * FROM my_table WHERE col2 = :field1")}

        # After the lookup, the `my_field` column in the message will contain:
        # [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>]
        sdf = sdf.join_lookup(lookup, fields)
    ```

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")

        # Select all columns from all rows of `my_table` where `col2` matches the value of `field1` in the message.
        fields = {"my_field": SQLiteLookupQueryField("SELECT * FROM my_table WHERE col2 = :field1", first_match_only=False)}

        # After the lookup, the `my_field` column in the message will contain:
        # [
        #   [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>],
        #   [<row2 col1 value>, <row2 col2 value>, ..., <row2 colN value>],
        #   ...
        #   [<rowN col1 value>, <rowN col2 value>, ..., <rowN colN value>],
        # ]
        sdf = sdf.join_lookup(lookup, fields)
    ```

    :param query: SQL query to execute.
    :param ttl: Time-to-live for cache in seconds. Default is 60.0.
    :param default: Default value if no result is found. Default is None.
    :param first_match_only: If True, only the first row is returned; otherwise, all rows are returned.
    """

    query: str = dataclasses.field()

    # TODO: remove on python 3.10+
    ttl: float = 60.0
    default: Any = None
    first_match_only: bool = True

    def build_query(self, on: str, value: dict[str, Any]) -> Tuple[str, dict[str, Any]]:
        return self.query, value

    def result(self, cursor: sqlite3.Cursor) -> Union[dict[str, Any], list[Any]]:
        """
        Extract the result from the cursor based on the field definition.

        :param cursor: The SQLite cursor containing the query results.

        :returns: The extracted data, either a single row or a list of rows.
        """
        if self.first_match_only:
            return cursor.fetchone() or self.default
        return cursor.fetchall()


class SQLiteLookup(BaseLookup[Union[SQLiteLookupField, SQLiteLookupQueryField]]):
    """
    Lookup join implementation for enriching streaming data with data from a SQLite database.

    This class queries a SQLite database for each field, using a persistent connection and per-field caching
    based on a configurable TTL. The cache is a least recently used (LRU) cache with a configurable maximum size.

    Example:

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")
        fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col2"], on="primary_key_col")}
        sdf = sdf.join_lookup(lookup, fields)
    ```

    :param path: Path to the SQLite database file.
    :param cache_size: Maximum number of fields to keep in the LRU cache. Default is 1000.
    """

    def __init__(self, path: str, cache_size: int = 1000):
        self.db_path = path
        self._conn: sqlite3.Connection = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False
        )
        self._cache: OrderedDict[tuple[str, Any], tuple[float, Any]] = (
            OrderedDict()
        )  # (field_name, key) -> (timestamp, value)
        self._cache_size = cache_size
        self._cache_hits = 0
        self._cache_misses = 0
        self._debug = logger.isEnabledFor(logging.DEBUG)

        logger.info(
            f"SQLiteLookup initialized with db_path={self.db_path}, cache_maxsize={self._cache_size}"
        )

    def __del__(self) -> None:
        if hasattr(self, "_conn"):
            self._conn.close()

    def _process_field(
        self, field: BaseSQLiteLookupField, on: str, value: dict[str, Any]
    ) -> Union[dict[str, Any], list[Any]]:
        """
        Execute the SQL query for a given field and return the result.

        :param field: The field definition (SQLiteLookupField or SQLiteLookupQueryField).
        :param on: The key to use in the WHERE clause for lookup.
        :param value: The message value, used to substitute parameters in the query.

        :returns: The extracted data, either a single row or a list of rows.
        """
        query, parameters = field.build_query(on, value)
        if self._debug:
            logger.debug(f"Executing SQL: {query}")

        try:
            cur = self._conn.execute(query, parameters)
        except sqlite3.Error as e:
            logger.error(f"SQLite error while executing query: {e}")
            raise

        return field.result(cur)

    def _get_from_cache(
        self, cache_key: tuple[str, str], ttl: float, now: float
    ) -> Any:
        """
        Retrieve a value from the cache if it exists and is not expired.

        :param cache_key: The cache key (field_name, key).
        :param ttl: Time-to-live for the cache entry in seconds.
        :param now: The current time (epoch seconds).

        :returns: The cached value if present and not expired, else None.
        """
        cached = self._cache.get(cache_key)
        if cached and now - cached[0] < ttl:
            if self._debug:
                logger.debug(f"Cache hit for {cache_key}")
            self._cache_hits += 1
            self._cache.move_to_end(cache_key)
            return copy.copy(cached[1])
        return MISSING

    def _set_cache(self, cache_key: tuple[str, str], value: Any, now: float) -> None:
        """
        Set a value in the cache and evict the least recently used item if the cache exceeds its maximum size.

        :param cache_key: The cache key (field_name, key).
        :param value: The value to cache.
        :param now: The current time (epoch seconds).
        """
        self._cache[cache_key] = (now, copy.copy(value))
        self._cache.move_to_end(cache_key)
        if len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)

    def join(
        self,
        fields: Mapping[
            str, Union[SQLiteLookupField, SQLiteLookupQueryField]
        ],  # TODO: Use BaseSQLiteLookupField when python 3.10+ is required
        on: str,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: Any,
    ) -> None:
        """
        Enrich the message value in-place by querying SQLite for each field and caching results per TTL.

        :param fields: Mapping of field names to BaseSQLiteLookupField objects specifying how to extract and map enrichment data.
        :param on: The key used in the WHERE clause for SQLiteLookupField lookup.
        :param value: The message value.
        :param key: The message key.
        :param timestamp: The message timestamp.
        :param headers: The message headers.

        :returns: None. The input value dictionary is updated in-place with the enriched data.
        """
        now = time.time()
        for field_name, field in fields.items():
            if field.ttl <= 0:
                value[field_name] = self._process_field(field, on, value)
                continue

            cache_key = (field_name, on)
            result = self._get_from_cache(cache_key, field.ttl, now)
            if result is MISSING:
                if self._debug:
                    logger.debug(f"Cache miss for {cache_key}, querying database")
                self._cache_misses += 1
                result = self._process_field(field, on, value)
                self._set_cache(cache_key, result, now)

            value[field_name] = result

    def cache_info(self) -> CacheInfo:
        """
        Get cache statistics for the SQLiteLookup LRU cache.

        :returns: A dictionary containing cache statistics: hits, misses, size, maxsize.
        """
        return CacheInfo(
            hits=self._cache_hits,
            misses=self._cache_misses,
            size=len(self._cache),
            maxsize=self._cache_size,
        )
