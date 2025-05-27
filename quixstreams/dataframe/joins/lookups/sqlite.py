import abc
import copy
import dataclasses
import logging
import re
import sqlite3
import time
from typing import Any, Literal, Mapping, Tuple, Union

from .base import BaseField, BaseLookup

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, kw_only=True)
class BaseSQLiteLookupField(BaseField, abc.ABC):
    ttl: float = 60.0
    default: Any = None
    first_match_only: bool = True

    @abc.abstractmethod
    def build_query(
        self, target_key: str, value: dict[str, Any]
    ) -> Tuple[str, Union[dict[str, Any], Tuple[str, ...]]]:
        """
        Build the SQL query string for this field.

        :param target_key: The key to use in the WHERE clause for lookup.
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

        # Select the value in `col1` from the table `my_table` where `col2` matches the `sdf.lookup_join` on parameter.
        fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col1", "col2"], on="col2")}

        # After the lookup the `my_field` column in the message will contains:
        # {"col1": <row1 col1 value>, "col2": <row1 col2 value>}
        sdf = sdf.lookup_join(lookup, fields)
    ```

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")

        # Select the value in `col1` from the table `my_table` where `col2` matches the `sdf.lookup_join` on parameter.
        fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col1", "col2"], on="col2", first_match_only=False)}

        # After the lookup the `my_field` column in the message will contains:
        # [
        #   {"col1": <row1 col1 value>, "col2": <row1 col2 value>},
        #   {"col1": <row2 col1 value>, "col2": <row2 col2 value>},
        #   ...
        #   {"col1": <rowN col1 value>, "col2": <rowN col2 value>,},
        # ]
        sdf = sdf.lookup_join(lookup, fields)
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

        :param identifer: the identifier to validate (table or column name).
        """
        if not re.match(r"^\w+$", identifier):
            raise ValueError(f"Invalid SQL identifier: {identifier}")

    def build_query(
        self, target_key: str, value: dict[str, Any]
    ) -> Tuple[str, Tuple[str, ...]]:
        """
        Build the SQL query string for this field.

        :param target_key: The key to use in the WHERE clause for lookup.
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
        return (query, (target_key,))

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

    The `sdf.lookup_join` `on` parameter is not used in the query itself, but is important for cache management. When caching is enabled, the query is executed once per TTL for each unique target key.

    Query results are returned as tuples of values, without additional deserialization.

    Example:

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")

        # Select all columns from the first row of `my_table` where `col2` matches the value of `field1` in the message.
        fields = {"my_field": SQLiteLookupQueryField("SELECT * FROM my_table WHERE col2 = :field1")}

        # After the lookup, the `my_field` column in the message will contain:
        # [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>]
        sdf = sdf.lookup_join(lookup, fields)
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
        sdf = sdf.lookup_join(lookup, fields)
    ```

    :param query: SQL query to execute.
    :param ttl: Time-to-live for cache in seconds. Default is 60.0.
    :param default: Default value if no result is found. Default is None.
    :param first_match_only: If True, only the first row is returned; otherwise, all rows are returned.
    """

    query: str

    def build_query(
        self, target_key: str, value: dict[str, Any]
    ) -> Tuple[str, dict[str, Any]]:
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


class SQLiteLookup(BaseLookup[BaseSQLiteLookupField]):
    """
    Lookup join implementation for enriching streaming data with data from a SQLite database.

    This class queries a SQLite database for each field, using a persistent connection and per-field caching
    based on a configurable TTL.

    Example:

    ```python
        lookup = SQLiteLookup(path="/path/to/db.sqlite")
        fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col1"])}
        sdf = sdf.lookup_join(lookup, fields)
    ```

    :param path: Path to the SQLite database file.
    """

    def __init__(self, path: str):
        self.db_path = path
        self._conn: sqlite3.Connection = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False
        )

        self._cache: dict[
            tuple[str, Any], tuple[float, Any]
        ] = {}  # (field_name, key) -> (timestamp, value)
        logger.info(f"SQLiteLookup initialized with db_path={self.db_path}")

    def __del__(self) -> None:
        if hasattr(self, "_conn"):
            self._conn.close()

    def _join_field(
        self, field: BaseSQLiteLookupField, target_key: str, value: dict[str, Any]
    ) -> Union[dict[str, Any], list[Any]]:
        query, parameters = field.build_query(target_key, value)
        logger.debug(f"Executing SQL: {query}")

        try:
            cur = self._conn.execute(query, parameters)
        except sqlite3.Error as e:
            logger.error(f"SQLite error while executing query: {e}")
            raise

        return field.result(cur)

    def join(
        self,
        fields: Mapping[str, BaseSQLiteLookupField],
        target_key: str,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: Any,
    ) -> None:
        """
        Enrich the message value in-place by querying SQLite for each field and caching results per TTL.

        :param fields: Mapping of field names to BaseSQLiteLookupField objects specifying how to extract and map enrichment data.
        :param target_key: The key used in the WHERE clause for SQLiteLookupField lookup.
        :param value: The message value.
        :param key: The message key.
        :param timestamp: The message timestamp.
        :param headers: The message headers.

        :returns: None. The input value dictionary is updated in-place with the enriched data.
        """
        now = time.time()

        for field_name, field in fields.items():
            if field.ttl <= 0:
                value[field_name] = self._join_field(field, target_key, value)
                continue

            cache_key = (field_name, target_key)
            cached = self._cache.get(cache_key)
            if cached and now - cached[0] < field.ttl:
                logger.debug(f"Cache hit for {cache_key}")
                value[field_name] = copy.copy(cached[1])
                continue

            logger.debug(f"Cache miss for {cache_key}, querying database")
            result = self._join_field(field, target_key, value)
            self._cache[cache_key] = (now, result)
            value[field_name] = copy.copy(result)
