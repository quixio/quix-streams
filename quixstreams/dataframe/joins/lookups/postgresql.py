import abc
import copy
import dataclasses
import logging
import re
import time
from collections import OrderedDict
from typing import Any, Literal, Mapping, Tuple, Union

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import cursor as pg_cursor
except ImportError as exc:
    raise ImportError(
        f"Package `{exc.name}` is missing: "
        'run "pip install quixstreams[postgresql]" to fix it'
    ) from exc

from .base import BaseField, BaseLookup
from .utils import CacheInfo

logger = logging.getLogger(__name__)


MISSING = object()


# @dataclasses.dataclass(frozen=True, kw_only=True)  # TODO: uncomment python 3.10+
@dataclasses.dataclass(frozen=True)
class BasePostgresLookupField(BaseField, abc.ABC):
    # TODO: Uncomment python 3.10+
    # ttl: float = 60.0
    # default: Any = None
    # first_match_only: bool = True

    @abc.abstractmethod
    def build_query(
        self, on: str, value: dict[str, Any]
    ) -> Tuple[sql.Composable, Union[dict[str, Any], Tuple[str, ...]]]:
        """
        Build the SQL query string for this field.

        :param on: The key to use in the WHERE clause for lookup.
        :param value: The message value, used to substitute parameters in the query.

        :returns: A tuple of the SQL query string and the parameters.
        """
        raise NotImplementedError("Subclasses must implement build_query method.")

    @abc.abstractmethod
    def result(self, cursor: pg_cursor) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """
        Extract the result from the cursor based on the field definition.

        :param cursor: The Postgres cursor containing the query results.

        :returns: The extracted data, either a single row or a list of rows.
        """
        raise NotImplementedError("Subclasses must implement result method.")


@dataclasses.dataclass(frozen=True)
class PostgresLookupField(BasePostgresLookupField):
    table: str
    columns: list[str]
    on: str
    order_by: str = ""
    order_by_direction: Literal["ASC", "DESC"] = "ASC"
    schema: str = "public"

    # TODO: remove on python 3.10+
    ttl: float = 60.0
    default: Any = None
    first_match_only: bool = True

    def __post_init__(self) -> None:
        if "*" in self.columns:
            raise ValueError(
                "Using '*' in columns is not allowed for PostgresLookupField."
            )

        for item in [
            *self.columns,
            self.table,
            self.on,
            self.schema,
            self.order_by,
            self.order_by_direction,
        ]:
            if item:
                self._validate_identifier(item)

    def _validate_identifier(self, identifier: str) -> None:
        """
        Validate SQL identifiers (table/column names) to allow only alphanumeric and underscore.

        :param identifier: the identifier to validate (table or column name).
        """
        if not re.match(r"^\w+$", identifier):
            raise ValueError(f"Invalid SQL identifier: {identifier}")

    def build_query(
        self, on: str, value: dict[str, Any]
    ) -> Tuple[sql.Composed, Tuple[str, ...]]:
        """
        Build the SQL query string for this field.

        :param on: The key to use in the WHERE clause for lookup.
        :param value: The message value, used to substitute parameters in the query.

        :returns: A tuple of the SQL query string and the parameters.
        """
        # Postgres doesn't support parameterization for SQL identifiers (columns, table names);
        # they are instead validated prior to query building to ensure they are safe.

        cols = sql.SQL(",").join([sql.Identifier(col) for col in self.columns])
        from_ = sql.Identifier(self.schema, self.table)
        query = sql.SQL("SELECT {cols} FROM {from_} WHERE {on} = %s").format(
            cols=cols,
            from_=from_,
            on=sql.Identifier(self.on),
        )

        if self.order_by:
            query += sql.SQL(" ORDER BY {} {}").format(
                sql.Identifier(self.order_by), sql.SQL(self.order_by_direction)
            )
        if self.first_match_only:
            query += sql.SQL(" LIMIT 1")
        return query, (on,)

    def result(self, cursor: pg_cursor) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """
        Extract the result from the cursor based on the field definition.

        :param cursor: The SQLite cursor containing the query results.

        :returns: The extracted data, either a single row or a list of rows.
        """
        if self.first_match_only:
            row = cursor.fetchone()
            return dict(zip(self.columns, row)) if row else self.default
        return [dict(zip(self.columns, row)) for row in cursor]


@dataclasses.dataclass(frozen=True)
class PostgresLookupQueryField(BasePostgresLookupField):
    query: str

    # TODO: remove on python 3.10+
    ttl: float = 60.0
    default: Any = None
    first_match_only: bool = True

    def build_query(
        self, on: str, value: dict[str, Any]
    ) -> Tuple[sql.Composable, dict[str, Any]]:
        return sql.SQL(self.query), value

    def result(self, cursor: pg_cursor) -> Union[list[Any], Any]:
        """
        Extract the result from the cursor based on the field definition.

        :param cursor: The Postgres cursor containing the query results.

        :returns: The extracted data, either a single row or a list of rows.
        """
        if self.first_match_only:
            return cursor.fetchone() or self.default
        return cursor.fetchall()


class PostgresLookup(BaseLookup[Union[PostgresLookupField, PostgresLookupQueryField]]):
    """
    Lookup join implementation for enriching streaming data with data from a Postgres database.

    This class queries a Postgres database for each field, using a persistent connection and per-field caching
    based on a configurable TTL. The cache is a "Least Recently Used" (LRU) cache with a configurable maximum size.

    Example:

    This is a join on kafka record column `k_colX` with table column `t_col2`
    (where their values are equal).

    ```python
        lookup = PostgresLookup(**credentials)
        fields = {"my_field": lookup.field(table="my_table", columns=["t_col2"], on="t_col1")}
        sdf = sdf.join_lookup(lookup, fields, on="k_colX")
    ```
    Note that `join_lookup` uses `on=<kafka message key>` if a column is not provided.
    """

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        connection_timeout_seconds: int = 30,
        statement_timeout_seconds: int = 30,
        cache_size: int = 1000,
        **kwargs,
    ):
        """
        :param host: PostgreSQL server address.
        :param port: PostgreSQL server port.
        :param dbname: PostgreSQL database name.
        :param user: Database username.
        :param password: Database user password.
        :param connection_timeout_seconds: Timeout for connection.
        :param statement_timeout_seconds: Timeout for DDL operations such as table
            creation or schema updates.
        :param cache_size: Maximum number of fields to keep in the LRU cache. Default is 1000.
        :param kwargs: Additional parameters for `psycopg2.connect`.
        """
        options = kwargs.pop("options", "")
        if "statement_timeout" not in options:
            options = f"{options} -c statement_timeout={statement_timeout_seconds}s"
        self._conn = psycopg2.connect(
            **{
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password,
                "connect_timeout": connection_timeout_seconds,
                "options": options,
            },
            **kwargs,
        )
        self._cache: OrderedDict[tuple[str, Any], tuple[float, Any]] = (
            OrderedDict()
        )  # (field_name, key) -> (timestamp, value)
        self._cache_size = cache_size
        self._cache_hits = 0
        self._cache_misses = 0
        self._debug = logger.isEnabledFor(logging.DEBUG)

        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        except psycopg2.Error as e:
            logger.error(f"Initial connection to database failed: {e}")
            raise
        else:
            logger.info(f"Authentication to Postgres DB '{dbname}' successful.")

        logger.info(f"{self.__class__.__name__} cache size: {cache_size}")

    def __del__(self):
        if hasattr(self, "_conn"):
            self._conn.close()

    def _process_field(
        self, field: BasePostgresLookupField, on: str, value: dict[str, Any]
    ) -> Union[dict[str, Any], list[Any]]:
        """
        Execute the SQL query for a given field and return the result.

        :param field: The field definition (SQLiteLookupField or SQLiteLookupQueryField).
        :param on: The key to use in the WHERE clause for lookup.
        :param value: The message value, used to substitute parameters in the query.

        :returns: The extracted data, either a single row or a list of rows.
        """
        query, parameters = field.build_query(on, value)

        cur = self._conn.cursor()
        if self._debug:
            logger.debug(f"Executing SQL: {query.as_string(cur)}")
        try:
            cur.execute(query, parameters)
        except psycopg2.Error as e:
            logger.error(f"Postgres error while executing query: {e}")
            raise
        else:
            return field.result(cur)
        finally:
            cur.close()

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

    def field(
        self,
        table: str,
        columns: list[str],
        on: str,
        order_by: str = "",
        order_by_direction: Literal["ASC", "DESC"] = "ASC",
        schema: str = "public",
        ttl: float = 60.0,
        default: Any = None,
        first_match_only: bool = True,
    ) -> PostgresLookupField:
        """
        Field definition for use with PostgresLookup in lookup joins.

        Table and column names are sanitized to prevent SQL injection.
        Rows will be deserialized into a dictionary with column names as keys.

        Example:
        With kafka records formatted as:
        row = {"k_colX": "value_a", "k_colY": "value_b"}

        We want to join this to DB table record(s) where table column `t_col2` has the
        same value as kafka row's key `k_colX` (`value_a`).

        ```python
            lookup = PostgresLookup(**credentials)

            # Select the value in `db_col1` from the table `my_table` where `col2` matches the `sdf.join_lookup` on parameter.
            fields = {"my_field": lookup.field(table="my_table", columns=["t_col1", "t_col2"], on="t_col2")}

            # After the lookup the `my_field` column in the message contains:
            # {"t_col1": <row1 t_col1 value>, "t_col2": <row1 t_col2 value>}
            sdf = sdf.join_lookup(lookup, fields, on="kafka_col1")
        ```

        ```python
            lookup = PostgresLookup(**credentials)

            # Select the value in `t_col1` from the table `my_table` where `t_col2` matches the `sdf.join_lookup` on parameter.
            fields = {"my_field": lookup.field(table="my_table", columns=["t_col1", "t_col2"], on="t_col2", first_match_only=False)}

            # After the lookup the `my_field` column in the message contains:
            # [
            #   {"t_col1": <row1 t_col1 value>, "t_col2": <row1 t_col2 value>},
            #   {"t_col1": <row2 t_col1 value>, "t_col2": <row2 t_col2 value>},
            #   ...
            #   {"t_col1": <rowN col1 value>, "t_col2": <rowN t_col2 value>,},
            # ]
            sdf = sdf.join_lookup(lookup, fields, on="k_colX")
        ```

        :param table: Name of the table to query in the Postgres database.
        :param columns: List of columns to select from the table.
        :param on: The column name to use in the WHERE clause for matching against the target key.
        :param order_by: Optional ORDER BY clause to sort the results.
        :param order_by_direction: Direction of the ORDER BY clause, either "ASC" or "DESC". Default is "ASC".
        :param schema: the table schema; if unsure leave as default ("public").
        :param ttl: Time-to-live for cache in seconds. Default is 60.0.
        :param default: Default value if no result is found. Default is None.
        :param first_match_only: If True, only the first row is returned; otherwise, all rows are returned.
        """
        return PostgresLookupField(
            table=table,
            columns=columns,
            on=on,
            order_by=order_by,
            order_by_direction=order_by_direction,
            schema=schema,
            ttl=ttl,
            default=default,
            first_match_only=first_match_only,
        )

    def query_field(
        self,
        query: str,
        ttl: float = 60.0,
        default: Any = None,
        first_match_only: bool = True,
    ) -> PostgresLookupQueryField:
        """
        Field definition for use with PostgresLookup in lookup joins.

        Enables advanced SQL queries with support for parameter substitution from message columns, allowing dynamic lookups.

        The `sdf.join_lookup` `on` parameter is not used in the query itself, but is important for cache management. When caching is enabled, the query is executed once per TTL for each unique target key.

        Query results are returned as tuples of values, without additional deserialization.

        Example:

        ```python
            lookup = PostgresLookup(**credentials)

            # Select all columns from the first row of `my_table` where `col2` matches the value of `field1` in the message.
            fields = {"my_field": lookup.query_field("SELECT * FROM my_table WHERE col2 = %(field_1)s")}

            # After the lookup, the `my_field` column in the message will contain:
            # [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>]
            sdf = sdf.join_lookup(lookup, fields)
        ```

        ```python
            lookup = PostgresLookup(**creds)

            # Select all columns from all rows of `my_table` where `col2` matches the value of `field1` in the message.
            fields = {"my_field": lookup.query_field("SELECT * FROM my_table WHERE col2 = %(field_1)s", first_match_only=False)}

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
        return PostgresLookupQueryField(
            query=query, ttl=ttl, default=default, first_match_only=first_match_only
        )

    def join(
        self,
        fields: Mapping[
            str, Union[PostgresLookupField, PostgresLookupQueryField]
        ],  # TODO: Use BasePostgresLookupField when python 3.10+ is required
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
