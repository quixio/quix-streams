"""
Initial-snapshot support for the MySQL CDC source.

This module holds the SQL half of the snapshot: identifier quoting, primary-key
discovery, the keyset-pagination query and the generator that walks a table with it.
Value encoding lives in `values.py`, because the binlog path needs the same contract
and neither path owns it.

Apart from `values`, it imports nothing else from the package, so the SQL builders can
be unit-tested without a MySQL server and without importing the source itself.
"""

import logging
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from .values import ColumnType, serialize_row

__all__ = (
    "build_snapshot_query",
    "discover_primary_key",
    "estimate_row_count",
    "is_checkpointable_key",
    "iter_snapshot_batches",
    "quote_identifier",
)

logger = logging.getLogger(__name__)

# Every page is logged at DEBUG; the running row count is logged at INFO this often.
_PROGRESS_LOG_EVERY_BATCHES = 50


def quote_identifier(name: str) -> str:
    """
    Quote a MySQL identifier, escaping any backtick it contains.

    Identifiers cannot be bound as parameters, so they are interpolated into the
    generated SQL; every *value* is a bound parameter instead.
    """
    return "`" + name.replace("`", "``") + "`"


def is_checkpointable_key(values: Sequence[Any]) -> bool:
    """
    True when every primary-key value survives a JSON round-trip unchanged.

    Snapshot progress is stored in the source's state store, which serializes values
    to JSON. Only `int` (excluding `bool`) and `str` come back as the exact value SQL
    compared against, so those are the only key types worth checkpointing; for any
    other type the snapshot simply restarts from the beginning after an interruption.
    Pagination itself always uses the raw Python values, so this never affects
    in-run correctness.
    """
    return bool(values) and all(
        isinstance(value, str)
        or (isinstance(value, int) and not isinstance(value, bool))
        for value in values
    )


def discover_primary_key(cursor: Any, database: str, table: str) -> List[str]:
    """
    Return the PRIMARY KEY column names of a table in index order.

    Returns an empty list when the table has no primary key; the caller decides
    whether that is fatal (it is for the initial snapshot, which needs a primary key
    to paginate on, and harmless for binlog streaming, which does not).
    """
    cursor.execute(
        "SELECT COLUMN_NAME FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND INDEX_NAME = 'PRIMARY' "
        "ORDER BY SEQ_IN_INDEX",
        (database, table),
    )
    return [row[0] for row in cursor.fetchall()]


def estimate_row_count(cursor: Any, database: str, table: str) -> Optional[int]:
    """
    Return the storage engine's row-count estimate for a table, or None if unknown.

    This is `information_schema.TABLES.TABLE_ROWS`, which InnoDB derives from index
    statistics: it is approximate and only used to give the snapshot logs a sense of
    scale. An exact `COUNT(*)` would mean a full table scan before the first row is
    produced, on exactly the large tables the batched snapshot exists for.
    """
    cursor.execute(
        "SELECT TABLE_ROWS FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (database, table),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def build_snapshot_query(
    database: str, table: str, pk_columns: List[str], resuming: bool
) -> str:
    """
    Build one page of the keyset-paginated snapshot query.

    First page:      SELECT * FROM `db`.`tbl` ORDER BY `id` LIMIT %s
    Resume page:     SELECT * FROM `db`.`tbl` WHERE `id` > %s ORDER BY `id` LIMIT %s
    Composite key:   SELECT * FROM `db`.`tbl` WHERE (`a`, `b`) > (%s, %s)
                     ORDER BY `a`, `b` LIMIT %s

    The row-constructor comparison resolves against the primary-key index on MySQL
    5.7+/8.x, and primary-key columns are NOT NULL by definition, so there is no
    NULL-comparison trap. Identifiers are quoted and interpolated; the cursor values
    and the page size are bound parameters.
    """
    if not pk_columns:
        raise ValueError("build_snapshot_query() requires at least one PK column")

    qualified = f"{quote_identifier(database)}.{quote_identifier(table)}"
    quoted_pk = [quote_identifier(column) for column in pk_columns]
    order_by = ", ".join(quoted_pk)

    where = ""
    if resuming:
        placeholders = ", ".join(["%s"] * len(quoted_pk))
        if len(quoted_pk) == 1:
            where = f" WHERE {order_by} > {placeholders}"
        else:
            where = f" WHERE ({order_by}) > ({placeholders})"

    return f"SELECT * FROM {qualified}{where} ORDER BY {order_by} LIMIT %s"  # noqa: S608


def iter_snapshot_batches(
    cursor: Any,
    database: str,
    table: str,
    pk_columns: List[str],
    batch_size: int,
    column_types: Mapping[str, ColumnType],
    start_after: Optional[Sequence[Any]] = None,
) -> Iterator[Tuple[List[Dict[str, Any]], List[Any]]]:
    """
    Yield `(changes, last_key_values)` for each keyset page until the table is read.

    Keyset pagination (`WHERE (pk...) > (last seen pk...)`) is used rather than a
    skip-count page: a skip count loses a row for every row deleted behind the cursor
    and gets slower with every page, while the seek form is index-driven and immune
    to concurrent deletes.

    `last_key_values` are the raw values returned by MySQL, so the next page's
    comparison never depends on them being JSON-serializable.

    :param cursor: an open DB-API cursor on the host being snapshotted.
    :param database: database (schema) name.
    :param table: table name.
    :param pk_columns: primary-key column names in index order.
    :param batch_size: maximum rows per page.
    :param column_types: declared MySQL type per column, from
        `values.fetch_column_types`. SET, JSON and BIT columns cannot be encoded to the
        cross-path contract from the Python value alone.
    :param start_after: primary-key values of the last row of a previous run;
        when given, the walk resumes strictly after that row.
    """
    first_page_query = build_snapshot_query(database, table, pk_columns, resuming=False)
    resume_page_query = build_snapshot_query(database, table, pk_columns, resuming=True)

    key_values: Optional[List[Any]] = list(start_after) if start_after else None
    batches = 0
    rows_read = 0

    while True:
        if key_values is None:
            cursor.execute(first_page_query, (batch_size,))
        else:
            cursor.execute(resume_page_query, (*key_values, batch_size))

        rows = cursor.fetchall()
        if not rows:
            return

        column_names = [description[0] for description in cursor.description]
        # MySQL column names are case-insensitive, and information_schema may spell
        # them differently from the result-set metadata.
        column_index = {name.lower(): index for index, name in enumerate(column_names)}
        pk_indexes = [column_index[name.lower()] for name in pk_columns]

        changes = [
            {
                "kind": "snapshot_insert",
                "schema": database,
                "table": table,
                "columnnames": column_names,
                "columnvalues": serialize_row(row, column_names, column_types),
                "oldkeys": {},
            }
            for row in rows
        ]
        key_values = [rows[-1][index] for index in pk_indexes]

        batches += 1
        rows_read += len(rows)
        logger.debug(
            "Snapshot of %s.%s: page %s, %s rows (%s so far)",
            database,
            table,
            batches,
            len(rows),
            rows_read,
        )
        if batches % _PROGRESS_LOG_EVERY_BATCHES == 0:
            logger.info(
                "Snapshot of %s.%s: %s rows read so far", database, table, rows_read
            )

        yield changes, key_values

        if len(rows) < batch_size:
            # A short page means the table is exhausted. Keyset pagination cannot skip
            # rows, so there is no need for one more round trip to confirm it.
            return
