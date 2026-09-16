"""The SQL half of the initial snapshot: keyset pagination over one table."""

import base64
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from .values import ColumnType, serialize_row

__all__ = (
    "SnapshotPlan",
    "build_snapshot_query",
    "decode_key",
    "discover_primary_key",
    "encode_key",
    "estimate_row_count",
    "iter_snapshot_batches",
    "quote_identifier",
)

logger = logging.getLogger(__name__)

_PROGRESS_LOG_EVERY_BATCHES = 50


@dataclass(frozen=True)
class SnapshotPlan:
    """
    Everything about a table's shape that one snapshot run needs.

    :param pk_columns: PRIMARY KEY columns in index order, the pagination key.
    :param columns: every column of the table in ordinal order, INVISIBLE ones included.
    :param column_types: declared MySQL type per column name.
    :param estimated_rows: `information_schema`'s row estimate, or None if it has none.
    """

    pk_columns: List[str]
    columns: List[str]
    column_types: Mapping[str, ColumnType]
    estimated_rows: Optional[int]


def quote_identifier(name: str) -> str:
    """Quote a MySQL identifier, escaping any backtick it contains."""
    return "`" + name.replace("`", "``") + "`"


def encode_key(values: Sequence[Any]) -> Optional[Tuple[List[Any], List[str]]]:
    """
    Render primary-key values as something the JSON-backed state store accepts.

    :param values: the key of the last row of a snapshot page, as MySQL returned it.
    :return: `(encoded values, type tags)` to store, or None if any value has no
        encoding, in which case that snapshot cannot be checkpointed.
    """
    if not values:
        return None
    encoded: List[Any] = []
    tags: List[str] = []
    for value in values:
        tag = _tag_of(value)
        if tag is None:
            return None
        encoded.append(_ENCODERS[tag](value))
        tags.append(tag)
    return encoded, tags


def decode_key(values: Sequence[Any], tags: Sequence[str]) -> Optional[List[Any]]:
    """
    Turn a stored key back into the values MySQL compared the last page against.

    :param tags: the tags `encode_key()` returned; an empty sequence means the key was
        stored before the tags were, when only ints and strings were ever stored.
    :return: the decoded key, or None if it cannot be decoded as stored.
    """
    if not tags:
        return list(values) if all(isinstance(v, (int, str)) for v in values) else None
    if len(tags) != len(values):
        return None
    decoded: List[Any] = []
    for value, tag in zip(values, tags):
        decoder = _DECODERS.get(tag)
        if decoder is None:
            return None
        try:
            decoded.append(decoder(value))
        except (ArithmeticError, TypeError, ValueError):
            return None
    return decoded


def _tag_of(value: Any) -> Optional[str]:
    # bool before int and datetime before date: each is a subclass of the next.
    for tag, kind in (
        ("bool", bool),
        ("int", int),
        ("str", str),
        ("float", float),
        ("bytes", (bytes, bytearray)),
        ("decimal", Decimal),
        ("datetime", datetime),
        ("date", date),
        ("timedelta", timedelta),
    ):
        if isinstance(value, kind):
            return tag
    return None


_ENCODERS = {
    "bool": bool,
    "int": int,
    "str": str,
    "float": float,
    "bytes": lambda value: base64.b64encode(bytes(value)).decode("ascii"),
    "decimal": str,
    "datetime": lambda value: value.isoformat(),
    "date": lambda value: value.isoformat(),
    "timedelta": lambda value: value // timedelta(microseconds=1),
}

_DECODERS = {
    "bool": bool,
    "int": int,
    "str": str,
    "float": float,
    "bytes": base64.b64decode,
    "decimal": Decimal,
    "datetime": datetime.fromisoformat,
    "date": date.fromisoformat,
    "timedelta": lambda value: timedelta(microseconds=int(value)),
}


def discover_primary_key(cursor: Any, database: str, table: str) -> List[str]:
    """
    :return: the PRIMARY KEY column names in index order, or an empty list.
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
    :return: `information_schema.TABLES.TABLE_ROWS`, an estimate, or None if unknown.
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
    database: str,
    table: str,
    columns: Sequence[str],
    pk_columns: Sequence[str],
    resuming: bool,
) -> str:
    """
    Build one page of the keyset-paginated snapshot query.

    Identifiers are quoted and interpolated; the cursor values and the page size are
    bound parameters.

    :param columns: the columns to read, in the order the events will carry them.
    :param resuming: emit the `WHERE (pk...) > (...)` clause, one placeholder per PK
        column, ahead of the page-size placeholder.
    :raises ValueError: if `columns` or `pk_columns` is empty.
    """
    if not columns:
        raise ValueError("build_snapshot_query() requires at least one column")
    if not pk_columns:
        raise ValueError("build_snapshot_query() requires at least one PK column")

    qualified = f"{quote_identifier(database)}.{quote_identifier(table)}"
    select_list = ", ".join(quote_identifier(column) for column in columns)
    quoted_pk = [quote_identifier(column) for column in pk_columns]
    order_by = ", ".join(quoted_pk)

    where = ""
    if resuming:
        placeholders = ", ".join(["%s"] * len(quoted_pk))
        if len(quoted_pk) == 1:
            where = f" WHERE {order_by} > {placeholders}"
        else:
            where = f" WHERE ({order_by}) > ({placeholders})"

    return f"SELECT {select_list} FROM {qualified}{where} ORDER BY {order_by} LIMIT %s"  # noqa: S608


def iter_snapshot_batches(
    cursor: Any,
    database: str,
    table: str,
    plan: SnapshotPlan,
    batch_size: int,
    start_after: Optional[Sequence[Any]] = None,
) -> Iterator[Tuple[List[Dict[str, Any]], List[Any]]]:
    """
    Yield `(changes, last_key_values)` for each keyset page until the table is read.

    :param cursor: an open DB-API cursor on the host being snapshotted.
    :param database: database (schema) name.
    :param table: table name.
    :param plan: the table's columns, PK and row estimate.
    :param batch_size: maximum rows per page.
    :param start_after: primary-key values of the last row of a previous run;
        when given, the walk resumes strictly after that row.
    :return: `last_key_values` are MySQL's raw values, not the encoded ones.
    """
    table_name = f"{database}.{table}"
    column_names = list(plan.columns)
    first_page_query = build_snapshot_query(
        database, table, column_names, plan.pk_columns, resuming=False
    )
    resume_page_query = build_snapshot_query(
        database, table, column_names, plan.pk_columns, resuming=True
    )
    # MySQL column names are case-insensitive.
    column_index = {name.lower(): index for index, name in enumerate(column_names)}
    pk_indexes = [column_index[name.lower()] for name in plan.pk_columns]

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

        changes = [
            {
                "kind": "snapshot_insert",
                "schema": database,
                "table": table,
                "columnnames": column_names,
                "columnvalues": serialize_row(
                    row, column_names, plan.column_types, table_name
                ),
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
            return
