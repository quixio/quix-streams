"""Binlog row events to the change dicts this connector produces."""

from typing import Any, Dict, List, Set, Tuple

from .config import MySqlCdcError
from .drivers import FIELD_TYPE, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from .values import serialize_binlog_values

__all__ = ("event_to_changes",)


def event_to_changes(event: Any) -> List[Dict[str, Any]]:
    """
    Convert one row event into the connector's change dicts, one per row.

    :param event: a `WriteRowsEvent`, `UpdateRowsEvent` or `DeleteRowsEvent`; anything
        else yields no changes.
    :raises MySqlCdcError: if MySQL wrote a row image this source cannot publish, or an
        event this source cannot decode faithfully.
    """
    _require_full_metadata(event)
    json_columns, float_columns = _typed_columns(event)
    table_name = f"{event.schema}.{event.table}"
    changes: List[Dict[str, Any]] = []

    def encode(
        values: Dict[str, Any], none_sources: Any
    ) -> Tuple[List[str], List[Any]]:
        return serialize_binlog_values(
            values, table_name, none_sources, json_columns, float_columns
        )

    if isinstance(event, WriteRowsEvent):
        for row in event.rows:
            names, values = encode(row["values"], row.get("none_sources"))
            changes.append(
                {
                    "kind": "insert",
                    "schema": event.schema,
                    "table": event.table,
                    "columnnames": names,
                    "columnvalues": values,
                    "oldkeys": {},
                }
            )
    elif isinstance(event, UpdateRowsEvent):
        for row in event.rows:
            names, values = encode(row["after_values"], row.get("after_none_sources"))
            keynames, keyvalues = encode(
                row["before_values"], row.get("before_none_sources")
            )
            changes.append(
                {
                    "kind": "update",
                    "schema": event.schema,
                    "table": event.table,
                    "columnnames": names,
                    "columnvalues": values,
                    "oldkeys": {"keynames": keynames, "keyvalues": keyvalues},
                }
            )
    elif isinstance(event, DeleteRowsEvent):
        for row in event.rows:
            keynames, keyvalues = encode(row["values"], row.get("none_sources"))
            changes.append(
                {
                    "kind": "delete",
                    "schema": event.schema,
                    "table": event.table,
                    "columnnames": [],
                    "columnvalues": [],
                    "oldkeys": {"keynames": keynames, "keyvalues": keyvalues},
                }
            )
    return changes


def _require_full_metadata(event: Any) -> None:
    """
    :raises MySqlCdcError: if the event was written while `binlog_row_metadata` was
        below FULL, or if the stream holds no metadata for its table at all.
    """
    table = event.table_map.get(event.table_id)
    if table is None:
        raise _unknown_table_error(f"{event.schema}.{event.table}", event.table_id)
    # `Table.column_name_flag` is set only on the path that read the event's own column
    # metadata. The source's INFORMATION_SCHEMA fallback names the columns anyway, so
    # this is the one thing that still distinguishes the two.
    if not table.column_name_flag:
        raise _stripped_metadata_error(f"{event.schema}.{event.table}")
    for column in event.columns:
        if (column.type == FIELD_TYPE.ENUM and column.enum_values is None) or (
            column.type == FIELD_TYPE.SET and column.set_values is None
        ):
            raise _stripped_metadata_error(f"{event.schema}.{event.table}")


def _typed_columns(event: Any) -> Tuple[Set[str], Set[str]]:
    """
    :return: the event's (JSON column names, FLOAT column names).
    """
    json_columns: Set[str] = set()
    float_columns: Set[str] = set()
    for column in event.columns:
        name = column.name
        if not name:
            continue
        if column.type == FIELD_TYPE.JSON:
            json_columns.add(name)
        elif column.type == FIELD_TYPE.FLOAT:
            float_columns.add(name)
    return json_columns, float_columns


def _unknown_table_error(table: str, table_id: Any) -> MySqlCdcError:
    return MySqlCdcError(
        f"The binlog stream holds no table metadata for {table} (table id {table_id}), "
        "so this event's columns, character sets and ENUM/SET values are all unknown "
        "and it cannot be decoded faithfully. The source is stopping with the position "
        "it last committed, which replays this event on the next start, where the "
        "table map is read again from the stream."
    )


def _stripped_metadata_error(table: str) -> MySqlCdcError:
    return MySqlCdcError(
        f"MySQL wrote an event for {table} while binlog_row_metadata was below FULL, "
        "which is the MySQL 8.x default, so the event predates this source raising it. "
        "It carries no ENUM or SET value lists and no integer signedness, so publishing "
        "it would ship an ENUM as null, a SET as an empty string and an UNSIGNED "
        "integer as a negative number, none of them distinguishable from the real "
        "value. Set binlog_row_metadata = FULL in that server's my.cnf, then restart "
        "this source with initial_snapshot=True and force_snapshot=True to re-read the "
        "table and re-anchor past those events."
    )
