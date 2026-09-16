"""Binlog row events to the change dicts this connector produces."""

from typing import Any, Dict, List, Set, Tuple

from .drivers import FIELD_TYPE, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from .values import serialize_binlog_values

__all__ = ("event_to_changes",)


def event_to_changes(event: Any) -> List[Dict[str, Any]]:
    """
    Convert one row event into the connector's change dicts, one per row.

    :param event: a `WriteRowsEvent`, `UpdateRowsEvent` or `DeleteRowsEvent`; anything
        else yields no changes.
    :raises MySqlCdcError: if MySQL wrote a row image this source cannot publish.
    """
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


def _typed_columns(event: Any) -> Tuple[Set[str], Set[str]]:
    """
    :return: the event's (JSON column names, FLOAT column names), from the table-map
        event, which names every column only under `binlog_row_metadata=FULL`.
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
