"""Value encoding shared by the snapshot path and the binlog path."""

import base64
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from .config import MySqlCdcError

__all__ = (
    "ColumnType",
    "fetch_column_types",
    "serialize_binlog_values",
    "serialize_row",
    "serialize_value",
)

logger = logging.getLogger(__name__)

# `pymysqlreplication/constants/NONE_SOURCE.py`: why a decoded value came back None.
EMPTY_SET = "empty set"

# NONE_SOURCE.COLS_BITMAP: the column was outside the row image MySQL wrote.
_COLS_BITMAP = "cols bitmap"

# NONE_SOURCE.JSON_PARTIAL_UPDATE: a JSON column a partial update did not resend.
_JSON_PARTIAL_UPDATE = "same with before values"

# The snapshot path ships what a `SELECT` renders, which for a FLOAT column is six
# significant digits on 8.0.46, 8.4.2 and 9.4.0 - DOUBLE renders shortest-round-trip
# and needs no rule.
_FLOAT_SIGNIFICANT_DIGITS = 6

_WARNED_TYPES: Set[Tuple[str, str, str]] = set()


@dataclass(frozen=True)
class ColumnType:
    """A column's declared MySQL type, reduced to what the encoding needs."""

    data_type: str
    bit_width: Optional[int] = None


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _warn_once(value: Any, column: Optional[str], table: Optional[str]) -> None:
    name = type(value).__name__
    key = (name, table or "<unknown>", column or "<unknown>")
    if key in _WARNED_TYPES:
        return
    _WARNED_TYPES.add(key)
    logger.warning(
        "No encoding rule for MySQL values of Python type %r (%s.%s); falling back "
        "to str(). The value is being shipped as its Python repr, which is unlikely to "
        "be what consumers expect - please report the column type.",
        name,
        key[1],
        key[2],
    )


def _mysql_time(value: timedelta) -> str:
    """Render a TIME as MySQL prints it: `[-]HH:MM:SS[.ffffff]`, hours up to 838."""
    negative = value < timedelta(0)
    if negative:
        value = -value
    hours, remainder = divmod(value.days * 86400 + value.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if value.microseconds:
        text = f"{text}.{value.microseconds:06d}"
    return f"-{text}" if negative else text


def serialize_value(
    value: Any, column: Optional[str] = None, table: Optional[str] = None
) -> Any:
    """
    Encode one MySQL column value as something a JSON serializer accepts.

    :param value: the value as pymysql or pymysqlreplication returned it.
    :param column: column name, used only to make the fallback warning locatable.
    :param table: `"<database>.<table>"`, used for the same.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        # Before int: bool is an int subclass and JSON keeps them distinct.
        return value
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (set, frozenset)):
        return ",".join(sorted(str(item) for item in value))
    if isinstance(value, (dict, list)):
        return serialize_json(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (int, float, str)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, timedelta):
        return _mysql_time(value)
    _warn_once(value, column, table)
    return str(value)


def serialize_json(value: Any) -> str:
    """
    Render a JSON column's binlog-decoded value as canonical JSON text.

    :raises UnicodeDecodeError: a string in the document is not valid UTF-8.
    """
    return _canonical_json(_jsonable(value))


def _jsonable(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        # The binlog JSON decoder returns object keys and string values as bytes.
        return bytes(value).decode("utf-8")
    if isinstance(value, dict):
        return {_jsonable(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        # MySQL's own JSON text renders a stored decimal as a JSON number.
        return float(value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, timedelta):
        return _mysql_time(value)
    return str(value)


def _float(value: Any) -> Any:
    if value is None or not isinstance(value, float):
        return serialize_value(value)
    return float(f"{value:.{_FLOAT_SIGNIFICANT_DIGITS}g}")


def _partial_image_error(table: str, columns: List[str]) -> MySqlCdcError:
    return MySqlCdcError(
        f"MySQL wrote a partial row image for {table}: {', '.join(columns)} carry no "
        "value, so this change cannot be published without dropping them silently. Set "
        "binlog_row_image = FULL in that server's my.cnf and reconnect every writer that "
        "was already connected (each caches the value it opened with), then restart this "
        "source with initial_snapshot=True and force_snapshot=True to re-read the table "
        "and re-anchor past the truncated events."
    )


def _partial_json_error(table: str, columns: List[str]) -> MySqlCdcError:
    return MySqlCdcError(
        f"MySQL wrote a partial JSON value for {table}: {', '.join(columns)} carry only "
        "the difference against a previous value this source does not hold. Set "
        "binlog_row_value_options = '' in that server's my.cnf and reconnect its writers, "
        "then restart this source with initial_snapshot=True and force_snapshot=True to "
        "re-read the table and re-anchor past the partial events."
    )


def serialize_binlog_values(
    values: Mapping[str, Any],
    table: str,
    none_sources: Optional[Mapping[str, str]] = None,
    json_columns: Iterable[str] = (),
    float_columns: Iterable[str] = (),
) -> Tuple[List[str], List[Any]]:
    """
    Serialize one binlog row image.

    :param values: the event's `values`/`before_values`/`after_values` mapping.
    :param table: `"<database>.<table>"`, for the errors and the fallback warning.
    :param none_sources: the event's matching `none_sources` map.
    :param json_columns: names of the row's JSON columns.
    :param float_columns: names of the row's FLOAT (not DOUBLE) columns.
    :return: `(column_names, column_values)`, the same length as each other.
    :raises MySqlCdcError: if MySQL sent no value for a column, which is not the same as
        sending SQL NULL.
    """
    sources = none_sources or {}
    json_names = set(json_columns)
    float_names = set(float_columns)
    names: List[str] = []
    encoded: List[Any] = []
    absent: List[str] = []
    partial_json: List[str] = []
    for column, value in values.items():
        if value is None:
            source = sources.get(column)
            if source == _COLS_BITMAP:
                absent.append(column)
            elif source == _JSON_PARTIAL_UPDATE:
                partial_json.append(column)
            names.append(column)
            encoded.append("" if source == EMPTY_SET else None)
            continue
        names.append(column)
        if column in json_names:
            encoded.append(serialize_json(value))
        elif column in float_names:
            encoded.append(_float(value))
        else:
            encoded.append(serialize_value(value, column, table))

    if absent:
        raise _partial_image_error(table, absent)
    if partial_json:
        raise _partial_json_error(table, partial_json)
    return names, encoded


def serialize_row(
    values: Sequence[Any],
    columns: Sequence[str],
    column_types: Mapping[str, ColumnType],
    table: str,
) -> List[Any]:
    """
    Serialize one snapshot row, in column order, using the declared column types.

    :param values: the row as pymysql returned it.
    :param columns: the column names, in the same order.
    :param column_types: from `fetch_column_types()`.
    :param table: `"<database>.<table>"`, for the fallback warning.
    """
    encoded: List[Any] = []
    for column, value in zip(columns, values):
        column_type = column_types.get(column) or column_types.get(column.lower())
        kind = column_type.data_type if column_type is not None else ""
        if value is None:
            encoded.append(None)
        elif kind == "set":
            encoded.append(
                ",".join(sorted(part for part in str(value).split(",") if part))
            )
        elif kind == "json":
            encoded.append(_canonical_json(json.loads(value)))
        elif kind == "bit":
            width = (column_type.bit_width if column_type else None) or 1
            number = (
                value if isinstance(value, int) else int.from_bytes(bytes(value), "big")
            )
            encoded.append(format(number, f"0{width}b"))
        else:
            encoded.append(serialize_value(value, column, table))
    return encoded


def fetch_column_types(cursor: Any, database: str, table: str) -> Dict[str, ColumnType]:
    """
    Read the declared type of every column in `database`.`table`, INVISIBLE ones too.

    :return: column name -> `ColumnType`, in ordinal order, for every column.
    """
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (database, table),
    )
    types: Dict[str, ColumnType] = {}
    for row in cursor.fetchall():
        name, data_type, column_type = str(row[0]), str(row[1]).lower(), str(row[2])
        types[name] = ColumnType(
            data_type=data_type, bit_width=_bit_width(data_type, column_type)
        )
    return types


def _bit_width(data_type: str, column_type: str) -> Optional[int]:
    """Parse the `n` out of `bit(n)`; BIT's own default width is 1."""
    if data_type != "bit":
        return None
    inside = column_type.partition("(")[2].partition(")")[0]
    return int(inside) if inside.isdigit() else 1
