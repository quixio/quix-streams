"""
The value contract shared by the snapshot path and the binlog path.

**The rule is cross-path equality.** The same MySQL value must serialize to the same
JSON whether it arrived through `pymysql` (the snapshot's text protocol) or through
`pymysqlreplication` (the binlog's row images). The two libraries hand back different
Python objects for the same column, so the contract is defined on the *MySQL type* and
each path normalizes to it:

| MySQL type            | snapshot gives | binlog gives          | emitted JSON            |
|-----------------------|----------------|-----------------------|-------------------------|
| INT/BIGINT/...        | int            | int                   | number                  |
| FLOAT                 | float (6 s.f.) | float32 widened       | number, 6 s.f.          |
| DOUBLE                | float          | float                 | number                  |
| DECIMAL               | Decimal        | Decimal               | string, scale kept      |
| CHAR/VARCHAR/TEXT     | str            | str                   | string                  |
| BINARY/VARBINARY/BLOB | bytes          | bytes                 | base64 string           |
| DATE/DATETIME         | date/datetime  | datetime              | ISO-8601 string         |
| TIME                  | timedelta      | timedelta             | str(value), "1:02:03"   |
| TIMESTAMP             | session-tz dt  | UTC datetime          | ISO-8601, UTC           |
| ENUM                  | str            | str                   | string                  |
| SET                   | "a,b"          | set, or None if empty | sorted, comma-joined    |
| JSON                  | raw JSON text  | dict/list/bytes/...   | canonical JSON string   |
| BIT(n)                | bytes          | "00000101"            | zero-padded bit string  |
| GEOMETRY              | bytes          | bytes                 | base64 string           |
| NULL                  | None           | None                  | null                    |

Four of those need more than the Python type to decide, which is why this module has
two entry points rather than one:

* **SET** arrives from the binlog as an unordered Python `set`, and MySQL's definition
  order cannot be recovered from it, so both sides sort. The *empty* set arrives as
  `None` (`row_event.py:353`), indistinguishable from SQL NULL by value alone - the
  binlog path tells them apart with the event's `none_sources` map, which records
  `"empty set"` for exactly this case.
* **JSON** arrives from the binlog as a `dict` with `bytes` keys and `bytes` string
  values (`json_binary.py:105,158`); `str()` on that produces `"{b'a': 2}"`, which no
  JSON parser accepts. Both sides emit canonical JSON text instead.
* **BIT** arrives from the binlog already rendered as a bit string
  (`row_event.py:417-438`) and from the snapshot as bytes, so the snapshot side needs
  the declared width to pad to.
* **FLOAT** is four bytes in the binlog and six significant digits in MySQL's text
  protocol, so the snapshot side has already lost precision the binlog side still has.
  The binlog value is rendered at the same six significant digits MySQL itself prints;
  that is the only precision both paths can agree on.

TIMESTAMP is not normalized here: the binlog decoder is hard-coded to
`datetime.utcfromtimestamp` and cannot be told otherwise, so the *session* time zone of
every connection the connector opens is pinned to UTC instead (`mysql_helper`).
"""

import base64
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

__all__ = (
    "ColumnType",
    "fetch_column_types",
    "serialize_binlog_values",
    "serialize_row",
    "serialize_value",
)

logger = logging.getLogger(__name__)

# `pymysqlreplication` records why a value came back None; this is the one reason that
# is not a SQL NULL (`constants/NONE_SOURCE.py`).
EMPTY_SET = "empty set"

# MySQL prints FLOAT with six significant digits, so that is the precision at which the
# binlog's four bytes and the snapshot's text can agree.
_FLOAT_SIGNIFICANT_DIGITS = 6

# Types that have already been reported by the `str()` fallback, so the warning is
# logged once per process per type instead of once per row.
_WARNED_TYPES: Set[str] = set()


@dataclass(frozen=True)
class ColumnType:
    """A column's declared MySQL type, reduced to what the contract needs."""

    data_type: str
    bit_width: Optional[int] = None


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _warn_once(value: Any, column: Optional[str]) -> None:
    name = type(value).__name__
    if name in _WARNED_TYPES:
        return
    _WARNED_TYPES.add(name)
    logger.warning(
        "No encoding rule for MySQL values of Python type %r (column %r); falling back "
        "to str(). The value is being shipped as its Python repr, which is unlikely to "
        "be what consumers expect - please report the column type.",
        name,
        column or "<unknown>",
    )


def serialize_value(value: Any, column: Optional[str] = None) -> Any:
    """
    Encode one MySQL column value as something a JSON serializer accepts.

    Dispatches on the Python type, which is enough for every column whose two
    representations already agree; the callers below handle the four that do not. See
    the module docstring for the contract this implements - it is a cross-path contract,
    not just a list of types.

    :param value: the value as pymysql or pymysqlreplication returned it.
    :param column: column name, used only to make the fallback warning locatable.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        # Before int: bool is an int subclass and JSON keeps them distinct.
        return value
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (set, frozenset)):
        # A SET column. Sorted because the binlog cannot supply definition order.
        return ",".join(sorted(str(item) for item in value))
    if isinstance(value, (dict, list)):
        # A JSON column decoded from the binlog's binary format.
        return serialize_json(value)
    if isinstance(value, Decimal):
        # str() rather than float(): it keeps the declared scale ("0.00") and does not
        # round a value MySQL stored exactly.
        return str(value)
    if isinstance(value, (int, float, str)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    # `timedelta` (TIME) renders as "1:02:03" through str(), identically on both paths.
    # Anything else reaching here is a type this contract has never seen: a crash
    # mid-stream would be worse than a stringified value, so it is stringified - but
    # loudly, once per type, so a Python repr can never ship unnoticed again.
    if not isinstance(value, timedelta):
        _warn_once(value, column)
    return str(value)


def serialize_json(value: Any) -> str:
    """
    Render a JSON column's binlog-decoded value as canonical JSON text.

    `pymysqlreplication` returns object keys and string values as `bytes`
    (`json_binary.py:105,158`), and nested values as whatever Python type the binary
    format encoded - including `Decimal` and `datetime` for values MySQL stored as
    opaque. Everything is decoded to JSON-native types first, then dumped with sorted
    keys and no spaces, which is exactly how the snapshot path re-dumps MySQL's own JSON
    text. Undecodable bytes raise rather than being mangled: a JSON column that is not
    valid UTF-8 is a fact the operator needs, not one to paper over.
    """
    return _canonical_json(_jsonable(value))


def _jsonable(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    if isinstance(value, dict):
        return {_jsonable(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        # MySQL's JSON text renders a stored decimal as a JSON number, and the snapshot
        # side reaches it through json.loads(), so a float is what both sides carry.
        return float(value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _float(value: Any) -> Any:
    """Round a binlog FLOAT to the six significant digits MySQL's text protocol shows."""
    if value is None or not isinstance(value, float):
        return serialize_value(value)
    return float(f"{value:.{_FLOAT_SIGNIFICANT_DIGITS}g}")


def serialize_binlog_values(
    values: Mapping[str, Any],
    none_sources: Optional[Mapping[str, str]] = None,
    json_columns: Iterable[str] = (),
    float_columns: Iterable[str] = (),
) -> List[Any]:
    """
    Serialize one binlog row image, in column order.

    :param values: the event's `values`/`before_values`/`after_values` mapping.
    :param none_sources: the event's matching `none_sources` map, which is the only way
        to tell an empty SET from a SQL NULL.
    :param json_columns: names of the row's JSON columns, needed because a JSON column
        holding a top-level string arrives as plain `bytes` and would otherwise be
        base64-encoded instead of quoted.
    :param float_columns: names of the row's FLOAT (not DOUBLE) columns.
    """
    sources = none_sources or {}
    json_names = set(json_columns)
    float_names = set(float_columns)
    encoded: List[Any] = []
    for column, value in values.items():
        if value is None:
            encoded.append("" if sources.get(column) == EMPTY_SET else None)
        elif column in json_names:
            encoded.append(serialize_json(value))
        elif column in float_names:
            encoded.append(_float(value))
        else:
            encoded.append(serialize_value(value, column))
    return encoded


def serialize_row(
    values: Sequence[Any],
    columns: Sequence[str],
    column_types: Mapping[str, ColumnType],
) -> List[Any]:
    """
    Serialize one snapshot row, in column order, using the declared column types.

    The type map is what lets this path reach the same JSON as the binlog path: a
    Python `str` cannot say whether it came from a SET, a JSON column or a VARCHAR, and
    BIT's declared width is not recoverable from the bytes MySQL returns.
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
            # FLOAT needs no work here: MySQL's text protocol has already rounded it to
            # the six significant digits the binlog side is rounded down to.
            encoded.append(serialize_value(value, column))
    return encoded


def fetch_column_types(cursor: Any, database: str, table: str) -> Dict[str, ColumnType]:
    """
    Read the declared type of every column, once per snapshot run.

    Only the three types whose two representations differ are acted on, but every column
    is returned so `serialize_row` can look up by name without a second dictionary.
    `COLUMN_TYPE` carries the BIT width (`bit(8)`), which `DATA_TYPE` alone does not.
    """
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
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
    """Parse the `n` out of `bit(n)`, defaulting to BIT's own default of 1."""
    if data_type != "bit":
        return None
    inside = column_type.partition("(")[2].partition(")")[0]
    return int(inside) if inside.isdigit() else 1
