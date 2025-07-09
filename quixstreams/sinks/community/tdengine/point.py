import math
import warnings
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from numbers import Integral

from .date_utils import DateHelper

EPOCH = datetime.fromtimestamp(0, tz=timezone.utc)

DEFAULT_WRITE_PRECISION = "ns"

_ESCAPE_MEASUREMENT = str.maketrans(
    {
        ",": r"\,",
        " ": r"\ ",
        "\n": r"\n",
        "\t": r"\t",
        "\r": r"\r",
    }
)

_ESCAPE_KEY = str.maketrans(
    {
        ",": r"\,",
        "=": r"\=",
        " ": r"\ ",
        "\n": r"\n",
        "\t": r"\t",
        "\r": r"\r",
    }
)

_ESCAPE_STRING = str.maketrans(
    {
        '"': r"\"",
        "\\": r"\\",
    }
)

try:
    import numpy as np

    _HAS_NUMPY = True
except ModuleNotFoundError:
    _HAS_NUMPY = False


date_helper = DateHelper()


class Point:
    def __init__(self, measurement_name):
        """Initialize defaults."""
        self._tags = {}
        self._fields = {}
        self._name = measurement_name
        self._time = None
        self._write_precision = DEFAULT_WRITE_PRECISION
        self._field_types = {}

    @classmethod
    def from_dict(
        cls, dictionary: dict, write_precision: str = DEFAULT_WRITE_PRECISION, **kwargs
    ):
        """
        Initialize point from 'dict' structure.

        The expected dict structure is:
            - measurement
            - tags
            - fields
            - time

        Example:
            .. code-block:: python

                # Use default dictionary structure
                dict_structure = {
                    "measurement": "h2o_feet",
                    "tags": {"location": "coyote_creek"},
                    "fields": {"water_level": 1.0},
                    "time": 1
                }
                point = Point.from_dict(dict_structure, "ns")

        Example:
            .. code-block:: python

                # Use custom dictionary structure
                dictionary = {
                    "name": "sensor_pt859",
                    "location": "warehouse_125",
                    "version": "2021.06.05.5874",
                    "pressure": 125,
                    "temperature": 10,
                    "created": 1632208639,
                }
                point = Point.from_dict(dictionary,
                                        write_precision=WritePrecision.S,
                                        record_measurement_key="name",
                                        record_time_key="created",
                                        record_tag_keys=["location", "version"],
                                        record_field_keys=["pressure", "temperature"])

        Int Types:
            The following example shows how to configure the types of integers fields.
            It is useful when you want to serialize integers always as ``float`` to avoid ``field type conflict``
            or use ``unsigned 64-bit integer`` as the type for serialization.

            .. code-block:: python

                # Use custom dictionary structure
                dict_structure = {
                    "measurement": "h2o_feet",
                    "tags": {"location": "coyote_creek"},
                    "fields": {
                        "water_level": 1.0,
                        "some_counter": 108913123234
                    },
                    "time": 1
                }

                point = Point.from_dict(dict_structure, field_types={"some_counter": "uint"})

        :param dictionary: dictionary for serialize into data Point
        :param write_precision: sets the precision for the supplied time values
        :key record_measurement_key: key of dictionary with specified measurement
        :key record_measurement_name: static measurement name for data Point
        :key record_time_key: key of dictionary with specified timestamp
        :key record_tag_keys: list of dictionary keys to use as a tag
        :key record_field_keys: list of dictionary keys to use as a field
        :key field_types: optional dictionary to specify types of serialized fields. Currently, is supported customization for integer types.
                          Possible integers types:
                            - ``int`` - serialize integers as "**Signed 64-bit integers**" - ``9223372036854775807i`` (default behaviour)
                            - ``uint`` - serialize integers as "**Unsigned 64-bit integers**" - ``9223372036854775807u``
                            - ``float`` - serialize integers as "**IEEE-754 64-bit floating-point numbers**". Useful for unify number types in your pipeline to avoid field type conflict - ``9223372036854775807``
                          The ``field_types`` can be also specified as part of incoming dictionary. For more info see an example above.
        :return: new data point
        """
        measurement_ = kwargs.get("record_measurement_name", None)
        if measurement_ is None:
            measurement_ = dictionary[
                kwargs.get("record_measurement_key", "measurement")
            ]
        point = cls(measurement_)

        record_tag_keys = kwargs.get("record_tag_keys", [])
        if record_tag_keys:
            for tag_key in record_tag_keys:
                if tag_key in dictionary:
                    point.tag(tag_key, dictionary[tag_key])
        elif "tags" in dictionary:
            for tag_key, tag_value in dictionary["tags"].items():
                point.tag(tag_key, tag_value)

        record_field_keys = kwargs.get("record_field_keys", [])
        if record_field_keys:
            for field_key in record_field_keys:
                if field_key in dictionary:
                    point.field(field_key, dictionary[field_key])
        else:
            for field_key, field_value in dictionary["fields"].items():
                point.field(field_key, field_value)

        record_time_key = kwargs.get("record_time_key", "time")
        if record_time_key in dictionary:
            point.time(dictionary[record_time_key], write_precision=write_precision)

        _field_types = kwargs.get("field_types", {})
        if "field_types" in dictionary:
            _field_types = dictionary["field_types"]
        # Map API fields types to Line Protocol types postfix:
        # - int: 'i'
        # - uint: 'u'
        # - float: ''
        point.field_types(
            dict(
                map(
                    lambda item: (
                        item[0],
                        "i" if item[1] == "int" else "u" if item[1] == "uint" else "",
                    ),
                    _field_types.items(),
                )
            )
        )

        return point

    def field_types(self, field_types: dict):
        self._field_types = field_types
        return self

    def time(self, time, write_precision=DEFAULT_WRITE_PRECISION):
        """
        Specify timestamp for DataPoint with declared precision.

        If time doesn't have specified timezone we assume that timezone is UTC.

        Examples::
            Point("h2o").field("val", 1).time("2009-11-10T23:00:00.123456Z")
            Point("h2o").field("val", 1).time(1257894000123456000)
            Point("h2o").field("val", 1).time(datetime(2009, 11, 10, 23, 0, 0, 123456))
            Point("h2o").field("val", 1).time(1257894000123456000, write_precision=WritePrecision.NS)


        :param time: the timestamp for your data
        :param write_precision: sets the precision for the supplied time values
        :return: this point
        """
        self._write_precision = write_precision
        self._time = time
        return self

    def tag(self, key, value):
        """Add tag with key and value."""
        self._tags[key] = value
        return self

    def field(self, field, value):
        """Add field with key and value."""
        self._fields[field] = value
        return self

    def to_line_protocol(self, precision=None):
        """
        Create LineProtocol.

         :param precision: required precision of LineProtocol. If it's not set then use the precision from ``Point``.
        """
        _measurement = _escape_key(self._name, _ESCAPE_MEASUREMENT)
        if _measurement.startswith("#"):
            message = f"""The measurement name '{_measurement}' start with '#'.

The output Line protocol will be interpret as a comment by InfluxDB. For more info see:
    - https://docs.influxdata.com/influxdb/latest/reference/syntax/line-protocol/#comments
"""
            warnings.warn(message, SyntaxWarning)
        _tags = _append_tags(self._tags)
        _fields = _append_fields(self._fields, self._field_types)
        if not _fields:
            return ""
        _time = _append_time(
            self._time, self._write_precision if precision is None else precision
        )

        return f"{_measurement}{_tags}{_fields}{_time}"

    @property
    def write_precision(self):
        """Get precision."""
        return self._write_precision

    @classmethod
    def set_str_rep(cls, rep_function):
        """Set the string representation for all Points."""
        cls.__str___rep = rep_function

    def __str__(self):
        """Create string representation of this Point."""
        return self.to_line_protocol()


def _append_tags(tags):
    _return = []
    for tag_key, tag_value in sorted(tags.items()):
        if tag_value is None:
            continue

        tag = _escape_key(tag_key)
        value = _escape_tag_value(tag_value)
        if tag != "" and value != "":
            _return.append(f"{tag}={value}")

    return f"{',' if _return else ''}{','.join(_return)} "


def _append_fields(fields, field_types):
    _return = []

    for field, value in sorted(fields.items()):
        if value is None:
            continue

        if (
            isinstance(value, float)
            or isinstance(value, Decimal)
            or _np_is_subtype(value, "float")
        ):
            if not math.isfinite(value):
                continue
            s = str(value)
            # It's common to represent whole numbers as floats
            # and the trailing ".0" that Python produces is unnecessary
            # in line-protocol, inconsistent with other line-protocol encoders,
            # and takes more space than needed, so trim it off.
            if s.endswith(".0"):
                s = s[:-2]
            _return.append(f"{_escape_key(field)}={s}")
        elif (
            isinstance(value, int) or _np_is_subtype(value, "int")
        ) and not isinstance(value, bool):
            _type = field_types.get(field, "i")
            _return.append(f"{_escape_key(field)}={str(value)}{_type}")
        elif isinstance(value, bool):
            _return.append(f"{_escape_key(field)}={str(value).lower()}")
        elif isinstance(value, str):
            _return.append(f'{_escape_key(field)}="{_escape_string(value)}"')
        else:
            raise ValueError(
                f'Type: "{type(value)}" of field: "{field}" is not supported.'
            )

    return f"{','.join(_return)}"


def _append_time(time, write_precision) -> str:
    if time is None:
        return ""
    return f" {int(_convert_timestamp(time, write_precision))}"


def _escape_key(tag, escape_list=None) -> str:
    if escape_list is None:
        escape_list = _ESCAPE_KEY
    return str(tag).translate(escape_list)


def _escape_tag_value(value) -> str:
    ret = _escape_key(value)
    if ret.endswith("\\"):
        ret += " "
    return ret


def _escape_string(value) -> str:
    return str(value).translate(_ESCAPE_STRING)


def _convert_timestamp(timestamp, precision=DEFAULT_WRITE_PRECISION):
    if isinstance(timestamp, Integral):
        return timestamp  # assume precision is correct if timestamp is int

    if isinstance(timestamp, str):
        timestamp = date_helper.parse_date(timestamp)

    if isinstance(timestamp, timedelta) or isinstance(timestamp, datetime):
        if isinstance(timestamp, datetime):
            timestamp = date_helper.to_utc(timestamp) - EPOCH

        ns = date_helper.to_nanoseconds(timestamp)

        if precision is None or precision == "ns":
            return ns
        elif precision == "us":
            return ns / 1e3
        elif precision == "ms":
            return ns / 1e6
        elif precision == "s":
            return ns / 1e9

    raise ValueError(timestamp)


def _np_is_subtype(value, np_type):
    if not _HAS_NUMPY or not hasattr(value, "dtype"):
        return False

    if np_type == "float":
        return np.issubdtype(value, np.floating)
    elif np_type == "int":
        return np.issubdtype(value, np.integer)
    return False
