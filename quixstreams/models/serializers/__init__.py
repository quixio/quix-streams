# ruff: noqa: F403
from .base import *
from .exceptions import *
from .json import JSONDeserializer, JSONSerializer
from .quix import (
    QuixDeserializer,
    QuixEventsSerializer,
    QuixTimeseriesSerializer,
)
from .schema_registry import *
from .simple_types import (
    BytesDeserializer,
    BytesSerializer,
    DoubleDeserializer,
    DoubleSerializer,
    IntegerDeserializer,
    IntegerSerializer,
    StringDeserializer,
    StringSerializer,
)

SERIALIZERS = {
    "str": StringSerializer,
    "string": StringSerializer,
    "bytes": BytesSerializer,
    "double": DoubleSerializer,
    "int": IntegerSerializer,
    "integer": IntegerSerializer,
    "json": JSONSerializer,
    "quix_timeseries": QuixTimeseriesSerializer,
    "quix_events": QuixEventsSerializer,
}

DESERIALIZERS = {
    "str": StringDeserializer,
    "string": StringDeserializer,
    "bytes": BytesDeserializer,
    "double": DoubleDeserializer,
    "int": IntegerDeserializer,
    "integer": IntegerDeserializer,
    "json": JSONDeserializer,
    "quix": QuixDeserializer,
}
