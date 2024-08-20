from .base import *
from .simple_types import (
    StringSerializer,
    StringDeserializer,
    BytesDeserializer,
    BytesSerializer,
    DoubleSerializer,
    DoubleDeserializer,
    IntegerDeserializer,
    IntegerSerializer,
)
from .exceptions import *
from .json import JSONSerializer, JSONDeserializer
from .schema_registry import *
from .quix import (
    QuixEventsSerializer,
    QuixTimeseriesSerializer,
    QuixDeserializer,
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
