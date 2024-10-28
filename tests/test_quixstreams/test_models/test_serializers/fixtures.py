import base64
import json
import time
from typing import Any, List, Mapping, Union

import pytest

from tests.utils import ConfluentKafkaMessageStub


@pytest.fixture()
def quix_timeseries_factory():
    def factory(
        binary: Mapping[str, List[Union[bytes, None]]] = None,
        numeric: Mapping[str, List[Union[int, float, None]]] = None,
        strings: Mapping[str, List[Union[str, None]]] = None,
        tags: Mapping[str, List[Union[str, None]]] = None,
        timestamps: List[int] = None,
        model_key: str = "TimeseriesData",
        codec_id: str = "JT",
        as_legacy: bool = False,
    ) -> ConfluentKafkaMessageStub:
        binary = binary or {}
        numeric = numeric or {}
        strings = strings or {}
        tags = tags or {}
        # Encode binary values to base64
        binary = {
            param: [
                base64.b64encode(item).decode() if item is not None else None
                for item in values
            ]
            for param, values in binary.items()
        }
        # Ensure that all parameters have the same length
        length = 0
        for params in (binary, numeric, strings, tags):
            for param, values in params.items():
                length = max(length, len(values))
                if length != len(values):
                    raise ValueError("Parameters must be of the same length")
        if timestamps and len(timestamps) != length:
            raise ValueError("Parameters must be of the same length")

        value = {
            "Timestamps": timestamps or [time.time_ns() for _ in range(length)],
            "StringValues": strings,
            "NumericValues": numeric,
            "BinaryValues": binary,
            "TagValues": tags,
        }
        # Note: Only legacy client uses S and E, so we just use 0 and 1 as placeholders
        message = ConfluentKafkaMessageStub(
            value=json.dumps(
                {"C": codec_id, "K": model_key, "V": value, "S": 0, "E": 1}
                if as_legacy
                else value
            ).encode(),
            headers=(
                None
                if as_legacy
                else [
                    ("__Q_ModelKey", model_key.encode()),
                    ("__Q_CodecId", codec_id.encode()),
                ]
            ),
        )
        return message

    return factory


class EventDataParams:
    __slots__ = (
        "id",
        "timestamp",
        "value",
        "tags",
    )

    def __init__(self, id: str, timestamp: int, value: str, tags: dict = None):
        self.id = id
        self.timestamp = timestamp
        self.value = value
        self.tags = tags


@pytest.fixture()
def quix_eventdata_params_factory():
    def factory(
        id: str, value: Any, timestamp: int = None, tags: dict = None
    ) -> EventDataParams:
        return EventDataParams(
            id=id,
            value=json.dumps(value),
            timestamp=timestamp or time.time_ns(),
            tags=tags or {},
        )

    return factory


@pytest.fixture()
def quix_eventdata_factory():
    def factory(
        params: EventDataParams,
        model_key: str = "EventData",
        codec_id: str = "JT",
        as_legacy: bool = False,
    ) -> ConfluentKafkaMessageStub:
        event = {
            "Timestamp": params.timestamp,
            "Id": params.id,
            "Value": params.value,
            "Tags": params.tags,
        }
        message = ConfluentKafkaMessageStub(
            value=json.dumps(
                {"C": codec_id, "K": model_key, "V": event, "S": 0, "E": 1}
                if as_legacy
                else event
            ).encode(),
            headers=(
                None
                if as_legacy
                else [
                    ("__Q_ModelKey", model_key.encode()),
                    ("__Q_CodecId", codec_id.encode()),
                ]
            ),
        )
        return message

    return factory


@pytest.fixture()
def quix_eventdata_list_factory():
    def factory(
        params: List[EventDataParams],
        model_key: str = "EventData[]",
        codec_id: str = "JT",
        as_legacy: bool = False,
    ) -> ConfluentKafkaMessageStub:
        events = [
            {
                "Timestamp": p.timestamp,
                "Id": p.id,
                "Value": p.value,
                "Tags": p.tags,
            }
            for p in params
        ]
        message = ConfluentKafkaMessageStub(
            value=json.dumps(
                {"C": codec_id, "K": model_key, "V": events, "S": 0, "E": 1}
                if as_legacy
                else events
            ).encode(),
            headers=(
                None
                if as_legacy
                else [
                    ("__Q_ModelKey", model_key.encode()),
                    ("__Q_CodecId", codec_id.encode()),
                ]
            ),
        )
        return message

    return factory
