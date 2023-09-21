import abc
import base64
import itertools
import time
from abc import ABCMeta
from typing import List, Mapping, Iterable, Optional, Union

from .base import SerializationContext
from .exceptions import SerializationError, IgnoreMessage
from .json import JSONDeserializer, JSONSerializer


Q_SPLITMESSAGEID_NAME = "__Q_SplitMessageId"

# Quix data types pass timestamp in the message body
# During parsing we take this value from the body to ensure it's not produced
# to another topic
Q_TIMESTAMP_KEY = "__Q_Timestamp"


class QModelKey:
    HEADER_NAME = "__Q_ModelKey"

    TIMESERIESDATA = "TimeseriesData"
    PARAMETERDATA = "ParameterData"
    EVENTDATA = "EventData"
    EVENTDATA_LIST = "EventData[]"

    KEYS_TO_IGNORE = (
        "StreamProperties",
        "StreamEnd",
        "ParameterDefinitions",
        "EventDefinitions",
    )
    KEYS_TO_PARSE = (
        TIMESERIESDATA,
        PARAMETERDATA,
        EVENTDATA,
        EVENTDATA_LIST,
    )
    ALLOWED_KEYS = KEYS_TO_PARSE + KEYS_TO_IGNORE


class QCodecId:
    HEADER_NAME = "__Q_CodecId"
    JSON = "J"
    JSON_TYPED = "JT"
    PROTOBUF = "PB"
    STRING = "S"
    BYTES = "B[]"
    NULL = "null"

    Q_CODECID_ALLOWED = (JSON, JSON_TYPED, PROTOBUF, STRING, BYTES, NULL)
    SUPPORTED_CODECS = (JSON, JSON_TYPED)


def _b64_decode_or_none(s) -> Optional[bytes]:
    if s is not None:
        return base64.b64decode(s)


class BaseQuixDeserializer(JSONDeserializer, metaclass=ABCMeta):
    @property
    def split_values(self) -> bool:
        """
        Each Quix message might contain data for multiple Rows.
        This property informs the downstream processors about that, so they can
        expect an Iterable instead of Mapping.
        """
        return True

    @abc.abstractmethod
    def deserialize(
        self, model_key: str, value: Union[List[Mapping], Mapping]
    ) -> Iterable[Mapping]:
        """
        Deserialization function for particular data type (Timeseries or EventData).

        :param model_key: value of "__Q_ModelKey" message header
        :param value: deserialized JSON value of the message, list or dict
        :return: Iterable of dicts
        """

    def __call__(self, value: bytes, ctx: SerializationContext) -> Iterable[dict]:
        """
        Deserialize messages from Quix formats (Timeseries or Events) using JSON codec.

        The concrete implementation for particular data type should be provided by
        method `deserialize()`.
        The code in this method only validates that incoming message is a Quix message.

        :param value: message value bytes
        :param ctx: serialization context
        :return: Iterable of dicts
        """

        # Validate message headers
        # Warning: headers can have multiple values for the same key, but in context
        # of this function it doesn't matter because we're looking for specific keys.
        headers_dict = (
            {key: value.decode() for key, value in ctx.headers}
            if ctx.headers is not None
            else {}
        )
        # Fail if the message is a part of a split
        if Q_SPLITMESSAGEID_NAME in headers_dict:
            raise SerializationError(
                f'Detected "{Q_SPLITMESSAGEID_NAME}" header but message splitting '
                f"is not supported"
            )

        codec_id = headers_dict.get(QCodecId.HEADER_NAME)
        if not codec_id:
            raise SerializationError(
                f'"{QCodecId.HEADER_NAME}" header is missing or empty'
            )
        model_key = headers_dict.get(QModelKey.HEADER_NAME)
        if not model_key:
            raise SerializationError(
                f'"{QModelKey.HEADER_NAME}" header is missing or empty'
            )

        # Ignore the protocol message if it doesn't contain any meaningful data
        if model_key in QModelKey.KEYS_TO_IGNORE:
            raise IgnoreMessage(
                f'Ignore Quix message with "{QModelKey.HEADER_NAME}": "{model_key}"'
            )

        # Fail if the codec is not JSON
        if codec_id not in QCodecId.SUPPORTED_CODECS:
            raise SerializationError(
                f'Unsupported "{QCodecId.HEADER_NAME}" value "{codec_id}"'
            )

        # Fail if the model_key is unknowun
        if model_key not in QModelKey.ALLOWED_KEYS:
            raise SerializationError(
                f'Unsupported "{QModelKey.HEADER_NAME}" value "{model_key}"'
            )

        # Parse JSON from the message value
        try:
            deserialized = self._loads(value, **self._loads_kwargs)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc

        # Pass deserialized value to the `deserialize` function
        for item in self.deserialize(model_key=model_key, value=deserialized):
            yield item


class QuixTimeseriesDeserializer(BaseQuixDeserializer):
    """
    Deserialize data from JSON formatted according to Quix Timeseries format.

    Example:
        Input:
        {
            "Timestamps": [123123123],
            "StringValues": {"a": ["str"]},
            "NumericValues": {"b": [1], "c": [1.1]},
            "BinaryValues: {"d": ["Ynl0ZXM="]},
            "TagValues": {"tag1": ["tag"]}
        }

        Output:
        {
            "a": "str",
            "b": 1,
            "c": 1.1,
            "d": b"bytes",
            "Tags": {"tag1": "tag"}},
            "__Q_Timestamp": 123123123
        }
    """

    def deserialize(
        self, model_key: str, value: Union[List[Mapping], Mapping]
    ) -> Iterable[Mapping]:
        # Fail if model key is unsupported
        if model_key not in (QModelKey.TIMESERIESDATA, QModelKey.PARAMETERDATA):
            raise SerializationError(
                f'"{QModelKey.HEADER_NAME}" must be "{QModelKey.TIMESERIESDATA}" '
                f'or {QModelKey.PARAMETERDATA}, got"{model_key}"'
            )
        if not isinstance(value, Mapping):
            raise SerializationError(f'Expected mapping, got type "{type(value)}"')

        timestamps = value["Timestamps"]
        # Make a list of parameters and iterators with values to get them one by one
        # and decode values from base64 if they're binary
        all_params = [
            (
                param_name,
                iter(param_values),
                True if param_type == "BinaryValues" else False,
            )
            for param_type in ("NumericValues", "StringValues", "BinaryValues")
            for param_name, param_values in value.get(param_type, {}).items()
        ]
        # Do the same with TagValues
        tags = [
            (tag, iter(values)) for tag, values in value.get("TagValues", {}).items()
        ]

        # Iterate over timestamps and get a corresponding parameter value
        # for each timestamp
        for timestamp_ns in timestamps:
            row_value = {
                param: _b64_decode_or_none(next(values)) if is_binary else next(values)
                for param, values, is_binary in all_params
            }
            row_value["Tags"] = {tag: next(values) for tag, values in tags}

            row_value[Q_TIMESTAMP_KEY] = timestamp_ns
            yield self._to_dict(row_value)


class QuixTimeseriesSerializer(JSONSerializer):
    """
    Serialize data to JSON formatted according to Quix Timeseries format.

    The serializable object must be dictionary, and each item must be of `str`, `int`,
    `float`, `bytes` or `bytearray` type.
    Otherwise, the `SerializationError` will be raised.

    Example of the format:
        Input:
        {'a': 1, 'b': 1.1, 'c': "string", 'd': b'bytes', 'Tags': {'tag1': 'tag'}}

        Output:
        {
            "Timestamps" [123123123],
            "NumericValues: {"a": [1], "b": [1.1]},
            "StringValues": {"c": ["string"]},
            "BinaryValues: {"d": ["Ynl0ZXM="]},
            "TagValues": {"tag1": ["tag']}
        }

    """

    # Quix Timeseries data must have the following headers set
    extra_headers = {
        QModelKey.HEADER_NAME: QModelKey.TIMESERIESDATA,
        QCodecId.HEADER_NAME: QCodecId.JSON_TYPED,
    }

    def __call__(
        self, value: Mapping, ctx: SerializationContext, timestamp_ns: int = None
    ) -> Union[str, bytes]:
        if not isinstance(value, Mapping):
            raise SerializationError(f"Expected Mapping, got {type(value)}")
        result = {
            "BinaryValues": {},
            "StringValues": {},
            "NumericValues": {},
            "Timestamps": [],
            "TagValues": {},
        }
        is_empty = True

        for key, item in value.items():
            # Omit None values because we cannot determine their type
            # Also omit '__Q_Timestamp' because it's not supposed to be forwarded
            if item is None or key == Q_TIMESTAMP_KEY:
                continue
            if key == "Tags":  # "Tags" is a special key
                if not isinstance(item, Mapping):
                    raise SerializationError(
                        f'"Tags" must be a Mapping, got type "{type(item)}"'
                    )
                tags = result["TagValues"]
                for tag_key, tag_value in item.items():
                    tags[tag_key] = [tag_value]
                continue

            if isinstance(item, (int, float)) and not isinstance(item, bool):
                params = result["NumericValues"].setdefault(key, [])
            elif isinstance(item, str):
                params = result["StringValues"].setdefault(key, [])
            elif isinstance(item, (bytes, bytearray)):
                params = result["BinaryValues"].setdefault(key, [])
                item = base64.b64encode(item).decode("ascii")
            else:
                raise SerializationError(
                    f'Item with key "{key}" has unsupported type "{type(item)}"'
                )
            params.append(item)
            is_empty = False

        # Add a timestamp only if there's a least one parameter
        if not is_empty:
            result["Timestamps"].append(timestamp_ns or time.time_ns())

        return self._to_json(result)


class QuixEventsDeserializer(BaseQuixDeserializer):
    """
    Deserialize data from JSON formatted according to Quix EventData format.

    Example:
        Input:
        {
            "Id": "an_event",
            "Value": "any_string",
            "Tags": {"tag1": "tag"}},
            "Timestamp":1692703362840389000
        }

        Output:
        {
            "Id": "an_event",
            "Value": "any_string"
            "Tags": {"tag1": "tag"}},
            "__Q_Timestamp": 1692703362840389000
        }
    """

    def deserialize(
        self, model_key: str, value: Union[List[Mapping], Mapping]
    ) -> Iterable[Mapping]:
        if model_key not in (QModelKey.EVENTDATA, QModelKey.EVENTDATA_LIST):
            raise SerializationError(
                f'"{QModelKey.HEADER_NAME}" must be "{QModelKey.EVENTDATA}" '
                f'or {QModelKey.EVENTDATA_LIST}, got"{model_key}"'
            )
        if model_key == QModelKey.EVENTDATA:
            yield self._to_dict(self._parse_event_data(value))
        elif model_key == QModelKey.EVENTDATA_LIST:
            for item in value:
                yield self._to_dict(self._parse_event_data(item))

    def _parse_event_data(self, value: Mapping) -> Mapping:
        if not isinstance(value, Mapping):
            raise SerializationError(f'Expected mapping, got type "{type(value)}"')

        row_value = {
            Q_TIMESTAMP_KEY: value["Timestamp"],
            "Id": value["Id"],
            "Value": value["Value"],
            "Tags": value.get("Tags", {}),
        }
        return row_value


class QuixEventsSerializer(JSONSerializer):
    """
    Serialize data to JSON formatted according to Quix EventData format.
    The input value is expected to be a dictionary with the following keys:
        - "Id" (type `str`, default - "")
        - "Value" (type `str`, default - ""),
        - "Tags" (type `dict`, default - {})

    Note: All the other fields will be ignored.

    Example:
        Input:
        {
            "Id": "an_event",
            "Value": "any_string"
            "Tags": {"tag1": "tag"}},
        }
        Output:
        {
            "Id": "an_event",
            "Value": "any_string",
            "Tags": {"tag1": "tag"}},
            "Timestamp":1692703362840389000
        }
    """

    # Quix EventData data must have the following headers set
    extra_headers = {
        QModelKey.HEADER_NAME: QModelKey.EVENTDATA,
        QCodecId.HEADER_NAME: QCodecId.JSON_TYPED,
    }

    def __call__(
        self, value: Mapping, ctx: SerializationContext, timestamp_ns: int = None
    ) -> Union[str, bytes]:
        if not isinstance(value, Mapping):
            raise SerializationError(f"Expected Mapping, got {type(value)}")

        event_value = value.get("Value", "")
        if not isinstance(event_value, str):
            raise SerializationError(
                f'Field "Value" is expected to be of type "str", got {type(event_value)}'
            )

        event_id = value.get("Id", "")
        if not isinstance(event_id, str):
            raise SerializationError(
                f'Field "Id" is expected to be of type "str", got {type(event_id)}'
            )
        tags = value.get("Tags") or {}
        if not isinstance(tags, dict):
            raise SerializationError(
                f'Field "Tags" is expected to be of type "dict", got {type(tags)}'
            )

        result = {
            "Id": event_id,
            "Value": event_value,
            "Tags": tags,
            "Timestamp": timestamp_ns or time.time_ns(),
        }

        return self._to_json(result)
