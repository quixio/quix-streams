import base64
import gzip
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Union

from quixstreams.models.types import HeadersMapping
from quixstreams.utils.json import (
    dumps as default_dumps,
)
from quixstreams.utils.json import (
    loads as default_loads,
)

from .base import SerializationContext
from .exceptions import IgnoreMessage, SerializationError
from .json import JSONDeserializer, JSONSerializer

Q_SPLITMESSAGEID_NAME = "__Q_SplitMessageId"

# Quix data types pass timestamp in the message body
# During parsing we take this value from the body to ensure it's not produced
# to another topic
Q_TIMESTAMP_KEY = "Timestamp"


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

    GZIP_PREFIX = "[GZIP]"


class LegacyHeaders:
    Q_CODEC_ID = "C"
    Q_MODEL_KEY = "K"
    VALUE = "V"
    VALUE_START_IDX = "S"
    VALUE_END_IDX = "E"


def _b64_decode_or_none(s) -> Optional[bytes]:
    if s is not None:
        return base64.b64decode(s)


class QuixDeserializer(JSONDeserializer):
    """
    Handles Deserialization for any Quix-formatted topic.

    Parses JSON data from either `TimeseriesData` and `EventData` (ignores the rest).
    """

    def __init__(
        self,
        loads: Callable[[Union[bytes, bytearray]], Any] = default_loads,
    ):
        """
        :param loads: function to parse json from bytes.
            Default - :py:func:`quixstreams.utils.json.loads`.
        """
        super().__init__(loads=loads)
        self._deserializers = {
            QModelKey.TIMESERIESDATA: self.deserialize_timeseries,
            QModelKey.PARAMETERDATA: self.deserialize_timeseries,
            QModelKey.EVENTDATA: self.deserialize_event_data,
            QModelKey.EVENTDATA_LIST: self.deserialize_event_data_list,
        }

    @property
    def split_values(self) -> bool:
        """
        Each Quix message might contain data for multiple Rows.
        This property informs the downstream processors about that, so they can
        expect an Iterable instead of Mapping.
        """
        return True

    def _timestamp_key_check(self, param_name, param_type):
        if param_name != Q_TIMESTAMP_KEY:
            return True
        # TODO: in future story, this functionality will be added
        raise SerializationError(
            f"There is a competing 'Timestamp' field name present at "
            f"{param_type}.{param_name}: you must rename your field "
            f"or specify a field to use for the 'Timestamps' parameter."
        )

    def deserialize_timeseries(
        self, value: Union[List[Mapping], Mapping]
    ) -> Iterable[Mapping]:
        if not isinstance(value, Mapping):
            raise SerializationError(f'Expected mapping, got type "{type(value)}"')

        timestamps = value["Timestamps"]
        # Make a list of parameters and iterators with values to get them one by one
        # and decode values from base64 if they're binary
        all_params = {
            param_name: (
                iter(param_values),
                True if param_type == "BinaryValues" else False,
            )
            for param_type in ("NumericValues", "StringValues", "BinaryValues")
            for param_name, param_values in value.get(param_type, {}).items()
            if self._timestamp_key_check(param_name, param_type)
        }
        # Do the same with TagValues
        tags = [
            (tag, iter(values)) for tag, values in value.get("TagValues", {}).items()
        ]

        # Iterate over timestamps and get a corresponding parameter value
        # for each timestamp
        for timestamp_ns in timestamps:
            row_value = {
                param: _b64_decode_or_none(next(values)) if is_binary else next(values)
                for param, (values, is_binary) in all_params.items()
            }
            row_value["Tags"] = {tag: next(values) for tag, values in tags}

            row_value[Q_TIMESTAMP_KEY] = timestamp_ns
            yield row_value

    def deserialize(
        self, model_key: str, value: Union[List[Mapping], Mapping]
    ) -> Iterable[Mapping]:
        """
        Deserialization function for particular data types (Timeseries or EventData).

        :param model_key: value of "__Q_ModelKey" message header
        :param value: deserialized JSON value of the message, list or dict
        :return: Iterable of dicts
        """
        return self._deserializers[model_key](value)

    def deserialize_event_data(self, value: Mapping) -> Iterable[Mapping]:
        yield self._parse_event_data(value)

    def deserialize_event_data_list(self, value: List[Mapping]) -> Iterable[Mapping]:
        for item in value:
            yield self._parse_event_data(item)

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

        # Warning: headers can have multiple values for the same key, but in context
        # of this function it doesn't matter because we're looking for specific keys.
        if as_legacy := self._is_legacy_format(value):
            headers_dict, value = self._parse_legacy_format(value)
        else:
            headers_dict = {key: value.decode() for key, value in ctx.headers or {}}
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

        # Check if compressed
        if codec_id.startswith(QCodecId.GZIP_PREFIX):
            codec_id = codec_id[len(QCodecId.GZIP_PREFIX) :]
            value = gzip.decompress(value)

        # Fail if the codec is not JSON
        if codec_id not in QCodecId.SUPPORTED_CODECS:
            raise SerializationError(
                f'Unsupported "{QCodecId.HEADER_NAME}" value "{codec_id}"'
            )

        # Fail if the model_key is unknown
        if model_key not in QModelKey.ALLOWED_KEYS:
            raise SerializationError(
                f'Unsupported "{QModelKey.HEADER_NAME}" value "{model_key}"'
            )

        # Parse JSON from the message value
        try:
            deserialized = self._loads(value) if not as_legacy else value
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc

        # Pass deserialized value to the `deserialize` function
        yield from self._deserializers[model_key](value=deserialized)

    def _is_legacy_format(self, value: bytes):
        try:
            if value.startswith(b"<"):
                raise SerializationError(
                    "Message splitting was detected in a legacy-formatted message; "
                    "it is not supported in the new client."
                )
            return value.startswith(b'{"C":') and b'"K":' in value
        except AttributeError:
            return False

    def _parse_legacy_format(self, value: bytes) -> Tuple[Mapping, Mapping]:
        value = self._loads(value)
        headers = {
            QCodecId.HEADER_NAME: value.pop(LegacyHeaders.Q_CODEC_ID),
            QModelKey.HEADER_NAME: value.pop(LegacyHeaders.Q_MODEL_KEY),
        }
        message = value.pop("V")
        return headers, message


class QuixSerializer(JSONSerializer):
    _legacy: Dict[str, str] = {}
    _extra_headers: Dict[str, str] = {}

    def __init__(
        self,
        as_legacy: bool = True,
        dumps: Callable[[Any], Union[str, bytes]] = default_dumps,
    ):
        """
        Serializer that returns data in json format.
        :param as_legacy: parse as the legacy format; Default = True
        :param dumps: a function to serialize objects to json.
            Default - :py:func:`quixstreams.utils.json.dumps`
        """
        super().__init__(dumps=dumps)
        self._as_legacy = as_legacy

    @property
    def extra_headers(self) -> HeadersMapping:
        # Legacy-formatted messages should not contain any headers
        return self._extra_headers if not self._as_legacy else {}

    def _as_legacy_format(self, value: Union[str, bytes]) -> bytes:
        if isinstance(value, str):
            # Encode value to bytes to get its correct byte length in case of non-ascii
            # symbols
            value = value.encode()
        start = (
            f'{{"{LegacyHeaders.Q_CODEC_ID}":"{self._legacy[QCodecId.HEADER_NAME]}",'
            f'"{LegacyHeaders.Q_MODEL_KEY}":"{self._legacy[QModelKey.HEADER_NAME]}",'
            f'"{LegacyHeaders.VALUE}":'
        ).encode()
        start_idx = len(start)
        end = (
            f',"{LegacyHeaders.VALUE_START_IDX}":{start_idx},'
            f'"{LegacyHeaders.VALUE_END_IDX}":{start_idx + len(value)}}}'
        ).encode()
        return start + value + end

    def _to_json(self, value: Any) -> Union[str, bytes]:
        serialized = super()._to_json(value)
        if self._as_legacy:
            return self._as_legacy_format(serialized)
        return serialized


class QuixTimeseriesSerializer(QuixSerializer):
    """
    Serialize data to JSON formatted according to Quix Timeseries format.

    The serializable object must be dictionary, and each item must be of `str`, `int`,
    `float`, `bytes` or `bytearray` type.
    Otherwise, the `SerializationError` will be raised.

    Input:
    ```python
    {'a': 1, 'b': 1.1, 'c': "string", 'd': b'bytes', 'Tags': {'tag1': 'tag'}}
    ```

    Output:
    ```json
    {
        "Timestamps": [123123123],
        "NumericValues": {"a": [1], "b": [1.1]},
        "StringValues": {"c": ["string"]},
        "BinaryValues": {"d": ["Ynl0ZXM="]},
        "TagValues": {"tag1": ["tag"]}
    }
    ```

    """

    _legacy = {
        QModelKey.HEADER_NAME: QModelKey.PARAMETERDATA,
        QCodecId.HEADER_NAME: QCodecId.JSON_TYPED,
    }
    _extra_headers = {
        QModelKey.HEADER_NAME: QModelKey.TIMESERIESDATA,
        QCodecId.HEADER_NAME: QCodecId.JSON_TYPED,
    }

    def __call__(self, value: Mapping, ctx: SerializationContext) -> Union[str, bytes]:
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
            # Timestamp gets checked at the end.
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

        # Add a timestamp only if there's at least one parameter
        if not is_empty:
            try:
                result["Timestamps"].append(value[Q_TIMESTAMP_KEY])
            except KeyError:
                raise SerializationError(
                    f"Missing required Quix field: '{Q_TIMESTAMP_KEY}'"
                )

        return self._to_json(result)


class QuixEventsSerializer(QuixSerializer):
    """
    Serialize data to JSON formatted according to Quix EventData format.
    The input value is expected to be a dictionary with the following keys:
        - "Id" (type `str`, default - "")
        - "Value" (type `str`, default - ""),
        - "Tags" (type `dict`, default - {})

    >***NOTE:*** All the other fields will be ignored.

    Input:
    ```python
    {
        "Id": "an_event",
        "Value": "any_string",
        "Tags": {"tag1": "tag"}}
    }
    ```

    Output:
    ```json
    {
        "Id": "an_event",
        "Value": "any_string",
        "Tags": {"tag1": "tag"}},
        "Timestamp":1692703362840389000
    }
    ```
    """

    # Quix EventData data must have the following headers set
    _extra_headers = {
        QModelKey.HEADER_NAME: QModelKey.EVENTDATA,
        QCodecId.HEADER_NAME: QCodecId.JSON_TYPED,
    }
    _legacy = {
        QModelKey.HEADER_NAME: QModelKey.EVENTDATA,
        QCodecId.HEADER_NAME: QCodecId.JSON_TYPED,
    }

    def __call__(self, value: Mapping, ctx: SerializationContext) -> Union[str, bytes]:
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
        }

        try:
            result[Q_TIMESTAMP_KEY] = value[Q_TIMESTAMP_KEY]
        except KeyError:
            raise SerializationError(
                f"Missing required Quix field: '{Q_TIMESTAMP_KEY}'"
            )

        return self._to_json(result)
