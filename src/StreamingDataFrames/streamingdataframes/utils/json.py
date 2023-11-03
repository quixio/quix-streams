from typing import Any

import orjson

_ORJSON_OPTIONS = orjson.OPT_PASSTHROUGH_DATETIME | orjson.OPT_PASSTHROUGH_DATACLASS


def dumps(value: Any) -> bytes:
    """
    Serialize to JSON using `orjson` package.

    :param value: value to serialize to JSON
    :return: bytes
    """
    return orjson.dumps(value, option=_ORJSON_OPTIONS)


def loads(value: bytes) -> Any:
    """
    Deserialize from JSON using `orjson` package.

    Main differences:
    - It returns `bytes`
    - It doesn't allow non-str keys in dictionaries

    :param value: value to deserialize from
    :return: object
    """
    return orjson.loads(value)
