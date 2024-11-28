from typing import Literal, Union

from .base import Format
from .json import JSONFormat
from .parquet import ParquetFormat

__all__ = ("Format", "FormatName", "JSONFormat", "ParquetFormat", "resolve_format")

FormatName = Literal["json", "parquet"]

_FORMATS: dict[FormatName, Format] = {
    "json": JSONFormat(),
    "parquet": ParquetFormat(),
}


class InvalidFormatError(Exception):
    """
    Raised when the format is specified incorrectly.
    """


def resolve_format(format: Union[FormatName, Format]) -> Format:
    """
    Resolves the format into a `Format` instance.

    :param format: The format to resolve, either a format name ("json",
        "parquet") or a `Format` instance.
    :return: An instance of `Format` corresponding to the specified format.
    :raises InvalidFormatError: If the format name is invalid.
    """
    if isinstance(format, Format):
        return format
    elif format_obj := _FORMATS.get(format):
        return format_obj

    allowed_formats = ", ".join(FormatName.__args__)  # type: ignore[attr-defined]
    raise InvalidFormatError(
        f'Invalid format name "{format}". '
        f"Allowed values: {allowed_formats}, "
        f"or an instance of a subclass of `Format`."
    )
