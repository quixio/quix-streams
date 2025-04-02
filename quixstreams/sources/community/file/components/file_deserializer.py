from typing import BinaryIO, Callable, Iterable, Optional, Union

from ..compressions import CompressionName
from ..formats import FORMATS, Format, FormatName

__all__ = ("raw_filestream_deserializer",)


def raw_filestream_deserializer(
    formatter: Union[Format, FormatName],
    compression: Optional[CompressionName],
) -> Callable[[BinaryIO], Iterable[object]]:
    """
    Returns a Callable that can deserialize a filestream into some form of iterable.
    """
    if isinstance(formatter, Format):
        return formatter.read
    elif format_obj := FORMATS.get(formatter):
        return format_obj(compression=compression).read

    allowed_formats = ", ".join(FormatName.__args__)
    raise ValueError(
        f'Invalid format name "{formatter}". '
        f"Allowed values: {allowed_formats}, "
        f"or an instance of a subclass of `Format`."
    )
