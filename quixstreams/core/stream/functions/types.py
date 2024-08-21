from typing import Callable, Any, Iterable, Protocol, Tuple, Union

__all__ = (
    "StreamCallback",
    "VoidExecutor",
    "ReturningExecutor",
    "ApplyCallback",
    "ApplyExpandedCallback",
    "FilterCallback",
    "UpdateCallback",
    "ApplyWithMetadataCallback",
    "ApplyWithMetadataExpandedCallback",
    "UpdateWithMetadataCallback",
    "FilterWithMetadataCallback",
    "TransformCallback",
    "TransformExpandedCallback",
)


class SupportsBool(Protocol):
    def __bool__(self) -> bool: ...


ApplyCallback = Callable[[Any], Any]
ApplyExpandedCallback = Callable[[Any], Iterable[Any]]
UpdateCallback = Callable[[Any], None]
FilterCallback = Callable[[Any], bool]

ApplyWithMetadataCallback = Callable[[Any, Any, int, Any], Any]
ApplyWithMetadataExpandedCallback = Callable[[Any, Any, int, Any], Iterable[Any]]
UpdateWithMetadataCallback = Callable[[Any, Any, int, Any], None]
FilterWithMetadataCallback = Callable[[Any, Any, int, Any], SupportsBool]

TransformCallback = Callable[[Any, Any, int, Any], Tuple[Any, Any, int, Any]]
TransformExpandedCallback = Callable[
    [Any, Any, int, Any], Iterable[Tuple[Any, Any, int, Any]]
]

StreamCallback = Union[
    ApplyCallback,
    ApplyExpandedCallback,
    UpdateCallback,
    FilterCallback,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    UpdateWithMetadataCallback,
    FilterWithMetadataCallback,
    TransformCallback,
    TransformExpandedCallback,
]

VoidExecutor = Callable[[Any, Any, int, Any], None]
ReturningExecutor = Callable[[Any, Any, int, Any], Tuple[Any, Any, int, Any]]
