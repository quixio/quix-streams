import pickle
from pickle import loads, dumps
from typing import TypeVar, Callable

__all__ = ("pickle_copier",)
T = TypeVar("T")

# Always use the latest pickle protocol
_PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL


def pickle_copier(obj: T) -> Callable[[], T]:
    """
    A utility function to copy objects using a "pickle" library.
    On average, it's faster than "copy.deepcopy".
    It accepts an object and returns a callable creating copies of this object.

    :param obj: an object to copy
    """

    serialized = dumps(obj, protocol=_PICKLE_PROTOCOL, fix_imports=False)
    return lambda: loads(serialized, fix_imports=False)
