from abc import ABC, abstractmethod
from typing import Callable, TypeVar

T = TypeVar('T')


class IStreamState(ABC):

    # @property
    # @abstractmethod
    # def on_flushing(self) -> Callable:
    #     """
    #     Raised immediately before a flush operation is performed.
    #     """
    #     ...
    #
    # @property
    # @abstractmethod
    # def on_flushed(self) -> Callable:
    #     """
    #     Raised immediately after a flush operation is completed.
    #     """
    #     ...

    @abstractmethod
    def flush(self):
        """
        Flushes the changes made to the in-memory state to the specified storage.
        """
        ...

    @abstractmethod
    def reset(self):
        """
        Reset the state to before in-memory modifications
        """
        ...


class StreamStateDefaultValueDelegate(Callable[[str], T]):
    ...

