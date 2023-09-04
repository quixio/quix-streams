import ctypes
from typing import Optional

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsKafkaTransport.CommitOptions import CommitOptions as coi


@nativedecorator
class CommitOptions(object):
    def __init__(self, net_pointer: ctypes.c_void_p = None):
        """
        Initializes a new instance of CommitOptions

        :param net_pointer: Pointer to an instance of a .net CommitOptions.
        """

        if net_pointer is None:
            self._interop = coi(coi.Constructor())
        else:
            self._interop = coi(net_pointer)

    @property
    def auto_commit_enabled(self) -> bool:
        """
        Gets whether automatic committing is enabled.
        If automatic committing is not enabled, other values are ignored.
        Default is True.
        """
        return self._interop.get_AutoCommitEnabled()

    @auto_commit_enabled.setter
    def auto_commit_enabled(self, value: bool) -> None:
        """
        Sets whether automatic committing is enabled.
        If automatic committing is not enabled, other values are ignored.
        Default is True.
        """
        self._interop.set_AutoCommitEnabled(value)

    @property
    def commit_interval(self) -> Optional[int]:
        """
        Gets the interval of automatic commit in ms. Default is 5000.
        """
        return self._interop.get_CommitInterval()

    @commit_interval.setter
    def commit_interval(self, value: Optional[int]) -> None:
        """
        Sets the interval of automatic commit in ms. Default is 5000.
        """
        self._interop.set_CommitInterval(value)

    @property
    def commit_every(self) -> Optional[int]:
        """
        Gets the number of messages to automatically commit at. Default is 5000.
        """
        return self._interop.get_CommitEvery()

    @commit_every.setter
    def commit_every(self, value: Optional[int]) -> None:
        """
        Sets the number of messages to automatically commit at. Default is 5000.
        """
        self._interop.set_CommitEvery(value)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
