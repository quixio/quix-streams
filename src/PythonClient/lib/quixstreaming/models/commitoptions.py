from typing import Optional

from .. import __importnet
import Quix.Sdk.Transport


class CommitOptions(object):
    def __init__(self, _net_object: Quix.Sdk.Transport.Fw.CommitOptions = None):
        """
        Initializes a new instance of CommitOptions

        :param net_object: The optional .net object representing CommitOptions.
        """

        if _net_object is None:
            _net_object = Quix.Sdk.Transport.Fw.CommitOptions()
        self.__wrapped = _net_object

    def convert_to_net(self):
        return self.__wrapped

    @staticmethod
    def convert_from_net(net_object: Quix.Sdk.Transport.Fw.CommitOptions):
        if net_object is None:
            raise Exception("net_object is none")
        return CommitOptions(_net_object=net_object)

    @property
    def auto_commit_enabled(self) -> bool:
        """
        Gets whether automatic committing is enabled.
        If automatic committing is not enabled, other values are ignored.
        Default is True.
        """
        return self.__wrapped.AutoCommitEnabled

    @auto_commit_enabled.setter
    def auto_commit_enabled(self, value: bool) -> None:
        """
        Sets whether automatic committing is enabled.
        If automatic committing is not enabled, other values are ignored.
        Default is True.
        """
        self.__wrapped.AutoCommitEnabled = value

    @property
    def commit_interval(self) -> Optional[int]:
        """
        Gets the interval of automatic commit in ms. Default is 5000.
        """
        return self.__wrapped.CommitInterval

    @commit_interval.setter
    def commit_interval(self, value: Optional[int]) -> None:
        """
        Sets the interval of automatic commit in ms. Default is 5000.
        """
        self.__wrapped.CommitInterval = value

    @property
    def commit_every(self) -> Optional[int]:
        """
        Gets the number of messages to automatically commit at. Default is 5000.
        """
        return self.__wrapped.CommitEvery

    @commit_every.setter
    def commit_every(self, value: Optional[int]) -> None:
        """
        Sets the number of messages to automatically commit at. Default is 5000.
        """
        self.__wrapped.CommitEvery = value