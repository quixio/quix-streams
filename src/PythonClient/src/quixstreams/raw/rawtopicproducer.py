import ctypes
from typing import Union

from .rawmessage import RawMessage
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.Raw.RawTopicProducer import RawTopicProducer as rtpi


@nativedecorator
class RawTopicProducer(object):
    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of RawTopicProducer

            Parameters:

            net_pointer (c_void_p): Pointer to an instance of a .net RawTopicProducer
        """
        self._interop = rtpi(net_pointer)

    """
    Write the packet to the output topic.

    params:
    (message): either bytes, bytearray or instance of RawMessage
    """

    def write(self, message: Union[RawMessage, bytes, bytearray]):
        if not isinstance(message, RawMessage):
            message = RawMessage(message)
        self._interop.Write(message.get_net_pointer())
