from ..native.Python.QuixSdkStreaming.Raw.RawOutputTopic import RawOutputTopic as roti

import ctypes


from .rawmessage import RawMessage
from typing import Union
from ..helpers.nativedecorator import nativedecorator


@nativedecorator
class RawOutputTopic(object):
    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of RawOutputTopic

            Parameters:

            net_pointer (c_void_p): Pointer to an instance of a .net RawOutputTopic
        """
        self._interop = roti(net_pointer)


    """
    Write the packet to the output topic.

    params:
    (message): either bytes, bytearray or instance of RawMessage
    """
    def write(self, message: Union[RawMessage, bytes, bytearray]):
        if not isinstance(message, RawMessage):
            message = RawMessage(message)
        self._interop.Write(message.get_net_pointer())
