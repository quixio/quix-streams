import ctypes
from typing import Union

from .rawmessage import RawMessage
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.Raw.RawTopicProducer import RawTopicProducer as rtpi


@nativedecorator
class RawTopicProducer(object):
    """
    Class to produce raw messages into a Topic (capable of producing non-quixstreams messages)
    """
    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of the RawTopicProducer class.

        Args:
            net_pointer: Pointer to an instance of a .NET RawTopicProducer object.
        """
        self._interop = rtpi(net_pointer)

    def publish(self, message: Union[RawMessage, bytes, bytearray]):
        """
        Publishes the given message to the associated topic producer.

        Args:
            message: The message to be published, which can be either
                a RawMessage instance, bytes, or a bytearray.
        """
        if not isinstance(message, RawMessage):
            message = RawMessage(message)
        self._interop.Publish(message.get_net_pointer())
