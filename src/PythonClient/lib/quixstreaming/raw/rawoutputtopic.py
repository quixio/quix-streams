import Quix.Sdk.Streaming.Raw
from .rawmessage import RawMessage
from typing import Union


class RawOutputTopic(object):
    def __init__(self, net_object: Quix.Sdk.Streaming.Raw.RawOutputTopic):
        self.__wrapped = net_object

    """
    Write the packet to the output topic.

    params:
    (message): either bytes, bytearray or instance of RawMessage
    """
    def write(self, message: Union[RawMessage, bytes, bytearray]):
        if not isinstance(message, RawMessage):
            message = RawMessage(message)
        self.__wrapped.Write(message.convert_to_net())