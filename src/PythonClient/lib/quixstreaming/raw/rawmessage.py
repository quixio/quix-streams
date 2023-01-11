import Quix.Sdk.Streaming.Raw
from typing import Union, Dict

from quixstreaming.helpers import NetToPythonConverter


class RawMessage(object):
    """
        Class to hold the raw value being read from the message broker
    """

    def __init__(self, data: Union[Quix.Sdk.Streaming.Raw.RawMessage, bytes, bytearray]):
        if isinstance(data, Quix.Sdk.Streaming.Raw.RawMessage):
            self.__wrapped = data
        elif isinstance(data, (bytes, bytearray)):
            self.__wrapped = Quix.Sdk.Streaming.Raw.RawMessage(data)
        else:
            raise Exception("Bad data type for the message")
        
        self.__metadata = None
        self.__value = None
    

    """
    Get associated .net object
    """
    def convert_to_net(self):
        return self.__wrapped    

    """
    Get the optional key of the message. Depending on broker and message it is not guaranteed
    """
    @property
    def key(self) -> str:
        """Get the optional key of the message. Depending on broker and message it is not guaranteed """
        return self.__wrapped.Key

    """
    Set the message key
    """
    @key.setter
    def key(self, value: str):
        """Set the message key"""
        self.__wrapped.Key = value


    """
    Get message value (bytes content of message)
    """
    @property
    def value(self):
        """Get message value (bytes content of message)"""
        if self.__value is None:
            self.__value = NetToPythonConverter.convert_list(self.__wrapped.Value)
        return self.__value

    @value.setter
    def value(self, value: Union[bytearray, bytes]):
        """Set message value (bytes content of message)"""
        self.__value = None
        self.__wrapped.Value = value

    """
    Get wrapped message metadata

    (returns Dict[str, str])
    """
    @property
    def metadata(self) -> Dict[str, str]:
        """Get the default Epoch used for Parameters and Events"""
        if self.__metadata is None:
            self.__metadata = NetToPythonConverter.convert_dict(self.__wrapped.Metadata)
        return self.__metadata

