import ctypes
from typing import Union, Dict

from ..helpers.nativedecorator import nativedecorator
from ..models.netdict import NetDict
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai, Array
from ..native.Python.QuixStreamsStreaming.Raw.RawMessage import RawMessage as rmi


@nativedecorator
class RawMessage(object):
    """
        Class to hold the raw value being read from the message broker
    """

    def __init__(self, data: Union[ctypes.c_void_p, bytes, bytearray]):
        if isinstance(data, ctypes.c_void_p):
            self._interop = rmi(data)
        elif isinstance(data, (bytes, bytearray)):
            data_uptr = Array.WriteBytes(data)
            self._interop = rmi(rmi.Constructor2(data_uptr))
        else:
            raise Exception("Bad data type '" + type(data) + "' for the message. Must be ctypes_c.void_p, bytes or bytearray.")

        self._metadata = None
        self._value = None

    """
    Get associated .net object pointer
    """

    def get_net_pointer(self):
        return self._interop.get_interop_ptr__()

    """
    Get the optional key of the message. Depending on broker and message it is not guaranteed
    """

    @property
    def key(self) -> bytes:
        """Get the optional key of the message. Depending on broker and message it is not guaranteed """
        keys_uptr = self._interop.get_Key()
        return Array.ReadBytes(keys_uptr)

    """
    Set the message key
    """

    @key.setter
    def key(self, value: Union[bytearray, bytes]):
        """Set the message key"""
        key_uptr = Array.WriteBytes(value)
        self._interop.set_Key(key_uptr)

    """
    Get message value (bytes content of message)
    """

    @property
    def value(self):
        """Get message value (bytes content of message)"""
        if self._value is None:
            val_uptr = self._interop.get_Value()
            self._value = ai.ReadBytes(val_uptr)
        return self._value

    @value.setter
    def value(self, value: Union[bytearray, bytes]):
        """Set message value (bytes content of message)"""
        self._value = None  # in case it is read back, will be set again
        data_uptr = Array.WriteBytes(value)
        self._interop.set_Value(data_uptr)

    """
    Get wrapped message metadata

    (returns Dict[str, str])
    """

    @property
    def metadata(self) -> Dict[str, str]:
        """Get the default Epoch used for Parameters and Events"""
        if self._metadata is None:
            self._metadata = NetDict.constructor_for_string_string(self._interop.get_Metadata())
        return self._metadata
