import ctypes
from typing import Union, Dict

from ..helpers.nativedecorator import nativedecorator
from ..models.netdict import NetDict
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai, Array
from ..native.Python.QuixStreamsStreaming.Raw.RawMessage import RawMessage as rmi


@nativedecorator
class RawMessage(object):
    """
    The message consumed from topic without any transformation.
    """

    def __init__(self, data: Union[ctypes.c_void_p, bytes, bytearray]):
        """
        Initializes a new instance of RawMessage.

        Args:
            data: The raw data to be stored in the message. Must be one of ctypes_c.void_p, bytes, or bytearray.
        """

        if isinstance(data, ctypes.c_void_p):
            self._interop = rmi(data)
        elif isinstance(data, (bytes, bytearray)):
            data_uptr = Array.WriteBytes(data)
            self._interop = rmi(rmi.Constructor2(data_uptr))
        else:
            raise Exception("Bad data type '" + type(data) + "' for the message. Must be ctypes_c.void_p, bytes or bytearray.")

        self._metadata = None
        self._value = None

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the RawMessage instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the RawMessage instance.
        """
        return self._interop.get_interop_ptr__()

    @property
    def key(self) -> bytes:
        """
        Gets the optional key of the message. Depending on the broker and message, it is not guaranteed.

        Returns:
            bytes: The optional key of the message.
        """
        keys_uptr = self._interop.get_Key()
        return Array.ReadBytes(keys_uptr)

    @key.setter
    def key(self, value: Union[bytearray, bytes]):
        """
        Sets the message key.

        Args:
            value: The key to set for the message.
        """
        key_uptr = Array.WriteBytes(value)
        self._interop.set_Key(key_uptr)

    @property
    def value(self):
        """
        Gets the message value (bytes content of the message).

        Returns:
            Union[bytearray, bytes]: The message value (bytes content of the message).
        """
        if self._value is None:
            val_uptr = self._interop.get_Value()
            self._value = ai.ReadBytes(val_uptr)
        return self._value

    @value.setter
    def value(self, value: Union[bytearray, bytes]):
        """
        Sets the message value (bytes content of the message).

        Args:
            value: The value to set for the message.
        """
        self._value = None  # in case it is read back, will be set again
        data_uptr = Array.WriteBytes(value)
        self._interop.set_Value(data_uptr)

    @property
    def metadata(self) -> Dict[str, str]:
        """
        Gets the wrapped message metadata.

        Returns:
            Dict[str, str]: The wrapped message metadata.
        """
        if self._metadata is None:
            self._metadata = NetDict.constructor_for_string_string(self._interop.get_Metadata())
        return self._metadata
