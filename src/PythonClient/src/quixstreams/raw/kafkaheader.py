import ctypes
from typing import Union

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.QuixStreamsKafka.KafkaHeader import KafkaHeader as khi


@nativedecorator
class KafkaHeader(object):
    """
    The key-value pair kafka header
    """

    def __init__(self,
                 key: str = None,
                 value: Union[str, bytes, bytearray] = None,
                 **kwargs):
        """
        Initializes a new instance of KafkaHeader.

        Args:
            key: The string key of the header
            value: The string or bytes value of the header
        """

        self._value = None
        self._key = None
        self._value_as_string = None

        if kwargs is not None and "net_pointer" in kwargs:
            net_pointer = kwargs["net_pointer"]
            self._interop = khi(net_pointer)
            return

        if key is None:
            raise ValueError(f"Parameter 'key' must not be none.")

        if not isinstance(key, str):
            raise TypeError(f"Parameter 'key' has incorrect type '{type(value)}. Must be string.")

        self._key = key

        if value is None:
            raise ValueError(f"Parameter 'value' must not be none.")

        if not isinstance(value, (str, bytes, bytearray)):
            raise TypeError(f"Parameter 'value' has incorrect type '{type(key)}'."
                            f" Must be string, bytes or bytearray.")

        if isinstance(value, str):
            self._value_as_string = value

            net_pointer = khi.Constructor2(key, value)
            self._interop = khi(net_pointer)
            return

        self._value = value
        self._value_as_string = None

        value_uptr = ai.WriteBytes(value)

        net_pointer = khi.Constructor(key, value_uptr)
        self._interop = khi(net_pointer)

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the KafkaHeader instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the KafkaHeader instance.
        """

        return self._interop.get_interop_ptr__()

    @property
    def key(self) -> bytes:
        """
        Gets the key of the header

        Returns:
            bytes: The key of the message.
        """
        if self._key is None:
            self._key = self._interop.get_Key()
        return self._key

    @property
    def value(self) -> Union[bytearray, bytes]:
        """
        Gets the value of the header as bytes.

        Returns:
            Union[bytearray, bytes]: The value of the header.
        """
        if self._value is None:
            val_uptr = self._interop.get_Value()
            self._value = ai.ReadBytes(val_uptr)
        return self._value

    def get_value_as_str(self) -> str:
        """
        Gets the value of the header as string

        Returns:
            str: The value of the header.
        """
        if self._value_as_string is None:
            self._value_as_string = self._interop.GetValueAsString()
        return self._value_as_string

