import ctypes
from enum import Enum
from typing import Union

from ..helpers.enumconverter import EnumConverter as ec
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.QuixStreamsStreaming.Models.ParameterValue import ParameterValue as pvi


class ParameterValueType(Enum):
    Empty = 0
    Numeric = 1
    String = 2
    Binary = 3


@nativedecorator
class ParameterValue(object):
    """Represents a single parameter value of either numeric, string, or binary type."""

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of ParameterValue.

        Args:
            net_pointer: The .net object pointer representing ParameterValue.
        """

        if net_pointer is None:
            raise Exception("ParameterValue constructor should not be invoked without a .net pointer")

        self._interop = pvi(net_pointer)

        self._type = ec.enum_to_another(self._interop.get_Type(), ParameterValueType)
        self._binary = None
        self._numeric = None
        self._string = None
        self._binary = None
        if self._type == ParameterValueType.Binary:
            val_uptr = self._interop.get_BinaryValue()
            if val_uptr is not None:
                self._binary = ai.ReadBytes(val_uptr)
                self._value = self._binary
        elif self._type == ParameterValueType.String:
            self._string = self._interop.get_StringValue()
            self._value = self._string
        elif self._type == ParameterValueType.Numeric:
            self._numeric = self._interop.get_NumericValue()
            self._value = self._numeric

    @property
    def numeric_value(self) -> float:
        """Gets the numeric value of the parameter if the underlying parameter is of numeric type."""
        return self._numeric

    @numeric_value.setter
    def numeric_value(self, value: float):
        """
        Sets the numeric value of the parameter and updates the type to numeric.

        Args:
            value: The numeric value to set.
        """

        if self._type != ParameterValueType.Numeric:
            self._type = ParameterValueType.Numeric
            self._string = None
            self._binary = None

        self._numeric = value
        self._value = value
        self._interop.set_NumericValue(value)

    @property
    def string_value(self) -> str:
        """Gets the string value of the parameter if the underlying parameter is of string type."""

        return self._string

    @string_value.setter
    def string_value(self, value: str):
        """
        Sets the string value of the parameter and updates the type to string.

        Args:
            value: The string value to set.
        """

        if self._type != ParameterValueType.String:
            self._type = ParameterValueType.String
            self._numeric = None
            self._binary = None

        self._string = value
        self._value = value
        self._interop.set_StringValue(value)

    @property
    def binary_value(self) -> bytes:
        """Gets the binary value of the parameter if the underlying parameter is of binary type."""
        return self._binary

    @binary_value.setter
    def binary_value(self, value: Union[bytearray, bytes]):
        """
        Sets the binary value of the parameter and updates the type to binary.

        Args:
            value: The binary value to set.
        """

        if self._type != ParameterValueType.Binary:
            self._type = ParameterValueType.Binary
            self._numeric = None
            self._string = None

        self._binary = value
        self._value = value

        uptr = ai.WriteBytes(value)
        self._interop.set_BinaryValue(uptr)

    @property
    def type(self) -> ParameterValueType:
        """Gets the type of value, which is numeric, string, binary if set, otherwise empty"""

        return self._type

    @property
    def value(self):
        """Gets the underlying value."""

        return self._value

    def get_net_pointer(self) -> ctypes.c_void_p:
        """Gets the associated .net object pointer."""

        return self._interop.get_interop_ptr__()
