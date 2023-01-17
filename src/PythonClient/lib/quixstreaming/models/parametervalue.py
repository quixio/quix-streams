from typing import Union
from enum import Enum
from .. import __importnet
import Quix.Sdk.Streaming


class ParameterValue(object):
    """
    Represents a single parameter value of either string or numeric type
    """

    def __init__(self, _net_object: Quix.Sdk.Streaming.Models.ParameterValue = None):
        """
            Initializes a new instance of ParameterValue.

            Parameters:

            net_object (.net object): The .net object representing ParameterValue.
        """
        if _net_object is None:
            raise Exception("Parameter value should not be invoked without a .net object")
        self.__wrapped = _net_object

    @property
    def numeric_value(self) -> float:
        """Gets the the numeric value of the parameter, if the underlying parameter is of numeric type"""

        return self.__wrapped.NumericValue

    @numeric_value.setter
    def numeric_value(self, value: float):
        """Sets the the numeric value of the parameter and updates the type to numeric"""

        self.__wrapped.NumericValue = value

    @property
    def string_value(self) -> str:
        """Gets the the string value of the parameter, if the underlying parameter is of string type"""

        return self.__wrapped.StringValue

    @string_value.setter
    def string_value(self, value: str):
        """Sets the the string value of the parameter and updates the type to string"""

        self.__wrapped.StringValue = value

    @property
    def binary_value(self) -> bytes:
        """Gets the the binary value of the parameter, if the underlying parameter is of binary type"""
        val = self.__wrapped.BinaryValue
        if val is None:
            return None
        return bytes(val)

    @binary_value.setter
    def binary_value(self, value: Union[bytearray, bytes]):
        """Sets the the binary value of the parameter and updates the type to binary"""

        self.__wrapped.BinaryValue = value

    @property
    def type(self) -> 'ParameterValueType':
        """Gets the type of value, which is numeric or string if set, else empty"""

        return ParameterValueType.convert_from_net(self.__wrapped.Type)

    @property
    def value(self):
        """Gets the underlying value"""

        return self.__wrapped.Value

    def convert_to_net(self):
        return self.__wrapped


class ParameterValueType(Enum):
    Empty = 0,
    Numeric = 1,
    String = 2,
    Binary = 3

    @staticmethod
    def convert_from_net(net_enum: Quix.Sdk.Streaming.Models.ParameterValueType):
        if net_enum == Quix.Sdk.Streaming.Models.ParameterValueType.Empty:
            return ParameterValueType.Empty
        if net_enum == Quix.Sdk.Streaming.Models.ParameterValueType.Numeric:
            return ParameterValueType.Numeric
        if net_enum == Quix.Sdk.Streaming.Models.ParameterValueType.String:
            return ParameterValueType.String
        if net_enum == Quix.Sdk.Streaming.Models.ParameterValueType.Binary:
            return ParameterValueType.Binary
        raise Exception("Invalid parameter value type")

    def convert_to_net(self):
        if self == ParameterValueType.Empty:
            return Quix.Sdk.Streaming.Models.ParameterValueType.Empty
        if self == ParameterValueType.Numeric:
            return Quix.Sdk.Streaming.Models.ParameterValueType.Numeric
        if self == ParameterValueType.String:
            return Quix.Sdk.Streaming.Models.ParameterValueType.String
        if self == ParameterValueType.Binary:
            return Quix.Sdk.Streaming.Models.ParameterValueType.Binary
