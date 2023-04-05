import ctypes
import pickle
from typing import Any

from .statetype import StateType
from ..helpers.enumconverter import EnumConverter as ec
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.QuixStreamsState.StateValue import StateValue as svi
from ..native.Python.QuixStreamsState.StateValue_StateType import StateType as StateTypeInterop


class StateValue(object):
    """
    A wrapper class for values that can be stored inside the storage.
    """

    def __init__(self, value: Any):
        """
        Initializes the wrapped value inside the store.

        Args:
            value: The value to be wrapped, which can be one of the following types:
                StateValue, str, int, float, bool, bytes, bytearray, or object (via pickle).
        """

        self._interop = None

        value_type = type(value)
        if isinstance(value, StateValue):
            self._interop = svi(value.get_net_pointer())
            self._type = value.type
            self._value = value.value
        elif isinstance(value, ctypes.c_void_p):
            self._interop = svi(value)
            self._type = ec.enum_to_another(self._interop.get_Type(), StateType)
            if self._type == StateType.String:
                self._value = self._interop.get_StringValue()
            elif self._type == StateType.Binary:
                self._value = ai.ReadBytes(self._interop.get_BinaryValue())
            elif self._type == StateType.Object:
                obj_bytes = ai.ReadBytes(self._interop.get_BinaryValue())
                if obj_bytes[0] != 128:  # '\x80'
                    raise Exception("Loaded state is not a pickled object")
                # could check for the version number, but that can increase in the future...
                if obj_bytes[-1] != 46:  # '.'
                    raise Exception("Loaded state is not a pickled object")
                self._value = pickle.loads(obj_bytes)
            elif self._type == StateType.Bool:
                self._value = self._interop.get_BoolValue()
            elif self._type == StateType.Long:
                self._value = self._interop.get_LongValue()
            elif self._type == StateType.Double:
                self._value = self._interop.get_DoubleValue()
            else:
                raise Exception("State value type is invalid: " + str(self._type))
        else:
            self._value = value
            if isinstance(value, str):
                self._interop = svi(svi.Constructor5(value))
                self._type = StateType.String
            elif isinstance(value, int):
                self._interop = svi(svi.Constructor2(value))
                self._type = StateType.Long
            elif isinstance(value, float):
                self._interop = svi(svi.Constructor6(value))
                self._type = StateType.Double
            elif isinstance(value, bool):
                self._interop = svi(svi.Constructor(value))
                self._type = StateType.Bool
            elif value_type == bytes or value_type == bytearray:
                byte_value_uptr = ai.WriteBytes(value)
                self._interop = svi(svi.Constructor3(byte_value_uptr))
                self._value = bytes(value) if value_type == bytearray else value
                self._type = StateType.Binary
            else:
                self._type = StateType.Object
                obj_bytes = pickle.dumps(value)
                byte_value_uptr = ai.WriteBytes(obj_bytes)
                dotnet_enum = ec.enum_to_another(self._type, StateTypeInterop)
                self._interop = svi(svi.Constructor4(byte_value_uptr, dotnet_enum))

    @property
    def type(self):
        """
        Gets the type of the wrapped value.

        Returns:
            StateType: The type of the wrapped value.
        """
        return self._type

    @property
    def value(self):
        """
        Gets the wrapped value.

        Returns:
            The wrapped value.
        """
        return self._value

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the .NET pointer of the wrapped value.

        Returns:
            ctypes.c_void_p: The .NET pointer of the wrapped value.
        """
        return self._interop.get_interop_ptr__()
