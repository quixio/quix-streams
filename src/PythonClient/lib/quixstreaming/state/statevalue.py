from .. import __importnet
from clr import System
import pickle
import Quix.Sdk.State


"""
Boxed value wrapping the supported types
"""
class StateValue(object):

    def __init__(self, value):
        """
        Initialize the boxed value inside the store

        Parameter
            value one of the types:
                Quix.Sdk.State.StateValue
                str
                int
                float
                bool
                bytes
                bytearray
                object (via pickle)


        """
        valueType = type(value)
        if isinstance(value, Quix.Sdk.State.StateValue):
            self.__wrapped = value
            internalType = self.__wrapped.Type
            if internalType == Quix.Sdk.State.StateValue.StateType.String:
                self.__value = self.__wrapped.StringValue
            elif internalType == Quix.Sdk.State.StateValue.StateType.Binary:
                self.__value = bytes(self.__wrapped.BinaryValue)
            elif internalType == Quix.Sdk.State.StateValue.StateType.Object:
                obj_bytes = bytes(self.__wrapped.BinaryValue)
                if obj_bytes[0] != 128: # '\x80'
                    raise Exception("Loaded state is not a pickled object")
                # could check for the version number, but that can increase in future...
                if obj_bytes[-1] != 46: # '.'
                    raise Exception("Loaded state is not a pickled object")
                self.__value = pickle.loads(obj_bytes)
            elif internalType == Quix.Sdk.State.StateValue.StateType.Bool:
                self.__value = self.__wrapped.BoolValue
            elif internalType == Quix.Sdk.State.StateValue.StateType.Long:
                self.__value = self.__wrapped.LongValue
            elif internalType == Quix.Sdk.State.StateValue.StateType.Double:
                self.__value = self.__wrapped.DoubleValue
            else:
                raise Exception("UNREACHABLE")
        elif isinstance(value, str) or \
                valueType == int or \
                valueType == float or \
                valueType == bool:
            self.__wrapped = Quix.Sdk.State.StateValue(value)
            self.__value = value
        elif valueType == bytes or valueType == bytearray:
            self.__wrapped = Quix.Sdk.State.StateValue(System.Array[System.Byte](value))
            self.__value = bytes(value) if valueType == bytearray else value
        else:
            self.__value = value
            obj_bytes = pickle.dumps(value)
            self.__wrapped = Quix.Sdk.State.StateValue(System.Array[System.Byte](obj_bytes), Quix.Sdk.State.StateValue.StateType.Object)

    def convert_to_net(self):
        """
            Get associated .net object
        """
        return self.__wrapped

    @property
    def value(self):
        """
            Get wrapped value
        """
        return self.__value

    def clear(self):
        """
            Clear content of whole storage ( all keys )
        """
        self.__wrapped.Clear()
