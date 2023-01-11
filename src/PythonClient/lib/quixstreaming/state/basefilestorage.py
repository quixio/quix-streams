from .. import __importnet
import Quix.Sdk.State
import pickle
from .statevalue import StateValue

class BaseFileStorage(object):

    def __init__(self, net_object):
        """
            abstract wrapper of the BaseFileStorage from .net object
        """
        self.__wrapped = net_object

    def get(self, key: str):
        """
            Get value at key
        """
        return StateValue(Quix.Sdk.State.Storage.StorageExtensions.Get(self.__wrapped, key)).value

    def set(self, key: str, value):
        """
            Set value at the key
        """

        Quix.Sdk.State.Storage.StorageExtensions.Set(self.__wrapped, key, StateValue(value).convert_to_net())

    
    def containsKey(self, key: str) -> bool:
        """
            Check whether the storage contains the key
        """
        return Quix.Sdk.State.Storage.StorageExtensions.ContainsKey(self.__wrapped, key)

    def getAllKeys(self):
        """
            Get set containing all the keys inside storage
        """
        return Quix.Sdk.State.Storage.StorageExtensions.GetAllKeys(self.__wrapped)

    def remove(self, key):
        """
            Remove key from the storage
        """
        return Quix.Sdk.State.Storage.StorageExtensions.Remove(self.__wrapped, key)

    def clear(self):
        """
            Clear storage ( remove all the keys )
        """
        Quix.Sdk.State.Storage.StorageExtensions.Clear(self.__wrapped)
