from .statevalue import StateValue
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage import LocalFileStorage as lfsi
from ..native.Python.QuixStreamsState.Storage.StorageExtensions import StorageExtensions as se


class LocalFileStorage(object):

    def __init__(self, storage_directory=None, auto_create_dir=True):
        """
            Initializes the LocalFileStorage containing the whole
        """
        self._pointer = lfsi.Constructor(storage_directory, auto_create_dir)

    def get(self, key: str) -> any:
        """
            Get value at key
            param key:
            return: one of
                str
                int
                float
                bool
                bytes
                bytearray
                object (via pickle)
        """
        return StateValue(se.Get(self._pointer, key)).value

    def set(self, key: str, value: any):
        """
            Set value at the key
            param key:
            param value:
                StateValue
                str
                int
                float
                bool
                bytes
                bytearray
                object (via pickle)
        """

        se.Set6(self._pointer, key, StateValue(value).get_net_pointer())

    def contains_key(self, key: str) -> bool:
        """
            Check whether the storage contains the key
        """
        return se.ContainsKey(self._pointer, key)

    def get_all_keys(self):
        """
            Get set containing all the keys inside storage
        """
        keys_uptr = se.GetAllKeys(self._pointer)
        return ai.ReadStrings(keys_uptr)

    def remove(self, key) -> None:
        """
            Remove key from the storage
        """
        se.Remove(self._pointer, key)

    def clear(self):
        """
            Clear storage ( remove all the keys )
        """
        se.Clear(self._pointer)
