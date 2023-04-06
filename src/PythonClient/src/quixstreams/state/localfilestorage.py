from typing import Any
from .statevalue import StateValue
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage import LocalFileStorage as lfsi
from ..native.Python.QuixStreamsState.Storage.StorageExtensions import StorageExtensions as se


class LocalFileStorage(object):
    """
    A directory storage containing the file storage for single process access purposes.
    Locking is implemented via in-memory mutex.
    """

    def __init__(self, storage_directory=None, auto_create_dir=True):
        """
        Initializes the LocalFileStorage instance.

        Args:
            storage_directory: The path to the storage directory.
            auto_create_dir: If True, automatically creates the storage directory if it doesn't exist.
        """
        self._pointer = lfsi.Constructor(storage_directory, auto_create_dir)

    def get(self, key: str) -> Any:
        """
        Gets the value at the specified key.

        Args:
            key: The key to retrieve the value for.

        Returns:
            Any: The value at the specified key, which can be one of the following types:
                str, int, float, bool, bytes, bytearray, or object (via pickle).
        """
        return StateValue(se.Get(self._pointer, key)).value

    def set(self, key: str, value: Any):
        """
        Sets the value at the specified key.

        Args:
            key: The key to set the value for.
            value: The value to be set, which can be one of the following types:
                StateValue, str, int, float, bool, bytes, bytearray, or object (via pickle).
        """

        se.Set6(self._pointer, key, StateValue(value).get_net_pointer())

    def contains_key(self, key: str) -> bool:
        """
        Checks if the storage contains the specified key.

        Args:
            key: The key to check for.

        Returns:
            bool: True if the storage contains the key, False otherwise.
        """
        return se.ContainsKey(self._pointer, key)

    def get_all_keys(self):
        """
        Retrieves a set containing all the keys in the storage.

        Returns:
            set[str]: A set of all keys in the storage.
        """
        keys_uptr = se.GetAllKeys(self._pointer)
        return ai.ReadStrings(keys_uptr)

    def remove(self, key) -> None:
        """
        Removes the specified key from the storage.

        Args:
            key: The key to be removed.
        """
        se.Remove(self._pointer, key)

    def clear(self):
        """
        Clears the storage by removing all keys and their associated values.
        """
        se.Clear(self._pointer)
