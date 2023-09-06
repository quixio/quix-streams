import ctypes
from typing import Any, List
from .statevalue import StateValue
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei
from ..native.Python.QuixStreamsState.Storage.IStateStorage import IStateStorage as issi
from ..native.Python.QuixStreamsState.Storage.StorageExtensions import StorageExtensions as se


class IStateStorage(object):
    """
    The minimum definition for a state storage
    """

    def __init__(self, net_pointer=ctypes.c_void_p):
        """
        Initializes a new instance of IStateStorage.

        NOTE: Do not initialize this class manually, it can be returned by classes for certain calls

        Args:
            net_pointer: The .net object representing a StreamStateManager.
        """

        if net_pointer is None:
            raise Exception("IStateStorage is none")

        self._pointer = net_pointer
        self._interop = issi(net_pointer)

    @property
    def is_case_sensitive(self) -> bool:
        """
        Returns whether the storage is case-sensitive
        """
        return self._interop.get_IsCaseSensitive()

    def get_or_create_sub_storage(self, sub_storage_name: str) -> 'IStateStorage':
        """
        Creates or retrieves the existing storage under this in hierarchy.

        Args:
            sub_storage_name: The name of the sub storage

        Returns:
            IStateStorage: The state storage for the given storage name
        """
        return IStateStorage(self._interop.GetOrCreateSubStorage(sub_storage_name))

    def delete_sub_storages(self) -> int:
        """
        Deletes the storages under this in hierarchy.

        Returns:
            int: The number of state storage deleted
        """
        return self._interop.DeleteSubStorages()

    def delete_sub_storage(self, sub_storage_name: str) -> bool:
        """
        Deletes a storage under this in hierarchy.

        Args:
            sub_storage_name: The name of the sub storage

        Returns:
            bool: Whether the state storage for the given storage name was deleted
        """
        return self._interop.DeleteSubStorage(sub_storage_name)

    def get_sub_storages(self) -> List[str]:
        """
        Gets the storages under this in hierarchy.

        Returns:
            IStateStorage: The storage names this store contains
        """
        return ei.ReadStrings(self._interop.GetSubStorages())

    # region functionality provided through StorageExtensions
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

        sv = StateValue(value)
        sv_pointer = sv.get_net_pointer()
        se.Set6(self._pointer, key, sv_pointer)

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

    # endregion

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._pointer
