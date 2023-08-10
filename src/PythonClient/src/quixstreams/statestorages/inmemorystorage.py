import threading
from typing import Dict, List

from ..statestorages.istatestorage import IStateStorage


class InMemoryStorage(IStateStorage):
    """Basic non-thread safe in-memory storage implementing IStateStorage protocol."""
    # IS IT REALLY NON-THREAD SAFE??

    def __init__(self):
        self._in_memory_state: Dict[str, bytes] = {}
        self._sub_states: Dict[str, 'InMemoryStorage'] = {}
        self._sub_state_lock = threading.Lock()

    def save_raw(self, key: str, data: bytes) -> None:
        """Save raw data into the key"""
        self._in_memory_state[key] = data

    def load_raw(self, key: str) -> bytes:
        """Load raw data from the key"""
        return self._in_memory_state[key]

    def remove(self, key: str) -> None:
        """Remove key from the storage"""
        self._in_memory_state.pop(key, None)

    def contains_key(self, key: str) -> bool:
        """Check if storage contains key"""
        return key in self._in_memory_state

    def get_all_keys(self) -> List[str]:
        """Get list of all keys in the storage"""
        return list(self._in_memory_state.keys())

    def clear(self) -> None:
        """Clear the storage / remove all keys from the storage"""
        self._in_memory_state.clear()

    def count(self) -> int:
        """Returns the number of keys in storage"""
        return len(self._in_memory_state)

    @property
    def is_case_sensitive(self) -> bool:
        """Returns whether the storage is case-sensitive"""
        return True

    def get_or_create_sub_storage(self, sub_storage_name: str) -> 'InMemoryStorage':
        """Creates or retrieves the existing storage under this in hierarchy."""
        with self._sub_state_lock:
            if sub_storage_name in self._sub_states:
                return self._sub_states[sub_storage_name]

            storage = InMemoryStorage()
            self._sub_states[sub_storage_name] = storage
            return storage

    def delete_sub_storage(self, sub_storage_name: str) -> bool:
        """Deletes a storage under this in hierarchy."""
        return self._sub_states.pop(sub_storage_name, None) is not None

    def delete_sub_storages(self) -> int:
        """Deletes the storages under this in hierarchy."""
        count = len(self._sub_states)
        self._sub_states.clear()
        return count

    def get_sub_storages(self) -> List[str]:
        """Gets the storages under this in hierarchy."""
        return list(self._sub_states.keys())
