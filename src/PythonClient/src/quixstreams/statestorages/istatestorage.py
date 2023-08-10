from abc import ABC, abstractmethod
from typing import List, Any

from ..states.serializers.bytevalueserializer import ByteValueSerializer
from ..statestorages.statevalue import StateValue


class IStateStorage(ABC):
    """The minimum definition for a state storage"""

    @abstractmethod
    def save_raw(self, key: str, data: bytes) -> None:
        """Save raw data into the key"""
        ...

    @abstractmethod
    def load_raw(self, key: str) -> bytes:
        """Load raw data from the key"""
        ...

    @abstractmethod
    def remove(self, key: str) -> None:
        """Remove key from the storage"""
        ...

    @abstractmethod
    def contains_key(self, key: str) -> bool:
        """Check if storage contains key"""
        ...

    @abstractmethod
    def get_all_keys(self) -> List[str]:
        """Get list of all keys in the storage"""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear the storage / remove all keys from the storage"""
        ...

    @abstractmethod
    def count(self) -> int:
        """Returns the number of keys in storage"""
        ...

    @property
    @abstractmethod
    def is_case_sensitive(self) -> bool:
        """Returns whether the storage is case-sensitive"""
        ...

    @abstractmethod
    def get_or_create_sub_storage(self, sub_storage_name: str) -> 'IStateStorage':
        """Creates or retrieves the existing storage under this in hierarchy."""
        ...

    @abstractmethod
    def delete_sub_storage(self, sub_storage_name: str) -> bool:
        """Deletes a storage under this in hierarchy."""
        ...

    @abstractmethod
    def delete_sub_storages(self) -> int:
        """Deletes the storages under this in hierarchy."""
        ...

    @abstractmethod
    def get_sub_storages(self) -> List[str]:
        """Gets the storages under this in hierarchy."""
        ...

    def set(self, key: str, value: Any) -> None:
        """Save value into the key"""
        state_value = value if isinstance(value, StateValue) else StateValue(value)
        self.save_raw(key, ByteValueSerializer.Serialize(state_value))

    def get(self, key: str) -> Any:
        """Load value from the key"""
        return ByteValueSerializer.Deserialize(self.load_raw(key)).value
