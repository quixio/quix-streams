from abc import ABC, abstractmethod
from typing import List, Any, Optional

from .statevalue import StateValue
from .serializers.bytevalueserializer import ByteValueSerializer


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
    def get_or_create_sub_storage(self, sub_storage_name: str, db_name: Optional[str] = None) -> 'IStateStorage':
        """Gets an existing sub-storage with the specified name or creates a new one if it does not already exist.

        Args:
            sub_storage_name: The name of the sub-storage to retrieve or create.
            db_name: The name of the database under which the storage will be created.
                     If this parameter is not specified, the storage will be created under the parent's database.

        Returns:
            IStateStorage: The state storage associated with the given sub-storage name.

        Raises:
            ValueError: Thrown when sub_storage_name is null or empty.
        """
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

    @property
    @abstractmethod
    def can_perform_transactions(self) -> bool:
        """Return True if transactions are supported, False otherwise."""
        ...

    def start_transaction(self) -> None:
        """Starts a transaction."""
        ...

    def commit_transaction(self) -> None:
        """Commits a transaction.

        Raises:
            Exception: Thrown if the transaction fails.
        """
        ...

    def set(self, key: str, value: Any) -> None:
        """Save value into the key"""
        state_value = value if isinstance(value, StateValue) else StateValue(value)
        self.save_raw(key, ByteValueSerializer.serialize(state_value))

    def get(self, key: str) -> Any:
        """Load value from the key"""
        return ByteValueSerializer.deserialize(self.load_raw(key)).value
