import threading
from typing import Dict, Optional, List, Tuple, Any
from rocksdict import Rdict, WriteBatch
import os


class RocksDbStorage:
    rocks_db_instances: Dict[str, 'RocksDbInstance'] = {}
    RESERVED_CF_NAME = "default"
    SUB_STORAGE_SEPARATOR = "/"

    def __init__(self, db_directory: str, storage_name: str = "default-storage"):
        if not db_directory or not storage_name:
            raise ValueError("db_directory and storage_name cannot be null or empty.")

        if storage_name == self.RESERVED_CF_NAME:
            raise ValueError(f"{storage_name} cannot be '{self.RESERVED_CF_NAME}' as it is reserved by rocksdb.")

        self.db = self.get_or_create_rocks_db_instance(db_directory)

        self.column_family = self.db.get_column_family(storage_name)
        self.db_directory = db_directory
        self.storage_name = storage_name

        self.write_batch = WriteBatch()
        self.use_write_batch = False
        self.sub_storages: Dict[str, RocksDbStorage] = {}

    def save_raw(self, key: str, data: bytes) -> None:
        if self.use_write_batch:
            self.write_batch.put(key, data, self.column_family)
        else:
            self.column_family[key] = data

    def load_raw(self, key: str) -> Optional[bytes]:
        return self.column_family.get(key, None)

    def remove(self, key: str) -> None:
        self.column_family.delete(key)

    def contains_key(self, key: str) -> bool:
        return self.column_family.get(key, None) is not None

    def get_all_keys(self) -> List[str]:
        return list(self.column_family.keys())

    def clear(self) -> None:
        self.db.drop_column_family(self.storage_name)
        self.column_family = self.db.create_column_family(self.storage_name)

    async def count(self) -> int:
        return len(self.get_all_keys())

    @property
    def is_case_sensitive(self) -> bool:
        """Returns whether the storage is case-sensitive"""
        return True

    @property
    def can_perform_transactions(self) -> bool:
        """Return True if transactions are supported, False otherwise."""
        return True

    @staticmethod
    def get_or_create_rocks_db_instance(db_directory: str) -> Rdict:
        if db_directory in RocksDbStorage.rocks_db_instances:
            db, ref_count = RocksDbStorage.rocks_db_instances[db_directory]
            RocksDbStorage.rocks_db_instances[db_directory] = (db, ref_count + 1)
            return db

        if not os.path.exists(db_directory):
            os.makedirs(db_directory)

        db = Rdict(db_directory)
        RocksDbStorage.rocks_db_instances[db_directory] = (db, 1)
        return db

    def start_transaction(self) -> None:
        self.use_write_batch = True

    def commit_transaction(self) -> None:
        for key, value in self.write_batch.items():
            self.db[key] = value
        self.write_batch.clear()
        self.use_write_batch = False

    def __del__(self) -> None:
        if self.db_directory in RocksDbStorage.rocks_db_instances:
            db, ref_count = RocksDbStorage.rocks_db_instances[self.db_directory]
            if ref_count <= 1:
                del RocksDbStorage.rocks_db_instances[self.db_directory]
                del db  # This should close and release the RocksDB instance
            else:
                RocksDbStorage.rocks_db_instances[self.db_directory] = (db, ref_count - 1)

    def get_sub_storage(self, sub_storage_name: str) -> "RocksDbStorage":
        if sub_storage_name in self.sub_storages:
            return self.sub_storages[sub_storage_name]

        sub_storage = RocksDbStorage(self.db_directory, f"{self.storage_name}-{sub_storage_name}")
        self.sub_storages[sub_storage_name] = sub_storage
        return sub_storage

    def remove_sub_storage(self, sub_storage_name: str) -> None:
        if sub_storage_name in self.sub_storages:
            del self.sub_storages[sub_storage_name]

    def dispose(self) -> None:
        # Explicitly release RocksDB resources.
        # The __del__ destructor will take care of decrementing the reference count.
        self.db.close()

    def remove_by_prefix(self, prefix: str) -> None:
        keys_to_remove = [key for key in self.db.keys() if key.startswith(prefix)]
        for key in keys_to_remove:
            del self.db[key]

    def get_by_prefix(self, prefix: str) -> Dict[str, bytes]:
        return {key: self.db[key] for key in self.db.keys() if key.startswith(prefix)}


class RocksDbInstance:
    def __init__(self, db, initial_ref_count):
        self._db = db
        self._ref_count = initial_ref_count
        self._ref_count_lock = threading.Lock()

    @property
    def db(self):
        return self._db

    def increment_ref_count(self):
        with self._ref_count_lock:
            self._ref_count += 1

    def decrement_ref_count(self):
        with self._ref_count_lock:
            self._ref_count -= 1
            if self._ref_count < 0:
                raise Exception("De-referenced the RocksDB instance more times than tracked")

    def is_used(self):
        return self._ref_count != 0