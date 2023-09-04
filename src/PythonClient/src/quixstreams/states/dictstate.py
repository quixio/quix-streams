from collections.abc import MutableMapping
from enum import Enum

from ..statestorages import IStateStorage


class DictState(MutableMapping):

    def __init__(self, storage: IStateStorage, logger_factory=None):

        if storage is None:
            raise ValueError("storage must not be None")

        self.logger = logger_factory.getLogger("DictionaryState") if logger_factory else None
        self.storage = storage
        self.in_memory_state = {}
        self.last_flush_hash = {}
        self.changes = {}
        self.clear_before_flush = False
        keys = self.storage.get_all_keys()
        for key in keys:
            self.in_memory_state[key] = self.storage.get(key)
        self.flushing_callbacks = []
        self.flushed_callbacks = []

    @property
    def is_case_sensitive(self):
        return self.storage.is_case_sensitive

    def add_flushing_callback(self, callback):
        self.flushing_callbacks.append(callback)

    def add_flushed_callback(self, callback):
        self.flushed_callbacks.append(callback)

    def __getitem__(self, key):
        if not self.is_case_sensitive:
            key = key.lower()
        return self.in_memory_state[key]

    def __setitem__(self, key, value):
        if not self.is_case_sensitive:
            key = key.lower()
        self.changes[key] = ChangeType.REMOVED if value is None else ChangeType.ADDED_OR_UPDATED
        self.in_memory_state[key] = value

    def __delitem__(self, key):
        if not self.is_case_sensitive:
            key = key.lower()
        del self.in_memory_state[key]
        self.changes[key] = ChangeType.REMOVED

    def __iter__(self):
        return iter(self.in_memory_state)

    def __len__(self):
        return len(self.in_memory_state)

    def clear(self):
        self.in_memory_state.clear()
        self.changes.clear()
        self.clear_before_flush = True

    def flush(self):
        if self.logger:
            self.logger.info("Flushing state.")
        for callback in self.flushing_callbacks:
            callback(self)

        if self.clear_before_flush:
            self.storage.clear()
            self.clear_before_flush = False
        for key, change_type in self.changes.items():
            if change_type == ChangeType.REMOVED:
                self.last_flush_hash.pop(key, None)
                self.storage.remove(key)
            else:
                value = self.in_memory_state[key]
                if value is None or value.is_null():
                    self.last_flush_hash.pop(key, None)
                    self.storage.remove(key)
                else:
                    hash_value = hash(value)
                    if self.last_flush_hash.get(key) == hash_value:
                        continue
                    self.last_flush_hash[key] = hash_value
                    self.storage.set(key, value)

        if self.logger:
            self.logger.info("Flushed {0} state changes.".format(len(self.changes)))

        self.changes.clear()
        for callback in self.flushed_callbacks:
            callback(self)

    def reset(self):
        if len(self.changes) == 0 and self.logger:
            self.logger.info("Resetting state not needed, empty")
            return
        if self.logger:
            self.logger.info("Resetting state")

        for key in self.changes.keys():
            del self.in_memory_state[key]
            value = self.storage.get(key)
            if value is not None:
                self.in_memory_state[key] = value

        if self.logger:
            self.logger.info("Reset {0} state changes.".format(len(self.changes)))
        self.changes.clear()

class ChangeType(Enum):
    REMOVED = 1
    ADDED_OR_UPDATED = 2
