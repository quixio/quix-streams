from quixstreams.statestorages.istatestorage import IStateStorage


class ScalarState:
    STORAGE_KEY = "SCALAR"

    def __init__(self, storage: IStateStorage, logger=None):
        self.storage = storage
        self.logger = logger
        self.clear_before_flush = False
        self.in_memory_value = self.storage.get(self.STORAGE_KEY) if self.storage.contains_key(self.STORAGE_KEY) else None
        self.last_flush_hash = self.compute_hash(self.in_memory_value)
        self.flushing_callbacks = []
        self.flushed_callbacks = []

    def compute_hash(self, value):
        return hash(value) if value else None

    def clear(self):
        self.in_memory_value = None
        self.clear_before_flush = True

    @property
    def value(self):
        return self.in_memory_value

    @value.setter
    def value(self, new_value):
        self.in_memory_value = new_value

    def add_flushing_callback(self, callback):
        self.flushing_callbacks.append(callback)

    def add_flushed_callback(self, callback):
        self.flushed_callbacks.append(callback)

    def flush(self):
        if self.logger:
            self.logger.debug("Flashing state.")

        for callback in self.flushing_callbacks:
            callback(self)

        if self.clear_before_flush:
            self.storage.clear()
            self.clear_before_flush = False

        if self.in_memory_value is None:
            self.last_flush_hash = None
            if self.STORAGE_KEY in self.storage:
                del self.storage[self.STORAGE_KEY]
        else:
            new_hash = self.compute_hash(self.in_memory_value)
            if self.last_flush_hash != new_hash:
                self.last_flush_hash = new_hash
                self.storage.set(self.STORAGE_KEY, self.in_memory_value)

        for callback in self.flushed_callbacks:
            callback(self)

        if self.logger:
            self.logger.debug("Flashed state.")

    def reset(self):
        current_hash = self.compute_hash(self.in_memory_value)
        if current_hash == self.last_flush_hash:
            if self.logger:
                self.logger.debug("Resetting state not needed, hash is the same.")
        else:
            if self.logger:
                self.logger.debug("Resetting state.")
            self.in_memory_value = self.storage.get(self.STORAGE_KEY)
            if self.logger:
                self.logger.debug("Reset state completed.")
