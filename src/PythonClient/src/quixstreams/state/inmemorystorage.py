from .localfilestorage import LocalFileStorage
from .statevalue import StateValue
from threading import Lock


class InMemoryStorage:
    """
    In memory storage with an optional backing store
    """

    def __init__(self, backing_storage: LocalFileStorage = None):
        self._in_memory_state = {}
        self._mutex = Lock()

        self._persisted_state = backing_storage

        if self._persisted_state is not None:
            self._mutex.acquire()
            all_keys = self._persisted_state.get_all_keys()
            for key in all_keys:
                self._in_memory_state[key] = self._persisted_state.get(key)
            self._mutex.release()

    def flush(self, *args, **kwargs):
        if self._persisted_state is None:
            return  # nothing to do

        self._mutex.acquire()

        # TODO improve on this to avoid unnecessary manipulations
        self._persisted_state.clear()

        for key, value in self._in_memory_state.items():
            self._persisted_state.set(key, value)
        self._mutex.release()

    def set(self, key, item):
        self.__setitem__(key, item)

    def __setitem__(self, key, item):
        self._mutex.acquire()
        if isinstance(item, StateValue):
            item = item.value
        self._in_memory_state[key.lower()] = item
        self._mutex.release()

    def get(self, key):
        return self.__getitem__(key)

    def __getitem__(self, key):
        return self._in_memory_state[key.lower()]

    def __len__(self):
        return len(self._in_memory_state)

    def remove(self, key):
        self.__delitem__(key)

    def __delitem__(self, key):
        del self._in_memory_state[key.lower()]

    def clear(self):
        self._mutex.acquire()
        self._in_memory_state.clear()
        self._mutex.release()

    def has_key(self, k):
        return k.lower() in self._in_memory_state

    def update(self, *args, **kwargs):
        self._mutex.acquire()
        self._in_memory_state.update(*args, **kwargs)
        self._mutex.release()

    def get_all_keys(self):
        return self.keys()

    def keys(self):
        return self._in_memory_state.keys()

    def values(self):
        return self._in_memory_state.values()

    def items(self):
        return self._in_memory_state.items()

    def pop(self, *args):
        self._mutex.acquire()
        result = self._in_memory_state.pop(*args)
        self._mutex.release()
        return result

    def __cmp__(self, dict_):
        return self.__cmp__(self.__dict__, dict_)

    def contains_key(self, key):
        return self.__contains__(key)

    def __contains__(self, key):
        return key.lower() in self._in_memory_state

    def __iter__(self):
        return iter(self._in_memory_state)
