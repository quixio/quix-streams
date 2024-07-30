import timeit
import pickle
from copy import deepcopy
from typing import Callable, Any, Optional

from quixstreams.utils.json import (
    dumps as json_dumps,
    loads as json_loads,
)


def get_copier(
    serializer: Optional[Callable[[Any], Any]] = None,
    deserializer: Callable[[Any], Any] = None,
):
    def copier(data):
        serialized = serializer(data) if serializer else data
        return lambda: deserializer(serialized)

    return copier


class CopyFactory:
    """
    Finds and return which cloning method is best based on a data sample, and then
    returns that cloner every time.
    """

    _methods = {
        "json": get_copier(serializer=json_dumps, deserializer=json_loads),
        "pickle": get_copier(serializer=pickle.dumps, deserializer=pickle.loads),
        "deepcopy": get_copier(deserializer=deepcopy),
    }

    def __init__(self, copier=None):
        self._copier = self._methods[copier] if isinstance(copier, str) else copier
        self._copiers = None

    def copiers(self, data):
        for copier in self._copiers:
            try:
                get_copy = copier(data)
                get_copy()
                return lambda: get_copy()
            except:
                pass
        return self._methods["deepcopy"]

    def copier(self, data):
        if not self._copier:
            self._set_copier(data)
        return self._copier(data)

    def _tester(self, cloner, data):
        def tester():
            get_copy = cloner(data)
            for i in range(2):
                get_copy()

        try:
            return timeit.timeit(tester)
        except:
            return None

    def _set_copier(self, data):
        methods = {}
        for m, f in self._methods.items():
            if m != "deepcopy" and (timed := self._tester(f, data)) is not None:
                methods[m] = timed
        if methods:
            self._copier = self._methods[min(methods, key=methods.get)]
        else:
            self._copier = self._methods["deepcopy"]
        self._copiers = [self._methods[m] for m in sorted(methods, key=methods.get)]
