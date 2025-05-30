from collections import OrderedDict
from typing import Any, Callable, Optional, Tuple

from quixstreams.utils.pickle import pickle_copier

from ..utils import CacheInfo
from .models import ConfigurationVersion, Field


class VersionDataLRU:
    """
    Least Recently Used (LRU) cache for configuration version data.

    This class caches the results of a function that generates data for a given
    ConfigurationVersion and a set of Fields, up to a maximum size. When the cache
    exceeds the maximum size, the least recently used item is evicted.

    :param func: The function to cache. It should take a ConfigurationVersion (or None)
        and a dictionary of Field objects, and return the computed data.
    :param maxsize: The maximum number of items to cache. Defaults to 128.

    """

    def __init__(
        self,
        func: Callable[[Optional[ConfigurationVersion], dict[str, Field]], Any],
        maxsize: int = 128,
    ):
        self.cache: OrderedDict[
            Tuple[Optional[ConfigurationVersion], Tuple[int, ...]], Any
        ] = OrderedDict()
        self.func = func
        self.maxsize = maxsize
        self.hits = 0
        self.misses = 0

    def __call__(
        self, version: Optional[ConfigurationVersion], fields: dict[str, Field]
    ) -> Any:
        """
        Get cached data for the given version and fields, or compute and cache it.

        :param version: The configuration version.
        :param fields: The fields to extract.

        :returns: The cached or newly computed data.
        """
        cache = self.cache
        key = (version, tuple(hash(field) for field in fields.values()))
        if key in cache:
            self.hits += 1
            cache.move_to_end(key)
            return cache[key]()

        self.misses += 1
        result = self.func(version, fields)
        cache[key] = pickle_copier(result)
        if len(cache) > self.maxsize:
            cache.popitem(last=False)
        return result

    def remove(
        self, version: Optional[ConfigurationVersion], fields: dict[str, Field]
    ) -> None:
        """
        Remove the cached data for the given version and fields, if present.

        :param version: The configuration version to remove from the cache.
        :param fields: The fields to remove from the cache.
        """
        key = (version, tuple(hash(field) for field in fields.values()))
        if key in self.cache:
            del self.cache[key]

    def info(self) -> CacheInfo:
        """
        Get cache statistics.

        :returns: A dictionary containing cache statistics.
        """
        return CacheInfo(
            hits=self.hits,
            misses=self.misses,
            size=len(self.cache),
            maxsize=self.maxsize,
        )
