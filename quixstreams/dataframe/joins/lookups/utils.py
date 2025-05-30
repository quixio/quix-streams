from typing import TypedDict


class CacheInfo(TypedDict):
    """
    Typed dictionary containing cache statistics for the LRU cache.

    :param hits: The number of cache hits.
    :param misses: The number of cache misses.
    :param size: The current size of the cache.
    :param maxsize: The maximum size of the cache.
    """

    hits: int
    misses: int
    size: int
    maxsize: int
