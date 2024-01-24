from typing import List


def dict_values(d: object) -> List:
    """
    Recursively unpacks a set of nested dicts to get a flattened list of leaves,
    where "leaves" are the first non-dict item.

    i.e {"a": {"b": {"c": 1}, "d": 2}, "e": 3} becomes [1, 2, 3]

    :param d: initially, a dict (with potentially nested dicts)

    :return: a list with all the leaves of the various contained dicts
    """
    if d:
        if isinstance(d, dict):
            return [i for v in d.values() for i in dict_values(v)]
        elif isinstance(d, list):
            return d
        return [d]
    return []
