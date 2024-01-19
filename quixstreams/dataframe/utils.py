from datetime import timedelta
from typing import Union


def ensure_milliseconds(delta: Union[int, timedelta]) -> int:
    """
    Convert timedelta to milliseconds.
    If the `delta` is not
    This function will also round the value to the closest milliseconds in case of
    higher precision.

    :param delta: `timedelta` object
    :return: timedelta value in milliseconds as `int`
    """
    if isinstance(delta, int):
        return delta
    elif isinstance(delta, timedelta):
        delta_ms = int(round(delta.total_seconds(), 3) * 1000)
        return delta_ms
    else:
        raise TypeError(
            f'Timedelta must be either "int" representing milliseconds '
            f'or "datetime.timedelta", got "{type(delta)}"'
        )
