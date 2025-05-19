from typing import Mapping, Optional


def keep_left_merger(left: Optional[Mapping], right: Optional[Mapping]) -> dict:
    """
    Merge two dictionaries, preferring values from the left dictionary
    """
    left = left or {}
    right = right or {}
    return {**right, **left}


def keep_right_merger(left: Optional[Mapping], right: Optional[Mapping]) -> dict:
    """
    Merge two dictionaries, preferring values from the right dictionary
    """
    left = left or {}
    right = right or {}
    # TODO: Add try-except everywhere and tell to pass a callback if one of the objects is not a mapping
    return {**left, **right}


def raise_merger(left: Optional[Mapping], right: Optional[Mapping]) -> dict:
    """
    Merge two dictionaries and raise an error if overlapping keys detected
    """
    left = left or {}
    right = right or {}
    if overlapping_columns := left.keys() & right.keys():
        overlapping_columns_str = ", ".join(sorted(overlapping_columns))
        raise ValueError(
            f"Overlapping columns: {overlapping_columns_str}."
            'You need to provide either an "on_overlap" value of '
            "'keep-left' or 'keep-right' or a custom merger function."
        )
    return {**left, **right}
