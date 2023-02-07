from enum import Enum


class AutoOffsetReset(Enum):
    Latest = 0
    """
    Latest, starts from newest message if there is no stored offset
    """

    Earliest = 1
    """
    Earliest, starts from the oldest message if there is no stored offset
    """

    Error = 2
    """
    Error, throws exception if there is no stored offset
    """