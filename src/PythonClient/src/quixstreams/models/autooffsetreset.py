from enum import Enum


class AutoOffsetReset(Enum):
    """
    Enum representing the policy on how a consumer should behave when consuming from a topic partition when there is no initial offset.
    """

    Latest = 0
    """
    Latest: Starts from the newest message if there is no stored offset.
    """

    Earliest = 1
    """
    Earliest: Starts from the oldest message if there is no stored offset.
    """

    Error = 2
    """
    Error: Throws an exception if there is no stored offset.
    """

