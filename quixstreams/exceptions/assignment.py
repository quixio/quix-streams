from .base import QuixException

__all__ = ("PartitionAssignmentError", "KafkaPartitionError")


class PartitionAssignmentError(QuixException):
    """
    Error happened during partition rebalancing.
    Raised from `on_assign`, `on_revoke` and `on_lost` callbacks
    """


class KafkaPartitionError(QuixException): ...
