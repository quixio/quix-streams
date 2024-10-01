from quixstreams.state.exceptions import StateError

__all__ = (
    "ColumnFamilyDoesNotExist",
    "ColumnFamilyAlreadyExists",
)


class ColumnFamilyDoesNotExist(StateError): ...


class ColumnFamilyAlreadyExists(StateError): ...
