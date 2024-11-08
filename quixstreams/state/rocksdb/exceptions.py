from quixstreams.state.exceptions import StateError

__all__ = ("ColumnFamilyAlreadyExists",)


class ColumnFamilyAlreadyExists(StateError): ...
