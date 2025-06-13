from quixstreams.exceptions import QuixException

__all__ = ("InvalidTopology", "InvalidOperation")


class InvalidTopology(QuixException): ...


class InvalidOperation(QuixException): ...
