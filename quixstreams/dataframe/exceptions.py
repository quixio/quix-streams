from quixstreams.exceptions.base import QuixException


__all__ = ("InvalidOperation",)


class InvalidOperation(QuixException): ...


class MissingColumn(QuixException): ...
