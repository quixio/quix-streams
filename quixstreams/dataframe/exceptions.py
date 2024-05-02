from quixstreams.exceptions.base import QuixException


__all__ = ("InvalidOperation",)


class InvalidOperation(QuixException): ...


class ColumnDoesNotExist(QuixException): ...


class InvalidColumnReference(QuixException): ...


class TopicNameCollision(QuixException): ...
