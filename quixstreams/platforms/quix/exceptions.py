from quixstreams.exceptions.base import QuixException


__all__ = (
    "MissingConnectionRequirements",
    "UndefinedQuixWorkspaceId",
    "QuixApiRequestFailure",
)


class MissingConnectionRequirements(QuixException): ...


class UndefinedQuixWorkspaceId(QuixException): ...


class QuixApiRequestFailure(QuixException): ...


class NoWorkspaceFound(QuixException): ...


class MultipleWorkspaces(QuixException): ...


class MissingQuixTopics(QuixException): ...


class QuixCreateTopicTimeout(QuixException): ...


class QuixCreateTopicFailure(QuixException): ...
