from quixstreams.exceptions import QuixException


class MissingTopicAdmin(QuixException):
    ...


class TopicValidationError(QuixException):
    ...


class MissingTopicForChangelog(QuixException):
    ...


class CreateTopicTimeout(Exception):
    ...


class CreateTopicFailure(Exception):
    ...
