from quixstreams.exceptions import QuixException


class TopicValidationError(QuixException):
    ...


class CreateTopicTimeout(QuixException):
    ...


class CreateTopicFailure(QuixException):
    ...
