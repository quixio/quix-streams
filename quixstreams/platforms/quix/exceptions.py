from typing import Optional

from quixstreams.exceptions.base import QuixException

__all__ = (
    "MissingConnectionRequirements",
    "UndefinedQuixWorkspaceId",
    "QuixApiRequestFailure",
)


class MissingConnectionRequirements(QuixException): ...


class UndefinedQuixWorkspaceId(QuixException): ...


class QuixApiRequestFailure(QuixException):
    def __init__(
        self,
        status_code: int,
        url: str,
        error_text: Optional[dict[str, str]] = None,
    ):
        self.status_code = status_code
        self.url = url
        self.error_text = error_text

    def __str__(self) -> str:
        str_out = f'Error {self.status_code} for url "{self.url}"'
        if self.error_text is not None:
            str_out = f"{str_out}: {self.error_text}"
        return str_out


class NoWorkspaceFound(QuixException): ...


class MultipleWorkspaces(QuixException): ...


class MissingQuixTopics(QuixException): ...


class QuixCreateTopicTimeout(QuixException): ...


class QuixCreateTopicFailure(QuixException): ...
