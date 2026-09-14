import json
from json import JSONDecodeError, loads
from typing import Optional, Union
from unittest import mock

import pytest
from typing_extensions import Type


class MockResponse:
    def __init__(
        self,
        url: str = "www.bad-url.com",
        status_code: int = 200,
        content: Union[str, bytes] = b"",
        request_exception: Optional[Type[Exception]] = None,
    ):
        self.url = url
        self.status_code = status_code
        self.content = content
        self.request_exception = request_exception

    def read(self) -> bytes:
        if isinstance(self.content, str):
            return self.content.encode()
        else:
            return self.content

    def json(self):
        try:
            return loads(self.content)
        except JSONDecodeError as e:
            raise json.JSONDecodeError(e.msg, e.doc, e.pos)

    @property
    def text(self) -> str:
        if isinstance(self.content, str):
            return self.content
        else:
            return self.content.decode()

    def raise_for_status(self):
        if self.request_exception:
            raise self.request_exception(
                f"{self.status_code}: {self.url}",
                request=mock.Mock(),
                response=self,
            )


@pytest.fixture()
def mock_response_factory():
    return MockResponse
