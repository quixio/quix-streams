from json import loads, JSONDecodeError
from typing import Union, Optional

import pytest
import requests
from typing_extensions import Type


class MockResponse:
    def __init__(
        self,
        url: str = "www.bad-url.com",
        status_code: int = 200,
        response_body: Union[str, bytes] = b"",
        request_exception: Optional[Type[Exception]] = None,
    ):
        self.url = url
        self.status_code = status_code
        self.text = response_body
        self.request_exception = request_exception

    def json(self):
        try:
            return loads(self.text)
        except JSONDecodeError as e:
            raise requests.exceptions.JSONDecodeError(e.msg, e.doc, e.pos)

    def raise_for_status(self):
        if self.request_exception:
            raise self.request_exception(
                f"{self.status_code}: {self.url}", response=self
            )


@pytest.fixture()
def mock_response_factory():
    return MockResponse
