from json import loads, JSONDecodeError
from typing import Union, Optional
from unittest.mock import create_autospec

import pytest
import requests
from typing_extensions import Type

from quixstreams.platforms.quix.api import QuixPortalApiService
from quixstreams.platforms.quix.config import QuixKafkaConfigsBuilder


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
def mock_quix_portal_api_factory():
    def mock_quix_portal_api():
        api_obj = create_autospec(QuixPortalApiService)
        api_obj.default_workspace_id = None
        return api_obj

    return mock_quix_portal_api


@pytest.fixture()
def quix_kafka_config_factory(mock_quix_portal_api_factory):
    def mock_quix_kafka_configs(workspace_id: str = None, api_class=None, **kwargs):
        if not api_class:
            api_class = mock_quix_portal_api_factory()
        return QuixKafkaConfigsBuilder(
            quix_portal_api_service=api_class, workspace_id=workspace_id, **kwargs
        )

    return mock_quix_kafka_configs


@pytest.fixture()
def mock_response_factory():
    return MockResponse
