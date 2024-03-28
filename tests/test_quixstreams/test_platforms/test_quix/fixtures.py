from json import loads, JSONDecodeError
from unittest.mock import create_autospec

import pytest
import requests

from quixstreams.platforms.quix.api import QuixPortalApiService
from quixstreams.platforms.quix.config import QuixKafkaConfigsBuilder


class MockResponse:
    def __init__(self, status_code=200, response_body=b"", request_exception=None):
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
                f"status code {self.status_code}", response=self
            )


@pytest.fixture()
def mock_quix_portal_api_factory():
    def mock_quix_portal_api(api_responses: dict = None):
        if not api_responses:
            api_responses = {}
        api_obj = create_autospec(QuixPortalApiService)
        api_obj.default_workspace_id = None
        for call, data in api_responses.items():
            api_obj.__getattribute__(call).return_value = data
        return api_obj

    return mock_quix_portal_api


@pytest.fixture()
def quix_kafka_config_factory(mock_quix_portal_api_factory):
    def mock_quix_kafka_configs(
        workspace_id: str = None, api_responses: dict = None, api_class=None, **kwargs
    ):
        if not api_class:
            api_class = mock_quix_portal_api_factory(api_responses)
        return QuixKafkaConfigsBuilder(
            quix_portal_api_service=api_class, workspace_id=workspace_id, **kwargs
        )

    return mock_quix_kafka_configs


@pytest.fixture()
def mock_response_factory():
    def factory(status_code=200, response_body=b"", request_exception=None):
        return MockResponse(
            status_code=status_code,
            response_body=response_body,
            request_exception=request_exception,
        )

    return factory
