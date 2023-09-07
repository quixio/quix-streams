from unittest.mock import create_autospec

import pytest

from src.quixstreams.dataframes.platforms.quix.api import QuixPortalApiService
from src.quixstreams.dataframes.platforms.quix.config import QuixKafkaConfigsBuilder


class MockResponse:
    def __init__(self, data, status_code):
        self.data = data
        self.status_code = status_code

    def json(self):
        return self.data

    @property
    def content(self):
        return self.data


@pytest.fixture()
def mock_response_factory():
    def mock_response(json_data, status_code=200):
        return MockResponse(json_data, status_code)

    return mock_response


@pytest.fixture()
def mock_quix_portal_api_factory(mock_response_factory):
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
