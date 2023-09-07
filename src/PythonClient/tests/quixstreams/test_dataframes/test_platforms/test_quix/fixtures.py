from unittest.mock import create_autospec

import pytest

from src.quixstreams.dataframes.platforms.quix.api import QuixPortalApiService
from src.quixstreams.dataframes.platforms.quix.config import QuixKafkaConfigsBuilder


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
