import json
import zipfile
from io import BytesIO
from unittest import mock
from unittest.mock import create_autospec

import httpx
import pytest

from quixstreams.platforms.quix.api import (
    QuixPortalApiService,
)
from quixstreams.platforms.quix.exceptions import (
    QuixApiRequestFailure,
    UndefinedQuixWorkspaceId,
)


class TestQuixPortalApiService:
    def test_no_workspace_id_provided(self):
        api = QuixPortalApiService(portal_api="http://portal.com", auth_token="token")
        with pytest.raises(UndefinedQuixWorkspaceId):
            api.get_topics()

    def test_get_workspace_certificate(self):
        zip_in_mem = BytesIO()
        with zipfile.ZipFile(zip_in_mem, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            zip_file.writestr("ca.cert", BytesIO(b"my cool cert stuff").getvalue())

        ws = "12345"
        with mock.patch.object(
            QuixPortalApiService, "client", create_autospec(httpx.Client)
        ) as client_mock:
            client_mock.get(
                f"/workspaces/{ws}/certificates"
            ).content = zip_in_mem.getvalue()
            api = QuixPortalApiService(
                portal_api="http://portal.com", auth_token="token"
            )
            result = api.get_workspace_certificate(ws)
        assert result == b"my cool cert stuff"

    def test_response_handler_valid_request(self, mock_response_factory):
        api = QuixPortalApiService(portal_api="http://fake.fake", auth_token="token")
        api._response_handler(mock_response_factory())

    def test_response_handler_bad_request(self, mock_response_factory):
        valid_json = b'"bad workspace"'
        code = 404
        url = "bad_url"
        api = QuixPortalApiService(portal_api="http://fake.fake", auth_token="token")

        with pytest.raises(QuixApiRequestFailure) as e:
            api._response_handler(
                mock_response_factory(
                    url=url,
                    status_code=code,
                    content=valid_json,
                    request_exception=httpx.HTTPStatusError,
                )
            )

        error_str = str(e.value)
        for s in [json.loads(valid_json), str(code), url]:
            assert s in error_str

    def test_response_handler_bad_request_invalid_json(self, mock_response_factory):
        invalid_json = b"bad workspace"
        code = 404
        url = "bad_url"

        with pytest.raises(json.JSONDecodeError):
            json.loads(invalid_json)

        api = QuixPortalApiService(portal_api="http://fake.fake", auth_token="token")

        with pytest.raises(QuixApiRequestFailure) as e:
            api._response_handler(
                mock_response_factory(
                    url=url,
                    status_code=code,
                    content=invalid_json,
                    request_exception=httpx.HTTPStatusError,
                )
            )

        error_str = str(e.value)
        for s in [invalid_json.decode(), str(code), url]:
            assert s in error_str
