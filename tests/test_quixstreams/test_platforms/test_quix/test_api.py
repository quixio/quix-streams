import json
import zipfile
from io import BytesIO
from unittest import mock
from unittest.mock import create_autospec, patch

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

    @patch("time.sleep")  # Mock sleep to speed up the test
    @patch("quixstreams.platforms.quix.api.httpx.Client")  # Mock the httpx.Client class
    def test_retry_on_connection_error_success_after_retries(
        self, mock_client_class, mock_sleep
    ):
        """Test that the retry decorator successfully retries on httpx.ConnectError and eventually succeeds."""
        ws = "test-workspace"
        expected_result = {"broker": "config"}

        # Create a mock client instance
        mock_client_instance = mock.Mock()
        mock_client_class.return_value = mock_client_instance

        # Mock response
        mock_response = mock.Mock()
        mock_response.json.return_value = expected_result

        # Mock the client to fail twice with different connection errors, then succeed
        mock_client_instance.get.side_effect = [
            httpx.ConnectError(
                "DNS resolution failed"
            ),  # First attempt fails with DNS error
            httpx.TimeoutException(
                "Connection timeout"
            ),  # Second attempt fails with timeout
            mock_response,  # Third attempt succeeds
        ]

        api = QuixPortalApiService(
            portal_api="http://portal.com", auth_token="token", default_workspace_id=ws
        )

        # Call the method that should retry
        result = api.get_librdkafka_connection_config(ws)

        # Verify the result
        assert result == expected_result

        # Verify it was called 3 times (2 failures + 1 success)
        assert mock_client_instance.get.call_count == 3

        # Verify sleep was called twice (for the 2 retries)
        assert mock_sleep.call_count == 2

        # Verify exponential backoff delays: 2s, 4s
        expected_delays = [2.0, 4.0]  # base_delay * (2 ** attempt) for attempts 1,2
        actual_delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert actual_delays == expected_delays

    @patch("time.sleep")  # Mock sleep to speed up the test
    @patch("quixstreams.platforms.quix.api.httpx.Client")  # Mock the httpx.Client class
    def test_retry_on_connection_error_max_retries_exceeded(
        self, mock_client_class, mock_sleep
    ):
        """Test that the retry decorator eventually gives up after max retries and raises the original error."""
        ws = "test-workspace"

        # Create a mock client instance
        mock_client_instance = mock.Mock()
        mock_client_class.return_value = mock_client_instance

        # Mock the client to fail with alternating connection errors
        mock_client_instance.get.side_effect = [
            httpx.ConnectError("DNS resolution failed"),
            httpx.TimeoutException("Connection timeout"),
            httpx.ConnectError("DNS resolution failed"),
            httpx.TimeoutException("Connection timeout"),
            httpx.ConnectError("DNS resolution failed"),
            httpx.TimeoutException("Final timeout"),  # Final attempt
        ]

        api = QuixPortalApiService(
            portal_api="http://portal.com", auth_token="token", default_workspace_id=ws
        )

        # Call the method that should retry and eventually fail
        with pytest.raises(httpx.TimeoutException, match="Final timeout"):
            api.get_librdkafka_connection_config(ws)

        # Verify it was called 6 times (5 retries + 1 final attempt)
        assert mock_client_instance.get.call_count == 6

        # Verify sleep was called 5 times (for the 5 retries)
        assert mock_sleep.call_count == 5

        # Verify exponential backoff delays: 2s, 4s, 8s, 16s, 32s
        expected_delays = [2.0, 4.0, 8.0, 16.0, 32.0]  # base_delay * (2 ** attempt)
        actual_delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert actual_delays == expected_delays

    @patch("quixstreams.platforms.quix.api.httpx.Client")  # Mock the httpx.Client class
    def test_retry_on_connection_error_no_retry_for_other_errors(
        self, mock_client_class
    ):
        """Test that the retry decorator does not retry for non-connection errors like HTTPStatusError."""
        ws = "test-workspace"

        # Create a mock client instance
        mock_client_instance = mock.Mock()
        mock_client_class.return_value = mock_client_instance

        # Mock the client to fail with HTTPStatusError (should not retry)
        mock_response = mock.Mock()
        mock_response.status_code = 404
        http_error = httpx.HTTPStatusError(
            "Not found", request=mock.Mock(), response=mock_response
        )
        mock_client_instance.get.side_effect = http_error

        api = QuixPortalApiService(
            portal_api="http://portal.com", auth_token="token", default_workspace_id=ws
        )

        # The HTTPStatusError should be caught by the response handler and converted to QuixApiRequestFailure
        # But the retry decorator should not interfere with this
        with pytest.raises(httpx.HTTPStatusError):
            api.get_librdkafka_connection_config(ws)

        # Verify it was only called once (no retries for HTTPStatusError)
        assert mock_client_instance.get.call_count == 1

    @patch("time.sleep")  # Mock sleep to speed up the test
    @patch("quixstreams.platforms.quix.api.httpx.Client")  # Mock the httpx.Client class
    def test_retry_on_timeout_exception_only(self, mock_client_class, mock_sleep):
        """Test that the retry decorator works specifically with TimeoutException."""
        ws = "test-workspace"
        expected_result = {"broker": "config"}

        # Create a mock client instance
        mock_client_instance = mock.Mock()
        mock_client_class.return_value = mock_client_instance

        # Mock response
        mock_response = mock.Mock()
        mock_response.json.return_value = expected_result

        # Mock the client to fail with TimeoutException, then succeed
        mock_client_instance.get.side_effect = [
            httpx.TimeoutException("Request timeout"),  # First attempt fails
            mock_response,  # Second attempt succeeds
        ]

        api = QuixPortalApiService(
            portal_api="http://portal.com", auth_token="token", default_workspace_id=ws
        )

        # Call the method that should retry
        result = api.get_librdkafka_connection_config(ws)

        # Verify the result
        assert result == expected_result

        # Verify it was called 2 times (1 failure + 1 success)
        assert mock_client_instance.get.call_count == 2

        # Verify sleep was called once (for the 1 retry)
        assert mock_sleep.call_count == 1

        # Verify exponential backoff delay: 2s
        expected_delays = [2.0]  # base_delay * (2 ** attempt) for attempt 1
        actual_delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert actual_delays == expected_delays
