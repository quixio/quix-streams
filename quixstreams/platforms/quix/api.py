import json
import logging
import time
from functools import wraps
from io import BytesIO
from typing import List, Literal, Optional
from zipfile import ZipFile

import httpx

from .env import QUIX_ENVIRONMENT
from .exceptions import (
    MissingConnectionRequirements,
    QuixApiRequestFailure,
    UndefinedQuixWorkspaceId,
)

__all__ = ("QuixPortalApiService",)

logger = logging.getLogger(__name__)


def retry_on_connection_error(max_retries: int = 5, base_delay: float = 1.0):
    """
    Retry decorator for httpx connection errors with exponential backoff.

    :param max_retries: Maximum number of retry attempts (default: 5)
    :param base_delay: Base delay in seconds for exponential backoff (default: 1.0)
    """
    start = 1
    stop = max_retries + 1

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(start, stop):
                try:
                    return func(*args, **kwargs)
                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    delay = base_delay * (2**attempt)
                    logger.warning(
                        f"API request failed (attempt {attempt}/{max_retries}): {e}. "
                        f"Retrying in {delay:.1f} seconds..."
                    )
                    time.sleep(delay)

            # Final attempt without retrying
            return func(*args, **kwargs)

        return wrapper

    return decorator


class QuixPortalApiService:
    """
    A light wrapper around the Quix Portal Api. If used in the Quix Platform, it will
    use that workspaces auth token and portal endpoint, else you must provide it.

    Function names closely reflect the respective API endpoint,
    each starting with the method [GET, POST, etc.] followed by the endpoint path.

    Results will be returned in the form of request's Response.json(), unless something
    else is required. Non-200's will raise exceptions.

    See the swagger documentation for more info about the endpoints.
    """

    def __init__(
        self,
        auth_token: str,
        portal_api: str,
        api_version: str = "2.0",
        default_workspace_id: Optional[str] = None,
    ):
        if not auth_token:
            raise MissingConnectionRequirements(
                f"A Quix Cloud auth token (SDK or PAT) is required; "
                f"set with environment variable {QUIX_ENVIRONMENT.SDK_TOKEN}"
            )

        self._api_version = api_version
        self._auth_token = auth_token
        self._portal_api_url = portal_api
        self._default_workspace_id = (
            default_workspace_id or QUIX_ENVIRONMENT.workspace_id
        )
        self._request_timeout = 30
        self._client: Optional[httpx.Client] = None

    @property
    def client(self) -> httpx.Client:
        if not self._client:
            self._client = httpx.Client(
                base_url=self._portal_api_url,
                event_hooks={"response": [self._response_handler]},
                headers={
                    "X-Version": self._api_version,
                    "Authorization": f"Bearer {self._auth_token}",
                },
            )
        return self._client

    @property
    def default_workspace_id(self) -> str:
        if not self._default_workspace_id:
            raise UndefinedQuixWorkspaceId(
                f"A Quix Cloud Workspace ID is required; "
                f"set with environment variable {QUIX_ENVIRONMENT.WORKSPACE_ID}"
            )
        return self._default_workspace_id

    @default_workspace_id.setter
    def default_workspace_id(self, value):
        self._default_workspace_id = value

    @retry_on_connection_error()
    def get_librdkafka_connection_config(
        self, workspace_id: Optional[str] = None, timeout: float = 30
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.client.get(
            f"/workspaces/{workspace_id}/broker/librdkafka",
            timeout=timeout,
        ).json()

    @retry_on_connection_error()
    def get_workspace_certificate(
        self, workspace_id: Optional[str] = None, timeout: float = 30
    ) -> Optional[bytes]:
        """
        Get a workspace TLS certificate if available.

        Returns `None` if certificate is not specified.

        :param workspace_id: workspace id, optional
        :param timeout: request timeout; Default 30
        :return: certificate as bytes if present, or None
        """
        workspace_id = workspace_id or self.default_workspace_id
        content = self.client.get(
            f"/workspaces/{workspace_id}/certificates", timeout=timeout
        ).content
        if not content:
            return None

        with ZipFile(BytesIO(content)) as z:
            with z.open("ca.cert") as f:
                return f.read()

    @retry_on_connection_error()
    def get_auth_token_details(self, timeout: float = 30) -> dict:
        return self.client.get("/auth/token/details", timeout=timeout).json()

    @retry_on_connection_error()
    def get_workspace(
        self, workspace_id: Optional[str] = None, timeout: float = 30
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.client.get(f"/workspaces/{workspace_id}", timeout=timeout).json()

    @retry_on_connection_error()
    def get_workspaces(self, timeout: float = 30) -> List[dict]:
        # TODO: This seems only return [] with Personal Access Tokens as of Sept 7 '23
        return self.client.get("/workspaces", timeout=timeout).json()

    @retry_on_connection_error()
    def get_topic(
        self, topic_name: str, workspace_id: Optional[str] = None, timeout: float = 30
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.client.get(
            f"/{workspace_id}/topics/{topic_name}", timeout=timeout
        ).json()

    @retry_on_connection_error()
    def get_topics(
        self,
        workspace_id: Optional[str] = None,
        timeout: float = 30,
    ) -> List[dict]:
        workspace_id = workspace_id or self.default_workspace_id
        return self.client.get(f"/{workspace_id}/topics", timeout=timeout).json()

    def post_topic(
        self,
        topic_name: str,
        topic_partitions: Optional[int] = None,
        topic_rep_factor: Optional[int] = None,
        topic_ret_minutes: Optional[int] = None,
        topic_ret_bytes: Optional[int] = None,
        cleanup_policy: Optional[Literal["compact", "delete"]] = None,
        workspace_id: Optional[str] = None,
        timeout: float = 30,
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        d = {
            "name": topic_name,
            "configuration": {
                "partitions": topic_partitions,
                "replicationFactor": topic_rep_factor,
                "retentionInMinutes": topic_ret_minutes,
                "retentionInBytes": topic_ret_bytes,
                "cleanupPolicy": cleanup_policy,
            },
        }
        return self.client.post(
            f"/{workspace_id}/topics", json=d, timeout=timeout
        ).json()

    def _response_handler(self, r: httpx.Response, *args, **kwargs):
        """
        Custom callback/hook that is called after receiving a request.Response

        Catches non-200's and passes both the original exception and the Response body.

        Note: *args and **kwargs expected for hook
        """
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            content = e.response.read()

            try:
                error_text = json.loads(content)
            except json.JSONDecodeError:
                error_text = e.response.text

            raise QuixApiRequestFailure(
                status_code=e.response.status_code,
                url=str(e.response.url),
                error_text=error_text,
            ) from e

    def __getstate__(self) -> object:
        """
        Drops the "_client" attribute to support pickling.
        httpx.Client is not pickleable by default.
        """
        state = self.__dict__.copy()
        state.pop("_client", None)
        return state
