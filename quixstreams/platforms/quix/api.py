from io import BytesIO
from typing import Optional, List, Literal
from urllib.parse import urljoin
from zipfile import ZipFile

import requests

from .env import QUIX_ENVIRONMENT
from .exceptions import (
    UndefinedQuixWorkspaceId,
    MissingConnectionRequirements,
    QuixApiRequestFailure,
)

__all__ = ("QuixPortalApiService",)
DEFAULT_PORTAL_API_URL = "https://portal-api.platform.quix.io/"


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
        auth_token: Optional[str] = None,
        portal_api: Optional[str] = None,
        api_version: Optional[str] = None,
        default_workspace_id: Optional[str] = None,
    ):
        self._portal_api = (
            portal_api or QUIX_ENVIRONMENT.portal_api or DEFAULT_PORTAL_API_URL
        )
        self._auth_token = auth_token or QUIX_ENVIRONMENT.sdk_token
        if not self._auth_token:
            raise MissingConnectionRequirements(
                f"A Quix Cloud auth token (SDK or PAT) is required; "
                f"set with environment variable {QUIX_ENVIRONMENT.SDK_TOKEN}"
            )
        self._default_workspace_id = (
            default_workspace_id or QUIX_ENVIRONMENT.workspace_id
        )
        self.api_version = api_version or "2.0"
        self.session = self._init_session()

    class SessionWithUrlBase(requests.Session):
        def __init__(self, url_base: str):
            self.url_base = url_base
            super().__init__()

        def request(self, method, url, **kwargs):
            # provided methods have timeout defined, but keep a default for adhoc calls
            timeout = kwargs.pop("timeout", 30)
            return super().request(
                method, urljoin(base=self.url_base, url=url), timeout=timeout, **kwargs
            )

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

    def _response_handler(self, r: requests.Response, *args, **kwargs):
        """
        Custom callback/hook that is called after receiving a request.Response

        Catches non-200's and passes both the original exception and the Response body.

        Note: *args and **kwargs expected for hook
        """
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            try:
                error_text = e.response.json()
            except requests.exceptions.JSONDecodeError:
                error_text = e.response.text

            raise QuixApiRequestFailure(
                status_code=e.response.status_code,
                url=e.response.url,
                error_text=error_text,
            )

    def _init_session(self) -> SessionWithUrlBase:
        s = self.SessionWithUrlBase(self._portal_api)
        s.hooks = {"response": self._response_handler}
        s.headers.update(
            {
                "X-Version": self.api_version,
                "Authorization": f"Bearer {self._auth_token}",
            }
        )
        return s

    def get_librdkafka_connection_config(
        self, workspace_id: Optional[str] = None
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.session.get(f"/workspaces/{workspace_id}/broker/librdkafka").json()

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
        content = self.session.get(
            f"/workspaces/{workspace_id}/certificates", timeout=timeout
        ).content
        if not content:
            return

        with ZipFile(BytesIO(content)) as z:
            with z.open("ca.cert") as f:
                return f.read()

    def get_auth_token_details(self, timeout: float = 30) -> dict:
        return self.session.get("/auth/token/details", timeout=timeout).json()

    def get_workspace(
        self, workspace_id: Optional[str] = None, timeout: float = 30
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.session.get(f"/workspaces/{workspace_id}", timeout=timeout).json()

    def get_workspaces(self, timeout: float = 30) -> List[dict]:
        # TODO: This seems only return [] with Personal Access Tokens as of Sept 7 '23
        return self.session.get("/workspaces", timeout=timeout).json()

    def get_topic(
        self, topic_name: str, workspace_id: Optional[str] = None, timeout: float = 30
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.session.get(
            f"/{workspace_id}/topics/{topic_name}", timeout=timeout
        ).json()

    def get_topics(
        self,
        workspace_id: Optional[str] = None,
        timeout: float = 30,
    ) -> List[dict]:
        workspace_id = workspace_id or self.default_workspace_id
        return self.session.get(f"/{workspace_id}/topics", timeout=timeout).json()

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
        return self.session.post(
            f"/{workspace_id}/topics", json=d, timeout=timeout
        ).json()
