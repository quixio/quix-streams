from io import BytesIO
from os import environ
from urllib.parse import urljoin
from zipfile import ZipFile

import requests
from typing import Optional, List

from ...exceptions import QuixException

__all__ = ("QuixPortalApiService",)


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
        portal_api: Optional[str] = None,
        auth_token: Optional[str] = None,
        api_version: Optional[str] = None,
        default_workspace_id: Optional[str] = None,
    ):
        self._portal_api = portal_api or environ.get(self.QuixEnvironmentMap.portal_api)
        self._auth_token = auth_token or environ.get(self.QuixEnvironmentMap.sdk_token)
        self._default_workspace_id = default_workspace_id or environ.get(
            self.QuixEnvironmentMap.workspace_id
        )
        self.api_version = api_version or "2.0"
        self.session = self._init_session()

    class MissingConnectionRequirements(QuixException):
        ...

    class UndefinedQuixWorkspaceId(QuixException):
        ...

    class QuixEnvironmentMap:
        sdk_token = "Quix__Sdk__Token"
        portal_api = "Quix__Portal__Api"
        workspace_id = "Quix__Workspace__Id"

    class SessionWithUrlBase(requests.Session):
        def __init__(self, url_base: str):
            self.url_base = url_base
            super().__init__()

        def request(self, method, url, **kwargs):
            timeout = kwargs.pop("timeout", 10)
            return super().request(
                method, urljoin(base=self.url_base, url=url), timeout=timeout, **kwargs
            )

    @property
    def default_workspace_id(self) -> str:
        if not self._default_workspace_id:
            raise self.UndefinedQuixWorkspaceId("You must provide a Quix Workspace ID")
        return self._default_workspace_id

    @default_workspace_id.setter
    def default_workspace_id(self, value):
        self._default_workspace_id = value

    def _init_session(self) -> SessionWithUrlBase:
        if not (self._portal_api and self._auth_token):
            missing = filter(
                None,
                [self._portal_api or "portal_api", self._auth_token or "auth_token"],
            )
            raise self.MissingConnectionRequirements(
                f"Using the API requires a portal and token. Missing: {missing}"
            )
        s = self.SessionWithUrlBase(self._portal_api)
        s.hooks = {"response": lambda r, *args, **kwargs: r.raise_for_status()}
        s.headers.update(
            {
                "X-Version": self.api_version,
                "Authorization": f"Bearer {self._auth_token}",
            }
        )
        return s

    def get_workspace_certificate(self, workspace_id: Optional[str] = None) -> bytes:
        workspace_id = workspace_id or self.default_workspace_id
        with ZipFile(
            BytesIO(
                self.session.get(f"/workspaces/{workspace_id}/certificates").content
            )
        ) as z:
            with z.open("ca.cert") as f:
                return f.read()

    def get_auth_token_details(self) -> dict:
        return self.session.get(f"/auth/token/details").json()

    def get_workspace(self, workspace_id: Optional[str] = None) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.session.get(f"/workspaces/{workspace_id}").json()

    def get_workspaces(self) -> List[dict]:
        # TODO: This seems only return [] with Personal Access Tokens as of Sept 7 '23
        return self.session.get("/workspaces").json()

    def get_topic(self, topic_name: str, workspace_id: Optional[str] = None) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        return self.session.get(f"/{workspace_id}/topics/{topic_name}").json()

    def get_topics(self, workspace_id: Optional[str] = None) -> List[dict]:
        workspace_id = workspace_id or self.default_workspace_id
        return self.session.get(f"/{workspace_id}/topics").json()

    def post_topic(
        self,
        topic_name: str,
        workspace_id: Optional[str] = None,
        topic_partitions: Optional[int] = None,
        topic_rep_factor: Optional[int] = None,
        topic_ret_minutes: Optional[int] = None,
        topic_ret_bytes: Optional[int] = None,
    ) -> dict:
        workspace_id = workspace_id or self.default_workspace_id
        d = {
            "name": topic_name,
            "configuration": {
                "partitions": topic_partitions or 2,
                "replicationFactor": topic_rep_factor or 2,
                "retentionInMinutes": topic_ret_minutes or 10080,
                "retentionInBytes": topic_ret_bytes or 52428800,
            },
        }
        return self.session.post(f"/{workspace_id}/topics", json=d).json()
