import io
import zipfile
from os import environ, getcwd
from pathlib import Path

import requests

__all__ = ("QuixKafkaConfigsFactory", "QuixPortalApiWrapper",)


class QuixEnvironmentMap:
    sdk_token = "Quix__Sdk__Token"
    portal_api = "Quix__Portal__Api"
    workspace_id = "Quix__Workspace__Id"


class QuixApiKafkaAuthConfigMap:
    names = {
        "saslMechanism": "sasl.mechanisms",
        "securityMode": "security.protocol",
        "address": "bootstrap.servers",
        "username": "sasl.username",
        "password": "sasl.password"
    }
    values = {
        "ScramSha256": "SCRAM-SHA-256",
        "SaslSsl": "SASL_SSL"
    }


class QuixPortalApiWrapper:
    # TODO: Better exception handling
    class BearerAuth(requests.auth.AuthBase):
        def __init__(self, token):
            self.token = token

        def __call__(self, r):
            r.headers["Authorization"] = "Bearer " + self.token
            return r

    def __init__(
            self, portal_api: str = None, auth_token: str = None,
            api_version: str = None, default_workspace_id: str = None
    ):
        self._portal_api = portal_api or environ.get(QuixEnvironmentMap.portal_api)
        self._auth_token = auth_token or environ.get(QuixEnvironmentMap.sdk_token)
        self.default_workspace_id = \
            default_workspace_id or environ.get(QuixEnvironmentMap.workspace_id)
        self.api_version = api_version or "2.0"

    def _auth_and_versioning(self, **kwargs):
        if not (self._portal_api and self._auth_token):
            raise Exception("Missing portal and/or access token")
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["X-Version"] = \
            kwargs["headers"].get("X-Version", self.api_version)
        return kwargs

    def get(self, uri, **kwargs):
        self._auth_and_versioning(**kwargs)
        return requests.get(
            url=f"{self._portal_api}{uri}",
            auth=self.BearerAuth(self._auth_token),
            **kwargs
        )

    def post(self, uri, **kwargs):
        self._auth_and_versioning(**kwargs)
        return requests.post(
            url=f"{self._portal_api}{uri}",
            auth=self.BearerAuth(self._auth_token),
            **kwargs
        )

    def get_workspace_certificate(self, workspace_id: str = None):
        if not workspace_id:
            workspace_id = self.default_workspace_id
        return self.get(uri=f"/workspaces/{workspace_id}/certificates")

    def get_auth_token_details(self):
        return self.get(uri=f"/auth/token/details")

    def get_workspace(self, workspace_id=None):
        if not workspace_id:
            workspace_id = self.default_workspace_id
        return self.get(uri=f"/workspaces/{workspace_id}")

    def get_workspaces(self):
        return self.get(uri="/workspaces")

    def get_topics(self, workspace_id=None):
        if not workspace_id:
            workspace_id = self.default_workspace_id
        return self.get(uri=f"/{workspace_id}/topics")

    def get_topic(self, topic_name, workspace_id=None):
        if not workspace_id:
            workspace_id = self.default_workspace_id
        return self.get(uri=f"/{workspace_id}/topics/{topic_name}")

    def post_topic(
            self, workspace_id: str, topic_name: str = "api-topic-test",
            topic_partitions: int = 2, topic_rep_factor: int = 2,
            topic_ret_minutes: int = 10080, topic_ret_bytes: int = 52428800
    ):
        # TODO: check to see if topic creation is fixed later...getting 403"s in v2
        if not workspace_id:
            workspace_id = self.default_workspace_id
        d = {
            "name": topic_name,
            "configuration": {
                "partitions": topic_partitions,
                "replicationFactor": topic_rep_factor,
                "retentionInMinutes": topic_ret_minutes,
                "retentionInBytes": topic_ret_bytes
            }
        }
        return self.post(uri=f"/{workspace_id}/topics", json=d)


class QuixKafkaConfigsFactory:
    # TODO: Consider alterations to the current Topic class to accommodate the
    # difference between topic name and id (hiding the workspace part from the user)
    def __init__(
            self, quix_portal_api_wrapper: QuixPortalApiWrapper = None,
            workspace_id: str = None, workspace_cert_path: str = None
    ):
        self.api = quix_portal_api_wrapper or QuixPortalApiWrapper()
        if workspace_id:
            self._workspace_id = workspace_id
        else:
            self._workspace_id = self.api.default_workspace_id
        self._workspace_cert_path = workspace_cert_path
        self._confluent_broker_config = None
        self._quix_broker_config = None
        self._quix_broker_settings = None
        self._workspace_meta = None

    @property
    def workspace_id(self):
        if not self._workspace_id:
            self.get_workspace_info()
        return self._workspace_id

    @property
    def quix_broker_config(self):
        if not self._quix_broker_config:
            self.get_workspace_info()
        return self._quix_broker_config

    @property
    def quix_broker_settings(self):
        if not self._quix_broker_settings:
            self.get_workspace_info()
        return self._quix_broker_settings

    @property
    def confluent_broker_config(self):
        if not self._confluent_broker_config:
            self.get_confluent_broker_config()
        return self._confluent_broker_config

    @property
    def workspace_cert_path(self):
        if not self._workspace_cert_path:
            self._set_workspace_cert()
        return self._workspace_cert_path

    @property
    def workspace_meta(self):
        if not self._workspace_meta:
            self.get_workspace_info()
        return self._workspace_meta

    def append_workspace_id(self, s: str):
        return f"{self.workspace_id}-{s}" if not s.startswith(self.workspace_id) else s

    def get_workspace_info(self, known_workspace_topic: str = None):
        """
        Queries for workspace data from the Quix API, regardless of instance cache.
        """
        # TODO: error handling with the wrong combo of ws_id and topic
        if self._workspace_id:
            ws_data = self.api.get_workspace(workspace_id=self._workspace_id).json()
        else:
            ws_data = self.search_for_topic_workspace(known_workspace_topic)
        if ws_data is None:
            raise Exception("No workspaces with given topic found")
        self._workspace_id = ws_data.pop("workspaceId")
        self._quix_broker_config = ws_data.pop("broker")
        self._quix_broker_settings = ws_data.pop("brokerSettings")
        self._workspace_meta = ws_data

    def search_workspace_for_topic(self, workspace_id: str, topic: str):
        """
        Search through all the topics in the given workspace id to see if there is a
        match with the provided topic.
        """
        topics = self.api.get_topics(workspace_id=workspace_id).json()
        for t in topics:
            if t["name"] == topic or t["id"] == topic:
                return workspace_id
        return None

    def search_for_topic_workspace(self, topic: str):
        """
        Find what workspace a topic belongs to.
        If there is only one workspace altogether, it is assumed to be the workspace.
        More than one means each workspace will be searched until the first hit.
        No match will return None, eventually raising an exception downstream.
        """
        ws_list = self.api.get_workspaces().json()
        if len(ws_list) > 1:
            if topic is None:
                raise Exception(
                    "More than 1 workspace was found, so you must provide a topic name "
                    "to find the correct workspace"
                )
            print(ws_list)
            for ws in ws_list:
                if self.search_workspace_for_topic(ws["workspaceId"], topic):
                    return ws
            return None
        else:
            return ws_list[0]

    def get_workspace_ssl_cert(self, extract_to_folder: Path = None):
        """
        Gets/unzips certificate from the API, returning the path it was dumped to.
        If no path was provided, will dump to /tmp.
        """
        if not extract_to_folder:
            extract_to_folder = Path("/tmp")
        filename = "ca.cert"
        full_path = extract_to_folder/filename
        if not full_path.is_file():
            extract_to_folder.mkdir(parents=True, exist_ok=True)
            r = self.api.get_workspace_certificate(workspace_id=self._workspace_id)
            zipfile.ZipFile(io.BytesIO(r.content)).extract("ca.cert", extract_to_folder)
        return full_path.as_posix()

    def _set_workspace_cert(self):
        """
        Will create a cert and return its path.
        If there was no path provided at init, one is generated based on the cwd and
        workspace_id.
        Used by this class when generating configs (does some extra folder and class
        config stuff that's not needed if you just want to get the cert only)
        """
        if self._workspace_cert_path:
            folder = Path(self._workspace_cert_path)
            if folder.name.endswith("cert"):
                folder = folder.parent
        else:
            folder = Path(getcwd())/"certificates"/self.workspace_id
        self._workspace_cert_path = self.get_workspace_ssl_cert(
            extract_to_folder=folder
        )
        return self._workspace_cert_path

    def get_confluent_broker_config(self, known_topic: str = None):
        self.get_workspace_info(known_workspace_topic=known_topic)
        cfg_out = {}
        for q_cfg, c_cfg in QuixApiKafkaAuthConfigMap.names.items():
            value = self.quix_broker_config[q_cfg]
            cfg_out[c_cfg] = QuixApiKafkaAuthConfigMap.values.get(value, value)
        cfg_out["ssl.endpoint.identification.algorithm"] = "none"
        cfg_out["ssl.ca.location"] = self.workspace_cert_path
        self._confluent_broker_config = cfg_out
        return self._confluent_broker_config

    def get_consumer_config(self, topics: list, group_id: str):
        """
        Get all the values you need in order to subscribe a consumer instance
        to a topic on a Quix Broker.
        """
        return (
            self.get_confluent_broker_config(topics[0]),
            [self.append_workspace_id(t) for t in topics],
            self.append_workspace_id(group_id)
        )
