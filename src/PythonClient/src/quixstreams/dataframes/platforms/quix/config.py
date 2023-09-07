from .api import QuixPortalApiService
from pathlib import Path
from os import getcwd
from requests import HTTPError
from typing import Optional


__all__ = ("QuixKafkaConfigsBuilder",)


class QuixKafkaConfigsBuilder:
    """
     Retrieves all the necessary information from the Quix API and builds all the
     objects required to connect a confluent-kafka client to the Quix Platform.

     If not executed within the Quix platform directly, you must provide a Quix
     "sdk-token" (aka workspace token) or Personal Access Token.

     Ideally you also know your workspace name or id. If not, you can search for it
     using a known topic name, but note the search space is limited to the access level
     of your token.

    If using Quix Platform, your most common use will be:

     api = QuixPortalApiInstance()
     builder = QuixKafkaConfigsBuilder(quix_portal_api_service=api)
     topics = ['topic_a', 'topic_b']
     cons_grp = 'my_consumer_group
     cfgs, topics, cons_grp = builder.get_confluent_client_configs(topics, cons_grp)

     Then, if using a Quix Runner:
     runner = Runner(
         broker_address=cfgs.pop('bootstrap_servers'),
         consumer_group=cons_grp,
         producer_extra_configs=cfgs,
         consumer_extra_configs=cfgs
     )
    """

    # TODO: Consider alterations to the current Topic class to accommodate the
    # difference between topic name and id (hiding the workspace part from the user)

    # TODO: Consider a workspace class?
    def __init__(
        self,
        quix_portal_api_service: Optional[QuixPortalApiService] = None,
        workspace_id: Optional[str] = None,
        workspace_cert_path: Optional[str] = None,
    ):
        self.api = quix_portal_api_service or QuixPortalApiService()
        if workspace_id:
            self._workspace_id = workspace_id
        else:
            self._workspace_id = self.api.default_workspace_id
        self._workspace_cert_path = workspace_cert_path
        self._confluent_broker_config = None
        self._quix_broker_config = None
        self._quix_broker_settings = None
        self._workspace_meta = None

    class NoWorkspaceFound(Exception):
        pass

    class MultipleWorkspaces(Exception):
        pass

    class QuixApiKafkaAuthConfigMap:
        names = {
            "saslMechanism": "sasl.mechanisms",
            "securityMode": "security.protocol",
            "address": "bootstrap.servers",
            "username": "sasl.username",
            "password": "sasl.password",
        }
        values = {"ScramSha256": "SCRAM-SHA-256", "SaslSsl": "SASL_SSL"}

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
        """
        Add the workspace ID to a given string, typically a topic or consumer group id

        :param s: the string to append to
        :return: the string with workspace_id appended
        """
        return f"{self.workspace_id}-{s}" if not s.startswith(self.workspace_id) else s

    def search_for_workspace(self, workspace_name_or_id: Optional[str] = None):
        # TODO: there is more to do here to accommodate the new "environments" in v2
        # as it stands now, the search wont work with Quix v2 platform correctly if
        # it's not a workspace_id
        """
        Search for a workspace given an expected workspace name or id.

        :param workspace_name_or_id: the expected name or id of a workspace
        :return: the workspace data dict if search success, else None
        """
        if not workspace_name_or_id:
            workspace_name_or_id = self._workspace_id
        try:
            return self.api.get_workspace(workspace_id=workspace_name_or_id)
        except HTTPError:
            # check to see if they provided the workspace name instead
            ws_list = self.api.get_workspaces()
            for ws in ws_list:
                if ws["name"] == workspace_name_or_id:
                    return ws
            return None

    def get_workspace_info(self, known_workspace_topic: Optional[str] = None):
        """
        Queries for workspace data from the Quix API, regardless of instance cache,
        and updates instance attributes from query result.

        :param known_workspace_topic: a topic you know to exist in some workspace
        """
        # TODO: more error handling with the wrong combo of ws_id and topic
        if self._workspace_id:
            ws_data = self.search_for_workspace(workspace_name_or_id=self._workspace_id)
        else:
            ws_data = self.search_for_topic_workspace(known_workspace_topic)
        if not ws_data:
            raise self.NoWorkspaceFound(
                "No workspace was found for the given workspace/auth-token/topic combo"
            )
        self._workspace_id = ws_data.pop("workspaceId")
        self._quix_broker_config = ws_data.pop("broker")
        self._quix_broker_settings = ws_data.pop("brokerSettings")
        self._workspace_meta = ws_data

    def search_workspace_for_topic(self, workspace_id: str, topic: str):
        """
        Search through all the topics in the given workspace id to see if there is a
        match with the provided topic.

        :param workspace_id: the workspace to search in
        :param topic: the topic to search for
        :return: the workspace_id if success, else None
        """
        topics = self.api.get_topics(workspace_id=workspace_id)
        for t in topics:
            if t["name"] == topic or t["id"] == topic:
                return workspace_id
        return None

    def search_for_topic_workspace(self, topic: str):
        """
        Find what workspace a topic belongs to.
        If there is only one workspace altogether, it is assumed to be the workspace.
        More than one means each workspace will be searched until the first hit.

        :param topic: the topic to search for
        :return: workspace data dict if topic search success, else None
        """
        ws_list = self.api.get_workspaces()
        if len(ws_list) > 1:
            if topic is None:
                raise self.MultipleWorkspaces(
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

    def get_workspace_ssl_cert(self, extract_to_folder: Optional[Path] = None) -> str:
        """
        Gets and extracts zipped certificate from the API to provided folder.
        If no path was provided, will dump to /tmp. Expects cert named 'ca.cert'.

        :param extract_to_folder: path to folder to dump zipped cert file to
        :return: full cert filepath as string
        """
        if not extract_to_folder:
            extract_to_folder = Path("/tmp")
        filename = "ca.cert"
        full_path = extract_to_folder / filename
        if not full_path.is_file():
            extract_to_folder.mkdir(parents=True, exist_ok=True)
            self.api.get_workspace_certificate(workspace_id=self._workspace_id).extract(
                "ca.cert", extract_to_folder
            )
        return full_path.as_posix()

    def _set_workspace_cert(self):
        """
        Will create a cert and assigns it to the workspace_cert_path property.
        If there was no path provided at init, one is generated based on the cwd and
        workspace_id.
        Used by this class when generating configs (does some extra folder and class
        config stuff that's not needed if you just want to get the cert only)

        :return: full cert filepath as string
        """
        if self._workspace_cert_path:
            folder = Path(self._workspace_cert_path)
            if folder.name.endswith("cert"):
                folder = folder.parent
        else:
            folder = Path(getcwd()) / "certificates" / self.workspace_id
        self._workspace_cert_path = self.get_workspace_ssl_cert(
            extract_to_folder=folder
        )
        return self._workspace_cert_path

    def get_confluent_broker_config(self, known_topic: Optional[str] = None) -> dict:
        """
        Get the full client config dictionary required to authenticate a confluent-kafka
        client to a Quix platform broker/workspace.

        The returned config can be used directly by any confluent-kafka-python consumer/
        producer (add your producer/consumer-specific configs afterward).

        :param known_topic: a topic known to exist in some workspace
        :return: a dict of confluent-kafka-python client settings (see librdkafka
        config for more details)
        """
        self.get_workspace_info(known_workspace_topic=known_topic)
        cfg_out = {}
        for q_cfg, c_cfg in self.QuixApiKafkaAuthConfigMap.names.items():
            value = self.quix_broker_config[q_cfg]
            cfg_out[c_cfg] = self.QuixApiKafkaAuthConfigMap.values.get(value, value)
        cfg_out["ssl.endpoint.identification.algorithm"] = "none"
        cfg_out["ssl.ca.location"] = self.workspace_cert_path
        self._confluent_broker_config = cfg_out
        return self._confluent_broker_config

    def get_confluent_client_configs(
        self, topics: list, consumer_group_id: Optional[str] = None
    ):
        """
        Get all the values you need in order to use a confluent_kafka-based client
        with a topic on a Quix platform broker/workspace.

        The returned config can be used directly by any confluent-kafka-python consumer/
        producer (add your producer/consumer-specific configs afterward).

        :param topics: list of topics
        :param consumer_group_id: consumer group id, if needed
        :return: a tuple with configs and altered versions of the topics
        and consumer group name
        """
        return (
            self.get_confluent_broker_config(topics[0]),
            [self.append_workspace_id(t) for t in topics],
            self.append_workspace_id(consumer_group_id) if consumer_group_id else None,
        )
