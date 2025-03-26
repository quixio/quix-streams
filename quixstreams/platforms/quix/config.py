import base64
import dataclasses
import logging
import time
from copy import deepcopy
from typing import Any, List, Optional

from requests import HTTPError

from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.models.topics import Topic, TopicConfig

from .api import QuixPortalApiService
from .exceptions import (
    MultipleWorkspaces,
    NoWorkspaceFound,
    QuixApiRequestFailure,
    QuixCreateTopicFailure,
    QuixCreateTopicTimeout,
    UndefinedQuixWorkspaceId,
)

logger = logging.getLogger(__name__)

__all__ = ("QuixKafkaConfigsBuilder", "QuixApplicationConfig")

QUIX_CONNECTIONS_MAX_IDLE_MS = 3 * 60 * 1000
QUIX_METADATA_MAX_AGE_MS = 3 * 60 * 1000

_QUIX_CLEANUP_POLICY_MAPPING = {
    "Delete": "delete",
    "Compact": "compact",
    "DeleteAndCompact": "delete,compact",
}


def _quix_cleanup_policy_to_kafka(quix_cleanup_policy: str) -> str:
    try:
        return _QUIX_CLEANUP_POLICY_MAPPING[quix_cleanup_policy]
    except KeyError:
        raise ValueError(
            f'Invalid value for "cleanupPolicy" parameter "{quix_cleanup_policy}"'
        )


def strip_workspace_id_prefix(workspace_id: str, s: str) -> str:
    """
    Remove the workspace ID from a given string if it starts with it.

    Only used for consumer groups.

    :param workspace_id: the workspace id
    :param s: the string to append to
    :return: the string with workspace_id prefix removed
    """
    return s[len(workspace_id) + 1 :] if s.startswith(workspace_id) else s


def prepend_workspace_id(workspace_id: str, s: str) -> str:
    """
    Add the workspace ID as a prefix to a given string if it does not have it.

    Only used for consumer groups.

    :param workspace_id: the workspace id
    :param s: the string to append to
    :return: the string with workspace_id prepended
    """
    return f"{workspace_id}-{s}" if not s.startswith(workspace_id) else s


@dataclasses.dataclass
class QuixApplicationConfig:
    """
    A convenience container class for Quix Application configs.
    """

    librdkafka_connection_config: ConnectionConfig
    librdkafka_extra_config: dict
    consumer_group: str


class QuixKafkaConfigsBuilder:
    """
    Retrieves all the necessary information from the Quix API and builds all the
    objects required to connect a confluent-kafka client to the Quix Platform.

    If not executed within the Quix platform directly, you must provide a Quix
    "streaming" (aka "sdk") token, or Personal Access Token.

    Ideally you also know your workspace name or id. If not, you can search for it
    using a known topic name, but note the search space is limited to the access level
    of your token.

    It also currently handles the app_auto_create_topics setting for Quix Applications.
    """

    # TODO: Consider a workspace class?
    def __init__(
        self,
        quix_sdk_token: Optional[str] = None,
        workspace_id: Optional[str] = None,
        quix_portal_api_service: Optional[QuixPortalApiService] = None,
        timeout: float = 30,
        topic_create_timeout: float = 60,
    ):
        """
        :param quix_portal_api_service: A QuixPortalApiService instance (else generated)
        :param workspace_id: A valid Quix Workspace ID (else searched for)
        """
        if quix_sdk_token:
            self.api = QuixPortalApiService(
                default_workspace_id=workspace_id, auth_token=quix_sdk_token
            )
        elif quix_portal_api_service:
            self.api = quix_portal_api_service
        else:
            raise ValueError(
                'Either "quix_sdk_token" or "quix_portal_api_service" must be provided'
            )

        try:
            self._workspace_id = workspace_id or self.api.default_workspace_id
        except UndefinedQuixWorkspaceId:
            self._workspace_id = ""
            logger.warning(
                "'workspace_id' argument was not provided nor set with "
                "'Quix__Workspace__Id' environment; if only one Workspace ID for the "
                "provided auth token exists (often true with SDK tokens), "
                "then that ID will be used. Otherwise, provide a known topic name to "
                "method 'get_workspace_info(topic)' to obtain desired Workspace ID."
            )
        self._librdkafka_connect_config: Optional[ConnectionConfig] = None
        self._quix_broker_settings: dict[str, Any] = {}
        self._workspace_meta: dict[str, Any] = {}
        self._timeout = timeout
        self._topic_create_timeout = topic_create_timeout

    @property
    def workspace_id(self) -> str:
        if not self._workspace_id:
            self.get_workspace_info()
        return self._workspace_id

    @property
    def quix_broker_settings(self) -> dict:
        if not self._quix_broker_settings:
            self.get_workspace_info()
        return self._quix_broker_settings

    @property
    def workspace_meta(self) -> dict:
        if not self._workspace_meta:
            self.get_workspace_info()
        return self._workspace_meta

    @property
    def librdkafka_connection_config(self) -> ConnectionConfig:
        if not self._librdkafka_connect_config:
            self._librdkafka_connect_config = self._get_librdkafka_connection_config()
        return self._librdkafka_connect_config

    @property
    def librdkafka_extra_config(self) -> dict:
        # Set the connection idle timeout and metadata max age to be less than
        # Azure's default 4 minutes.
        # Azure LB kills the inbound TCP connections after 4 mins and these settings
        # help to handle that.
        # More about this issue:
        # - https://github.com/confluentinc/librdkafka/issues/3109
        # - https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations#producer-and-consumer-configurations-1
        return {
            "connections.max.idle.ms": QUIX_CONNECTIONS_MAX_IDLE_MS,
            "metadata.max.age.ms": QUIX_METADATA_MAX_AGE_MS,
        }

    @classmethod
    def convert_topic_response(cls, api_response: dict) -> Topic:
        """
        Converts a GET or POST ("create") topic API response to a Topic object

        :param api_response: the dict response from a get or create topic call
        :return: a corresponding Topic object
        """
        extra_config = {}

        topic_config = api_response["configuration"]
        extra_config["retention.ms"] = topic_config["retentionInMinutes"] * 60 * 1000
        extra_config["retention.bytes"] = topic_config["retentionInBytes"]

        # Map value returned by Quix API to Kafka Admin API format
        if topic_config.get("cleanupPolicy"):
            cleanup_policy = _quix_cleanup_policy_to_kafka(
                topic_config["cleanupPolicy"]
            )
            extra_config["cleanup.policy"] = cleanup_policy

        config = TopicConfig(
            num_partitions=topic_config["partitions"],
            replication_factor=topic_config["replicationFactor"],
            extra_config=extra_config,
        )
        topic = Topic(
            name=api_response["id"],
            create_config=config,
            quix_name=api_response["name"],
        )
        topic.broker_config = config
        return topic

    def strip_workspace_id_prefix(self, s: str) -> str:
        """
        Remove the workspace ID from a given string if it starts with it.

        Only used for consumer groups.

        :param s: the string to append to
        :return: the string with workspace_id prefix removed
        """
        return strip_workspace_id_prefix(self.workspace_id, s)

    def prepend_workspace_id(self, s: str) -> str:
        """
        Add the workspace ID as a prefix to a given string if it does not have it.

        Only used for consumer groups.

        :param s: the string to append to
        :return: the string with workspace_id prepended
        """
        return prepend_workspace_id(self.workspace_id, s)

    def search_for_workspace(
        self,
        workspace_name_or_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Optional[dict]:
        # TODO: there is more to do here to accommodate the new "environments" in v2
        # as it stands now, the search won't work with Quix v2 platform correctly if
        # it's not a workspace_id
        """
        Search for a workspace given an expected workspace name or id.

        :param workspace_name_or_id: the expected name or id of a workspace
        :param timeout: response timeout (seconds); Default 30

        :return: the workspace data dict if search success, else None
        """
        timeout = timeout if timeout is not None else self._timeout
        if not workspace_name_or_id:
            workspace_name_or_id = self._workspace_id
        try:
            return self.api.get_workspace(
                workspace_id=workspace_name_or_id, timeout=timeout
            )
        except HTTPError:
            # check to see if they provided the workspace name instead
            ws_list = self.api.get_workspaces(timeout=timeout)
            for ws in ws_list:
                if ws["name"] == workspace_name_or_id:
                    return ws
            raise

    def _set_workspace_info(self, workspace_data: dict):
        ws_data = deepcopy(workspace_data)
        self._workspace_id = ws_data.pop("workspaceId")
        try:
            self._quix_broker_settings = ws_data.pop("brokerSettings")
        except KeyError:  # hold-over for platform v1
            self._quix_broker_settings = {
                "brokerType": ws_data["brokerType"],
                "syncTopics": False,
            }
        self._workspace_meta = ws_data

    def get_workspace_info(
        self,
        known_workspace_topic: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> dict:
        """
        Queries for workspace data from the Quix API, regardless of instance cache,
        and updates instance attributes from query result.

        :param known_workspace_topic: a topic you know to exist in some workspace
        :param timeout: response timeout (seconds); Default 30
        """
        # TODO: more error handling with the wrong combo of ws_id and topic
        if self._workspace_id:
            ws_data = self.search_for_workspace(
                workspace_name_or_id=self._workspace_id, timeout=timeout
            )
        else:
            ws_data = self.search_for_topic_workspace(
                known_workspace_topic, timeout=timeout
            )

        if not ws_data:
            raise NoWorkspaceFound(
                "No workspace was found for the given workspace/auth-token/topic combo"
            )
        self._set_workspace_info(ws_data)
        return ws_data

    def search_workspace_for_topic(
        self, workspace_id: str, topic: str, timeout: Optional[float] = None
    ) -> Optional[str]:
        """
        Search through all the topics in the given workspace id to see if there is a
        match with the provided topic.

        :param workspace_id: the workspace to search in
        :param topic: the topic to search for
        :param timeout: response timeout (seconds); Default 30

        :return: the workspace_id if success, else None
        """
        topics = self.api.get_topics(
            workspace_id=workspace_id,
            timeout=timeout if timeout is not None else self._timeout,
        )
        for t in topics:
            if t["name"] == topic or t["id"] == topic:
                return workspace_id

        return None

    def search_for_topic_workspace(
        self, topic: Optional[str], timeout: Optional[float] = None
    ) -> Optional[dict]:
        """
        Find what workspace a topic belongs to.
        If there is only one workspace altogether, it is assumed to be the workspace.
        More than one means each workspace will be searched until the first hit.

        :param topic: the topic to search for
        :param timeout: response timeout (seconds); Default 30

        :return: workspace data dict if topic search success, else None
        """
        ws_list = self.api.get_workspaces(
            timeout=timeout if timeout is not None else self._timeout
        )
        if len(ws_list) == 1:
            return ws_list[0]
        if topic is None:
            raise MultipleWorkspaces(
                "More than 1 workspace was found, so you must provide a topic name "
                "to find the correct workspace"
            )
        for ws in ws_list:
            if self.search_workspace_for_topic(
                ws["workspaceId"], topic, timeout=timeout
            ):
                return ws

        return None

    def create_topic(self, topic: Topic, timeout: Optional[float] = None) -> dict:
        """
        The actual API call to create the topic.

        :param topic: a Topic instance
        :param timeout: response timeout (seconds); Default 30
        """
        cfg = topic.create_config
        if cfg is None:
            raise RuntimeError("Topic config not set")

        # settings that must be ints or Nones
        ret_ms = cfg.extra_config.get("retention.ms")
        ret_bytes = cfg.extra_config.get("retention.bytes")

        # an exception is raised (status code) if topic is not created successfully
        logger.info(
            f'Creating topic "{topic.name}" '
            f'with a config: "{topic.create_config.as_dict() if topic.create_config is not None else {} }"'
        )
        resp = self.api.post_topic(
            topic_name=topic.name,
            workspace_id=self.workspace_id,
            topic_partitions=cfg.num_partitions,
            topic_rep_factor=cfg.replication_factor,
            topic_ret_bytes=ret_bytes if ret_bytes is None else int(ret_bytes),
            topic_ret_minutes=ret_ms if ret_ms is None else int(ret_ms) // 60000,
            cleanup_policy=cfg.extra_config.get("cleanup.policy"),  # type: ignore[arg-type]
            timeout=timeout if timeout is not None else self._timeout,
        )
        logger.debug(
            f"Creation of topic {topic.name} acknowledged by broker. Must wait "
            f"for 'Ready' status before topic is actually available"
        )
        return resp

    def get_or_create_topic(
        self,
        topic: Topic,
        timeout: Optional[float] = None,
    ) -> dict:
        """
        Get or create topics in a Quix cluster as part of initializing the Topic
        object to obtain the true topic name.

        :param topic: a `Topic` object
        :param timeout: response timeout (seconds); Default 30
        marked as "Ready" (and thus ready to produce to/consume from).
        """
        try:
            return self.get_topic(topic=topic, timeout=timeout)
        except QuixApiRequestFailure as e:
            if e.status_code != 404:
                raise

            # Topic likely does not exist (anything but success 404's; could inspect
            # error string, but that creates a dependency on it never changing).
            try:
                return self.create_topic(topic, timeout=timeout)
            except QuixApiRequestFailure:
                # Multiple apps likely tried to create at the same time.
                # If this fails, it raises with all previous API errors
                return self.get_topic(topic=topic, timeout=timeout)

    def wait_for_topic_ready_statuses(
        self,
        topics: List[Topic],
        timeout: Optional[float] = None,
        finalize_timeout: Optional[float] = None,
    ):
        """
        After the broker acknowledges topics for creation, they will be in a
        "Creating" status; they not usable until they are set to a status of "Ready".

        This blocks until all topics are marked as "Ready" or the timeout is hit.

        :param topics: a list of `Topic` objects
        :param timeout: response timeout (seconds); Default 30
        :param finalize_timeout: topic finalization timeout (seconds); Default 60
        marked as "Ready" (and thus ready to produce to/consume from).
        """
        logger.debug("Confirming all topics are ready in Quix Cloud...")
        exceptions = {}
        topic_ids = {topic.name for topic in topics}
        stop_time = time.monotonic() + (finalize_timeout or self._topic_create_timeout)
        while topic_ids and time.monotonic() < stop_time:
            time.sleep(1)
            for topic_resp in (
                topic_resps := [
                    t for t in self.get_topics(timeout=timeout) if t["id"] in topic_ids
                ]
            ):
                if topic_resp["status"] == "Ready":
                    logger.debug(f"Topic {topic_resp['name']} creation finalized")
                    topic_ids.remove(topic_resp["id"])
                elif topic_resp["status"] == "Error":
                    logger.debug(f"Topic {topic_resp['name']} encountered an error")
                    exceptions[topic_resp["name"]] = topic_resp["lastError"]
                    topic_ids.remove(topic_resp["id"])
        if exceptions:
            raise QuixCreateTopicFailure(f"Quix topic validation failed: {exceptions}")
        if topic_ids:
            raise QuixCreateTopicTimeout(
                f"Waiting for 'Ready' status timed out for Quix "
                f"topics: {[t['name'] for t in topic_resps if t['id'] in topic_ids]}"
            )

    def get_topic(self, topic: Topic, timeout: Optional[float] = None) -> dict:
        """
        return the topic ID (the actual cluster topic name) if it exists, else raise

        :param topic: The Topic to get
        :param timeout: response timeout (seconds); Default 30

        :return: response dict of the topic info if topic found, else None
        :raises QuixApiRequestFailure: when topic does not exist
        """
        return self.api.get_topic(
            topic_name=topic.name,
            workspace_id=self.workspace_id,
            timeout=timeout if timeout is not None else self._timeout,
        )

    def get_topics(self, timeout: Optional[float] = None) -> List[dict]:
        return self.api.get_topics(
            workspace_id=self.workspace_id,
            timeout=timeout if timeout is not None else self._timeout,
        )

    def _get_librdkafka_connection_config(self) -> ConnectionConfig:
        """
        Get the full client config required to authenticate a confluent-kafka
        client to a Quix platform broker/workspace as a ConnectionConfig
        """
        librdkafka_dict = self.api.get_librdkafka_connection_config(self.workspace_id)
        if (cert := librdkafka_dict.pop("ssl.ca.cert", None)) is not None:
            librdkafka_dict["ssl.ca.pem"] = base64.b64decode(cert)
        return ConnectionConfig.from_librdkafka_dict(librdkafka_dict)

    def get_application_config(self, consumer_group_id: str) -> QuixApplicationConfig:
        """
        Get all the necessary attributes for an Application to run on Quix Cloud.

        :param consumer_group_id: consumer group id, if needed
        :return: a QuixApplicationConfig instance
        """
        return QuixApplicationConfig(
            self.librdkafka_connection_config,
            self.librdkafka_extra_config,
            self.prepend_workspace_id(consumer_group_id),
        )
