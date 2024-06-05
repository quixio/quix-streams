import base64
import dataclasses
import logging
import time
from copy import deepcopy
from typing import Optional, Set, Union, List

from requests import HTTPError

from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.models.topics import Topic
from .api import QuixPortalApiService
from .exceptions import (
    NoWorkspaceFound,
    MultipleWorkspaces,
    MissingQuixTopics,
    UndefinedQuixWorkspaceId,
    QuixCreateTopicFailure,
    QuixCreateTopicTimeout,
    QuixApiRequestFailure,
)

logger = logging.getLogger(__name__)

__all__ = ("QuixKafkaConfigsBuilder", "QuixApplicationConfig")

QUIX_CONNECTIONS_MAX_IDLE_MS = 3 * 60 * 1000
QUIX_METADATA_MAX_AGE_MS = 3 * 60 * 1000


def strip_workspace_id_prefix(workspace_id: str, s: str) -> str:
    """
    Remove the workspace ID from a given string if it starts with it,
    typically a topic or consumer group id

    :param workspace_id: the workspace id
    :param s: the string to append to
    :return: the string with workspace_id prefix removed
    """
    return s[len(workspace_id) + 1 :] if s.startswith(workspace_id) else s


def prepend_workspace_id(workspace_id: str, s: str) -> str:
    """
    Add the workspace ID as a prefix to a given string if it does not have it,
    typically a topic or consumer group it

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
    consumer_group: Optional[str] = None


class QuixKafkaConfigsBuilder:
    """
    Retrieves all the necessary information from the Quix API and builds all the
    objects required to connect a confluent-kafka client to the Quix Platform.

    If not executed within the Quix platform directly, you must provide a Quix
    "streaming" (aka "sdk") token, or Personal Access Token.

    Ideally you also know your workspace name or id. If not, you can search for it
    using a known topic name, but note the search space is limited to the access level
    of your token.

    It also currently handles the app_auto_create_topics setting for Application.Quix.
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
            self._workspace_id = None
            logger.warning(
                "'workspace_id' argument was not provided nor set with "
                "'Quix__Workspace__Id' environment; if only one Workspace ID for the "
                "provided auth token exists (often true with SDK tokens), "
                "then that ID will be used. Otherwise, provide a known topic name to "
                "method 'get_workspace_info(topic)' to obtain desired Workspace ID."
            )
        self._librdkafka_connect_config = None
        self._quix_broker_settings = None
        self._workspace_meta = None
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

    def strip_workspace_id_prefix(self, s: str) -> str:
        """
        Remove the workspace ID from a given string if it starts with it,
        typically a topic or consumer group id

        :param s: the string to append to
        :return: the string with workspace_id prefix removed
        """
        return strip_workspace_id_prefix(self.workspace_id, s)

    def prepend_workspace_id(self, s: str) -> str:
        """
        Add the workspace ID as a prefix to a given string if it does not have it,
        typically a topic or consumer group it

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

    def search_for_topic_workspace(
        self, topic: str, timeout: Optional[float] = None
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

    def _create_topic(self, topic: Topic, timeout: Optional[float] = None):
        """
        The actual API call to create the topic

        :param topic: a Topic instance
        """
        topic_name = self.strip_workspace_id_prefix(topic.name)
        cfg = topic.config

        # settings that must be ints or Nones
        ret_ms = cfg.extra_config.get("retention.ms")
        ret_bytes = cfg.extra_config.get("retention.bytes")

        # an exception is raised (status code) if topic is not created successfully
        self.api.post_topic(
            topic_name=topic_name,
            workspace_id=self.workspace_id,
            topic_partitions=cfg.num_partitions,
            topic_rep_factor=cfg.replication_factor,
            topic_ret_bytes=ret_bytes if ret_bytes is None else int(ret_bytes),
            topic_ret_minutes=ret_ms if ret_ms is None else int(ret_ms) // 60000,
            cleanup_policy=cfg.extra_config.get("cleanup.policy"),
            timeout=timeout if timeout is not None else self._timeout,
        )
        logger.info(
            f"Creation of topic {topic_name} acknowledged by broker. Must wait "
            f"for 'Ready' status before topic is actually available"
        )

    def _finalize_create(
        self,
        topics: Set[str],
        timeout: Optional[float] = None,
        finalize_timeout: Optional[float] = None,
    ):
        """
        After the broker acknowledges the topics are created, they will be in a
        "Creating", and will not be ready to consume from/produce to until they are
        set to a status of "Ready". This will block until all topics passed are marked
        as "Ready" or the timeout is hit.

        :param topics: set of topic names
        :param finalize_timeout: topic finalization timeout (seconds); Default 60
        """
        exceptions = {}
        stop_time = time.time() + (
            finalize_timeout if finalize_timeout else self._topic_create_timeout
        )
        while topics and time.time() < stop_time:
            # Each topic seems to take 10-15 seconds each to finalize (at least in dev)
            time.sleep(1)
            for topic in [
                t for t in self.get_topics(timeout=timeout) if t["id"] in topics.copy()
            ]:
                if topic["status"] == "Ready":
                    logger.debug(f"Topic {topic['name']} creation finalized")
                    topics.remove(topic["id"])
                elif topic["status"] == "Error":
                    logger.debug(f"Topic {topic['name']} encountered an error")
                    exceptions[topic["name"]] = topic["lastError"]
                    topics.remove(topic["id"])
        if exceptions:
            raise QuixCreateTopicFailure(f"Failed to create Quix topics: {exceptions}")
        if topics:
            raise QuixCreateTopicTimeout(
                f"Creation succeeded, but waiting for 'Ready' status timed out "
                f"for topics: {[self.strip_workspace_id_prefix(t) for t in topics]}"
            )

    def create_topics(
        self,
        topics: List[Topic],
        timeout: Optional[float] = None,
        finalize_timeout: Optional[float] = None,
    ):
        """
        Create topics in a Quix cluster.

        :param topics: a list of `Topic` objects
        :param timeout: response timeout (seconds); Default 30
        :param finalize_timeout: topic finalization timeout (seconds); Default 60
        marked as "Ready" (and thus ready to produce to/consume from).
        """
        logger.info("Attempting to create topics...")
        current_topics = {t["id"]: t for t in self.get_topics(timeout=timeout)}
        finalize = set()
        for topic in topics:
            topic_name = self.prepend_workspace_id(topic.name)
            exists = self.prepend_workspace_id(topic_name) in current_topics
            if not exists or current_topics[topic_name]["status"] != "Ready":
                if exists:
                    logger.debug(
                        f"Topic {self.strip_workspace_id_prefix(topic_name)} exists but does "
                        f"not have 'Ready' status. Added to finalize check."
                    )
                else:
                    try:
                        self._create_topic(topic, timeout=timeout)
                    # TODO: more robust error handling to better identify issues
                    # See how it's handled in the admin client and maybe consolidate
                    # logic via TopicManager
                    except HTTPError as e:
                        # Topic was maybe created by another instance
                        if "already exists" not in e.response.text:
                            raise
                finalize.add(topic_name)
            else:
                logger.debug(
                    f"Topic {self.strip_workspace_id_prefix(topic_name)} exists and is Ready"
                )
        logger.info(
            "Topic creations acknowledged; waiting for 'Ready' statuses..."
            if finalize
            else "No topic creations required!"
        )
        self._finalize_create(
            finalize, timeout=timeout, finalize_timeout=finalize_timeout
        )

    def get_topic(
        self, topic_name: str, timeout: Optional[float] = None
    ) -> Optional[dict]:
        """
        return the topic ID (the actual cluster topic name) if it exists, else None

        >***NOTE***: if the name registered in Quix is instead the workspace-prefixed
        version, this returns None unless that exact name was created WITHOUT the
        Quix API.

        :param topic_name: name of the topic
        :param timeout: response timeout (seconds); Default 30

        :return: response dict of the topic info if topic found, else None
        """
        try:
            return self.api.get_topic(
                topic_name,
                workspace_id=self.workspace_id,
                timeout=timeout if timeout is not None else self._timeout,
            )
        except QuixApiRequestFailure as e:
            if e.status_code == 404:
                return
            raise

    def get_topics(self, timeout: Optional[float] = None) -> List[dict]:
        return self.api.get_topics(
            workspace_id=self.workspace_id,
            timeout=timeout if timeout is not None else self._timeout,
        )

    def confirm_topics_exist(
        self, topics: Union[List[Topic], List[str]], timeout: Optional[float] = None
    ):
        """
        Confirm whether the desired set of topics exists in the Quix workspace.

        :param topics: a list of `Topic` or topic names
        :param timeout: response timeout (seconds); Default 30
        """
        if isinstance(topics[0], Topic):
            topics = [topic.name for topic in topics]
        logger.info("Confirming required topics exist...")
        current_topics = [t["id"] for t in self.get_topics(timeout=timeout)]
        missing_topics = []
        for name in topics:
            if name not in current_topics:
                missing_topics.append(self.strip_workspace_id_prefix(name))
            else:
                logger.debug(f"Topic {self.strip_workspace_id_prefix(name)} confirmed!")
        if missing_topics:
            raise MissingQuixTopics(f"Topics do no exist: {missing_topics}")

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
