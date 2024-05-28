import base64
import dataclasses
import logging
import time
from os import getcwd
from pathlib import Path
from tempfile import gettempdir
from typing import Optional, Tuple, Set, Mapping, Union, List

from requests import HTTPError

from quixstreams.models.topics import Topic
from quixstreams.kafka.configuration import ConnectionConfig
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

__all__ = ("QuixKafkaConfigsBuilder",)

QUIX_CONNECTIONS_MAX_IDLE_MS = 3 * 60 * 1000
QUIX_METADATA_MAX_AGE_MS = 3 * 60 * 1000


# Map Kafka configuration params from Quix format to librdkafka's
_QUIX_PARAMS_NAMES_MAP = {
    "saslMechanism": "sasl.mechanisms",
    "securityMode": "security.protocol",
    "address": "bootstrap.servers",
    "username": "sasl.username",
    "password": "sasl.password",
}

# Map values of `sasl.mechanisms` from Quix format to librdkafka's
_QUIX_SASL_MECHANISM_MAP = {
    "ScramSha256": "SCRAM-SHA-256",
    "ScramSha512": "SCRAM-SHA-512",
    "Gssapi": "GSSAPI",
    "Plain": "PLAIN",
    "OAuthBearer": "OAUTHBEARER",
}

# Map values of `security.protocol` from Quix format to librdkafka's
_QUIX_SECURITY_PROTOCOL_MAP = {
    "Ssl": "ssl",
    "SaslSsl": "sasl_ssl",
    "Sasl": "sasl_plaintext",
    "PlainText": "plaintext",
}


@dataclasses.dataclass
class TopicCreationConfigs:
    name: Optional[str] = None  # Required when not created by a Quix App.
    num_partitions: int = 1
    replication_factor: Optional[int] = None
    retention_bytes: Optional[int] = None
    retention_minutes: Optional[int] = None
    optionals: Optional[Mapping] = None


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
        workspace_cert_path: Optional[str] = None,
        quix_portal_api_service: Optional[QuixPortalApiService] = None,
        timeout: float = 30,
        topic_create_timeout: float = 60,
    ):
        """
        :param quix_portal_api_service: A QuixPortalApiService instance (else generated)
        :param workspace_id: A valid Quix Workspace ID (else searched for)
        :param workspace_cert_path: path to an existing workspace cert (else retrieved)
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
                "No workspace ID was provided directly or found via environment; "
                "if there happens to be only one valid workspace for your provided "
                "auth token (often the case with 'streaming' aka 'SDK' tokens), "
                "then this will find and use that ID. Otherwise, you may need to "
                "provide a known topic name later on to help find the applicable ID."
            )
        self._workspace_cert_path = workspace_cert_path
        self._confluent_broker_config = None
        self._quix_broker_config = None
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
    def quix_broker_config(self) -> dict:
        if not self._quix_broker_config:
            self.get_workspace_info()
        return self._quix_broker_config

    @property
    def quix_broker_settings(self) -> dict:
        if not self._quix_broker_settings:
            self.get_workspace_info()
        return self._quix_broker_settings

    @property
    def confluent_broker_config(self) -> dict:
        if not self._confluent_broker_config:
            self.get_confluent_broker_config()
        return self._confluent_broker_config

    @property
    def workspace_cert_path(self) -> str:
        if not self._workspace_cert_path:
            self._set_workspace_cert()
        return self._workspace_cert_path

    @property
    def workspace_meta(self) -> dict:
        if not self._workspace_meta:
            self.get_workspace_info()
        return self._workspace_meta

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

    def get_workspace_info(
        self,
        known_workspace_topic: Optional[str] = None,
        timeout: Optional[float] = None,
    ):
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
        self._workspace_id = ws_data.pop("workspaceId")
        self._quix_broker_config = ws_data.pop("broker")
        try:
            self._quix_broker_settings = ws_data.pop("brokerSettings")
        except KeyError:  # hold-over for platform v1
            self._quix_broker_settings = {
                "brokerType": ws_data["brokerType"],
                "syncTopics": False,
            }
        self._workspace_meta = ws_data

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

    def _write_ssl_cert(self, cert: bytes, folder: Optional[Path] = None) -> str:
        folder = folder or Path(gettempdir())
        full_path = folder / "ca.cert"
        if not full_path.is_file():
            folder.mkdir(parents=True, exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(cert)
        return full_path.as_posix()

    def get_workspace_ssl_cert(
        self, extract_to_folder: Optional[Path] = None, timeout: Optional[float] = None
    ) -> Optional[str]:
        """
        Gets and extracts zipped certificate from the API to provided folder if the
        SSL certificate is specified in broker configuration.

        If no path was provided, will dump to /tmp. Expects cert named 'ca.cert'.

        :param extract_to_folder: path to folder to dump zipped cert file to
        :param timeout: response timeout (seconds); Default 30

        :return: full cert filepath as string or `None` if certificate is not specified
        """
        if (cert := self.api.get_workspace_certificate(
                workspace_id=self._workspace_id,
                timeout=timeout if timeout is not None else self._timeout
        )) is not None:
            return self._write_ssl_cert(cert, extract_to_folder)

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

    def get_topic(self, topic_name: str) -> Optional[dict]:
        """
        return the topic ID (the actual cluster topic name) if it exists, else None

        >***NOTE***: if the name registered in Quix is instead the workspace-prefixed
        version, this returns None unless that exact name was created WITHOUT the
        Quix API.

        :param topic_name: name of the topic

        :return: response dict of the topic info if topic found, else None
        """
        try:
            return self.api.get_topic(topic_name, workspace_id=self.workspace_id)
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

    def _set_workspace_cert(
            self, cert: Optional[bytes] = None, timeout: Optional[float] = None
    ) -> str:
        """
        Will create a cetimeout=timeout if timeout is not None else self._timeoutrt and assigns it to the workspace_cert_path property.
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

        if cert is not None:
            self._workspace_cert_path = self._write_ssl_cert(cert, folder)
        else:
            self._workspace_cert_path = self.get_workspace_ssl_cert(folder, timeout)
        return self._workspace_cert_path

    def get_confluent_broker_config(
        self, known_topic: Optional[str] = None, timeout: Optional[float] = None
    ) -> dict:
        """
        Get the full client config dictionary required to authenticate a confluent-kafka
        client to a Quix platform broker/workspace.

        The returned config can be used directly by any confluent-kafka-python consumer/
        producer (add your producer/consumer-specific configs afterward).

        :param known_topic: a topic known to exist in some workspace
        :param timeout: response timeout (seconds); Default 30

        :return: a dict of confluent-kafka-python client settings (see librdkafka
        config for more details)
        """
        self.get_workspace_info(known_workspace_topic=known_topic, timeout=timeout)
        cfg_out = {}
        for quix_param_name, rdkafka_param_name in _QUIX_PARAMS_NAMES_MAP.items():
            # Map broker config received from Quix to librdkafka format
            param_value = self.quix_broker_config[quix_param_name]

            # Also map values of "security.protocol" and "sasl.mechanisms" from Quix
            # to librdkafka format
            if rdkafka_param_name == "security.protocol":
                param_value = _QUIX_SECURITY_PROTOCOL_MAP[param_value]
            elif rdkafka_param_name == "sasl.mechanisms":
                param_value = _QUIX_SASL_MECHANISM_MAP[param_value]
            cfg_out[rdkafka_param_name] = param_value

        # Specify SSL certificate if it's provided for the broker
        ssl_cert_path = self._set_workspace_cert()
        if ssl_cert_path is not None:
            cfg_out["ssl.ca.location"] = ssl_cert_path

        # Set the connection idle timeout and metadata max age to be less than
        # Azure's default 4 minutes.
        # Azure LB kills the inbound TCP connections after 4 mins and these settings
        # help to handle that.
        # More about this issue:
        # - https://github.com/confluentinc/librdkafka/issues/3109
        # - https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations#producer-and-consumer-configurations-1
        # These values can be overwritten on the Application level by passing
        # `extra_consumer_config` or `extra_producer_config` parameters.
        cfg_out["connections.max.idle.ms"] = QUIX_CONNECTIONS_MAX_IDLE_MS
        cfg_out["metadata.max.age.ms"] = QUIX_METADATA_MAX_AGE_MS
        self._confluent_broker_config = cfg_out
        return self._confluent_broker_config

    def get_confluent_client_configs(
        self,
        topics: list,
        consumer_group_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[dict, List[str], Optional[str]]:
        """
        Get all the values you need in order to use a confluent_kafka-based client
        with a topic on a Quix platform broker/workspace.

        The returned config can be used directly by any confluent-kafka-python consumer/
        producer (add your producer/consumer-specific configs afterward).

        The topics and consumer group are appended with any necessary values.

        :param topics: list of topics
        :param consumer_group_id: consumer group id, if needed
        :param timeout: response timeout (seconds); Default 30

        :return: a tuple with configs and altered versions of the topics
        and consumer group name
        """
        return (
            self.get_confluent_broker_config(topics[0], timeout=timeout),
            [self.prepend_workspace_id(t) for t in topics],
            self.prepend_workspace_id(consumer_group_id) if consumer_group_id else None,
        )

    def get_librdkafka_broker_config(self) -> ConnectionConfig:
        """
        Get the full client config dictionary required to authenticate a confluent-kafka
        client to a Quix platform broker/workspace as a ConnectionConfig

        :return: a ConnectionConfig object
        """
        librdkafka_dict = self.api.get_librdkafka_broker_config(self.workspace_id)
        if (cert := librdkafka_dict.pop("ssl.ca.cert", None)) is not None:
            cert = base64.b64decode(cert)
            librdkafka_dict["ssl.ca.location"] = self._set_workspace_cert(cert)
        return ConnectionConfig.from_librdkafka_dict(librdkafka_dict)

    def get_librdkafka_extra_configs(self) -> dict:
        # Set the connection idle timeout and metadata max age to be less than
        # Azure's default 4 minutes.
        # Azure LB kills the inbound TCP connections after 4 mins and these settings
        # help to handle that.
        # More about this issue:
        # - https://github.com/confluentinc/librdkafka/issues/3109
        # - https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations#producer-and-consumer-configurations-1
        # These values can be overwritten on the Application level by passing
        # `extra_consumer_config` or `extra_producer_config` parameters.
        return {
            "connections.max.idle.ms": QUIX_CONNECTIONS_MAX_IDLE_MS,
            "metadata.max.age.ms": QUIX_METADATA_MAX_AGE_MS,
        }

    def get_application_config(
        self, consumer_group_id: str
    ) -> Tuple[ConnectionConfig, dict, Optional[str]]:
        """
        Get all the necessary attributes for an Application to run on Quix Cloud.

        :param consumer_group_id: consumer group id, if needed
        :return: a tuple with broker configs, extra configs, and consumer_group
        and consumer group name
        """
        return (
            self.get_librdkafka_broker_config(),
            self.get_librdkafka_extra_configs(),
            self.prepend_workspace_id(consumer_group_id),
        )
