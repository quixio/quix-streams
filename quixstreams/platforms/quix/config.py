import dataclasses
import logging
import time
from os import getcwd
from pathlib import Path
from tempfile import gettempdir
from typing import Optional, Tuple, List, Iterable, Set, Mapping, Union, Dict

from requests import HTTPError

from quixstreams.exceptions import QuixException
from quixstreams.models import Topic
from .api import QuixPortalApiService

logger = logging.getLogger(__name__)

__all__ = (
    "QuixKafkaConfigsBuilder",
    "TopicCreationConfigs",
)

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
        quix_portal_api_service: Optional[QuixPortalApiService] = None,
        workspace_id: Optional[str] = None,
        workspace_cert_path: Optional[str] = None,
    ):
        """
        :param quix_portal_api_service: A QuixPortalApiService instance (else generated)
        :param workspace_id: A valid Quix Workspace ID (else searched for)
        :param workspace_cert_path: path to an existing workspace cert (else retrieved)
        """
        self.api = quix_portal_api_service or QuixPortalApiService(
            default_workspace_id=workspace_id
        )
        try:
            self._workspace_id = workspace_id or self.api.default_workspace_id
        except self.api.UndefinedQuixWorkspaceId:
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

        # TODO: consider a class extension with stuff that's only for Application.Quix
        # since this is slowly building up.
        # Application.Quix only
        self.app_auto_create_topics: bool = True
        self.create_topic_configs: Dict[str, TopicCreationConfigs] = {}

    class NoWorkspaceFound(QuixException):
        ...

    class MultipleWorkspaces(QuixException):
        ...

    class MissingQuixTopics(QuixException):
        ...

    class CreateTopicTimeout(QuixException):
        ...

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

    def strip_workspace_id(self, s: str) -> str:
        return (
            s[len(self._workspace_id) + 1 :] if s.startswith(self._workspace_id) else s
        )

    def append_workspace_id(self, s: str) -> str:
        """
        Add the workspace ID to a given string, typically a topic or consumer group id

        :param s: the string to append to
        :return: the string with workspace_id appended
        """
        return f"{self.workspace_id}-{s}" if not s.startswith(self.workspace_id) else s

    def search_for_workspace(
        self, workspace_name_or_id: Optional[str] = None
    ) -> Optional[dict]:
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
        try:
            self._quix_broker_settings = ws_data.pop("brokerSettings")
        except KeyError:  # hold-over for platform v1
            self._quix_broker_settings = {
                "brokerType": ws_data["brokerType"],
                "syncTopics": False,
            }
        self._workspace_meta = ws_data

    def search_workspace_for_topic(
        self, workspace_id: str, topic: str
    ) -> Optional[str]:
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

    def search_for_topic_workspace(self, topic: str) -> Optional[dict]:
        """
        Find what workspace a topic belongs to.
        If there is only one workspace altogether, it is assumed to be the workspace.
        More than one means each workspace will be searched until the first hit.

        :param topic: the topic to search for
        :return: workspace data dict if topic search success, else None
        """
        ws_list = self.api.get_workspaces()
        if len(ws_list) == 1:
            return ws_list[0]
        if topic is None:
            raise self.MultipleWorkspaces(
                "More than 1 workspace was found, so you must provide a topic name "
                "to find the correct workspace"
            )
        for ws in ws_list:
            if self.search_workspace_for_topic(ws["workspaceId"], topic):
                return ws

    def get_workspace_ssl_cert(
        self, extract_to_folder: Optional[Path] = None
    ) -> Optional[str]:
        """
        Gets and extracts zipped certificate from the API to provided folder if the
        SSL certificate is specified in broker configuration.

        If no path was provided, will dump to /tmp. Expects cert named 'ca.cert'.

        :param extract_to_folder: path to folder to dump zipped cert file to
        :return: full cert filepath as string or `None` if certificate is not specified
        """
        certificate_bytes = self.api.get_workspace_certificate(
            workspace_id=self._workspace_id
        )
        if certificate_bytes is None:
            return
        extract_to_folder = extract_to_folder or Path(gettempdir())
        full_path = extract_to_folder / "ca.cert"
        if not full_path.is_file():
            extract_to_folder.mkdir(parents=True, exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(certificate_bytes)
        return full_path.as_posix()

    def _create_topic(self, topic: TopicCreationConfigs):
        """
        The actual API call to create the topic

        :param topic: a TopicCreationConfigs instance
        """
        topic_name = self.strip_workspace_id(topic.name)
        # an exception is raised (status code) if topic is not created successfully
        self.api.post_topic(
            topic_name=topic_name,
            workspace_id=self.workspace_id,
            topic_partitions=topic.num_partitions,
            topic_rep_factor=topic.replication_factor,
            topic_ret_bytes=topic.retention_bytes,
            topic_ret_minutes=topic.retention_minutes,
        )
        logger.info(
            f"Creation of topic {topic_name} acknowledged by broker. Must wait "
            f"for 'Ready' status before topic is actually available"
        )

    def _finalize_create(self, topics: Set[str], timeout: Optional[int] = None):
        """
        After the broker acknowledges the topics are created, they will be in a
        "Creating", and will not be ready to consume from/produce to until they are
        set to a status of "Ready". This will block until all topics passed are marked
        as "Ready" or the timeout is hit.

        :param topics: set of topic names
        :param timeout: amount of seconds allowed to finalize, else raise exception
        """
        stop_time = time.time() + (timeout or len(topics) * 30)
        while topics and time.time() < stop_time:
            # Each topic seems to take 10-15 seconds each to finalize (at least in dev)
            time.sleep(1)
            for topic in [t for t in self.get_topics() if t["id"] in topics.copy()]:
                if topic["status"] == "Ready":
                    logger.debug(f"Topic {topic['name']} creation finalized")
                    topics.remove(topic["id"])
        if topics:
            raise self.CreateTopicTimeout(
                f"Creation succeeded, but waiting for 'Ready' status timed out "
                f"for topics: {[self.strip_workspace_id(t) for t in topics]}"
            )

    def create_topics(
        self,
        topics: Iterable[TopicCreationConfigs],
        finalize_timeout_seconds: Optional[int] = None,
    ):
        """
        Create topics in a Quix cluster.

        :param topics: an iterable with TopicCreationConfigs instances
        :param finalize_timeout_seconds: How long to wait for the topics to be
        marked as "Ready" (and thus ready to produce to/consume from).
        """
        logger.info("Attempting to create topics...")
        current_topics = {t["id"]: t for t in self.get_topics()}
        finalize = set()
        for topic in topics:
            topic_name = self.append_workspace_id(topic.name)
            exists = self.append_workspace_id(topic_name) in current_topics
            if not exists or current_topics[topic_name]["status"] != "Ready":
                if exists:
                    logger.debug(
                        f"Topic {self.strip_workspace_id(topic_name)} exists but does "
                        f"not have 'Ready' status. Added to finalize check."
                    )
                else:
                    try:
                        self._create_topic(topic)
                    except HTTPError as e:
                        # Topic was maybe created by another instance
                        if "already exists" not in e.response.text:
                            raise
                finalize.add(topic_name)
            else:
                logger.debug(
                    f"Topic {self.strip_workspace_id(topic_name)} exists and is Ready"
                )
        logger.info(
            "Topic creations acknowledged; waiting for 'Ready' statuses..."
            if finalize
            else "No topic creations required!"
        )
        self._finalize_create(finalize, timeout=finalize_timeout_seconds)

    def get_topics(self) -> List[dict]:
        return self.api.get_topics(workspace_id=self.workspace_id)

    def confirm_topics_exist(
        self, topics: Iterable[Union[Topic, TopicCreationConfigs]]
    ):
        """
        Confirm whether the desired set of topics exists in the Quix workspace.

        :param topics: an iterable with Either Topic or TopicCreationConfigs instances
        """
        logger.info("Confirming required topics exist...")
        current_topics = [t["id"] for t in self.get_topics()]
        missing_topics = []
        for topic in topics:
            if topic.name not in current_topics:
                missing_topics.append(self.strip_workspace_id(topic.name))
            else:
                logger.debug(f"Topic {self.strip_workspace_id(topic.name)} confirmed!")
        if missing_topics:
            raise self.MissingQuixTopics(f"Topics do no exist: {missing_topics}")

    def _set_workspace_cert(self) -> str:
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

        cfg_out["ssl.endpoint.identification.algorithm"] = "none"
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
        self, topics: list, consumer_group_id: Optional[str] = None
    ) -> Tuple[dict, List[str], Optional[str]]:
        """
        Get all the values you need in order to use a confluent_kafka-based client
        with a topic on a Quix platform broker/workspace.

        The returned config can be used directly by any confluent-kafka-python consumer/
        producer (add your producer/consumer-specific configs afterward).

        The topics and consumer group are appended with any necessary values.

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
