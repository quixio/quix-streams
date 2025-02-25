import logging
import pprint
import time
from asyncio import Future
from typing import Dict, List, Mapping, Optional, Union

from confluent_kafka.admin import (
    AdminClient,
    ConfigResource,
    KafkaException,
)
from confluent_kafka.admin import (
    TopicMetadata as ConfluentTopicMetadata,
)
from confluent_kafka.cimpl import NewTopic

from quixstreams.kafka import ConnectionConfig

from .exceptions import CreateTopicFailure, CreateTopicTimeout, TopicPermissionError
from .topic import Topic, TopicConfig

logger = logging.getLogger(__name__)

__all__ = ("TopicAdmin",)


def confluent_topic_config(topic: str) -> ConfigResource:
    return ConfigResource(2, topic)


class TopicAdmin:
    """
    For performing "admin"-level operations on a Kafka cluster, mostly around topics.

    Primarily used to create and inspect topic configurations.
    """

    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        logger: logging.Logger = logger,
        extra_config: Optional[Mapping] = None,
    ):
        """
        :param broker_address: Connection settings for Kafka.
            Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
            or a ConnectionConfig object if authentication is required.
        :param logger: a Logger instance to attach librdkafka logging to
        :param extra_config: optional configs (generally accepts producer configs)
        """
        if isinstance(broker_address, str):
            broker_address = ConnectionConfig(bootstrap_servers=broker_address)

        self._inner_admin: Optional[AdminClient] = None
        self._config = {
            **(extra_config or {}),
            **broker_address.as_librdkafka_dict(),
            "logger": logger,
        }

    @property
    def admin_client(self) -> AdminClient:
        if not self._inner_admin:
            self._inner_admin = AdminClient(self._config)
            # Poll the AdminClient once to trigger the OAuth callback
            # in case it's provided
            self._inner_admin.poll(0)
        return self._inner_admin

    def list_topics(self, timeout: float = -1) -> Dict[str, ConfluentTopicMetadata]:
        """
        Get a list of topics and their metadata from a Kafka cluster

        :param timeout: response timeout (seconds); Default infinite (-1)

        :return: a dict of topic names and their metadata objects
        """
        # TODO: allow filtering based on a prefix ignore list?
        return self.admin_client.list_topics(timeout=timeout).topics

    def inspect_topics(
        self,
        topic_names: List[str],
        timeout: float = 30,
    ) -> Dict[str, Optional[TopicConfig]]:
        """
        A simplified way of getting the topic configurations of the provided topics
        from the cluster (if they exist).

        :param topic_names: a list of topic names
        :param timeout: response timeout (seconds)

        >***NOTE***: `timeout` must be >0 here (expects non-neg, and 0 != inf).

        :return: a dict with topic names and their respective `TopicConfig`
        """
        futures_dict = {}
        cluster_topics = self.list_topics(timeout=timeout)
        if existing_topics := [
            topic for topic in topic_names if topic in cluster_topics
        ]:
            futures_dict = self.admin_client.describe_configs(
                [confluent_topic_config(topic) for topic in existing_topics],
                request_timeout=timeout,
            )
        configs = {}
        auth_failed_topics = []
        for config_resource, config in futures_dict.items():
            try:
                configs[config_resource.name] = {
                    c.name: c.value for c in config.result().values()
                }
            except KafkaException as e:
                if e.args[0].name() == "TOPIC_AUTHORIZATION_FAILED":
                    auth_failed_topics.append(config_resource.name)
                else:
                    raise
        if auth_failed_topics:
            failed_topics_str = ", ".join([f'"{t}"' for t in auth_failed_topics])
            raise TopicPermissionError(
                f"Failed to access configs for topics {failed_topics_str}; "
                f'verify the "DescribeConfigs" operation is allowed for your credentials'
            )

        return {
            topic: (
                TopicConfig(
                    num_partitions=len(cluster_topics[topic].partitions),
                    replication_factor=len(
                        cluster_topics[topic].partitions[0].replicas
                    ),
                    extra_config=configs[topic],
                )
                if topic in existing_topics
                else None
            )
            for topic in topic_names
        }

    def _finalize_create(
        self, futures: Dict[str, Future], finalize_timeout: float = 60
    ):
        """
        The confirmation step for topic creation.

        :param futures: a dict of futures as generated by Confluent's
            `AdminClient.create_topics()`
        :param finalize_timeout: topic finalization timeout (seconds)
        """
        exceptions = {}
        stop_time = time.time() + finalize_timeout
        while futures and time.time() < stop_time:
            time.sleep(1)
            for topic_name in list(futures.keys()):
                future = futures[topic_name]
                if future.done():
                    try:
                        future.result()
                        logger.info(f'Topic "{topic_name}" has been created')
                    except KafkaException as e:
                        error_name = e.args[0].name()
                        # Topic was maybe created by another instance
                        if error_name == "TOPIC_ALREADY_EXISTS":
                            logger.info(f'Topic "{topic_name}" already exists')
                        elif error_name == "TOPIC_AUTHORIZATION_FAILED":
                            exceptions[topic_name] = (
                                f'Failed to create topic "{topic_name}"; '
                                f'verify if "Create" and "Read" operations '
                                f"are allowed for your credentials"
                            )
                        else:
                            exceptions[topic_name] = e.args[0].str()
                    # Not sure how these get raised, but they are supposedly possible
                    except (TypeError, ValueError) as e:
                        exceptions[topic_name] = str(e)
                    del futures[topic_name]
        if exceptions:
            raise CreateTopicFailure(f"Failed to create topics: {exceptions}")
        if futures:
            raise CreateTopicTimeout(
                f"Timed out waiting for creation status for topics:\n"
                f"{pprint.pformat([topic_name for topic_name in futures])}"
            )

    def create_topics(
        self, topics: List[Topic], timeout: float = 30, finalize_timeout: float = 60
    ):
        """
        Create the given list of topics and confirm they are ready.

        Also raises an exception with detailed printout should the creation
        fail (it ignores issues for a topic already existing).

        :param topics: a list of `Topic`
        :param timeout: creation acknowledge timeout (seconds)
        :param finalize_timeout: topic finalization timeout (seconds)

        >***NOTE***: `timeout` must be >0 here (expects non-neg, and 0 != inf).
        """

        existing_topics = self.list_topics(timeout=timeout)
        topics_to_create = [
            topic for topic in topics if topic.name not in existing_topics
        ]
        if not topics_to_create:
            return

        for topic in topics_to_create:
            logger.info(
                f'Creating a new topic "{topic.name}" '
                f'with a config: "{topic.create_config.as_dict() if topic.create_config is not None else {}}"'
            )

        confl_new_topics = []
        for topic in topics_to_create:
            if topic.create_config is None:
                confl_new_topic = NewTopic(topic=topic.name)
            else:
                confl_new_topic = NewTopic(
                    topic=topic.name,
                    num_partitions=topic.create_config.num_partitions,
                    replication_factor=topic.create_config.replication_factor,
                    config=topic.create_config.extra_config,
                )
            confl_new_topics.append(confl_new_topic)

        self._finalize_create(
            self.admin_client.create_topics(
                confl_new_topics,
                request_timeout=timeout,
            ),
            finalize_timeout=finalize_timeout,
        )

    # support pickling by dropping the inner admin
    def __getstate__(self) -> object:
        state = self.__dict__.copy()
        state.pop("_inner_admin", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._inner_admin = None
