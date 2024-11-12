import logging

from proto.datetime_helpers import DatetimeWithNanoseconds

from quixstreams.models import Topic
from quixstreams.sources import Source

from .components import GCPPubSubConfig
from .components.gcp_consumer import GCPPubSubConsumer

__all__ = ("GCPPubSubSource",)


logger = logging.getLogger(__name__)


class GCPPubSubSource(Source):
    """
    This source enables reading from a Google Cloud Platform (GCP) PubSub topic,
    dumping it to a kafka topic using desired SDF-based transformations.

    Provides "at-least-once" guarantees.

    Currently, forwarding message keys ("ordered messages" in GCP) is unsupported.

    The incoming message value will be in bytes, so transform in your SDF accordingly.

    Example Usage:

    ```python
    from quixstreams import Application
    from quixstreams.sources.community.gcp_pubsub import GCPPubSubConfig, GCPPubSubSource

    # for authorization
    config = GCPPubSubConfig(
        credentials_path="/path/to/google/auth/creds.json"
    )
    source = GCPPubSubSource(
        config=config,
        project_id="my_gcp_project",
        topic_name="my_gcp_pubsub_topic_name",  # NOTE: NOT the full GCP path!
        subscription_name="my_gcp_pubsub_topic_name",  # NOTE: NOT the full GCP path!
        create_subscription=True,
    )
    app = Application(
        broker_address="localhost:9092",
        auto_offset_reset="earliest",
        consumer_group="gcp",
        loglevel="DEBUG"
    )
    sdf = app.dataframe(source=source).print(metadata=True)

    if __name__ == "__main__":
        app.run()
    ```
    """

    def __init__(
        self,
        config: GCPPubSubConfig,
        project_id: str,
        topic_name: str,
        subscription_name: str,
        commit_every: int = 100,
        commit_interval: float = 5.0,
        create_subscription: bool = False,
        shutdown_timeout: float = 10.0,
    ):
        """
        :param config: a CGPPubSubConfig (authentication).
        :param project_id: a GCP project ID.
        :param topic_name: a GCP PubSub topic name (NOT the full path).
        :param subscription_name: a GCP PubSub subscription name (NOT the full path).
        :param commit_every: max records allowed to be processed before committing.
        :param commit_interval: max allowed elapsed time between commits.
        :param create_subscription: whether to attempt to create a subscription at
            startup; if it already exists, it instead logs its details (DEBUG level).
        :param shutdown_timeout: How long to wait for a graceful shutdown of the source.
        """
        self._config = config
        self._project_id = project_id
        self._topic_name = topic_name
        self._subscription_name = subscription_name
        self._commit_every = commit_every
        self._commit_interval = commit_interval
        self._create_subscription = create_subscription
        super().__init__(
            name=subscription_name,
            shutdown_timeout=shutdown_timeout,
        )

    def default_topic(self) -> Topic:
        return Topic(
            name=f"gcp-pubsub_{self._subscription_name}_{self._topic_name}",
            key_deserializer="str",
            value_deserializer="bytes",
            key_serializer="str",
            value_serializer="bytes",
        )

    def _handle_pubsub_item(self, message):
        timestamp: DatetimeWithNanoseconds = message.publish_time
        kafka_msg = self.serialize(
            key=message.ordering_key or None,
            value=message.data,
            timestamp_ms=int(timestamp.timestamp() * 1000),
        )
        self.produce(
            key=kafka_msg.key,
            value=kafka_msg.value,
            timestamp=kafka_msg.timestamp,
        )

    def run(self):
        with GCPPubSubConsumer(
            config=self._config,
            project_id=self._project_id,
            topic_name=self._topic_name,
            subscription_name=self._subscription_name,
            create_subscription=self._create_subscription,
            async_function=self._handle_pubsub_item,
            default_poll_timeout=self._commit_interval,
            max_batch_size=self._commit_every,
        ) as consumer:
            while self._running:
                consumer.poll()
                self.flush()
                consumer.commit()
