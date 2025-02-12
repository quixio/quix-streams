import datetime
import logging
from typing import Optional

from quixstreams.models import Topic
from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    Source,
)

from .consumer import PubSubConsumer, PubSubMessage

__all__ = ("PubSubSource",)


logger = logging.getLogger(__name__)


class PubSubSource(Source):
    """
    This source enables reading from a Google Cloud Pub/Sub topic,
    dumping it to a kafka topic using desired SDF-based transformations.

    Provides "at-least-once" guarantees.

    Currently, forwarding message keys ("ordered messages" in Pub/Sub) is unsupported.

    The incoming message value will be in bytes, so transform in your SDF accordingly.

    Example Usage:

    ```python
    from quixstreams import Application
    from quixstreams.sources.community.pubsub import PubSubSource
    from os import environ

    source = PubSubSource(
        # Suggested: pass JSON-formatted credentials from an environment variable.
        service_account_json = environ["PUBSUB_SERVICE_ACCOUNT_JSON"],
        project_id="<project ID>",
        topic_id="<topic ID>",  # NOTE: NOT the full /x/y/z path!
        subscription_id="<subscription ID>",  # NOTE: NOT the full /x/y/z path!
        create_subscription=True,
    )
    app = Application(
        broker_address="localhost:9092",
        auto_offset_reset="earliest",
        consumer_group="gcp",
        loglevel="INFO"
    )
    sdf = app.dataframe(source=source).print(metadata=True)

    if __name__ == "__main__":
        app.run()
    ```
    """

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        subscription_id: str,
        service_account_json: Optional[str] = None,
        commit_every: int = 100,
        commit_interval: float = 5.0,
        create_subscription: bool = False,
        enable_message_ordering: bool = False,
        shutdown_timeout: float = 10.0,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param project_id: a Google Cloud project ID.
        :param topic_id: a Pub/Sub topic ID (NOT the full path).
        :param subscription_id: a Pub/Sub subscription ID (NOT the full path).
        :param service_account_json: a Google Cloud Credentials JSON as a string
            Can instead use environment variables (which have different behavior):
            - "GOOGLE_APPLICATION_CREDENTIALS" set to a JSON filepath i.e. /x/y/z.json
            - "PUBSUB_EMULATOR_HOST" set to a URL if using an emulated Pub/Sub
        :param commit_every: max records allowed to be processed before committing.
        :param commit_interval: max allowed elapsed time between commits.
        :param create_subscription: whether to attempt to create a subscription at
            startup; if it already exists, it instead logs its details (DEBUG level).
        :param enable_message_ordering: When creating a Pub/Sub subscription, whether
            to allow message ordering. NOTE: does NOT affect existing subscriptions!
        :param shutdown_timeout: How long to wait for a graceful shutdown of the source.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            name=subscription_id,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._credentials = service_account_json
        self._project_id = project_id
        self._topic_id = topic_id
        self._subscription_id = subscription_id
        self._commit_every = commit_every
        self._commit_interval = commit_interval
        self._create_subscription = create_subscription
        self._enable_message_ordering = enable_message_ordering
        self._client: Optional[PubSubConsumer] = None

    def default_topic(self) -> Topic:
        return Topic(
            name=f"pubsub_{self._subscription_id}_{self._topic_id}",
            key_deserializer="str",
            value_deserializer="bytes",
            key_serializer="str",
            value_serializer="bytes",
        )

    def _handle_pubsub_message(self, message: PubSubMessage):
        timestamp: datetime.datetime = message.publish_time
        kafka_msg = self.serialize(
            key=message.ordering_key,  # an empty string if ordering not enabled
            value=message.data,
            timestamp_ms=int(timestamp.timestamp() * 1000),
        )
        self.produce(
            key=kafka_msg.key,
            value=kafka_msg.value,
            timestamp=kafka_msg.timestamp,
            headers=dict(message.attributes),
        )

    def setup(self):
        self._client = PubSubConsumer(
            credentials=self._credentials,
            project_id=self._project_id,
            topic_id=self._topic_id,
            subscription_id=self._subscription_id,
            create_subscription=self._create_subscription,
            enable_message_ordering=self._enable_message_ordering,
            async_function=self._handle_pubsub_message,
            max_batch_size=self._commit_every,
            batch_timeout_secs=self._commit_interval,
        ).__enter__()

    def run(self):
        with self._client as consumer:
            while self._running:
                consumer.poll_and_process_batch()
                self.flush()
                consumer.commit()
