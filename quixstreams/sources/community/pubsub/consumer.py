import json
import logging
import time
from concurrent.futures import TimeoutError
from typing import Callable, Optional

try:
    from google.api_core.exceptions import NotFound
    from google.cloud.pubsub_v1 import SubscriberClient
    from google.cloud.pubsub_v1.subscriber.client import Client as SClient
    from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
    from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
    from google.oauth2 import service_account
    from google.pubsub_v1.types import Subscription
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[pubsub]" to use PubSubSource'
    ) from exc


# Include PubSubMessage here so all google package imports happen in 1 location
__all__ = ("PubSubConsumer", "PubSubMessage")

logger = logging.getLogger(__name__)


class PubSubSubscriptionNotFound(Exception):
    """Raised when an expected subscription does not exist"""


class PubSubConsumer:
    def __init__(
        self,
        project_id: str,
        topic_id: str,
        subscription_id: str,
        credentials: Optional[str] = None,
        max_batch_size: int = 100,
        batch_timeout_secs: float = 10.0,
        commit_timeout_secs: int = 30,
        default_poll_timeout_secs: float = 5.0,
        create_subscription: bool = False,
        enable_message_ordering: bool = False,
        async_function: Optional[Callable[[PubSubMessage], None]] = None,
    ):
        if credentials:
            self._credentials = service_account.Credentials.from_service_account_info(
                json.loads(credentials, strict=False),
                scopes=["https://www.googleapis.com/auth/pubsub"],
            )
        else:
            self._credentials = None
            logger.info(
                "No credential JSON argument was handed; defaulting to Google Cloud's "
                "environment variables for authentication"
            )
        self._project_id = project_id
        self._topic_id = topic_id
        self._subscription_id = subscription_id
        self._max_batch_size = max_batch_size
        self._batch_timeout = batch_timeout_secs
        self._commit_timeout = commit_timeout_secs
        self._create_subscription = create_subscription
        self._enable_message_ordering = enable_message_ordering
        self._async_function = async_function
        self._default_poll_timeout = default_poll_timeout_secs

        self._message_ack_ids = []
        self._consumer: Optional[SClient] = None
        self._async_listener: Optional[StreamingPullFuture] = None

    def start(self):
        if not self._consumer:
            self._consumer = SubscriberClient(credentials=self._credentials).__enter__()
            if self._create_subscription:
                subscription_result = self.handle_subscription()
                logger.debug(f"Subscription info: {subscription_result}")
            if self._async_function:
                self.subscribe()

    def stop(self):
        if self._consumer:
            if self._async_listener:
                self._async_listener.cancel()
                self._async_listener.result()
            self._consumer.close()

    @property
    def topic_path(self):
        return self._consumer.topic_path(self._project_id, self._topic_id)

    @property
    def subscription_path(self):
        return self._consumer.subscription_path(self._project_id, self._subscription_id)

    def _async_pull_callback(self, message: PubSubMessage) -> None:
        # this should produce the message to kafka
        self._async_function(message)
        # append messages for later committing once producer is flushed
        self._message_ack_ids.append(message.ack_id)

    def poll_and_process(self, timeout: Optional[float] = None):
        """
        This uses the asynchronous puller to retrieve and handle a message with its
        assigned callback.

        Committing is a separate step.
        """
        try:
            self._async_listener.result(timeout=timeout or self._default_poll_timeout)
        except TimeoutError:
            return
        except NotFound:
            raise PubSubSubscriptionNotFound(
                f"Subscription '{self._subscription_id}' (to topic '{self._topic_id}') "
                f"does not exist. Set `create_subscription=True` to create it."
            )

    def poll_and_process_batch(self):
        """
        Polls and processes until either the max_batch_size or batch_timeout is reached.
        """
        timeout = self._batch_timeout
        poll_start_time = time.monotonic()
        while (
            len(self._message_ack_ids) < self._max_batch_size
            and (elapsed := (time.monotonic() - poll_start_time)) < timeout
        ):
            self.poll_and_process(timeout=timeout - elapsed)

    def subscribe(self):
        """
        Asynchronous subscribers require subscribing (synchronous do not).

        NOTE: This will not detect whether the subscription exists.
        """
        self._async_listener = self._consumer.subscribe(
            self.subscription_path, callback=self._async_pull_callback
        )

    def handle_subscription(self) -> Subscription:
        """
        Handles subscription management in one place.

        Subscriptions work similarly to Kafka consumer groups.

        - Each topic can have multiple subscriptions (consumer group ~= subscription).

        - A subscription can have multiple subscribers (similar to consumers in a group).

        - NOTE: exactly-once adds message methods (ack_with_response) when enabled.
        """
        try:
            return self._consumer.get_subscription(subscription=self.subscription_path)
        except NotFound:
            logger.debug(f"creating subscription {self.subscription_path}")
            return self._consumer.create_subscription(
                request={
                    "ack_deadline_seconds": self._commit_timeout,
                    "name": self.subscription_path,
                    "topic": self.topic_path,
                    "enable_message_ordering": self._enable_message_ordering,
                },
            )

    def commit(self):
        if not self._message_ack_ids:
            return
        self._consumer.acknowledge(
            subscription=self.subscription_path,
            ack_ids=self._message_ack_ids,
        )
        logger.debug(
            f"Sending acknowledgments for {len(self._message_ack_ids)} Pub/Sub messages..."
        )
        self._message_ack_ids = []

    def __enter__(self) -> "PubSubConsumer":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self._consumer.__exit__(exc_type, exc_val, exc_tb)
