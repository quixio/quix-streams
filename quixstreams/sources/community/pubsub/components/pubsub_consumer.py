from __future__ import annotations

import json
import logging
import time
from concurrent.futures import TimeoutError
from typing import Callable, Optional

try:
    from google.cloud.pubsub_v1 import SubscriberClient
    from google.cloud.pubsub_v1.subscriber.client import Client as SClient
    from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
    from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
    from google.oauth2 import service_account
    from google.pubsub_v1.types import Subscription
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[gcp_pubsub]" to use PubSubSource'
    ) from exc


# Include PubSubMessage here so all google package imports happen in 1 location
__all__ = ("PubSubConsumer", "PubSubMessage")

logger = logging.getLogger(__name__)


class PubSubConsumer:
    def __init__(
        self,
        project_id: str,
        topic_id: str,
        subscription_id: str,
        credentials: Optional[str] = None,
        max_batch_size: int = 100,
        commit_timeout_secs: int = 30,
        default_poll_timeout: float = 5.0,
        create_subscription: bool = False,
        async_function: Optional[Callable[[PubSubMessage], None]] = None,
    ):
        if credentials:
            self._credentials = service_account.Credentials.from_service_account_info(
                json.loads(credentials, strict=False),
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
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
        self._commit_timeout_secs = commit_timeout_secs
        self._create_subscription = create_subscription
        self._async_function = async_function
        self._default_poll_timeout = default_poll_timeout

        self._messages = []
        self._consumer: Optional[SClient] = None
        self._async_listener: Optional[StreamingPullFuture] = None

    def start_consumer(self):
        if not self._consumer:
            self._consumer = SubscriberClient(credentials=self._credentials).__enter__()
            if self._create_subscription:
                subscription_result = self.handle_subscription()
                logger.debug(f"Subscription info: {subscription_result}")
            if self._async_function:
                self.subscribe()

    def stop_consumer(self):
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
        # append messages for committing once producer is flushed
        self._messages.append(message)

    def poll(self, timeout: Optional[float] = None):
        """
        This uses the asynchronous puller with a callback to handle messages.
        """
        if not timeout:
            timeout = self._default_poll_timeout
        poll_start_time = time.monotonic()
        while (
            len(self._messages) < self._max_batch_size
            and (elapsed := (time.monotonic() - poll_start_time)) < timeout
        ):
            try:
                self._async_listener.result(timeout=timeout - elapsed)
            except TimeoutError:
                return

    def subscribe(self):
        """
        Asynchronous subscribers require subscribing (synchronous do not).
        """
        self._async_listener = self._consumer.subscribe(
            self.subscription_path, callback=self._async_pull_callback
        )

    def handle_subscription(self) -> Subscription:
        """
        Subscriptions work similarly to Kafka consumer groups.

        - Each topic can have multiple subscriptions (consumer group ~= subscription).

        - A subscription can have multiple subscribers (similar to consumers in a group).

        - NOTE: exactly-once adds message methods (ack_with_response) when enabled.
        """
        try:
            return self._consumer.get_subscription(subscription=self.subscription_path)
        except Exception as e:
            print(e.__class__)
            logger.debug(f"creating subscription {self.subscription_path}")
            return self._consumer.create_subscription(
                request=dict(
                    # TODO: create pattern for exactly once behavior
                    #  (although not sure we can truly guarantee it)
                    # enable_exactly_once_delivery=True,
                    # TODO: setting for message ordering
                    # enable_message_ordering=True,
                    ack_deadline_seconds=self._commit_timeout_secs,
                    name=self.subscription_path,
                    topic=self.topic_path,
                ),
            )

    def commit(self):
        if not self._messages:
            return
        self._consumer.acknowledge(
            subscription=self.subscription_path,
            ack_ids=[message.ack_id for message in self._messages],
        )
        logger.debug(
            f"Sending acknowledgments for {len(self._messages)} PubSub messages..."
        )
        self._messages = []

    def __enter__(self):
        self.start_consumer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._consumer.__exit__(exc_type, exc_val, exc_tb)
