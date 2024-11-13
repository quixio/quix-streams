from __future__ import annotations

import logging
import time
from concurrent.futures import TimeoutError
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    # This allows the typing to work while not necessarily having the package installed
    from google.cloud.pubsub_v1 import SubscriberClient
    from google.cloud.pubsub_v1.subscriber.client import Client as SClient
    from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
    from google.cloud.pubsub_v1.subscriber.message import Message
    from google.pubsub_v1.types import Subscription

from .config import GCPPubSubConfig

__all__ = ("GCPPubSubConsumer",)

logger = logging.getLogger(__name__)


class GCPPubSubConsumer:
    def __init__(
        self,
        config: GCPPubSubConfig,
        project_id: str,
        topic_name: str,
        subscription_name: str,
        max_batch_size: int = 100,
        commit_timeout_secs: int = 30,
        default_poll_timeout: float = 5.0,
        create_subscription: bool = False,
        async_function: Optional[Callable[[Message], None]] = None,
    ):
        try:
            from google.cloud.pubsub_v1 import SubscriberClient  # noqa: F401
            from google.cloud.pubsub_v1.subscriber.client import (
                Client as SClient,  # noqa: F401
            )
            from google.cloud.pubsub_v1.subscriber.futures import (
                StreamingPullFuture,  # noqa: F401
            )
            from google.cloud.pubsub_v1.subscriber.message import (
                Message,  # noqa: F401
            )
            from google.pubsub_v1.types import Subscription  # noqa: F401
        except ImportError:
            raise ImportError(
                "Missing python package 'google-cloud-pubsub'; do "
                "`pip install google-cloud-pubsub` to use this connector."
            )

        self._config = config
        self._project_id = project_id
        self._topic_name = topic_name
        self._subscription_name = subscription_name
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
            self._consumer = SubscriberClient().__enter__()
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
        return self._consumer.topic_path(self._project_id, self._topic_name)

    @property
    def subscription_path(self):
        return self._consumer.subscription_path(
            self._project_id, self._subscription_name
        )

    def _async_pull_callback(self, message: Message) -> None:
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
        Asynchronous subscribers require pulling.
        """
        self._async_listener = self._consumer.subscribe(
            self.subscription_path, callback=self._async_pull_callback
        )

    def handle_subscription(self) -> Subscription:
        """
        Subscriptions work similarly to Kafka consumer groups.

        - Each topic can have multiple subscriptions (consumer group ~= subscription).

        - A subscription can have multiple subscribers (similar to consumers in a group).

        - NOTE: exactly once adds message methods (ack_with_response) when enabled.
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
