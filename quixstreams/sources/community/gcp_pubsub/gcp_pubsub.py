import logging
import os
import time
from concurrent.futures import TimeoutError
from dataclasses import dataclass
from typing import Callable, Optional

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.client import Client as SClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message
from google.pubsub_v1.types import Subscription
from proto.datetime_helpers import DatetimeWithNanoseconds

from quixstreams.models import Topic
from quixstreams.sources import Source

__all__ = ("GCPPubSubConfig", "GCPPubSubSource")


logger = logging.getLogger(__name__)


@dataclass
class GCPPubSubConfig:
    """
    A convenient way to utilize authentication that normally can only be done
    with environment variables.
    """

    credentials_path: Optional[str] = None
    emulated_host_url: Optional[str] = None

    def __post_init__(self):
        if emulated_host_env := os.getenv("PUBSUB_EMULATOR_HOST"):
            if self.emulated_host_url and self.emulated_host_url != emulated_host_env:
                raise ValueError(
                    f"'emulated_host_url' ('{self.emulated_host_url}') and "
                    "environment variable 'PUBSUB_EMULATOR_HOST' "
                    f"('{emulated_host_env}') are both used; set one only."
                )
            print("USING EMULATOR HOST!")
            return
        if self.emulated_host_url:
            logger.info(
                "Setting environment variable 'PUBSUB_EMULATOR_HOST' "
                "to the provided 'emulated_host_url'"
            )
            os.environ["PUBSUB_EMULATOR_HOST"] = self.emulated_host_url
            return

        if creds_env := os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            if self.credentials_path and self.credentials_path != creds_env:
                raise ValueError(
                    f"'credentials_path' ('{self.emulated_host_url}') and "
                    "environment variable 'GOOGLE_APPLICATION_CREDENTIALS' "
                    f"('{creds_env}') are both used; set one only."
                )
            return
        if self.credentials_path:
            logger.info(
                "Setting environment variable 'GOOGLE_APPLICATION_CREDENTIALS' "
                "to the provided 'credentials_path'"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path
            return
        raise ValueError("Must provide a 'credentials_path' or 'emulated_host_url'")


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
        create_subscription=False,
        async_function: Optional[Callable[[Message], None]] = None,
    ):
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
        - Each topic can have multiple subscriptions (consumer group ~= subscription)
        - A subscription can have multiple subscribers (similar to consumers in a group)
        - NOTE: exactly once adds message methods (ack_with_response) when enabled.
        """
        try:
            return self._consumer.get_subscription(subscription=self.subscription_path)
        except Exception as e:
            print(e.__class__)
            logger.debug(f"creating subscription {self.subscription_path}")
            return self._consumer.create_subscription(
                request=dict(
                    # TODO: create pattern for exactly once behavior (although not sure we can truly guarantee it)
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
        self._messages = []
        logger.debug("consumed message acknowledgements sent!")

    def __enter__(self):
        self.start_consumer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._consumer.__exit__(exc_type, exc_val, exc_tb)


class GCPPubSubSource(Source):
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
