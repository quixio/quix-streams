import logging
import os
from dataclasses import dataclass
from typing import MutableSequence, Optional

from google.api_core import retry
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.publisher.client import Client as PClient
from google.cloud.pubsub_v1.subscriber.client import Client as SClient
from google.cloud.pubsub_v1.types import PublisherOptions
from google.pubsub_v1.types import (
    PubsubMessage,
    PullResponse,
    ReceivedMessage,
    Subscription,
)
from proto.datetime_helpers import DatetimeWithNanoseconds

from quixstreams.models import Topic
from quixstreams.sources import Source

__all__ = ("GCPPubSubConfig", "GCPPubSubSource")


logger = logging.getLogger(__name__)


@dataclass
class GCPPubSubConfig:
    project_id: str
    subscription_name: str
    topic_name: str
    max_pull_batch_size: int = 10
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
            print("USING EMULATOR HOST!")
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
    def __init__(self, config: GCPPubSubConfig):
        self._config = config
        self._consumer: Optional[SClient] = None
        self._messages = None

    def start_consumer(self):
        if not self._consumer:
            self._consumer = SubscriberClient().__enter__()
            # subscription_result = self.subscribe()
            # logger.debug(f"Subscription request succeeded: {subscription_result}")

    def stop_consumer(self):
        if self._consumer:
            self._consumer.close()

    @property
    def topic_path(self):
        return self._consumer.topic_path(
            self._config.project_id, self._config.topic_name
        )

    @property
    def subscription_path(self):
        return self._consumer.subscription_path(
            self._config.project_id, self._config.subscription_name
        )

    def poll(self, timeout: float = 5.0) -> MutableSequence[ReceivedMessage]:
        response: PullResponse = self._consumer.pull(
            subscription=self.subscription_path,
            max_messages=self._config.max_pull_batch_size,
            timeout=timeout,
            retry=retry.Retry(deadline=300),
        )
        logger.debug(
            f"Consumed {len(response.received_messages)} messages from {self.topic_path}"
        )
        self._messages = response.received_messages
        return self._messages

    def subscribe(self) -> Subscription:
        """
        Subscriptions work similarly to Kafka consumer groups, though there is no true
        "subscribe" action; this just creates a subscription if it doesn't exist.
        - Each topic can have multiple subscriptions (consumer group ~= subscription)
        - A subscription can have multiple subscribers (similar to consumers in a group)
        """
        try:
            return self._consumer.get_subscription(subscription=self.subscription_path)
        except Exception as e:
            print(e)
            logger.debug(f"creating subscription {self.subscription_path}")
            return self._consumer.create_subscription(
                request=dict(
                    # TODO: create pattern for exactly once behavior (although not sure we can truly guarantee it)
                    # enable_exactly_once_delivery=True,
                    enable_message_ordering=True,
                    name=self.subscription_path,
                    topic=self.topic_path,
                )
            )

    def commit(self):
        if not self._messages:
            return
        self._consumer.acknowledge(
            subscription=self.subscription_path,
            ack_ids=[message.ack_id for message in self._messages],
        )
        logger.debug("consumed message acknowledgements sent!")

    def __enter__(self):
        self.start_consumer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._consumer.__exit__(exc_type, exc_val, exc_tb)


# TODO: remove this once testing finished
class GCPPubSubProducer:
    def __init__(self, config: GCPPubSubConfig, use_keys: bool = True):
        self._config = config
        self._use_keys = use_keys
        self._producer: Optional[PClient] = None

    @property
    def topic_path(self):
        return self._producer.topic_path(
            self._config.project_id, self._config.topic_name
        )

    @property
    def subscription_path(self):
        return self._producer.subscription_path(
            self._config.project_id, self._config.subscription_name
        )

    def __enter__(self):
        self._producer = PublisherClient(
            # allows using message keys like how they are used in Kafka
            publisher_options=PublisherOptions(enable_message_ordering=self._use_keys)
        ).__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._producer.__exit__(exc_type, exc_val, exc_tb)

    def produce(self, value: bytes, key: str = ""):
        # Publish a message to the topic
        if key and not self._use_keys:
            raise ValueError("Key use is disabled on producer; set 'use_keys' to True")
        future = self._producer.publish(
            topic=self.topic_path,
            data=value,
            ordering_key=key,
        )

        # Result is the message ID
        message_id = future.result()
        print(f"Published message ID: {message_id}")

    def create_topic(self):
        self._producer.create_topic(name=self.topic_path)


class GCPPubSubSource(Source):
    def __init__(
        self,
        config: GCPPubSubConfig,
        name: Optional[str] = None,
        poll_timeout: float = 30.0,
        shutdown_timeout: float = 60.0,
    ):
        self._config = config
        self._poll_timeout = poll_timeout
        super().__init__(
            name=name or self._config.subscription_name,
            shutdown_timeout=shutdown_timeout,
        )

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_deserializer="str",
            value_deserializer="bytes",
            key_serializer="str",
            value_serializer="bytes",
        )

    def run(self):
        with GCPPubSubConsumer(config=self._config) as consumer:
            while self._running:
                for item in (messages := consumer.poll(self._poll_timeout)):
                    message: PubsubMessage = item.message
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
                if messages:
                    self.flush()
                    consumer.commit()
