from dataclasses import dataclass
from typing import MutableSequence, Optional

from google.api_core import retry
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.client import Client
from google.pubsub_v1.types.pubsub import PullResponse, ReceivedMessage

from quixstreams.sources import Source


@dataclass
class GCPPubSubConfig:
    project_id: str
    subscription_name: str
    topic_name: str
    max_pull_batch_size: int = 10

    @property
    def topic_path(self):
        return f"projects/{self.project_id}/topic/{self.topic_name}"

    @property
    def subscription_path(self):
        return f"projects/{self.project_id}/subscriptions/{self.subscription_name}"


class GCPConsumer:
    def __init__(self, config: GCPPubSubConfig):
        self._config = config
        self._consumer: Optional[Client] = None
        self._messages = None

    def poll(self, timeout: float = 5.0) -> MutableSequence[ReceivedMessage]:
        response: PullResponse = self._consumer.pull(
            subscription=self._config.subscription_path,
            max_messages=self._config.max_pull_batch_size,
            retry=retry.Retry(deadline=300),
            timeout=timeout,
        )
        self._messages = response.received_messages
        return self._messages

    def commit(self):
        if not self._messages:
            return
        self._consumer.acknowledge(
            subscription=self._config.subscription_path,
            ack_ids=[message.ack_id for message in self._messages],
        )

    def __enter__(self):
        self._consumer = SubscriberClient().__enter__()
        self._consumer.create_subscription(
            request={"enable_exactly_once_delivery": True},
            name=self._config.subscription_path,
            topic=self._config.topic_path,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._consumer.__exit__(exc_type, exc_val, exc_tb)


class GCPPubSubSource(Source):
    def run(self):
        pass
