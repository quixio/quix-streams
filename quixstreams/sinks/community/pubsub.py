import concurrent.futures
import logging
from collections import defaultdict
from typing import Any

try:
    from google.api_core.future import Future
    from google.cloud import exceptions as google_exceptions
    from google.cloud import pubsub_v1
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[pubsub]" to use PubSubSink'
    ) from exc

from quixstreams.models.types import HeaderValue
from quixstreams.sinks.base import BaseSink

logger = logging.getLogger(__name__)

TopicPartition = tuple[str, int]


class PubSubSinkTopicNotFoundError(Exception):
    """Raised when the specified topic does not exist."""


class PubSubSink(BaseSink):
    """A sink that publishes messages to Google Cloud Pub/Sub."""

    def __init__(self, project_id: str, topic_id: str, **kwargs) -> None:
        """
        Initialize the PubSubSink.

        :param project_id: GCP project ID
        :param topic_id: Pub/Sub topic ID
        :param kwargs: Additional keyword arguments passed to PublisherClient
            Most notably, `credentials` can be used to specify credentials to use
            for authentication.
        """
        self._publisher = pubsub_v1.PublisherClient(**kwargs)
        self._topic = self._publisher.topic_path(project_id, topic_id)
        self._futures: dict[TopicPartition, list[Future]] = defaultdict(list)

        try:
            self._publisher.get_topic(request={"topic": self._topic})
        except google_exceptions.NotFound:
            raise PubSubSinkTopicNotFoundError(f"Topic `{self._topic}` does not exist.")

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: list[tuple[str, HeaderValue]],
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """
        Publish a message to Pub/Sub.
        """
        data = value if isinstance(value, bytes) else str(value).encode()
        future = self._publisher.publish(topic=self._topic, data=data)
        self._futures[(topic, partition)].append(future)

    def flush(self, topic: str, partition: int) -> None:
        """
        Wait for all publish operations to complete.
        """
        if futures := self._futures.pop((topic, partition), None):
            concurrent.futures.wait(futures)
