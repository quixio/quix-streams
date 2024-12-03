import concurrent.futures
import json
from collections import defaultdict
from typing import Any, Callable, Optional, Union

try:
    from google.api_core import exceptions as google_exceptions
    from google.api_core.future import Future
    from google.cloud import pubsub_v1
    from google.oauth2 import service_account
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[pubsub]" to use PubSubSink'
    ) from exc

from quixstreams.models.types import HeadersTuples
from quixstreams.sinks.base import BaseSink, SinkBackpressureError

__all__ = ("PubSubSink", "PubSubTopicNotFoundError")

TopicPartition = tuple[str, int]


class PubSubTopicNotFoundError(Exception):
    """Raised when the specified topic does not exist."""


class PubSubSink(BaseSink):
    """A sink that publishes messages to Google Cloud Pub/Sub."""

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        service_account_json: Optional[str] = None,
        value_serializer: Callable[[Any], Union[bytes, str]] = json.dumps,
        key_serializer: Callable[[Any], str] = bytes.decode,
        flush_timeout: int = 5,
        **kwargs,
    ) -> None:
        """
        Initialize the PubSubSink.

        :param project_id: GCP project ID.
        :param topic_id: Pub/Sub topic ID.
        :param service_account_json: an optional JSON string with service account credentials
            to connect to Pub/Sub.
            The internal `PublisherClient` will use the Application Default Credentials if not provided.
            See https://cloud.google.com/docs/authentication/provide-credentials-adc for more info.
            Default - `None`.
        :param value_serializer: Function to serialize the value to string or bytes
            (defaults to json.dumps).
        :param key_serializer: Function to serialize the key to string
            (defaults to bytes.decode).
        :param kwargs: Additional keyword arguments passed to PublisherClient.
        """

        # Parse the service account credentials from JSON
        if service_account_json is not None:
            service_account_info = json.loads(service_account_json, strict=False)
            kwargs["credentials"] = (
                service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=["https://www.googleapis.com/auth/pubsub"],
                )
            )

        self._publisher = pubsub_v1.PublisherClient(**kwargs)
        self._topic = self._publisher.topic_path(project_id, topic_id)
        self._value_serializer = value_serializer
        self._key_serializer = key_serializer
        self._flush_timeout = flush_timeout
        self._futures: dict[TopicPartition, list[Future]] = defaultdict(list)

        try:
            self._publisher.get_topic(request={"topic": self._topic})
        except google_exceptions.NotFound:
            raise PubSubTopicNotFoundError(f"Topic `{self._topic}` does not exist.")

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """
        Publish a message to Pub/Sub.
        """
        if isinstance(value, bytes):
            data = value
        else:
            data = self._value_serializer(value)
            if not isinstance(data, bytes):
                data = str(data).encode()

        key = self._key_serializer(key)
        kwargs = {
            "topic": self._topic,
            "data": data,
            "_key": key,
            "_timestamp": str(timestamp),
            "_offset": str(offset),
            **dict(headers),
        }

        future = self._publisher.publish(**kwargs)
        self._futures[(topic, partition)].append(future)

    def flush(self, topic: str, partition: int) -> None:
        """
        Wait for all publish operations to complete successfully.
        """
        if futures := self._futures.pop((topic, partition), None):
            result = concurrent.futures.wait(
                futures,
                timeout=self._flush_timeout,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            if result.not_done or any(f.exception() for f in result.done):
                raise SinkBackpressureError(
                    retry_after=5.0,
                    topic=topic,
                    partition=partition,
                )
