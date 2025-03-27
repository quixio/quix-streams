try:
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers as es_helpers
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[elasticsearch]" to use ElasticSearchSink'
    ) from exc
import logging
import sys
import time
from typing import Callable, Optional

from quixstreams.sinks import SinkBatch
from quixstreams.sinks.base import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
)
from quixstreams.sinks.base.item import SinkItem

__all__ = ("ElasticsearchSink",)


logger = logging.getLogger(__name__)


DEFAULT_MAPPING = {"mappings": {"dynamic": "true"}}


def _default_document_id_setter(item: SinkItem) -> str:
    key = item.key
    if isinstance(key, bytes):
        return str(key.decode())
    return str(key)


class ElasticsearchSink(BatchingSink):
    """
    Pushes data to an ElasticSearch index.

    By default, uses the kafka message key as the document ID, and dynamically generates
    the field types.

    You can pass your own type mapping or document ID setter for custom behavior.
    """

    def __init__(
        self,
        url: str,
        index: str,
        mapping: Optional[dict] = None,
        document_id_setter: Optional[
            Callable[[SinkItem], Optional[str]]
        ] = _default_document_id_setter,
        batch_size: int = 500,
        max_bulk_retries: int = 3,
        ignore_bulk_upload_errors: bool = False,
        add_message_metadata: bool = False,
        add_topic_metadata: bool = False,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        **kwargs,
    ):
        """
        :param url: the ElasticSearch host url
        :param index: the ElasticSearch index name
        :param mapping: a custom mapping; the default dynamically maps all field types
        :param document_id_setter: how to select the document id; the default is the Kafka message key
        :param batch_size: how large each chunk size is with bulk
        :param max_bulk_retries: number of retry attempts for each bulk batch
        :param ignore_bulk_upload_errors: ignore any errors that occur when attempting an upload
        :param add_message_metadata: add key, timestamp, and headers as `__{field}`
        :param add_topic_metadata: add topic, partition, and offset as `__{field}`
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        :param kwargs: additional kwargs that are passed to the ElasticSearch client
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._client_kwargs = {"hosts": url, **kwargs}
        self._url = url
        self._index = index
        self._document_id_setter = document_id_setter
        self._mapping = mapping or DEFAULT_MAPPING
        self._batch_size = batch_size
        self._max_bulk_retries = max_bulk_retries
        self._ignore_bulk_errors = ignore_bulk_upload_errors
        self._add_message_metadata = add_message_metadata
        self._add_topic_metadata = add_topic_metadata
        self._client: Optional[Elasticsearch] = None

    def setup(self):
        self._client = Elasticsearch(**self._client_kwargs)
        self._client.info()
        self._create_index()

    def _create_index(self):
        """Create index if it doesn't exist."""
        if not self._client.indices.exists(index=self._index):
            logger.info(
                f"Creating Index '{self._index}' with mapping: '{self._mapping}'..."
            )
            self._client.indices.create(index=self._index, body=self._mapping)
        else:
            logger.debug(f"Index '{self._index}' already exists.")

    def _add_metadata(self, topic: str, partition: int, record: SinkItem) -> dict:
        value = record.value
        if self._add_message_metadata:
            # tuples and bytes are one of the few unsupported common types, so we
            # convert them so users don't need to manage it.
            key = record.key
            if isinstance(key, tuple):
                key = [k.decode() if isinstance(k, bytes) else k for k in key]
            elif isinstance(key, bytes):
                key = key.decode()
            value["__key"] = key
            value["__headers"] = {
                k: v.decode() if isinstance(v, bytes) else v
                for k, v in dict(record.headers or {}).items()
            }
            value["__timestamp"] = record.timestamp
        if self._add_topic_metadata:
            value["__topic"] = topic
            value["__partition"] = partition
            value["__offset"] = record.offset
        return value

    def write(self, batch: SinkBatch) -> None:
        start = time.monotonic()
        min_timestamp = sys.maxsize
        max_timestamp = -1
        add_metadata = self._add_message_metadata or self._add_topic_metadata

        for chunk in batch.iter_chunks(n=self._batch_size):
            records = []
            for item in chunk:
                ts = item.timestamp
                records.append(
                    {
                        # TODO: Allow users to dynamically select indexes
                        # _index: custom_index_function,
                        "_id": self._document_id_setter(item)
                        if self._document_id_setter
                        else None,
                        "_source": self._add_metadata(
                            batch.topic, batch.partition, item
                        )
                        if add_metadata
                        else item.value,
                    }
                )
                min_timestamp = min(ts, min_timestamp)
                max_timestamp = max(ts, max_timestamp)

            # TODO: raise a SinkBackoff once we see how max retries due to network
            #   failures manifest
            try:
                success, failure = es_helpers.bulk(
                    client=self._client,
                    actions=records,
                    index=self._index,
                    max_retries=self._max_bulk_retries,
                    max_backoff=30,
                    raise_on_error=not self._ignore_bulk_errors,
                )
                # Note: this will only occur when self._ignore_bulk_errors is True,
                # as failures should otherwise raise.
                if failure:
                    logger.warning(
                        f"Ignoring ElasticSearch upload errors "
                        f'due to "ignore_bulk_errors" setting: {failure}'
                    )
            except es_helpers.BulkIndexError as e:
                logger.error(f"{e}, {e.errors}")
                raise

        logger.info(
            "Sent documents to ElasticSearch; "
            f"messages_processed={batch.size} "
            f"host_address={self._url} "
            f"index={self._index} "
            f"min_timestamp={min_timestamp} "
            f"max_timestamp={max_timestamp} "
            f"time_elapsed={round(time.monotonic() - start, 2)}s"
        )
