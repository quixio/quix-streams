import datetime
import logging
import re
import sys
import time
import uuid
from functools import partial
from typing import Callable, Literal, Optional, Union
from urllib.parse import quote_plus as qp

import bson

try:
    from pymongo import MongoClient, ReplaceOne, UpdateMany, UpdateOne
    from pymongo.collection import Collection
    from pymongo.errors import (
        ExecutionTimeout,
        NetworkTimeout,
        WriteConcernError,
    )
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[mongodb]" to use MongoDBSink'
    ) from exc

from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch
from quixstreams.sinks.base.item import SinkItem

__all__ = ("MongoDBSink",)

logger = logging.getLogger(__name__)

MongoValue = Union[
    str,
    int,
    float,
    bool,
    None,
    datetime.datetime,
    bytes,
    list,
    dict,
    bson.ObjectId,
    re.Pattern,
    bson.Decimal128,
    bson.Code,
    uuid.UUID,
]
MongoQueryFilter = dict[str, MongoValue]


_UPDATE_MAP = {
    "UpdateOne": UpdateOne,
    "ReplaceOne": ReplaceOne,
    "UpdateMany": UpdateMany,
}
_ATTEMPT_BACKOFF = 5
_SINK_BACKOFF = 30


def _default_document_matcher(record: SinkItem) -> MongoQueryFilter:
    return {"_id": record.key}


class MongoDBSink(BatchingSink):
    def __init__(
        self,
        host: str,
        db: str,
        collection: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 27017,
        document_matcher: Callable[
            [SinkItem], MongoQueryFilter
        ] = _default_document_matcher,
        update_method: Literal["UpdateOne", "UpdateMany", "ReplaceOne"] = "UpdateOne",
        upsert: bool = True,
        add_message_metadata: bool = False,
        add_topic_metadata: bool = False,
        authentication_timeout_ms: int = 15000,
        value_selector: Optional[Callable[[MongoValue], MongoValue]] = None,
        **kwargs,
    ) -> None:
        """
        A connector to sink processed data to MongoDB in batches.

        :param host: MongoDB hostname; example "localhost"
        :param db: MongoDB database name
        :param collection: MongoDB collection name
        :param username: username, if authentication is required
        :param password: password, if authentication is required
        :param port: port used by MongoDB host if not using the default of 27017
        :param document_matcher: How documents are selected to update.
            A callable that accepts a `BatchItem` and returns a MongoDB "query filter".
            If no match, will insert if `upsert=True`, where `_id` will be either the
            included value if specified, else a random `ObjectId`.
            - Default: matches on `_id`, with `_id` assumed to be the kafka key.
        :param upsert: Create documents if no matches with `document_matcher`.
        :param update_method: How documents found with `document_matcher` are updated.
            'Update*' options will only update fields included in the kafka message.
            'Replace*' option fully replaces the document with the contents of kafka message.
            "UpdateOne": Updates the first matching document (usually based on `_id`).
            "UpdateMany": Updates ALL matching documents (usually NOT based on `_id`).
            "ReplaceOne": Replaces the first matching document (usually based on `_id`).
            Default: "UpdateOne".
        :param add_message_metadata: add key, timestamp, and headers as `__{field}`
        :param add_topic_metadata: add topic, partition, and offset as `__{field}`
        :param value_selector: An optional callable that allows final editing of the
            outgoing document (right before submitting it).
            Largely used when a field is necessary for `document_matcher`,
            but not otherwise.
            NOTE: metadata is added before this step, so don't accidentally
            exclude it here!
        """
        super().__init__()
        auth_stub = f"{qp(username)}:{qp(password)}@" if username else ""
        self._client_kwargs = {
            "host": f"mongodb://{auth_stub}{host}",
            "port": port,
            **kwargs,
        }
        self._db_name = db
        self._collection_name = collection
        self._document_matcher = document_matcher
        self._update_method = _UPDATE_MAP[update_method]
        self._upsert = upsert
        self._add_message_metadata = add_message_metadata
        self._add_topic_metadata = add_topic_metadata
        self._value_selector = value_selector
        self._auth_timeout_ms = authentication_timeout_ms
        self._client: Optional[MongoClient] = None
        self._collection: Optional[Collection] = None

    def setup(self):
        self._client = MongoClient(
            serverSelectionTimeoutMS=self._auth_timeout_ms,
            **self._client_kwargs,
        )
        db = self._client[self._db_name]
        # confirms connection
        db.command("ping", maxTimeMS=10000)
        self._collection = db[self._collection_name]

    def _add_metadata(self, topic: str, partition: int, record: SinkItem) -> MongoValue:
        value = record.value
        if self._add_message_metadata:
            # tuples are one of the few unsupported common types, so we convert them
            # so users don't need to manage it.
            value["__key"] = (
                str(record.key) if isinstance(record.key, tuple) else record.key
            )
            value["__headers"] = dict(record.headers or {})
            value["__timestamp"] = record.timestamp
        if self._add_topic_metadata:
            value["__topic"] = topic
            value["__partition"] = partition
            value["__offset"] = record.offset
        return value

    def write(self, batch: SinkBatch) -> None:
        """
        Note: Transactions could be an option here, but then each record requires a
        network call, and the transaction has size limits...so `bulk_write` is used
        instead, with the downside that duplicate writes may occur if errors arise.
        """
        start = time.monotonic()
        min_timestamp = sys.maxsize
        max_timestamp = -1
        doc_match = self._document_matcher
        update_method = self._update_method
        upsert = self._upsert
        value_selector = self._value_selector
        is_update = self._update_method != ReplaceOne

        if self._add_message_metadata or self._add_topic_metadata:
            added_metadata = partial(self._add_metadata, batch.topic, batch.partition)
        else:
            added_metadata = None

        records = []
        for record in batch:
            ts = record.timestamp
            value = added_metadata(record) if added_metadata else record.value
            value = value_selector(value) if value_selector else value
            records.append(
                update_method(
                    doc_match(record) if doc_match else None,
                    {"$set": value} if is_update else value,
                    upsert=upsert,
                )
            )
            min_timestamp = min(ts, min_timestamp)
            max_timestamp = max(ts, max_timestamp)

        attempts_remaining = 3
        while attempts_remaining:
            try:
                # We don't need to worry about manually batching our writes, the MongoDB
                # client does it for us automatically (chunks every 10k records).
                # It will block until all writes are successful.
                self._collection.bulk_write(records)
            except (NetworkTimeout, ExecutionTimeout, WriteConcernError) as e:
                logger.error(f"MongoDB: Encountered a network or server error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    logger.error(
                        f"Sleeping for {_ATTEMPT_BACKOFF} seconds and re-attempting. "
                        f"Attempts remaining: {attempts_remaining}"
                    )
                    time.sleep(_ATTEMPT_BACKOFF)
            else:
                logger.info(
                    "Sent update requests to MongoDB; "
                    f"messages_processed={batch.size} "
                    f"host_address={self._client.address[0]} "
                    f"database={self._db_name} "
                    f"collection={self._collection_name} "
                    f"min_timestamp={min_timestamp} "
                    f"max_timestamp={max_timestamp} "
                    f"time_elapsed={round(time.monotonic() - start, 2)}s"
                )
                return
        logger.error(
            "Consecutive MongoDB write attempts failed; "
            f"performing a longer backoff of {_SINK_BACKOFF} seconds..."
        )
        raise SinkBackpressureError(retry_after=_SINK_BACKOFF)
