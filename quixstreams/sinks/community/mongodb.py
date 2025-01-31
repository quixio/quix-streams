import datetime
import logging
import re
import uuid
from functools import partial
from typing import Callable, Literal, Union

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

_UPDATE_MAP = {
    "UpdateOne": UpdateOne,
    "ReplaceOne": ReplaceOne,
    "UpdateMany": UpdateMany,
}


def _default_id_setter(record: SinkItem):
    # TODO: Maybe we just leave the key as-is instead of converting to a string?
    key = record.key
    if isinstance(key, bytes):
        key = key.decode()
    return str(key)


def _default_document_matcher(record: SinkItem):
    key = record.key
    if isinstance(key, bytes):
        key = key.decode()
    return {"_id": str(key)}


class MongoDBSink(BatchingSink):
    def __init__(
        self,
        url: str,
        db: str,
        collection: str,
        document_matcher: Callable[[SinkItem], MongoValue] = _default_document_matcher,
        update_method: Literal["UpdateOne", "UpdateMany", "ReplaceOne"] = "UpdateOne",
        upsert: bool = True,
        include_message_metadata: bool = False,
        include_topic_metadata: bool = False,
    ) -> None:
        """
        A connector to sink processed data to MongoDB in batches.

        :param url: MongoDB url; most commonly `mongodb://username:password@host:port`
        :param db: MongoDB database name
        :param collection: MongoDB collection name
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
        :param include_message_metadata: include key, timestamp, and headers as `__{field}`
        :param include_topic_metadata: include topic, partition, and offset as `__{field}`
        """

        super().__init__()

        self._client = MongoClient(url)
        self._db_name = db
        self._collection_name = collection
        self._collection: Collection = self._client[db][collection]
        self._document_matcher = document_matcher
        self._update_method = _UPDATE_MAP[update_method]
        self._upsert = upsert
        self._include_message_metadata = include_message_metadata
        self._include_topic_metadata = include_topic_metadata
        # TODO: include a value filter in case additional trimming is desired
        #  after fields are no longer needed for document_matcher purposes.

    def _add_metadata(self, topic: str, partition: int, record: SinkItem):
        value = record.value
        if self._include_message_metadata:
            # tuples are pretty much the only thing not covered
            # TODO: different way to handle this?
            value["__key"] = (
                str(record.key) if isinstance(record.key, tuple) else record.key
            )
            value["__headers"] = dict(record.headers)
            value["__timestamp"] = record.timestamp
        if self._include_topic_metadata:
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
        doc_match = self._document_matcher
        update_method = self._update_method
        upsert = self._upsert
        is_update = self._update_method != ReplaceOne

        if self._include_message_metadata or self._include_topic_metadata:
            added_metadata = partial(self._add_metadata, batch.topic, batch.partition)
        else:
            added_metadata = None

        records = []
        for record in batch:
            value = added_metadata(record) if added_metadata else record.value
            records.append(
                update_method(
                    doc_match(record) if doc_match else None,
                    {"$set": value} if is_update else value,
                    upsert=upsert,
                )
            )
        try:
            # We don't need to worry about manually batching our writes, the MongoDB
            # client does it for us automatically (chunks every 10k records).
            # It will block until all writes are successful.
            self._collection.bulk_write(records)
            logger.info(
                f"MongoDB: successfully submitted {len(records)} update requests to "
                f'collection "{self._collection_name}" for database "{self._db_name}" '
                f'at "{self._client.address[0]}"'
            )
        except (NetworkTimeout, ExecutionTimeout, WriteConcernError):
            logger.error("MongoDB: Encountered a server-side error; retrying batch.")
            raise SinkBackpressureError
