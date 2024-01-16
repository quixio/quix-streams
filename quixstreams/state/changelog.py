from quixstreams.topic_manager import TopicManagerType, BytesTopic
from quixstreams.rowproducer import RowProducer

from typing import Optional

_COLUMN_FAMILY_HEADER = "__column_family__"


class ChangelogWriter:
    """
    Typically created and handed to `PartitionTransaction`s to produce state changes to
    a changelog topic.
    """

    def __init__(self, topic: BytesTopic, partition_num: int, producer: RowProducer):
        self._topic = topic
        self._partition_num = partition_num
        self._producer = producer

    def produce(self, key: bytes, cf_name: str, value: Optional[bytes] = None):
        msg = self._topic.serialize(key=key, value=value)
        self._producer.produce(
            key=msg.key,
            value=msg.value,
            topic=self._topic.name,
            partition=self._partition_num,
            headers={_COLUMN_FAMILY_HEADER: cf_name},
        )


class ChangelogManager:
    """
    A simple interface for adding changelog topics during store init and
    generating changelog writers (generally for each new `Store` transaction).
    """

    _writer = ChangelogWriter

    def __init__(self, topic_manager: TopicManagerType, producer: RowProducer):
        self._topic_manager = topic_manager
        self._producer = producer

    def add_changelog(self, source_topic_name: str, suffix: str, consumer_group: str):
        self._topic_manager.changelog_topic(
            source_topic_name=source_topic_name,
            suffix=suffix,
            consumer_group=consumer_group,
        )

    def get_writer(
        self, source_topic_name: str, suffix: str, partition_num: int
    ) -> _writer:
        return self._writer(
            topic=self._topic_manager.changelog_topics[source_topic_name][suffix],
            partition_num=partition_num,
            producer=self._producer,
        )
