from quixstreams.kafka import TopicAdmin
from quixstreams.rowproducer import RowProducer
from quixstreams.context import message_context
from quixstreams.models import Topic

from typing import Dict, Optional


class ChangelogManager:
    # TODO: simplify this?
    def __init__(self, topic_admin: TopicAdmin, producer: RowProducer):
        self._topic_admin = topic_admin
        self._producer = producer
        self._changelog_writers: Dict[str, ChangelogWriter] = {}

    def add_changelog_topic(self, source_topic_name, suffix):
        print(f"TOPIC ADMIN: {self._topic_admin.quix_config_builder}")
        topic = self._topic_admin.changelog_topic(source_topic_name, suffix)
        writer = ChangelogWriter(topic=topic, producer=self._producer)
        self._changelog_writers[topic.name] = writer
        return writer


class ChangelogWriter:
    def __init__(self, topic: Topic, producer: RowProducer):
        self._topic = topic
        self._producer = producer

    @property
    def key(self):
        return message_context().key

    @property
    def partition(self):
        return message_context().partition

    @property
    def topic(self):
        return self._topic

    def produce(self, key: bytes, value: Optional[bytes] = None):
        # TODO: stuff with column families in the headers
        msg = self.topic.serialize(key=key, value=value)
        self._producer.produce(key=msg.key, value=msg.value, topic=self.topic.name)
