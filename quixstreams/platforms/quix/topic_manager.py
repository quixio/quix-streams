from quixstreams.topic_manager import TopicManager
from quixstreams.models.topics import TopicKafkaConfigs
from quixstreams.kafka.admin import Admin
from .config import QuixKafkaConfigsBuilder

from typing import Callable, List, Mapping, Optional
from functools import partial


__all__ = ("QuixTopicManager",)


class QuixTopicManager(TopicManager):
    _is_quix = True

    def __init__(
        self,
        quix_config_builder: QuixKafkaConfigsBuilder,
        admin_client: Admin,
        auto_create_topics: bool = True,
    ):
        super().__init__(
            admin_client=admin_client, auto_create_topics=auto_create_topics
        )
        self._quix_config_builder = quix_config_builder

    @property
    def _topic_creator(self) -> Callable[[List[Mapping]], None]:
        func = partial(
            self._quix_config_builder.create_topics, finalize_timeout_seconds=30
        )
        return func

    @property
    def _topic_validator(self):
        return self._quix_config_builder.confirm_topics_exist

    def _apply_topic_prefix(self, name: str) -> str:
        return self._quix_config_builder.append_workspace_id(name)

    def _strip_changelog_chars(self, value):
        stripped = self._quix_config_builder.strip_workspace_id(value)
        return f"{stripped[:5]}{stripped[-5:]}"

    def _format_changelog_name(
        self, consumer_group: str, source_topic_name: str, suffix: str
    ):
        strip = self._strip_changelog_chars

        return self._quix_config_builder.append_workspace_id(
            f"changelog__{strip(consumer_group)}-{strip(source_topic_name)}-{suffix[:9]}"
        )

    def _process_changelog_topic_configs(
        self,
        name: str,
        kafka_configs: Optional[TopicKafkaConfigs] = None,
        default_optionals: Optional[Mapping] = None,
    ) -> TopicKafkaConfigs:
        return super()._process_changelog_topic_configs(
            name=name,
            kafka_configs=kafka_configs,
            default_optionals=default_optionals or {},
        )
