from ...runner import Runner
from ...dataframe.dataframe import StreamingDataFrame
from ...models.topics import Topic
from .config import QuixKafkaConfigsBuilder
from typing import Optional, List

__all__ = ("QuixRunner",)


class QuixRunner(Runner):
    """
    An extension of the runner class that simplifies connecting to the Quix Platform,
    assuming environment is properly configured (by default in the platform).
    """

    def __init__(
        self,
        consumer_group: str,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        **kwargs,
    ):
        """
        :param consumer_group: desired kafka consumer group name/id
        :param quix_config_builder: QuixConfigBuilder instance if env not configured
        :param consumer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
        :param producer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
        :param kwargs: any other desired Runner arguments.
        """
        if not quix_config_builder:
            quix_config_builder = QuixKafkaConfigsBuilder()
        self.config_builder = quix_config_builder
        consumer_extra_config = consumer_extra_config or {}
        producer_extra_config = producer_extra_config or {}
        cfgs = self.config_builder.get_confluent_broker_config()
        super().__init__(
            broker_address=cfgs.pop("bootstrap.servers"),
            consumer_group=self.config_builder.append_workspace_id(consumer_group),
            consumer_extra_config={**cfgs, **consumer_extra_config},
            producer_extra_config={**cfgs, **producer_extra_config},
            **kwargs,
        )

    def _get_df_input_topics(self) -> List[Topic]:
        """
        append the workspace id name to all the input topics given via a
        StreamingDataFrame
        """
        for topic in self.dataframe.topics_in:
            self.dataframe.topics_in[
                topic
            ].real_name = self.config_builder.append_workspace_id(topic)
        return super()._get_df_input_topics()

    def _edit_df_output_topics(self):
        """
        append the workspace id name to all the output topics given via a
        StreamingDataFrame.
        """
        for topic in self.dataframe.topics_out:
            self.dataframe.topics_out[
                topic
            ].real_name = self.config_builder.append_workspace_id(topic)

    def _df_init(self, dataframe: StreamingDataFrame):
        """
        Set up the kafka-related attributes of the StreamingDataFrame object.
        """
        super()._df_init(dataframe)
        self._edit_df_output_topics()
