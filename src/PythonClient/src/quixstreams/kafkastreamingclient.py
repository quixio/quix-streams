from typing import Dict, Union

from .configuration import SecurityOptions
from .helpers.enumconverter import EnumConverter as ec
from .helpers.nativedecorator import nativedecorator
from .models import CommitOptions, CommitMode, AutoOffsetReset, CodecType
from .models.netdict import NetDict
from .native.Python.QuixStreamsTelemetry.Kafka.AutoOffsetReset import AutoOffsetReset as AutoOffsetResetInterop
from .native.Python.QuixStreamsStreaming.KafkaStreamingClient import KafkaStreamingClient as sci
from .native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions import KafkaStreamingClientExtensions as kscei
from .native.Python.QuixStreamsStreaming.Models.CommitMode import CommitMode as CommitModeInterop
from .raw import RawTopicConsumer, RawTopicProducer
from .topicconsumer import TopicConsumer
from .topicproducer import TopicProducer


@nativedecorator
class KafkaStreamingClient(object):
    """
    A Kafka streaming client capable of creating topic consumer and producers.
    """

    def __init__(self, broker_address: str, security_options: SecurityOptions = None, properties: Dict[str, str] = None, debug: bool = False):
        """
        Initializes a new instance of the KafkaStreamingClient.

        Args:
            broker_address: The address of the Kafka cluster.
            security_options: Optional security options for the Kafka client.
            properties: Optional extra properties for broker configuration.
            debug: Whether debugging should be enabled. Defaults to False.
        """

        secu_opts_hptr = None
        if security_options is not None:
            secu_opts_hptr = security_options.get_net_pointer()

        net_properties_hptr = None
        if properties is not None:
            net_properties = NetDict.constructor_for_string_string()
            for key in properties:
                net_properties[key] = properties[key]
            net_properties_hptr = net_properties.get_net_pointer()

        self._interop = sci(sci.Constructor(broker_address, secu_opts_hptr, properties=net_properties_hptr, debug=debug))

    def get_topic_consumer(self, topic: str, consumer_group: str = None, commit_settings: Union[CommitOptions, CommitMode] = None,
                              auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Latest) -> TopicConsumer:
        """
        Gets a topic consumer capable of subscribing to receive incoming streams.

        Args:
            topic: The name of the topic.
            consumer_group: The consumer group ID to use for consuming messages. Defaults to None.
            commit_settings: The settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.
            auto_offset_reset: The offset to use when there is no saved offset for the consumer group. Defaults to AutoOffsetReset.Latest.

        Returns:
            TopicConsumer: An instance of TopicConsumer for the specified topic.
        """

        net_offset_reset = AutoOffsetResetInterop.Latest
        if auto_offset_reset is not None:
            net_offset_reset = ec.enum_to_another(auto_offset_reset, AutoOffsetResetInterop)

        if isinstance(commit_settings, CommitMode):
            net_commit_settings = ec.enum_to_another(commit_settings, CommitModeInterop)

            hptr = kscei.GetTopicConsumer(self._interop.get_interop_ptr__(), topic, consumer_group, net_commit_settings, net_offset_reset)
        else:
            if isinstance(commit_settings, CommitOptions):
                hptr = self._interop.GetTopicConsumer(topic, consumer_group, commit_settings.get_net_pointer(), net_offset_reset)
            else:
                hptr = self._interop.GetTopicConsumer(topic, consumer_group, None, net_offset_reset)

        return TopicConsumer(hptr)

    def get_topic_producer(self, topic: str) -> TopicProducer:
        """
        Gets a topic producer capable of publishing stream messages.

        Args:
            topic: The name of the topic.

        Returns:
            TopicProducer: An instance of TopicProducer for the specified topic.
        """
        hptr = self._interop.GetTopicProducer(topic)
        return TopicProducer(hptr)

    def get_raw_topic_consumer(self, topic: str, consumer_group: str = None, auto_offset_reset: Union[AutoOffsetReset, None] = None) -> RawTopicConsumer:
        """
        Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages.

        Args:
            topic: The name of the topic.
            consumer_group: The consumer group ID to use for consuming messages. Defaults to None.
            auto_offset_reset: The offset to use when there is no saved offset for the consumer group. Defaults to None.

        Returns:
            RawTopicConsumer: An instance of RawTopicConsumer for the specified topic.
        """

        py_offset_reset = AutoOffsetReset.Earliest
        if auto_offset_reset is not None:
            py_offset_reset = ec.enum_to_another(auto_offset_reset, AutoOffsetResetInterop)

        raw_topic_hptr = self._interop.GetRawTopicConsumer(topic, consumer_group, py_offset_reset)
        return RawTopicConsumer(raw_topic_hptr)

    def get_raw_topic_producer(self, topic: str) -> RawTopicProducer:
        """
        Gets a topic producer capable of publishing non-quixstreams messages.

        Args:
            topic: The name of the topic.

        Returns:
            RawTopicProducer: An instance of RawTopicProducer for the specified topic.
        """
        raw_topic_hptr = self._interop.GetRawTopicProducer(topic)
        return RawTopicProducer(raw_topic_hptr)
