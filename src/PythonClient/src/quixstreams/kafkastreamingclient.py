from typing import Dict, Union

from .topicconsumer import TopicConsumer
from .models.netdict import NetDict
from .topicproducer import TopicProducer
from .configuration import SecurityOptions
from .raw import RawTopicConsumer, RawTopicProducer

from .models import CommitOptions, CommitMode, AutoOffsetReset

from .native.Python.QuixSdkStreaming.KafkaStreamingClient import KafkaStreamingClient as sci
from .native.Python.QuixSdkStreaming.KafkaStreamingClientExtensions import KafkaStreamingClientExtensions as kscei
from .native.Python.QuixSdkProcess.Kafka.AutoOffsetReset import AutoOffsetReset as AutoOffsetResetInterop
from .native.Python.QuixSdkStreaming.Models.CommitMode import CommitMode as CommitModeInterop
from .helpers.enumconverter import EnumConverter as ec
from .helpers.nativedecorator import nativedecorator


@nativedecorator
class KafkaStreamingClient(object):
    """
        Class that is capable of creating input and output topics for reading and writing
    """

    def __init__(self, broker_address: str, security_options: SecurityOptions = None, properties: Dict[str, str] = None, debug: bool = False):
        """
            Creates a new instance of KafkaStreamingClient that is capable of creating input and output topics for reading and writing

            Parameters:

            brokerAddress (string): Address of Kafka cluster

            security_options (string): Optional security options

            properties: Optional extra properties for broker configuration

            debug (string): Whether debugging should be enabled
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

    def create_topic_consumer(self, topic: str, consumer_group: str = "Default", commit_settings: Union[CommitOptions, CommitMode] = None, auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Earliest) -> TopicConsumer:
        """
            Opens an input topic capable of reading incoming streams

            Parameters:

            topic (string): Name of the topic

            consumer_group (string): The consumer group id to use for consuming messages

            commit_settings (CommitOptions, CommitMode): the settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.

            auto_offset_reset (AutoOffsetReset): The offset to use when there is no saved offset for the consumer group. Defaults to earliest
        """

        net_offset_reset = AutoOffsetResetInterop.Earliest
        if auto_offset_reset is not None:
            net_offset_reset = ec.enum_to_another(auto_offset_reset, AutoOffsetResetInterop)

        if isinstance(commit_settings, CommitMode):
            net_commit_settings = ec.enum_to_another(commit_settings, CommitModeInterop)

            hptr = kscei.CreateTopicConsumer(self._interop.get_interop_ptr__(), topic, consumer_group, net_commit_settings, net_offset_reset)
        else:
            if isinstance(commit_settings, CommitOptions):
                hptr = self._interop.CreateTopicConsumer(topic, consumer_group, commit_settings.get_net_pointer(), net_offset_reset)
            else:
                hptr = self._interop.CreateTopicConsumer(topic, consumer_group, None, net_offset_reset)

        return TopicConsumer(hptr)

    def create_topic_producer(self, topic: str) -> TopicProducer:
       """
           Opens an output topic capable of sending outgoing streams

           Parameters:

           topic (string): Name of the topic
       """
       hptr = self._interop.CreateTopicProducer(topic)
       return TopicProducer(hptr)
    
    def create_raw_topic_consumer(self, topic: str, consumer_group: str = None, auto_offset_reset: Union[AutoOffsetReset, None] = None) -> RawTopicConsumer:
        """
            Opens an input topic for reading raw data from the stream

            Parameters:

            topic (string): Name of the topic
            consumer_group (string): Consumer group ( optional )
        """

        py_offset_reset = AutoOffsetReset.Earliest
        if auto_offset_reset is not None:
            py_offset_reset = ec.enum_to_another(auto_offset_reset, AutoOffsetResetInterop)

        raw_topic_hptr = self._interop.CreateRawTopicConsumer(topic, consumer_group, py_offset_reset)
        return RawTopicConsumer(raw_topic_hptr)

    def create_raw_topic_producer(self, topic: str) -> RawTopicProducer:
        """
           Opens an input topic for writing raw data to the stream

           Parameters:

           topic (string): Name of the topic
        """
        raw_topic_hptr = self._interop.CreateRawTopicProducer(topic)
        return RawTopicProducer(raw_topic_hptr)
