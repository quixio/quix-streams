from typing import Dict, Union

from clr import System
from deprecated import deprecated

from quixstreaming import __importnet, InputTopic, OutputTopic
import Quix.Sdk.Streaming
from quixstreaming.configuration import SecurityOptions
from .helpers import PythonToNetConverter
from .raw import RawInputTopic, RawOutputTopic

from quixstreaming import CommitOptions, CommitMode, AutoOffsetReset


class StreamingClient(object):
    """
        Class that is capable of creating input and output topics for reading and writing
    """

    def __init__(self, broker_address: str, security_options: SecurityOptions = None, properties: Dict[str, str] = None, debug: bool = False):
        """
            Creates a new instance of StreamingClient that is capable of creating input and output topics for reading and writing

            Parameters:

            brokerAddress (string): Address of Kafka cluster

            security_options (string): Optional security options

            properties: Optional extra properties for broker configuration

            debug (string): Whether debugging should enabled
        """
        self.__wrapped = None  # the wrapped .net StreamingClientFactory
        secu_opts = None
        if security_options is not None:
            secu_opts = security_options.convert_to_net()

        net_properties = None
        if properties is not None:
            net_properties = System.Collections.Generic.Dictionary[System.String, System.String]({})
            for key in properties:
                net_properties[key] = properties[key]

        self.__wrapped = Quix.Sdk.Streaming.StreamingClient(broker_address, secu_opts, net_properties, debug)

    @deprecated(reason="Renamed to open_input_topic, this will be removed in release 0.4.0")
    def create_input_topic(self, topic: str, consumer_group: str = "Default", commit_settings: Union[CommitOptions, CommitMode] = None) -> InputTopic:
        """
            Opens an input topic capable of reading incoming streams

            Parameters:

            topic (string): Name of the topic

            consumer_group (string): The consumer group id to use for consuming messages

            commit_settings (CommitOptions, CommitMode): the settings to use for committing
        """
        return self.open_input_topic(topic, consumer_group, commit_settings)

    def open_input_topic(self, topic: str, consumer_group: str = "Default", commit_settings: Union[CommitOptions, CommitMode] = None, auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Earliest) -> InputTopic:
        """
            Opens an input topic capable of reading incoming streams

            Parameters:

            topic (string): Name of the topic

            consumer_group (string): The consumer group id to use for consuming messages

            commit_settings (CommitOptions, CommitMode): the settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.

            auto_offset_reset (AutoOffsetReset): The offset to use when there is no saved offset for the consumer group. Defaults to earliest
        """
        if isinstance(commit_settings, CommitMode):
            dotnet_obj = Quix.Sdk.Streaming.StreamingClientExtensions.OpenInputTopic(self.__wrapped, topic, consumer_group, commit_settings.convert_to_net(), auto_offset_reset.convert_to_net())
        else:
            if isinstance(commit_settings, CommitOptions):
                dotnet_obj = self.__wrapped.OpenInputTopic(topic, consumer_group, commit_settings.convert_to_net(), auto_offset_reset.convert_to_net())
            else:
                dotnet_obj = self.__wrapped.OpenInputTopic(topic, consumer_group, None, auto_offset_reset.convert_to_net())

        return InputTopic(dotnet_obj)

    @deprecated(reason="Renamed to open_output_topic, this will be removed in release 0.4.0")
    def create_output_topic(self, topic: str) -> OutputTopic:
        """
            Opens an output topic capable of sending outgoing streams

            Parameters:

            topic (string): Name of the topic
        """
        return self.open_output_topic(topic)

    def open_output_topic(self, topic: str) -> OutputTopic:
        """
            Opens an output topic capable of sending outgoing streams

            Parameters:

            topic (string): Name of the topic
        """
        dotnet_obj = self.__wrapped.OpenOutputTopic(topic)
        return OutputTopic(dotnet_obj)
    
    def open_raw_input_topic(self, topic: str, consumer_group: str = None, auto_offset_reset: Union[AutoOffsetReset, None] = None) -> RawInputTopic:
        """
            Opens an inpput topic for reading raw data from the stream

            Parameters:

            topic (string): Name of the topic
            consumer_group (string): Consumer group ( optional )
        """

        if auto_offset_reset is not None:
            auto_offset_reset = auto_offset_reset.convert_to_net()
        
        dotnet_obj = self.__wrapped.OpenRawInputTopic(topic, consumer_group, auto_offset_reset)
        return RawInputTopic(dotnet_obj)

    def open_raw_output_topic(self, topic: str) -> RawOutputTopic:
        """
            Opens an inpput topic for writing raw data to the stream

            Parameters:

            topic (string): Name of the topic
        """
        dotnet_obj = self.__wrapped.OpenRawOutputTopic(topic)
        return RawOutputTopic(dotnet_obj)
