from datetime import timedelta
from typing import Dict, Union
import ctypes

from .models.autooffsetreset import AutoOffsetReset
from .models.commitoptions import CommitOptions
from .models.commitmode import CommitMode
from .topicconsumer import TopicConsumer
from .models.netdict import NetDict
from .raw.rawtopicconsumer import RawTopicConsumer
from .topicproducer import TopicProducer
from .raw.rawtopicproducer import RawTopicProducer

from .native.Python.QuixSdkStreaming.QuixStreamingClient import QuixStreamingClient as qsci
from .native.Python.QuixSdkStreaming.QuixStreamingClientExtensions import QuixStreamingClientExtensions as qscei
from .native.Python.QuixSdkStreaming.QuixStreamingClient_TokenValidationConfiguration import TokenValidationConfiguration as tvci
from .native.Python.QuixSdkProcess.Kafka.AutoOffsetReset import AutoOffsetReset as AutoOffsetResetInterop
from .native.Python.QuixSdkStreaming.Models.CommitMode import CommitMode as CommitModeInterop
from .native.Python.SystemPrivateUri.System.Uri import Uri as ui
from .helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from .helpers.enumconverter import EnumConverter as ec
from .helpers.nativedecorator import nativedecorator


@nativedecorator
class TokenValidationConfiguration(object):

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of TokenValidationConfiguration

            Parameters:

            net_pointer (c_void_p): Pointer to an instance of a .net TokenValidationConfiguration
        """
        if net_pointer is None:
            raise Exception("Native pointer must not be null")

        self._interop = tvci(net_pointer)

    @property
    def enabled(self) -> bool:
        """
            Gets whether token validation and warnings are enabled. Defaults to true.
        """
        return self._interop.get_Enabled()

    @enabled.setter
    def enabled(self, value: bool):
        """
            Sets whether token validation and warnings are enabled. Defaults to true.
        """
        self._interop.set_Enabled(value)

    @property
    def warning_before_expiry(self) -> Union[timedelta, None]:
        """
            Gets whether if the token expires within this period, a warning will be displayed. Defaults to 2 days. Set to None to disable the check
        """

        ptr = self._interop.get_WarningBeforeExpiry()
        value = dtc.timespan_to_python(ptr)
        return value

    @warning_before_expiry.setter
    def warning_before_expiry(self, value: Union[timedelta, None]):
        """
            Sets whether if the token expires within this period, a warning will be displayed. Defaults to 2 days. Set to None to disable the check
        """

        ptr = dtc.timedelta_to_dotnet(value)

        self._interop.set_WarningBeforeExpiry(ptr)

    @property
    def warn_about_pat_token(self) -> bool:
        """
            Gets whether to warn if the provided token is not PAT token. Defaults to true.
        """
        return self._interop.get_WarnAboutNonPatToken()
        return self.__wrapped.WarnAboutNonPatToken

    @warn_about_pat_token.setter
    def warn_about_pat_token(self, value: bool):
        """
            Sets whether to warn if the provided token is not PAT token. Defaults to true.
        """
        return self._interop.set_WarnAboutNonPatToken(value)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._pointer


class QuixStreamingClient(object):
    """
        Class that is capable of creating input and output topics for reading and writing
    """

    def __init__(self, token: str = None, auto_create_topics: bool = True, properties: Dict[str, str] = None, debug: bool = False):
        """
            Creates a new instance of quixstreamsClient that is capable of creating input and output topics for reading and writing

            Parameters:

            token (string): The token to use when talking to Quix. When not provided, Quix__Sdk__Token environment variable will be used

            auto_create_topics (string): Whether topics should be auto created if they don't exist yet. Optional, defaults to true

            properties: Optional extra properties for broker configuration

            debug (string): Whether debugging should enabled
        """

        net_properties_hptr = None
        if properties is not None:
            net_properties = NetDict.constructor_for_string_string()
            for key in properties:
                net_properties[key] = properties[key]
            net_properties_hptr = net_properties.get_net_pointer()

        self._interop = qsci(qsci.Constructor(token, auto_create_topics, properties=net_properties_hptr, debug=debug))

    def create_topic_consumer(self, topic_id_or_name: str, consumer_group: str = "Default", commit_settings: Union[CommitOptions, CommitMode] = None, auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Earliest) -> TopicConsumer:
        """
            Opens an input topic capable of reading incoming streams

            Parameters:

            topic_id_or_name (string): Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

            consumer_group (string): The consumer group id to use for consuming messages

            commit_settings (CommitOptions, CommitMode): the settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.

            auto_offset_reset (AutoOffsetReset): The offset to use when there is no saved offset for the consumer group. Defaults to earliest
        """
        py_offset_reset = AutoOffsetReset.Earliest
        if auto_offset_reset is not None:
            py_offset_reset = ec.enum_to_another(auto_offset_reset, AutoOffsetResetInterop)


        if isinstance(commit_settings, CommitMode):
            net_commit_settings = ec.enum_to_another(commit_settings, CommitModeInterop)

            hptr = qscei.CreateTopicConsumer(self._interop.get_interop_ptr__(), topic_id_or_name, consumer_group, net_commit_settings, py_offset_reset)
        else:
            if isinstance(commit_settings, CommitOptions):
                hptr = self._interop.CreateTopicConsumer(topic_id_or_name, consumer_group, commit_settings.get_net_pointer(), py_offset_reset)
            else:
                hptr = self._interop.CreateTopicConsumer(topic_id_or_name, consumer_group, None, py_offset_reset)

        return TopicConsumer(hptr)

    def open_topic_producer(self, topic_id_or_name: str) -> TopicProducer:
        """
            Opens an output topic capable of sending outgoing streams

            Parameters:

            topic_id_or_name (string): Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
        """

        dotnet_pointer = self._interop.CreateTopicProducer(topic_id_or_name)
        return TopicProducer(dotnet_pointer)
    
    def open_raw_topic_consumer(self, topic_id_or_name: str, consumer_group: str = None, auto_offset_reset: Union[AutoOffsetReset, None] = None) -> RawTopicConsumer:
        """
            Opens an input topic for reading raw data from the stream

            Parameters:

            topic_id_or_name (string): Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
            consumer_group (string): Consumer group ( optional )
        """

        py_offset_reset = AutoOffsetReset.Earliest
        if auto_offset_reset is not None:
            py_offset_reset = ec.enum_to_another(auto_offset_reset, AutoOffsetResetInterop)

        dotnet_pointer = self._interop.CreateRawTopicConsumer(topic_id_or_name, consumer_group, py_offset_reset)
        return RawTopicConsumer(dotnet_pointer)

    def open_raw_topic_producer(self, topic_id_or_name: str) -> RawTopicProducer:
        """
            Opens an input topic for writing raw data to the stream

            Parameters:

            topic_id_or_name (string): Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
        """
        dotnet_pointer = self._interop.CreateRawTopicProducer(topic_id_or_name)
        return RawTopicProducer(dotnet_pointer)

    @property
    def token_validation_config(self) -> TokenValidationConfiguration:
        """
            Gets the configuration for token validation.
        """

        return TokenValidationConfiguration(self._interop.get_TokenValidationConfig())

    @token_validation_config.setter
    def token_validation_config(self, value: TokenValidationConfiguration):
        """
            Sets the configuration for token validation.
        """
        raise NotImplementedError("TODO")
        if value is None:
            self.__wrapped.TokenValidationConfig = None
            return
        self.__wrapped.TokenValidationConfig = value.convert_to_net()

    @property
    def api_url(self) -> str:
        """
            Gets the base API uri. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.
        """

        uri = ui(self._interop.get_ApiUrl())
        return uri.ToString()

    @api_url.setter
    def api_url(self, value: str):
        """
            Sets the base API uri. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.
        """
        self._interop.set_ApiUrl(ui.Constructor(value))

    @property
    def cache_period(self) -> timedelta:
        """
            Gets the period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute.
        """

        ptr = self._interop.get_CachePeriod()
        value = dtc.timespan_to_python(ptr)
        return value

    @cache_period.setter
    def cache_period(self, value: timedelta):
        """
            Sets the period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute.
        """
        ptr = dtc.timedelta_to_dotnet(value)

        self._interop.set_CachePeriod(ptr)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
