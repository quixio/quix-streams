import ctypes
from datetime import timedelta
from typing import Dict, Union

from .helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from .helpers.enumconverter import EnumConverter as ec
from .helpers.nativedecorator import nativedecorator
from .models.autooffsetreset import AutoOffsetReset
from .models.commitmode import CommitMode
from .models.commitoptions import CommitOptions
from .models.netdict import NetDict
from .native.Python.QuixStreamsTelemetry.Kafka.AutoOffsetReset import AutoOffsetReset as AutoOffsetResetInterop
from .native.Python.QuixStreamsStreaming.Models.CommitMode import CommitMode as CommitModeInterop
from .native.Python.QuixStreamsStreaming.QuixStreamingClient import QuixStreamingClient as qsci
from .native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions import QuixStreamingClientExtensions as qscei
from .native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration import TokenValidationConfiguration as tvci
from .native.Python.SystemPrivateUri.System.Uri import Uri as ui
from .raw.rawtopicconsumer import RawTopicConsumer
from .raw.rawtopicproducer import RawTopicProducer
from .topicconsumer import TopicConsumer
from .topicproducer import TopicProducer


@nativedecorator
class TokenValidationConfiguration(object):

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TokenValidationConfiguration.

        Args:
            net_pointer: Pointer to an instance of a .NET TokenValidationConfiguration.
        """
        if net_pointer is None:
            raise Exception("Native pointer must not be null")

        self._interop = tvci(net_pointer)

    @property
    def enabled(self) -> bool:
        """
        Gets whether token validation and warnings are enabled. Defaults to true.

        Returns:
            bool: True if token validation and warnings are enabled, False otherwise.
        """
        return self._interop.get_Enabled()

    @enabled.setter
    def enabled(self, value: bool):
        """
        Sets whether token validation and warnings are enabled. Defaults to true.

        Args:
            value: True to enable token validation and warnings, False to disable.
        """
        self._interop.set_Enabled(value)

    @property
    def warning_before_expiry(self) -> Union[timedelta, None]:
        """
        Gets the period within which, if the token expires, a warning will be displayed. Defaults to 2 days. Set to None to disable the check.

        Returns:
            Union[timedelta, None]: The period within which a warning will be displayed if the token expires or None if the check is disabled.
        """

        ptr = self._interop.get_WarningBeforeExpiry()
        value = dtc.timespan_to_python(ptr)
        return value

    @warning_before_expiry.setter
    def warning_before_expiry(self, value: Union[timedelta, None]):
        """
        Sets the period within which, if the token expires, a warning will be displayed. Defaults to 2 days. Set to None to disable the check.

        Args:
            value: The new period within which a warning will be displayed if the token expires or None to disable the check.
        """

        ptr = dtc.timedelta_to_dotnet(value)

        self._interop.set_WarningBeforeExpiry(ptr)

    @property
    def warn_about_pat_token(self) -> bool:
        """
        Gets whether to warn if the provided token is not a PAT token. Defaults to true.

        Returns:
            bool: True if the warning is enabled, False otherwise.
        """
        return self._interop.get_WarnAboutNonPatToken()

    @warn_about_pat_token.setter
    def warn_about_pat_token(self, value: bool):
        """
        Sets whether to warn if the provided token is not a PAT token. Defaults to true.

        Args:
            value: True to enable the warning, False to disable.
        """
        return self._interop.set_WarnAboutNonPatToken(value)

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .NET object pointer.

        Returns:
            ctypes.c_void_p: The .NET pointer
        """
        return self._pointer


class QuixStreamingClient(object):
    """
    Streaming client for Kafka configured automatically using Environment Variables and Quix platform endpoints.
    Use this Client when you use this library together with Quix platform.
    """

    def __init__(self, token: str = None, auto_create_topics: bool = True, properties: Dict[str, str] = None, debug: bool = False):
        """
        Initializes a new instance of the QuixStreamingClient capable of creating topic consumers and producers.

        Args:
            token: The token to use when talking to Quix. If not provided, the Quix__Sdk__Token environment variable will be used. Defaults to None.
            auto_create_topics: Whether topics should be auto-created if they don't exist yet. Defaults to True.
            properties: Additional broker properties. Defaults to None.
            debug: Whether debugging should be enabled. Defaults to False.
        """

        net_properties_hptr = None
        if properties is not None:
            net_properties = NetDict.constructor_for_string_string()
            for key in properties:
                net_properties[key] = properties[key]
            net_properties_hptr = net_properties.get_net_pointer()

        self._interop = qsci(qsci.Constructor(token, auto_create_topics, properties=net_properties_hptr, debug=debug))

    def get_topic_consumer(self, topic_id_or_name: str, consumer_group: str = None, commit_settings: Union[CommitOptions, CommitMode] = None,
                              auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Latest) -> TopicConsumer:
        """
        Opens a topic consumer capable of subscribing to receive incoming streams.

        Args:
            topic_id_or_name: ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.
            consumer_group: The consumer group ID to use for consuming messages. If None, the consumer group is not used, and only consuming new messages. Defaults to None.
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

            hptr = qscei.GetTopicConsumer(self._interop.get_interop_ptr__(), topic_id_or_name, consumer_group, net_commit_settings, net_offset_reset)
        else:
            if isinstance(commit_settings, CommitOptions):
                hptr = self._interop.GetTopicConsumer(topic_id_or_name, consumer_group, commit_settings.get_net_pointer(), net_offset_reset)
            else:
                hptr = self._interop.GetTopicConsumer(topic_id_or_name, consumer_group, None, net_offset_reset)

        return TopicConsumer(hptr)

    def get_topic_producer(self, topic_id_or_name: str) -> TopicProducer:
        """
        Gets a topic producer capable of producing outgoing streams.

        Args:
            topic_id_or_name: ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.

        Returns:
            TopicProducer: An instance of TopicProducer for the specified topic.
        """
        dotnet_pointer = self._interop.GetTopicProducer(topic_id_or_name)

        return TopicProducer(dotnet_pointer)

    def get_raw_topic_consumer(self, topic_id_or_name: str, consumer_group: str = None, auto_offset_reset: Union[AutoOffsetReset, None] = None) -> RawTopicConsumer:
        """
        Gets a topic consumer for consuming raw data from the stream.

        Args:
            topic_id_or_name: ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.
            consumer_group: The consumer group ID to use for consuming messages. Defaults to None.
            auto_offset_reset: The offset to use when there is no saved offset for the consumer group. Defaults to None.

        Returns:
            RawTopicConsumer: An instance of RawTopicConsumer for the specified topic.
        """

        net_offset_reset = AutoOffsetResetInterop.Latest
        if auto_offset_reset is not None:
            net_offset_reset = ec.enum_to_another(auto_offset_reset, AutoOffsetResetInterop)

        dotnet_pointer = self._interop.GetRawTopicConsumer(topic_id_or_name, consumer_group, net_offset_reset)
        return RawTopicConsumer(dotnet_pointer)

    def get_raw_topic_producer(self, topic_id_or_name: str) -> RawTopicProducer:
        """
        Gets a topic producer for producing raw data to the stream.

        Args:
            topic_id_or_name: ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.

        Returns:
            RawTopicProducer: An instance of RawTopicProducer for the specified topic.
        """

        dotnet_pointer = self._interop.GetRawTopicProducer(topic_id_or_name)
        return RawTopicProducer(dotnet_pointer)

    @property
    def token_validation_config(self) -> TokenValidationConfiguration:
        """
        Gets the configuration for token validation.

        Returns:
            TokenValidationConfiguration: The current token validation configuration.
        """

        return TokenValidationConfiguration(self._interop.get_TokenValidationConfig())

    @token_validation_config.setter
    def token_validation_config(self, value: TokenValidationConfiguration):
        """
        Sets the configuration for token validation.

        Args:
            value: The new token validation configuration.
        """
        raise NotImplementedError("TODO")
        if value is None:
            self.__wrapped.TokenValidationConfig = None
            return
        self.__wrapped.TokenValidationConfig = value.convert_to_net()

    @property
    def api_url(self) -> str:
        """
        Gets the base API URI. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.

        Returns:
            str: The current base API URI.
        """

        uri = ui(self._interop.get_ApiUrl())
        return uri.ToString()

    @api_url.setter
    def api_url(self, value: str):
        """
        Sets the base API URI. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.

        Args:
            value: The new base API URI.
        """
        self._interop.set_ApiUrl(ui.Constructor(value))

    @property
    def cache_period(self) -> timedelta:
        """
        Gets the period for which some API responses will be cached to avoid an excessive amount of calls. Defaults to 1 minute.

        Returns:
            timedelta: The current cache period.
        """

        ptr = self._interop.get_CachePeriod()
        value = dtc.timespan_to_python(ptr)
        return value

    @cache_period.setter
    def cache_period(self, value: timedelta):
        """
        Sets the period for which some API responses will be cached to avoid an excessive amount of calls. Defaults to 1 minute.

        Args:
            value: The new cache period.
        """
        ptr = dtc.timedelta_to_dotnet(value)

        self._interop.set_CachePeriod(ptr)

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .NET object pointer.

        Returns:
            ctypes.c_void_p: The .NET pointer
        """
        return self._interop.get_interop_ptr__()
