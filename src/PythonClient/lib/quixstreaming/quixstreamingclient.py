from datetime import timedelta
from typing import Dict, Union

from clr import System

from quixstreaming import __importnet, InputTopic, OutputTopic
import Quix.Sdk.Streaming

from .helpers import PythonToNetConverter, NetToPythonConverter
from .raw import RawInputTopic, RawOutputTopic

from quixstreaming import CommitOptions, CommitMode, AutoOffsetReset

class TokenValidationConfiguration(object):

    def __init__(self, net_object):
        """
            Initializes a new instance of TokenValidationConfiguration

            Parameters:

            net_object (.net object): The .net object representing a TokenValidationConfiguration
        """
        self.__wrapped = net_object  # the wrapped .net TokenValidationConfiguration

        if self.__wrapped is None:
            self.__wrapped = Quix.Sdk.Streaming.TokenValidationConfiguration()

    @property
    def enabled(self) -> bool:
        """
            Gets whether token validation and warnings are enabled. Defaults to true.
        """
        return self.__wrapped.Enabled

    @enabled.setter
    def enabled(self, value: bool):
        """
            Sets whether token validation and warnings are enabled. Defaults to true.
        """
        self.__wrapped.Enabled = value

    @property
    def warning_before_expiry(self) -> Union[timedelta, None]:
        """
            Gets whether if the token expires within this period, a warning will be displayed. Defaults to 2 days. Set to None to disable the check
        """
        return NetToPythonConverter.convert_timespan(self.__wrapped.WarningBeforeExpiry)

    @warning_before_expiry.setter
    def warning_before_expiry(self, value: Union[timedelta, None]):
        """
            Sets whether if the token expires within this period, a warning will be displayed. Defaults to 2 days. Set to None to disable the check
        """
        self.__wrapped.WarningBeforeExpiry = PythonToNetConverter.convert_timedelta(value)

    @property
    def warn_about_pat_token(self) -> bool:
        """
            Gets whether to warn if the provided token is not PAT token. Defaults to true.
        """
        return self.__wrapped.WarnAboutNonPatToken

    @warn_about_pat_token.setter
    def warn_about_pat_token(self, value: bool):
        """
            Sets whether to warn if the provided token is not PAT token. Defaults to true.
        """
        self.__wrapped.WarnAboutNonPatToken = value

    def convert_to_net(self):
        return self.__wrapped


class QuixStreamingClient(object):
    """
        Class that is capable of creating input and output topics for reading and writing
    """

    def __init__(self, token: str = None, auto_create_topics: bool = True, properties: Dict[str, str] = None, debug: bool = False):
        """
            Creates a new instance of QuixStreamingClient that is capable of creating input and output topics for reading and writing

            Parameters:

            token (string): The token to use when talking to Quix. When not provided, Quix__Sdk__Token environment variable will be used

            auto_create_topics (string): Whether topics should be auto created if they don't exist yet. Optional, defaults to true

            properties: Optional extra properties for broker configuration

            debug (string): Whether debugging should enabled
        """
        self.__wrapped = None  # the wrapped .net QuixStreamingClient

        net_properties = None
        if properties is not None:
            net_properties = System.Collections.Generic.Dictionary[System.String, System.String]({})
            for key in properties:
                net_properties[key] = properties[key]

        self.__wrapped = Quix.Sdk.Streaming.QuixStreamingClient(token, auto_create_topics, net_properties, debug)

    def open_input_topic(self, topic_id_or_name: str, consumer_group: str = "Default", commit_settings: Union[CommitOptions, CommitMode] = None, auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Earliest) -> InputTopic:
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
            py_offset_reset = auto_offset_reset.convert_to_net()

        py_commit_settings = None
        if commit_settings is not None:
            py_commit_settings = commit_settings.convert_to_net()

        if isinstance(commit_settings, CommitMode):
            dotnet_obj = Quix.Sdk.Streaming.QuixStreamingClientExtensions.OpenInputTopic(self.__wrapped, topic_id_or_name, consumer_group, py_commit_settings, py_offset_reset)
        else:
            if isinstance(commit_settings, CommitOptions):
                dotnet_obj = self.__wrapped.OpenInputTopic(topic_id_or_name, consumer_group, py_commit_settings, py_offset_reset)
            else:
                dotnet_obj = self.__wrapped.OpenInputTopic(topic_id_or_name, consumer_group, None, py_offset_reset)

        return InputTopic(dotnet_obj)

    def open_output_topic(self, topic_id_or_name: str) -> OutputTopic:
        """
            Opens an output topic capable of sending outgoing streams

            Parameters:

            topic_id_or_name (string): Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
        """
        dotnet_obj = self.__wrapped.OpenOutputTopic(topic_id_or_name)
        return OutputTopic(dotnet_obj)
    
    def open_raw_input_topic(self, topic_id_or_name: str, consumer_group: str = None, auto_offset_reset: Union[AutoOffsetReset, None] = None) -> RawInputTopic:
        """
            Opens an input topic for reading raw data from the stream

            Parameters:

            topic_id_or_name (string): Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
            consumer_group (string): Consumer group ( optional )
        """

        if auto_offset_reset is not None:
            auto_offset_reset = auto_offset_reset.convert_to_net()
        
        dotnet_obj = self.__wrapped.OpenRawInputTopic(topic_id_or_name, consumer_group, auto_offset_reset)
        return RawInputTopic(dotnet_obj)

    def open_raw_output_topic(self, topic_id_or_name: str) -> RawOutputTopic:
        """
            Opens an input topic for writing raw data to the stream

            Parameters:

            topic_id_or_name (string): Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
        """
        dotnet_obj = self.__wrapped.OpenRawOutputTopic(topic_id_or_name)
        return RawOutputTopic(dotnet_obj)

    @property
    def token_validation_config(self) -> TokenValidationConfiguration:
        """
            Gets the configuration for token validation.
        """
        return TokenValidationConfiguration(self.__wrapped.TokenValidationConfig)

    @token_validation_config.setter
    def token_validation_config(self, value: TokenValidationConfiguration):
        """
            Sets the configuration for token validation.
        """
        if value is None:
            self.__wrapped.TokenValidationConfig = None
            return
        self.__wrapped.TokenValidationConfig = value.convert_to_net()

    @property
    def api_url(self) -> str:
        """
            Gets the base API uri. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.
        """
        return self.__wrapped.ApiUrl.ToString()

    @api_url.setter
    def api_url(self, value: str):
        """
            Sets the base API uri. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.
        """
        self.__wrapped.ApiUrl = System.Uri(value)

    @property
    def cache_period(self) -> timedelta:
        """
            Gets the period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute.
        """
        return NetToPythonConverter.convert_timespan(self.__wrapped.CachePeriod)

    @cache_period.setter
    def cache_period(self, value: timedelta):
        """
            Sets the period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute.
        """
        self.__wrapped.CachePeriod = PythonToNetConverter.convert_timedelta(value)
