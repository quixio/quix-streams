from enum import Enum

from .helpers.enumconverter import EnumConverter as ec
from .native.Python.MicrosoftExtensionsLoggingAbstractions.Microsoft.Extensions.Logging.LogLevel import LogLevel as LogLevelInterop
from .native.Python.QuixStreamsKafka.QuixStreams.Logging import Logging as LoggingInterop


class LogLevel(Enum):
    Trace = 0
    Debug = 1
    Information = 2
    Warning = 3
    Error = 4
    Critical = 5
    Disabled = 6


class Logging:

    @staticmethod
    def update_factory(level: LogLevel):
        net_loglevel = ec.enum_to_another(level, LogLevelInterop)
        LoggingInterop.UpdateFactory(net_loglevel)

