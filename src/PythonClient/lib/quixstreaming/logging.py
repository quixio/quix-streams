from enum import Enum

from . import __importnet
import Microsoft.Extensions.Logging.Abstractions
import System
import Quix

class LogLevel(Enum):
    Trace = 0,
    Debug = 1,
    Information = 2,
    Warning = 3,
    Error = 4,
    Critical = 5,
    Disabled = 6

    def convert_to_net(self):
        if self == LogLevel.Trace:
            return Microsoft.Extensions.Logging.LogLevel.Trace
        if self == LogLevel.Debug:
            return Microsoft.Extensions.Logging.LogLevel.Debug
        if self == LogLevel.Information:
            return Microsoft.Extensions.Logging.LogLevel.Information
        if self == LogLevel.Warning:
            return Microsoft.Extensions.Logging.LogLevel.Warning
        if self == LogLevel.Error:
            return Microsoft.Extensions.Logging.LogLevel.Error
        if self == LogLevel.Critical:
            return Microsoft.Extensions.Logging.LogLevel.Critical
        if self == LogLevel.Disabled:
            return 6  # none is causing problems and this also works

class Logging:

    @staticmethod
    def update_factory(level: LogLevel):
        dotnetlevel = level.convert_to_net()
        Quix.Sdk.Logging.UpdateFactory(dotnetlevel)
