from enum import Enum
from .. import __importnet
import Quix.Sdk.Process


class EventLevel(Enum):
    Trace = 0,
    Debug = 1,
    Information = 2,
    Warning = 3,
    Error = 4,
    Critical = 5

    @staticmethod
    def convert_from_net(net_enum: Quix.Sdk.Process.Models.EventLevel):
        if net_enum == Quix.Sdk.Process.Models.EventLevel.Trace:
            return EventLevel.Trace
        if net_enum == Quix.Sdk.Process.Models.EventLevel.Debug:
            return EventLevel.Debug
        if net_enum == Quix.Sdk.Process.Models.EventLevel.Information:
            return EventLevel.Information
        if net_enum == Quix.Sdk.Process.Models.EventLevel.Warning:
            return EventLevel.Warning
        if net_enum == Quix.Sdk.Process.Models.EventLevel.Error:
            return EventLevel.Error
        if net_enum == Quix.Sdk.Process.Models.EventLevel.Critical:
            return EventLevel.Critical
        print('WARNING: An unhandled StreamEndType (' + net_enum + ') was read, using Information instead.')
        return EventLevel.Information

    def convert_to_net(self):
        if self == EventLevel.Trace:
            return Quix.Sdk.Process.Models.EventLevel.Trace
        if self == EventLevel.Debug:
            return Quix.Sdk.Process.Models.EventLevel.Debug
        if self == EventLevel.Information:
            return Quix.Sdk.Process.Models.EventLevel.Information
        if self == EventLevel.Warning:
            return Quix.Sdk.Process.Models.EventLevel.Warning
        if self == EventLevel.Error:
            return Quix.Sdk.Process.Models.EventLevel.Error
        if self == EventLevel.Critical:
            return Quix.Sdk.Process.Models.EventLevel.Critical
