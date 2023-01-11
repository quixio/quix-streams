from enum import Enum
from .. import __importnet
import Quix.Sdk.Process


class AutoOffsetReset(Enum):
    Latest = 0,
    """
    Latest, starts from newest message if there is no stored offset
    """

    Earliest = 1,
    """
    Earliest, starts from the oldest message if there is no stored offset
    """

    Error = 2
    """
    Error, throws exception if there is no stored offset
    """

    @staticmethod
    def convert_from_net(net_enum: Quix.Sdk.Process.Kafka.AutoOffsetReset):
        if net_enum == Quix.Sdk.Process.Kafka.AutoOffsetReset.Latest:
            return AutoOffsetReset.Latest
        if net_enum == Quix.Sdk.Process.Kafka.AutoOffsetReset.Earliest:
            return AutoOffsetReset.Earliest
        if net_enum == Quix.Sdk.Process.Kafka.AutoOffsetReset.Error:
            return AutoOffsetReset.Error
        raise Exception('An unhandled AutoOffsetReset (' + str(net_enum) + ')')

    def convert_to_net(self):
        if self == AutoOffsetReset.Latest:
            return Quix.Sdk.Process.Kafka.AutoOffsetReset.Latest
        if self == AutoOffsetReset.Earliest:
            return Quix.Sdk.Process.Kafka.AutoOffsetReset.Earliest
        if self == AutoOffsetReset.Error:
            return Quix.Sdk.Process.Kafka.AutoOffsetReset.Error
        raise Exception('An unhandled AutoOffsetReset (' + str(self) + ')')
