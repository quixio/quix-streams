from enum import Enum

from .. import __importnet
import Quix.Sdk.Streaming


class CommitMode(Enum):
    Automatic = 0,
    Manual = 1

    @staticmethod
    def convert_from_net(net_enum: Quix.Sdk.Streaming.Models.CommitMode):
        if net_enum == Quix.Sdk.Streaming.Models.CommitMode.Automatic:
            return CommitMode.Automatic
        if net_enum == Quix.Sdk.Streaming.Models.CommitMode.Manual:
            return CommitMode.Manual
        raise Exception('An unhandled CommitMode (' + str(net_enum) + ')')

    def convert_to_net(self):
        if self == CommitMode.Automatic:
            return Quix.Sdk.Streaming.Models.CommitMode.Automatic
        if self == CommitMode.Manual:
            return Quix.Sdk.Streaming.Models.CommitMode.Manual
        raise Exception('An unhandled CommitMode (' + str(self) + ')')
