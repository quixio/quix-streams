from enum import Enum
from .. import __importnet
import Quix.Sdk.Process


class StreamEndType(Enum):
    Closed = 0,
    Aborted = 1,
    Terminated = 2

    @staticmethod
    def convert_from_net(net_enum: Quix.Sdk.Process.Models.StreamEndType):
        if net_enum == Quix.Sdk.Process.Models.StreamEndType.Closed:
            return StreamEndType.Closed
        if net_enum == Quix.Sdk.Process.Models.StreamEndType.Aborted:
            return StreamEndType.Aborted
        if net_enum == Quix.Sdk.Process.Models.StreamEndType.Terminated:
            return StreamEndType.Terminated
        print('WARNING: An unhandled StreamEndType (' + net_enum + ') was read, using Closed instead.')
        return StreamEndType.Closed

    def convert_to_net(self):
        if self == StreamEndType.Closed:
            return Quix.Sdk.Process.Models.StreamEndType.Closed
        if self == StreamEndType.Aborted:
            return Quix.Sdk.Process.Models.StreamEndType.Aborted
        if self == StreamEndType.Terminated:
            return Quix.Sdk.Process.Models.StreamEndType.Terminated
