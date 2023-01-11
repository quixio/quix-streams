from enum import Enum

from .. import __importnet
import Quix.Sdk.Streaming


class SaslMechanism(Enum):
    Gssapi = 0,
    Plain = 1,
    ScramSha256 = 2,
    ScramSha512 = 3,
    OAuthBearer = 4

    @staticmethod
    def convert_from_net(net_enum: Quix.Sdk.Streaming.Configuration.SaslMechanism):
        if net_enum == Quix.Sdk.Streaming.Configuration.SaslMechanism.Gssapi:
            return SaslMechanism.Gssapi
        if net_enum == Quix.Sdk.Streaming.Configuration.SaslMechanism.Plain:
            return SaslMechanism.Plain
        if net_enum == Quix.Sdk.Streaming.Configuration.SaslMechanism.ScramSha256:
            return SaslMechanism.ScramSha256
        if net_enum == Quix.Sdk.Streaming.Configuration.SaslMechanism.ScramSha512:
            return SaslMechanism.ScramSha512
        if net_enum == Quix.Sdk.Streaming.Configuration.SaslMechanism.OAuthBearer:
            return SaslMechanism.OAuthBearer
        raise Exception('An unhandled SaslMechanism (' + str(net_enum) + ')')

    def convert_to_net(self):
        if self == SaslMechanism.Gssapi:
            return Quix.Sdk.Streaming.Configuration.SaslMechanism.Gssapi
        if self == SaslMechanism.Plain:
            return Quix.Sdk.Streaming.Configuration.SaslMechanism.Plain
        if self == SaslMechanism.ScramSha256:
            return Quix.Sdk.Streaming.Configuration.SaslMechanism.ScramSha256
        if self == SaslMechanism.ScramSha512:
            return Quix.Sdk.Streaming.Configuration.SaslMechanism.ScramSha512
        if self == SaslMechanism.OAuthBearer:
            return Quix.Sdk.Streaming.Configuration.SaslMechanism.OAuthBearer
        raise Exception('An unhandled SaslMechanism (' + str(self) + ')')
