from enum import Enum


class SaslMechanism(Enum):
    Gssapi = 0
    Plain = 1
    ScramSha256 = 2
    ScramSha512 = 3
    OAuthBearer = 4
