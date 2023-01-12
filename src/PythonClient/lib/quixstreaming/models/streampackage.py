from quixstreaming.models.netdict import NetDict
from quixstreaming.models.netlist import NetList
from quixstreaming import __importnet
from quixstreaming.helpers import *
from datetime import datetime
import Quix.Sdk.Process


class StreamPackage(object):
    """
        Default model implementation for non-typed message packages of the Process layer. It holds a value and its type.
    """

    @property
    def type(self) -> str:
        """Type of the content value"""
        return self.__wrapped.Type

    @property
    def value(self) -> object:
        """Content value of the package"""
        return self.__wrapped.Value

    @property
    def transport_context(self) -> NetDict:
        """Get the additional metadata for the stream"""
        return NetDict(self.__wrapped.TransportContext)

    def to_json(self) -> str:
        """
            Serialize the package into Json
        """
        return self.__wrapped.ToJson()
        

    def __init__(self, net_object: Quix.Sdk.Process.Models.StreamPackage):
        """
            Initializes a new instance of StreamPackage.

            NOTE: Do not initialize this class manually. Will be initialized by StreamReader.on_package_received
            Parameters:

            net_object (.net object): The .net object representing StreamPackage.
        """

        self.__wrapped: Quix.Sdk.Process.Models.StreamPackage = net_object

