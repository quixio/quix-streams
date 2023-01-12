from quixstreaming.models.netdict import NetDict
from quixstreaming.models.netlist import NetList
from quixstreaming import __importnet
from quixstreaming.helpers import *
from datetime import datetime
import Quix.Sdk.Process


class StreamProperties(object):
    """
        Provides additional context for the stream
    """

    @property
    def name(self) -> str:
        """Get the human friendly name of the stream"""
        return self.__wrapped.Name

    @property
    def location(self) -> str:
        """Get the location of the stream in data catalogue. For example: /cars/ai/carA/."""
        return self.__wrapped.Location

    @property
    def metadata(self) -> NetDict:
        """Get the additional metadata for the stream"""
        return NetDict(self.__wrapped.Metadata)

    @property
    def parents(self) -> NetList:
        """Get The ids of streams this stream is derived from"""
        return NetList(self.__wrapped.Parents)

    @property
    def time_of_recording(self) -> datetime:
        """Get The ids of streams this stream is derived from"""
        return NetToPythonConverter.convert_datetime(self.__wrapped.TimeOfRecording)

    def __init__(self, net_object: Quix.Sdk.Process.Models.StreamProperties):
        """
            Initializes a new instance of StreamProperties.

            NOTE: Do not initialize this class manually. Will be initialized by StreamReader.on_stream_properties_changed
            Parameters:

            net_object (.net object): The .net object representing StreamProperties.
        """

        self.__wrapped: Quix.Sdk.Process.Models.StreamProperties = net_object

