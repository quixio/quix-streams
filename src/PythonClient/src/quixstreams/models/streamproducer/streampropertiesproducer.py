import ctypes
from datetime import datetime
from typing import Dict, List

from ..netdict import NetDict
from ..netlist import NetList
from ...helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ...helpers.nativedecorator import nativedecorator
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer import StreamPropertiesProducer as sppi


@nativedecorator
class StreamPropertiesProducer(object):
    """
    Represents properties and metadata of the stream.
    All changes to these properties are automatically published to the underlying stream.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamPropertiesProducer.

        Args:
            net_pointer: Pointer to an instance of a .net StreamPropertiesProducer.
        """

        if net_pointer is None:
            raise Exception("StreamPropertiesProducer is none")

        self._interop = sppi(net_pointer)

    @property
    def name(self) -> str:
        """
        Gets the human friendly name of the stream.

        Returns:
            str: The human friendly name of the stream.
        """
        return self._interop.get_Name()

    @name.setter
    def name(self, value: str):
        """
        Sets the human friendly name of the stream.

        Args:
            value: The new human friendly name of the stream.
        """
        self._interop.set_Name(value)

    @property
    def location(self) -> str:
        """
        Gets the location of the stream in the data catalogue.

        Returns:
            str: The location of the stream in the data catalogue, e.g., "/cars/ai/carA/".
        """
        return self._interop.get_Location()

    @location.setter
    def location(self, value: str):
        """
        Sets the location of the stream in the data catalogue.

        Args:
            value: The new location of the stream in the data catalogue.
        """
        self._interop.set_Location(value)

    @property
    def metadata(self) -> Dict[str, str]:
        """"
        Gets the metadata of the stream.

        Returns:
            Dict[str, str]: The metadata of the stream.
        """
        ptr = self._interop.get_Metadata()
        return NetDict.constructor_for_string_string(ptr)

    @property
    def parents(self) -> List[str]:
        """
        Gets the list of stream ids of the parent streams.

        Returns:
            List[str]: The list of stream ids of the parent streams.
        """
        list_ptr = self._interop.get_Parents()
        return NetList.constructor_for_string(list_ptr)

    @property
    def time_of_recording(self) -> datetime:
        """
        Gets the datetime of the stream recording.

        Returns:
            datetime: The datetime of the stream recording.
        """

        hptr = self._interop.get_TimeOfRecording()
        value = dtc.datetime_to_python(hptr)
        return value

    @time_of_recording.setter
    def time_of_recording(self, value: datetime):
        """
        Sets the time of the stream recording.

        Args:
            value: The new time of the stream recording.
        """
        hptr = dtc.datetime_to_dotnet(value)
        try:
            self._interop.set_TimeOfRecording(hptr)
        finally:
            InteropUtils.free_hptr(hptr)  # native will hold a reference to it if it has to

    @property
    def flush_interval(self) -> int:
        """
        Gets the automatic flush interval of the properties metadata into the channel (in milliseconds).

        Returns:
            int: The automatic flush interval in milliseconds, default is 30000.
        """
        return self._interop.get_FlushInterval()

    @flush_interval.setter
    def flush_interval(self, value: int):
        """
        Sets the automatic flush interval of the properties metadata into the channel (in milliseconds).

        Args:
            value: The new flush interval in milliseconds.
        """
        self._interop.set_FlushInterval(value)

    def flush(self):
        """
        Immediately publishes the properties yet to be sent instead of waiting for the flush timer (20ms).
        """
        self._interop.Flush()
