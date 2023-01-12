from quixstreaming.models import StreamProperties
from datetime import datetime
from quixstreaming import __importnet
from quixstreaming.helpers import *
import Quix.Sdk.Streaming


class StreamPropertiesWriter(StreamProperties):
    """
        Provides additional context for the stream
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamWriter.StreamPropertiesWriter = None):
        """
            Initializes a new instance of StreamPropertiesWriter.

            Parameters:

            net_object (.net object): The .net object representing StreamPropertiesWriter.
        """

        if net_object is None:
            raise Exception("StreamPropertiesWriter is none")
        else:
            self.__wrapped = net_object

        super(StreamPropertiesWriter, self).__init__(self.__wrapped)

    @StreamProperties.name.setter
    def name(self, value: str):
        """Set the human friendly name of the stream"""
        self.__wrapped.Name = value

    @StreamProperties.location.setter
    def location(self, value: str):
        """Set the location of the stream in data catalogue. For example: /cars/ai/carA/."""
        self.__wrapped.Location = value

    @StreamProperties.time_of_recording.setter
    def time_of_recording(self, value: datetime):
        """Set the time of recording for the stream. Commonly set to utc now."""
        self.__wrapped.TimeOfRecording = PythonToNetConverter.convert_datetime(value)

    @property
    def flush_interval(self) -> int:
        """
            Get automatic flush interval of the properties metadata into the channel [ in milliseconds ]
            Defaults to 30000.
        """
        return self.__wrapped.FlushInterval

    @flush_interval.setter
    def flush_interval(self, value: int):
        """
            Set automatic flush interval of the properties metadata into the channel [ in milliseconds ]
        """
        self.__wrapped.FlushInterval = value

    def flush(self):
        """Immediately writes the properties yet to be sent instead of waiting for the flush timer (20ms)"""
        self.__wrapped.Flush()
