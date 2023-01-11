from typing import Dict

from quixstreaming.eventhook import EventHook
from datetime import datetime, timedelta
from quixstreaming.helpers import *

from quixstreaming import __importnet
import Quix.Sdk.Streaming


class StreamPropertiesReader(object):

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamReader.StreamPropertiesReader):
        """
            Initializes a new instance of StreamPropertiesReader.
            NOTE: Do not initialize this class manually, use StreamReader.properties to access an instance of it

            Parameters:

            net_object (.net object): The .net object representing a StreamPropertiesReader
        """
        if net_object is None:
            raise Exception("StreamPropertiesReader is none")
        else:
            self.__wrapped = net_object

        self.on_changed = EventHook(name="StreamPropertiesReader.on_changed")
        """
        Raised when stream properties changed. This is an event without parameters. Interrogate the class raising
        it to get new state
        """

        def __on_changed_net_handler():
            self.on_changed.fire()
        self.__wrapped.OnChanged += __on_changed_net_handler

    @property
    def name(self) -> str:
        """ Gets the name of the stream """
        return self.__wrapped.Name

    @property
    def location(self) -> str:
        """ Gets the location of the stream """
        return self.__wrapped.Location

    @property
    def time_of_recording(self) -> datetime:
        """ Gets the datetime of the recording """
        return NetToPythonConverter.convert_datetime(self.__wrapped.TimeOfRecording)

    @property
    def metadata(self) -> Dict[str, str]:
        """ Gets the metadata of the stream """
        return NetToPythonConverter.convert_dict(self.__wrapped.Metadata)

    @property
    def parents(self) -> datetime:
        """ Gets the metadata of the stream """
        return NetToPythonConverter.convert_list(self.__wrapped.Parents)