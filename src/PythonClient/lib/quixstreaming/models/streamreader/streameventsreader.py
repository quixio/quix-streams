from typing import List

from quixstreaming.eventhook import EventHook

from quixstreaming import __importnet, EventDefinition, EventData
import Quix.Sdk.Streaming


class StreamEventsReader(object):

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamReader.StreamEventsReader):
        """
            Initializes a new instance of StreamEventsReader.
            NOTE: Do not initialize this class manually, use StreamReader.events to access an instance of it

            Parameters:

            net_object (.net object): The .net object representing a StreamEventsReader
        """
        if net_object is None:
            raise Exception("StreamEventsReader is none")
        self.__wrapped = net_object

        self.on_read = EventHook(name="StreamEventsReader.on_read")
        """
        Raised when an event data package is read for the stream
        
        Has one argument of type EventData
        """

        self.on_definitions_changed = EventHook(name="StreamEventsReader.on_definitions_changed")
        """
        Raised when the definitions have changed for the stream. Access "definitions" for latest set of event definitions 

        Has no arguments
        """

        def __on_read_net_handler(arg):
            self.on_read.fire(EventData.convert_from_net(arg))
        self.__wrapped.OnRead += __on_read_net_handler

        def __on_definitions_changed_net_handler():
            self.on_definitions_changed.fire()
        self.__wrapped.OnDefinitionsChanged += __on_definitions_changed_net_handler

    @property
    def definitions(self) -> List[EventDefinition]:
        """ Gets the latest set of event definitions """
        items = []
        for element in self.__wrapped.Definitions:
            item = EventDefinition(element)
            items.append(item)
        return items
