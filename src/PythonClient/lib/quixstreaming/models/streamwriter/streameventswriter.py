from datetime import datetime, timedelta
from typing import Union
from quixstreaming.builders import *
from quixstreaming.models.netdict import NetDict
from quixstreaming import __importnet, EventData
from quixstreaming.helpers import *
import Quix.Sdk.Streaming
import pandas as pd

from quixstreaming.models.parametersbuffer import ParametersBuffer


class StreamEventsWriter(object):
    """
        Group all the Events properties, builders and helpers that allow to stream event values and event definitions to the platform.
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamWriter.StreamEventsWriter = None):
        """
            Initializes a new instance of StreamEventsWriter.

            Events:

            net_object (.net object): The .net object representing StreamEventsWriter.
        """

        if net_object is None:
            raise Exception("StreamEventsWriter is none")
        else:
            self.__wrapped = net_object

    def flush(self):
        """
        Immediately writes the event definitions from the buffer without waiting for buffer condition to fulfill (200ms timeout)
        """
        self.__wrapped.Flush()

    @property
    def default_tags(self) -> NetDict:
        """Get default tags injected to all Events Values sent by the writer."""
        return NetDict(self.__wrapped.DefaultTags)

    @property
    def default_location(self) -> str:
        """
            Gets the default Location of the events. Event definitions added with add_definition  will be inserted at this location.
            See add_location for adding definitions at a different location without changing default.
            Example: "/Group1/SubGroup2"
        """
        return self.__wrapped.DefaultLocation

    @default_location.setter
    def default_location(self, value: str):
        """
            Sets the default Location of the events. Event definitions added with add_definition will be inserted at this location.
            See add_location for adding definitions at a different location without changing default.
            Example: "/Group1/SubGroup2"
        """
        self.__wrapped.DefaultLocation = value

    @property
    def epoch(self) -> datetime:
        """Get the default epoch used for event values"""
        return NetToPythonConverter.convert_datetime(self.__wrapped.Epoch)

    @epoch.setter
    def epoch(self, value: datetime):
        """Set the default epoch used for event values"""
        self.__wrapped.Epoch = PythonToNetConverter.convert_datetime(value)

    def write(self, data: Union[EventData, pd.DataFrame], **columns) -> None:
        """Writes event into the stream.

        Parameters: data: EventData object or a Pandas dataframe. columns: Column names if the dataframe has
        different columns from 'id', 'timestamp' and 'value'. For instance if 'id' is in the column 'event_id',
        id='event_id' must be passed as an argument.

        Raises:
            TypeError if the data argument is neither an EventData nor Pandas dataframe.
        """
        if isinstance(data, EventData):
            net_object = data.convert_to_net()
            self.__wrapped.Write(net_object)
        elif isinstance(data, pd.DataFrame):
            id = 'id' if 'id' not in columns else columns['id']
            timestamp = 'timestamp' if 'timestamp' not in columns else columns['timestamp']
            value = 'value' if 'value' not in columns else columns['value']
            for row in data.itertuples():
                event = EventData(event_id=getattr(row, id), time=getattr(row, timestamp), value=getattr(row, value))
                obj = event.convert_to_net()
                self.__wrapped.Write(obj)
        else:
            raise TypeError(str(type(data)) + " is not supported.")


    def add_timestamp(self, time: Union[datetime, timedelta]) -> EventDataBuilder:
        """
        Start adding a new set of event values at the given timestamp.
        :param time: The time to use for adding new event values.
                     | datetime: The datetime to use for adding new event values. NOTE, epoch is not used
                     | timedelta: The time since the default epoch to add the event values at

        :return: EventDataBuilder
        """
        if time is None:
            raise ValueError("'time' must not be None")
        if isinstance(time, datetime):
            netdate = PythonToNetConverter.convert_datetime(time)
            return EventDataBuilder(self.__wrapped.AddTimestamp(netdate))
        if isinstance(time, timedelta):
            nettimespan = PythonToNetConverter.convert_timedelta(time)
            return EventDataBuilder(self.__wrapped.AddTimestamp(nettimespan))
        raise ValueError("'time' must be either datetime or timedelta")

    def add_timestamp_milliseconds(self, milliseconds: int) -> EventDataBuilder:
        """
        Start adding a new set of event values at the given timestamp.
        :param milliseconds: The time in milliseconds since the default epoch to add the event values at
        :return: EventDataBuilder
        """
        return EventDataBuilder(self.__wrapped.AddTimestampMilliseconds(milliseconds))

    def add_timestamp_nanoseconds(self, nanoseconds: int) -> EventDataBuilder:
        """
        Start adding a new set of event values at the given timestamp.
        :param nanoseconds: The time in nanoseconds since the default epoch to add the event values at
        :return: EventDataBuilder
        """
        return EventDataBuilder(self.__wrapped.AddTimestampNanoseconds(nanoseconds))

    def add_definition(self, event_id: str, name: str = None, description: str = None) -> EventDefinitionBuilder:
        """
        Add new event definition to the StreamPropertiesWriter. Configure it with the builder methods.
        :param event_id: The id of the event. Must match the event id used to send data.
        :param name: The human friendly display name of the event
        :param description: The description of the event
        :return: EventDefinitionBuilder to define properties of the event or add additional events
        """
        return EventDefinitionBuilder(self.__wrapped.AddDefinition(event_id, name, description))

    def add_location(self, location: str) -> EventDefinitionBuilder:
        """
        Add a new location in the events groups hierarchy
        :param location: The group location
        :return: EventDefinitionBuilder to define the events under the specified location
        """
        return EventDefinitionBuilder(self.__wrapped.AddLocation(location))
