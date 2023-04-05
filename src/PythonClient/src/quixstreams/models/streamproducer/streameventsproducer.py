import ctypes
from datetime import datetime, timedelta
from typing import Union, Dict

import pandas as pd

from ... import EventData
from ...builders import *
from ...helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ...helpers.nativedecorator import nativedecorator
from ...native.Python.InteropHelpers.ExternalTypes.System.Dictionary import Dictionary as di
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer import StreamEventsProducer as sepi


@nativedecorator
class StreamEventsProducer(object):
    """
    Helper class for producing EventDefinitions and EventData.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamEventsProducer.

        Args:
            net_pointer: Pointer to an instance of a .NET StreamEventsProducer.
        """

        if net_pointer is None:
            raise Exception("StreamEventsProducer is none")

        self._interop = sepi(net_pointer)

    def flush(self):
        """
        Immediately publishes the event definitions from the buffer without waiting for buffer condition to fulfill
        (200ms timeout). TODO: Verify 200ms timeout value.
        """
        self._interop.Flush()

    @property
    def default_tags(self) -> Dict[str, str]:
        """Gets default tags injected to all event values sent by the producer."""
        dic_hptr = self._interop.get_DefaultTags()
        return InteropUtils.invoke_and_free(dic_hptr, lambda x: di.ReadStringStrings(di.ReadAnyHPtrToUPtr(x)), default={})

    @property
    def default_location(self) -> str:
        """
        Gets the default Location of the events. Event definitions added with add_definition will be inserted at this location.
        See add_location for adding definitions at a different location without changing default.
        Example: "/Group1/SubGroup2"
        """
        return self._interop.get_DefaultLocation()

    @default_location.setter
    def default_location(self, value: str):
        """
        Sets the default Location of the events. Event definitions added with add_definition will be inserted at this location.
        See add_location for adding definitions at a different location without changing default.

        Args:
            value: Location string, e.g., "/Group1/SubGroup2".
        """
        self._interop.set_DefaultLocation(value)

    @property
    def epoch(self) -> datetime:
        """The unix epoch from, which all other timestamps in this model are measured from in nanoseconds."""

        ptr = self._interop.get_Epoch()
        value = dtc.datetime_to_python(ptr)
        return value

    @epoch.setter
    def epoch(self, value: datetime):
        """Sets the default epoch used for event values."""
        dotnet_value = dtc.datetime_to_dotnet(value)
        self._interop.set_Epoch(dotnet_value)

    def publish(self, data: Union[EventData, pd.DataFrame], **columns) -> None:
        """
        Publish an event into the stream.

        Args:
            data: EventData object or a pandas dataframe.
            columns: Column names if the dataframe has different columns from 'id', 'timestamp', and 'value'.
                For instance, if 'id' is in the column 'event_id', id='event_id' must be passed as an argument.

        Raises:
            TypeError: If the data argument is neither an EventData nor pandas dataframe.
        """
        if isinstance(data, EventData):
            self._interop.Publish(data.get_net_pointer())
        elif isinstance(data, pd.DataFrame):
            id = 'id' if 'id' not in columns else columns['id']
            timestamp = 'timestamp' if 'timestamp' not in columns else columns['timestamp']
            value = 'value' if 'value' not in columns else columns['value']
            for row in data.itertuples():
                event = EventData(event_id=getattr(row, id), time=getattr(row, timestamp), value=getattr(row, value))
                self._interop.Publish(event.get_net_pointer())
        else:
            raise TypeError(str(type(data)) + " is not supported.")

    def add_timestamp(self, time: Union[datetime, timedelta]) -> EventDataBuilder:
        """
        Start adding a new set of event values at the given timestamp.

        Args:
            time: The time to use for adding new event values.
                * datetime: The datetime to use for adding new event values. NOTE, epoch is not used.
                * timedelta: The time since the default epoch to add the event values at

        Returns:
            EventDataBuilder: Event data builder to add event values at the provided time.
        """
        if time is None:
            raise ValueError("'time' must not be None")
        if isinstance(time, datetime):
            try:
                dotnet_date = dtc.datetime_to_dotnet(time)
                return EventDataBuilder(self._interop.AddTimestamp(dotnet_date))
            finally:
                InteropUtils.free_hptr(dotnet_date)  # dotnet will hold a reference to it, we no longer need it
        if isinstance(time, timedelta):
            dotnet_timespan = dtc.timedelta_to_dotnet(time)
            return EventDataBuilder(self._interop.AddTimestamp2(dotnet_timespan))
        raise ValueError("'time' must be either datetime or timedelta")

    def add_timestamp_milliseconds(self, milliseconds: int) -> EventDataBuilder:
        """
        Start adding a new set of event values at the given timestamp.

        Args:
            milliseconds: The time in milliseconds since the default epoch to add the event values at.

        Returns:
            EventDataBuilder: Event data builder to add event values at the provided time.
        """
        return EventDataBuilder(self._interop.AddTimestampMilliseconds(milliseconds))

    def add_timestamp_nanoseconds(self, nanoseconds: int) -> EventDataBuilder:
        """
        Start adding a new set of event values at the given timestamp.

        Args:
            nanoseconds: The time in nanoseconds since the default epoch to add the event values at.

        Returns:
            EventDataBuilder: Event data builder to add event values at the provided time.
        """
        return EventDataBuilder(self._interop.AddTimestampNanoseconds(nanoseconds))

    def add_definition(self, event_id: str, name: str = None, description: str = None) -> EventDefinitionBuilder:
        """
        Add new Event definition to define properties like Name or Level, among others.

        Args:
            event_id: The id of the event. Must match the event id used to send data.
            name: The human-friendly display name of the event.
            description: The description of the event.

        Returns:
            EventDefinitionBuilder: EventDefinitionBuilder to define properties of the event or add additional events.
        """
        return EventDefinitionBuilder(self._interop.AddDefinition(event_id, name, description))

    def add_location(self, location: str) -> EventDefinitionBuilder:
        """
        Add a new location in the events groups hierarchy.

        Args:
            location: The group location.

        Returns:
            EventDefinitionBuilder: EventDefinitionBuilder to define the events under the specified location.
        """
        return EventDefinitionBuilder(self._interop.AddLocation(location))
