import ctypes
from datetime import datetime, timedelta
from typing import Dict, Union

import pandas as pd

from ..helpers.dotnet.datetimeconverter import DateTimeConverter
from ..helpers.nativedecorator import nativedecorator
from ..helpers.timeconverter import TimeConverter
from ..native.Python.InteropHelpers.ExternalTypes.System.Dictionary import Dictionary as di
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils as iu
from ..native.Python.QuixStreamsStreaming.Models.EventData import EventData as edi


@nativedecorator
class EventData(object):
    """Represents a single point in time with event value and tags attached to it."""

    def __init__(self, event_id: str = None, time: Union[int, str, datetime, pd.Timestamp] = None, value: str = None, net_pointer: ctypes.c_void_p = None):
        """
        Initializes a new instance of EventData.

        Args:
            event_id: The unique id of the event the value belongs to.
            time: The time at which the event has occurred in nanoseconds since epoch or as a datetime.
            value: The value of the event.
            net_pointer: Pointer to an instance of a .net EventData.
        """
        if net_pointer is None:
            if event_id is None:
                raise Exception("event_id must be set")
            if time is None:
                raise Exception("time must be set")
            converted_time = EventData._convert_time(time)
            self._interop = edi(edi.Constructor(eventId=event_id, timestampNanoseconds=converted_time, eventValue=value))
        else:
            self._interop = edi(net_pointer)

    @staticmethod
    def _convert_time(time):
        if isinstance(time, datetime):
            if isinstance(time, pd.Timestamp):
                time = time.to_pydatetime()
            return TimeConverter.to_unix_nanoseconds(time)
        if isinstance(time, str):
            return TimeConverter.from_string(time)
        return time

    def __str__(self):
        text = "Time: " + str(self.timestamp_nanoseconds)
        text += "\r\n  Tags: " + str(self.tags)
        text += "\r\n  Value: " + str(self.value)
        return text

    @property
    def id(self) -> str:
        """Gets the globally unique identifier of the event."""
        return self._interop.get_Id()

    @id.setter
    def id(self, value: str) -> None:
        """Sets the globally unique identifier of the event."""
        self._interop.set_Id(value)

    @property
    def value(self) -> str:
        """Gets the value of the event."""
        return self._interop.get_Value()

    @value.setter
    def value(self, value: str) -> None:
        """Sets the value of the event."""
        self._interop.set_Value(value)

    @property
    def tags(self) -> Dict[str, str]:
        """Gets the tags for the timestamp.

        If a key is not found, it returns None.
        The dictionary key is the tag id.
        The dictionary value is the tag value.
        """
        tags_hptr = self._interop.get_Tags()
        try:
            tags_uptr = di.ReadAnyHPtrToUPtr(tags_hptr)
            return di.ReadStringStrings(tags_uptr)
        finally:
            iu.free_hptr(tags_hptr)

    @property
    def timestamp_nanoseconds(self) -> int:
        """Gets timestamp in nanoseconds."""
        return self._interop.get_TimestampNanoseconds()

    @property
    def timestamp_milliseconds(self) -> int:
        """Gets timestamp in milliseconds."""
        return self._interop.get_TimestampMilliseconds()

    @property
    def timestamp(self) -> datetime:
        """Gets the timestamp in datetime format."""
        dt_hptr = self._interop.get_Timestamp()
        return DateTimeConverter.datetime_to_python(dt_hptr)

    @property
    def timestamp_as_time_span(self) -> timedelta:
        """Gets the timestamp in timespan format."""

        ts_uptr = self._interop.get_TimestampAsTimeSpan()
        return DateTimeConverter.timespan_to_python(ts_uptr)

    def clone(self):
        """Clones the event data.

        Returns:
            EventData: Cloned EventData object.
        """
        cloned_pointer = self._interop.Clone()
        return EventData(net_pointer=cloned_pointer)

    def add_tag(self, tag_id: str, tag_value: str) -> 'EventData':
        """Adds a tag to the event.

        Args:
            tag_id: The id of the tag.
            tag_value: The value to set.

        Returns:
            EventData: The updated EventData object.
        """
        hptr = self._interop.AddTag(tag_id, tag_value)
        iu.free_hptr(hptr)
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'EventData':
        """Adds tags from the specified dictionary. Conflicting tags will be overwritten.

        Args:
            tags: The tags to add.

        Returns:
            EventData: The updated EventData object.
        """
        if tags is None:
            return self

        for key, val in tags.items():
            self._interop.AddTag(key, val)
        return self

    def remove_tag(self, tag_id: str) -> 'EventData':
        """Removes a tag from the event.

        Args:
            tag_id: The id of the tag to remove.

        Returns:
            EventData: The updated EventData object.
        """
        hptr = self._interop.RemoveTag(tag_id)
        iu.free_hptr(hptr)
        return self

    def get_net_pointer(self):
        """Gets the associated .net object pointer.

        Returns:
            The .net object pointer.
        """
        return self._interop.get_interop_ptr__()

