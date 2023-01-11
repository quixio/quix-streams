from datetime import datetime, timedelta
from typing import Dict, Union

from quixstreaming.helpers import *
from .netdict import ReadOnlyNetDict

from .. import __importnet
import Quix.Sdk.Streaming
from ..helpers.nettopythonconverter import NetToPythonConverter
import clr
clr.AddReference('System.Collections')
import System.Collections.Generic
import pandas as pd

class EventData(object):
    """
    Represents a single point in time with event value and tags attached to it
    """

    def __init__(self, event_id: str = None, time: Union[int, str, datetime, pd.Timestamp] = None, value: str = None, _net_object: Quix.Sdk.Streaming.Models.EventData = None):
        """
            Initializes a new instance of EventData.

            Parameters:
            :param event_id: the unique id of the event the value belongs to
            :param time: the time at which the event has occurred in nanoseconds since epoch or as a datetime
            :param value: the value of the event
            :param _net_object: this is wrapped .net object, which you can safely ignore. Here for internal purposes
        """
        if _net_object is None:
            if event_id is None:
                raise Exception("event_id must be set")
            if time is None:
                raise Exception("time must be set")
            converted_time = EventData.__convert_time(time)
            _net_object = Quix.Sdk.Streaming.Models.EventData(event_id, converted_time,  value)
        self.__wrapped = _net_object

    @staticmethod
    def __convert_time(time):
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

    @staticmethod
    def convert_from_net(net_object: Quix.Sdk.Streaming.Models.EventData):
        if net_object is None:
            raise Exception("EventData is none")
        return EventData(_net_object=net_object)

    @property
    def timestamp(self) -> int:
        """Gets the timestamp of events in nanoseconds since unix epoch"""
        return self.__wrapped.Timestamp

    @property
    def id(self) -> str:
        """Gets the globally unique identifier of the event"""
        return self.__wrapped.Id

    @id.setter
    def id(self, value: str) -> None:
        """Sets the globally unique identifier of the event"""
        self.__wrapped.Id = value

    @property
    def value(self) -> str:
        """Gets the value of the event"""
        return self.__wrapped.Value

    @value.setter
    def value(self, value: str) -> None:
        """Sets the value of the event"""
        self.__wrapped.Value = value

    @property
    def tags(self) -> ReadOnlyNetDict:
        """
        Tags for the timestamp. When key is not found, returns None
        The dictionary key is the tag id
        The dictionary value is the tag value
        """

        return ReadOnlyNetDict(self.__wrapped.Tags)

    @property
    def timestamp_nanoseconds(self) -> int:
        """Gets timestamp in nanoseconds"""

        return self.__wrapped.TimestampNanoseconds

    @property
    def timestamp_milliseconds(self) -> int:
        """Gets timestamp in milliseconds"""

        return self.__wrapped.TimestampNanoseconds

    @property
    def timestamp(self) -> datetime:
        """Gets the timestamp in datetime format"""
        return NetToPythonConverter.convert_datetime(self.__wrapped.Timestamp)

    @property
    def timestamp_as_time_span(self) -> timedelta:
        """Gets the timestamp in timespan format"""
        return NetToPythonConverter.convert_timespan(self.__wrapped.TimestampAsTimeSpan)

    def clone(self):
        """ Clones the event data """
        cloned = self.__wrapped.Clone()
        return EventData.convert_from_net(cloned)

    def add_tag(self, tag_id: str, tag_value: str)  -> 'EventData':
        """
            Adds a tag to the event
            :param tag_id: The id of the tag to the set the value for
            :param tag_value: the value to set

        :return: EventData
        """
        self.__wrapped.AddTag(tag_id, tag_value)
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'EventData':
        """
            Adds the tags from the specified dictionary. Conflicting tags will be overwritten
            :param tags: The tags to add

        :return: EventData
        """

        if tags is None:
            return self

        prep_tags_dict = System.Collections.Generic.List[System.Collections.Generic.KeyValuePair[str, str]]([])
        for key, val in tags.items():
            prep_tags_dict.Add(System.Collections.Generic.KeyValuePair[str, str](key, val))
        self.__wrapped.AddTags(prep_tags_dict)
        return self

    def remove_tag(self, tag_id: str)  -> 'EventData':
        """
            Removes a tag from the event
            :param tag_id: The id of the tag to remove

        :return: EventData
        """
        self.__wrapped.RemoveTag(tag_id)
        return self

    def convert_to_net(self):
        return self.__wrapped