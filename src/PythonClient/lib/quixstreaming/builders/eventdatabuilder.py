from typing import Dict

from quixstreaming import __importnet
import Quix.Sdk.Streaming
import clr
clr.AddReference('System.Collections')
import System.Collections.Generic

class EventDataBuilder(object):
    """
        Builder for creating event data packages for StreamPropertiesWriter
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamWriter.EventDataBuilder):
        """
            Initializes a new instance of EventDataBuilder.

            Events:

            net_object (.net object): The .net object representing EventDataBuilder.
        """

        self.__wrapped = net_object

    def add_value(self, event_id: str, value: str) -> 'EventDataBuilder':
        """
        Adds new event at the time the builder is created for
        :param event_id: The id of the event to set the value for
        :param value: the string value
        :return: The builder
        """
        self.__wrapped = self.__wrapped.AddValue(event_id, value)
        return self

    def add_tag(self, tag_id: str, value: str) -> 'EventDataBuilder':
        """
        Sets tag value for the values
        :param tag_id: The id of the tag
        :param value: The value of the tag
        :return: The builder
        """
        self.__wrapped = self.__wrapped.AddTag(tag_id, value)
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'EventDataBuilder':
        """
            Copies the tags from the specified dictionary. Conflicting tags will be overwritten
            :param tags: The tags to add

        :return: EventDataBuilder
        """

        if tags is None:
            return self

        prep_tags_dict = System.Collections.Generic.List[System.Collections.Generic.KeyValuePair[str, str]]([])
        for key, val in tags.items():
            prep_tags_dict.Add(System.Collections.Generic.KeyValuePair[str, str](key, val))
        self.__wrapped.AddTags(prep_tags_dict)
        return self

    def write(self):
        """
        Writes the values to the StreamEventsWriter buffer. See StreamEventsWriter buffer settings for more information when the values are sent to the broker
        """
        self.__wrapped.Write()
