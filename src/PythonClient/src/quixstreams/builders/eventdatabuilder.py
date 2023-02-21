import ctypes
from typing import Dict

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixSdkStreaming.Models.StreamProducer.EventDataBuilder import EventDataBuilder as edbi


@nativedecorator
class EventDataBuilder(object):
    """
        Builder for creating event data packages for StreamPropertiesProducer
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of EventDataBuilder.

            Parameters:

            net_pointer: Pointer to an instance of a .net EventDataBuilder.
        """

        if net_pointer is None:
            raise Exception("EventDataBuilder is none")

        self._interop = edbi(net_pointer)
        self._entered = False

    def __enter__(self):
        self._entered = True

    def add_value(self, event_id: str, value: str) -> 'EventDataBuilder':
        """
        Adds new event at the time the builder is created for
        :param event_id: The id of the event to set the value for
        :param value: the string value
        :return: The builder
        """

        new = edbi(self._interop.AddValue(event_id, value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_tag(self, tag_id: str, value: str) -> 'EventDataBuilder':
        """
        Sets tag value for the values
        :param tag_id: The id of the tag
        :param value: The value of the tag
        :return: The builder
        """

        new = edbi(self._interop.AddTag(tag_id, value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'EventDataBuilder':
        """
            Copies the tags from the specified dictionary. Conflicting tags will be overwritten
            :param tags: The tags to add

        :return: EventDataBuilder
        """
        raise NotImplementedError("TODO")

        if tags is None:
            return self

        # TODO
        prep_tags_dict = System.Collections.Generic.List[System.Collections.Generic.KeyValuePair[str, str]]([])
        for key, val in tags.items():
            prep_tags_dict.Add(System.Collections.Generic.KeyValuePair[str, str](key, val))
        self.__wrapped.AddTags(prep_tags_dict)
        return self

    def write(self):
        """
        Writes the values to the StreamEventsProducer buffer. See StreamEventsProducer buffer settings for more information when the values are sent to the broker
        """

        self._interop.Write()

        if not self._entered:
            self.dispose()
