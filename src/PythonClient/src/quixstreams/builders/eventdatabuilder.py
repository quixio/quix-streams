import ctypes
from typing import Dict

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder import EventDataBuilder as edbi


@nativedecorator
class EventDataBuilder(object):
    """Builder for creating event data packages for StreamEventsProducer."""

    def __init__(self, net_pointer: ctypes.c_void_p):
        """Initializes a new instance of EventDataBuilder.

        Args:
            net_pointer: Pointer to an instance of a .net EventDataBuilder.
        """
        if net_pointer is None:
            raise Exception("EventDataBuilder is none")

        self._interop = edbi(net_pointer)
        self._entered = False

    def __enter__(self):
        self._entered = True

    def add_value(self, event_id: str, value: str) -> 'EventDataBuilder':
        """Adds new event at the time the builder is created for.

        Args:
            event_id: The id of the event to set the value for.
            value: The string value.

        Returns:
            The builder.
        """
        new = edbi(self._interop.AddValue(event_id, value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_tag(self, tag_id: str, value: str) -> 'EventDataBuilder':
        """Sets tag value for the values.

        Args:
            tag_id: The id of the tag.
            value: The value of the tag.

        Returns:
            The builder.
        """
        new = edbi(self._interop.AddTag(tag_id, value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'EventDataBuilder':
        """Copies the tags from the specified dictionary. Conflicting tags will be overwritten.

        Args:
            tags: The tags to add.

        Returns:
            The builder.
        """
        if tags is None:
            return self

        for key, val in tags.items():
            self.add_tag(key, val)  # TODO use the bulk add self._interop.AddTags()
        return self

    def publish(self):
        """Publishes the values to the StreamEventsProducer buffer.

        See StreamEventsProducer buffer settings for more information on when the values are sent to the broker.
        """
        self._interop.Publish()

        if not self._entered:
            self.dispose()
