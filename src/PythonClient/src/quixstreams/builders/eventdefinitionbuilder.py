import ctypes

from ..helpers.enumconverter import EnumConverter as ec
from ..helpers.nativedecorator import nativedecorator
from ..models.eventlevel import EventLevel
from ..native.Python.QuixStreamsTelemetry.Models.EventLevel import EventLevel as InteropEventLevel
from ..native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder import EventDefinitionBuilder as edbi


@nativedecorator
class EventDefinitionBuilder(object):
    """
        Builder for creating event and group definitions for StreamPropertiesProducer
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of EventDefinitionBuilder.

            Parameters:

            net_pointer: Pointer to an instance of a .net EventDefinitionBuilder.
        """

        if net_pointer is None:
            raise Exception("EventDefinitionBuilder is none")

        self._interop = edbi(net_pointer)

    def set_level(self, level: EventLevel) -> 'EventDefinitionBuilder':
        """
        Set the custom properties of the event
        :param level: The severity level of the event
        :return: The builder
        """

        converted_level = ec.enum_to_another(level, InteropEventLevel)
        new = edbi(self._interop.SetLevel(converted_level))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_custom_properties(self, custom_properties: str) -> 'EventDefinitionBuilder':
        """
        Set the custom properties of the event
        :param custom_properties: The custom properties of the event
        :return: The builder
        """

        new = edbi(self._interop.SetCustomProperties(custom_properties))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_definition(self, event_id: str, name: str = None, description: str = None) -> 'EventDefinitionBuilder':
        """
        Add new event definition to the StreamPropertiesProducer. Configure it with the builder methods.
        :param event_id: The id of the event. Must match the event id used to send data.
        :param name: The human friendly display name of the event
        :param description: The description of the event
        :return: The builder
        """

        new = edbi(self._interop.AddDefinition(event_id, name, description))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self
