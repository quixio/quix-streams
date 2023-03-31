import ctypes

from ..helpers.enumconverter import EnumConverter as ec
from ..helpers.nativedecorator import nativedecorator
from ..models.eventlevel import EventLevel
from ..native.Python.QuixStreamsTelemetry.Models.EventLevel import EventLevel as InteropEventLevel
from ..native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder import EventDefinitionBuilder as edbi


@nativedecorator
class EventDefinitionBuilder(object):
    """Builder for creating EventDefinitions within StreamPropertiesProducer."""

    def __init__(self, net_pointer: ctypes.c_void_p):
        """Initializes a new instance of EventDefinitionBuilder.

        Args:
            net_pointer: Pointer to an instance of a .net EventDefinitionBuilder.
        """
        if net_pointer is None:
            raise Exception("EventDefinitionBuilder is none")

        self._interop = edbi(net_pointer)

    def set_level(self, level: EventLevel) -> 'EventDefinitionBuilder':
        """Set severity level of the Event.

        Args:
            level: The severity level of the event.

        Returns:
            The builder.
        """
        converted_level = ec.enum_to_another(level, InteropEventLevel)
        new = edbi(self._interop.SetLevel(converted_level))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_custom_properties(self, custom_properties: str) -> 'EventDefinitionBuilder':
        """Set custom properties of the Event.

        Args:
            custom_properties: The custom properties of the event.

        Returns:
            The builder.
        """
        new = edbi(self._interop.SetCustomProperties(custom_properties))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_definition(self, event_id: str, name: str = None, description: str = None) -> 'EventDefinitionBuilder':
        """Add new Event definition, to define properties like Name or Level, among others.

        Args:
            event_id: Event id. This must match the event id you use to publish event values.
            name: Human friendly display name of the event.
            description: Description of the event.

        Returns:
            Event definition builder to define the event properties.
        """
        new = edbi(self._interop.AddDefinition(event_id, name, description))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self
