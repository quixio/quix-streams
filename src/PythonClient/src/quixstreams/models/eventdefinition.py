import ctypes

from .eventlevel import EventLevel
from ..helpers.enumconverter import EnumConverter as ec
from ..native.Python.QuixStreamsStreaming.Models.EventDefinition import EventDefinition as edi


class EventDefinition(object):
    """
    Describes additional context for the event
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of EventDefinition

        NOTE: Do not initialize this class manually. Instances of it are available on StreamEventsConsumer.definitions
        :param net_pointer: Pointer to an instance of a .net EventDefinition
        """

        if net_pointer is None:
            raise Exception("StreamPropertiesConsumer is none")
        with (interop := edi(net_pointer)):
            self.id: str = interop.get_Id()
            """Gets the globally unique identifier of the event"""

            self.name: str = interop.get_Name()
            """Gets the human friendly display name of the event"""

            self.level: EventLevel = ec.enum_to_another(interop.get_Level(), EventLevel)
            """Gets the level of the event"""

            self.custom_properties: str = interop.get_CustomProperties()
            """
            Gets the optional field for any custom properties that do not exist on the event.
            For example this could be a json string, describing all possible event values
            """

            self.description: str = interop.get_Description()
            """Gets the description of the event"""

            self.location: str = interop.get_Location()
            """Gets the location of the event within the Event hierarchy. Example: "/", "car/chassis/suspension"""

    def __str__(self):
        text = "id: " + str(self.id)
        text += "\r\n  location: " + str(self.location)
        text += "\r\n  name: " + str(self.name)
        text += "\r\n  description: " + str(self.description)
        text += "\r\n  level: " + str(self.level)
        text += "\r\n  custom_properties: " + str(self.custom_properties)
        return text
