from .eventlevel import EventLevel

from ..native.Python.QuixSdkStreaming.Models.EventDefinition import EventDefinition as edi
from ..helpers.enumconverter import EnumConverter as ec
import ctypes


class EventDefinition(object):
    """
    Describes additional context for the event
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of EventDefinition

        NOTE: Do not initialize this class manually. Instances of it are available on StreamEventsReader.definitions
        :param net_pointer: Pointer to an instance of a .net EventDefinition
        """

        if net_pointer is None:
            raise Exception("StreamPropertiesReader is none")
        with (interop := edi(net_pointer)):

            self.id: str = interop.get_Id()
            """Gets the globally unique identifier of the event"""

            self.name: str = interop.get_Name() 
            """Gets the human friendly display name of the event"""
    
            self.level: EventLevel = ec.enum_to_another(interop.get_Level(), EventLevel)
            """Gets the human friendly display name of the event"""
    
            self.custom_properties: str = interop.get_CustomProperties()
            """
            Gets the optional field for any custom properties that do not exist on the event.
            For example this could be a json string, describing the optimal value range of this event
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
