from .eventlevel import EventLevel

from .. import __importnet
import Quix.Sdk.Streaming


class EventDefinition(object):
    """
    Describes additional context for the event
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.EventDefinition):
        """
        Initializes a new instance of EventDefinition

        NOTE: Do not initialize this class manually. Instances of it are available on StreamEventsReader.definitions
        :param net_object: The .net object representing EventDefinition.
        """

        if net_object is None:
            raise Exception("StreamPropertiesReader is none")
        self.__wrapped = net_object

    def __str__(self):
        text = "id: " + str(self.id)
        text += "\r\n  location: " + str(self.location)
        text += "\r\n  name: " + str(self.name)
        text += "\r\n  description: " + str(self.description)
        text += "\r\n  level: " + str(self.level)
        text += "\r\n  custom_properties: " + str(self.custom_properties)
        return text

    @property
    def id(self) -> str:
        """Gets the globally unique identifier of the event"""
        return self.__wrapped.Id

    @property
    def name(self) -> str:
        """Gets the human friendly display name of the event"""
        return self.__wrapped.Name

    @property
    def level(self) -> EventLevel:
        """Gets the human friendly display name of the event"""
        return EventLevel.convert_from_net(self.__wrapped.Level)

    @property
    def custom_properties(self) -> str:
        """
        Gets the optional field for any custom properties that do not exist on the event.
        For example this could be a json string, describing the optimal value range of this event
        """
        return self.__wrapped.CustomProperties

    @property
    def description(self) -> str:
        """Gets the description of the event"""
        return self.__wrapped.Description

    @property
    def location(self) -> str:
        """Gets the location of the event within the Event hierarchy. Example: "/", "car/chassis/suspension"""
        return self.__wrapped.Location
