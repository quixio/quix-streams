from quixstreaming import __importnet
import Quix.Sdk.Streaming

from ..models.eventlevel import EventLevel


class EventDefinitionBuilder(object):
    """
        Builder for creating event and group definitions for StreamPropertiesWriter
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamWriter.EventDefinitionBuilder):
        """
            Initializes a new instance of EventDefinitionBuilder.

            Events:

            net_object (.net object): The .net object representing EventDefinitionBuilder.
        """

        self.__wrapped = net_object

    def set_level(self, level: EventLevel) -> 'EventDefinitionBuilder':
        """
        Set the custom properties of the event
        :param level: The severity level of the event
        :return: The builder
        """
        self.__wrapped = self.__wrapped.SetLevel(level.convert_to_net())
        return self

    def set_custom_properties(self, custom_properties: str) -> 'EventDefinitionBuilder':
        """
        Set the custom properties of the event
        :param custom_properties: The custom properties of the event
        :return: The builder
        """
        self.__wrapped = self.__wrapped.SetCustomProperties(custom_properties)
        return self

    def add_definition(self, event_id: str, name: str = None, description: str = None) -> 'EventDefinitionBuilder':
        """
        Add new event definition to the StreamPropertiesWriter. Configure it with the builder methods.
        :param event_id: The id of the event. Must match the event id used to send data.
        :param name: The human friendly display name of the event
        :param description: The description of the event
        :return: The builder
        """
        self.__wrapped = self.__wrapped.AddDefinition(event_id, name, description)
        return self
