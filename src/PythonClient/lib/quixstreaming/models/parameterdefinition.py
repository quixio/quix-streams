from .. import __importnet
import Quix.Sdk.Streaming


class ParameterDefinition(object):
    """
    Describes additional context for the parameter
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.ParameterDefinition):
        """
        Initializes a new instance of ParameterDefinition

        NOTE: Do not initialize this class manually. Instances of it are available on StreamParametersReader.definitions
        :param net_object: The .net object representing ParameterDefinition.
        """
        self.__wrapped = net_object

    def __str__(self):
        text = "id: " + str(self.id)
        text += "\r\n  location: " + str(self.location)
        text += "\r\n  name: " + str(self.name)
        text += "\r\n  description: " + str(self.description)
        text += "\r\n  unit: " + str(self.unit)
        text += "\r\n  format: " + str(self.format)
        text += "\r\n  minimum_value: " + str(self.minimum_value)
        text += "\r\n  maximum_value: " + str(self.maximum_value)
        text += "\r\n  custom_properties: " + str(self.custom_properties)
        return text

    @property
    def id(self) -> str:
        """Gets the globally unique identifier of the parameter"""
        return self.__wrapped.Id

    @property
    def name(self) -> str:
        """Gets the human friendly display name of the parameter"""
        return self.__wrapped.Name

    @property
    def format(self) -> str:
        """Gets the formatting to apply on the value for display purposes"""
        return self.__wrapped.Format

    @property
    def unit(self) -> str:
        """Gets the unit of the parameter"""
        return self.__wrapped.Unit

    @property
    def maximum_value(self) -> float:
        """Gets the maximum value of the parameter"""
        return self.__wrapped.MaximumValue

    @property
    def minimum_value(self) -> float:
        """Gets the minimum value of the parameter"""
        return self.__wrapped.MinimumValue

    @property
    def custom_properties(self) -> str:
        """
        Gets the optional field for any custom properties that do not exist on the parameter.
        For example this could be a json string, describing the optimal value range of this parameter
        """
        return self.__wrapped.CustomProperties

    @property
    def description(self) -> str:
        """Gets the description of the parameter"""
        return self.__wrapped.Description

    @property
    def location(self) -> str:
        """Gets the location of the parameter within the Parameter hierarchy. Example: "/", "car/chassis/suspension"""
        return self.__wrapped.Location
