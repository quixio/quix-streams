from quixstreaming import __importnet
import Quix.Sdk.Streaming


class ParameterDefinitionBuilder(object):
    """
        Builder for creating parameter and group definitions for StreamPropertiesWriter
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamWriter.ParameterDefinitionBuilder):
        """
            Initializes a new instance of ParameterDefinitionBuilder.

            Parameters:

            net_object (.net object): The .net object representing ParameterDefinitionBuilder.
        """

        self.__wrapped = net_object

    def set_range(self, minimum_value: float, maximum_value: float) -> 'ParameterDefinitionBuilder':
        """
        Set the minimum and maximum range of the parameter
        :param minimum_value: The minimum value
        :param maximum_value: The maximum value
        :return: The builder
        """
        self.__wrapped = self.__wrapped.SetRange(minimum_value, maximum_value)
        return self

    def set_unit(self, unit: str) -> 'ParameterDefinitionBuilder':
        """
        Set the unit of the parameter
        :param unit: The unit of the parameter
        :return: The builder
        """
        self.__wrapped = self.__wrapped.SetUnit(unit)
        return self

    def set_format(self, format: str) -> 'ParameterDefinitionBuilder':
        """
        Set the format of the parameter
        :param format: The format of the parameter
        :return: The builder
        """
        self.__wrapped = self.__wrapped.SetFormat(format)
        return self

    def set_custom_properties(self, custom_properties: str) -> 'ParameterDefinitionBuilder':
        """
        Set the custom properties of the parameter
        :param custom_properties: The custom properties of the parameter
        :return: The builder
        """
        self.__wrapped = self.__wrapped.SetCustomProperties(custom_properties)
        return self

    def add_definition(self, parameter_id: str, name: str = None, description: str = None) -> 'ParameterDefinitionBuilder':
        """
        Add new parameter definition to the StreamPropertiesWriter. Configure it with the builder methods.
        :param parameter_id: The id of the parameter. Must match the parameter id used to send data.
        :param name: The human friendly display name of the parameter
        :param description: The description of the parameter
        :return: The builder
        """
        self.__wrapped = self.__wrapped.AddDefinition(parameter_id, name, description)
        return self
