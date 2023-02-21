import ctypes
from ..native.Python.QuixSdkStreaming.Models.StreamProducer.ParameterDefinitionBuilder import ParameterDefinitionBuilder as pdbi
from ..helpers.nativedecorator import nativedecorator


@nativedecorator
class ParameterDefinitionBuilder(object):
    """
        Builder for creating parameter and group definitions for StreamPropertiesProducer
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of ParameterDefinitionBuilder.

            Parameters:

            net_pointer: Pointer to an instance of a .net ParameterDefinitionBuilder.
        """

        if net_pointer is None:
            raise Exception("ParameterDefinitionBuilder is none")

        self._interop = pdbi(net_pointer)

    def set_range(self, minimum_value: float, maximum_value: float) -> 'ParameterDefinitionBuilder':
        """
        Set the minimum and maximum range of the parameter
        :param minimum_value: The minimum value
        :param maximum_value: The maximum value
        :return: The builder
        """

        new = pdbi(self._interop.SetRange(minimum_value, maximum_value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_unit(self, unit: str) -> 'ParameterDefinitionBuilder':
        """
        Set the unit of the parameter
        :param unit: The unit of the parameter
        :return: The builder
        """
        new = pdbi(self._interop.SetUnit(unit))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_format(self, format: str) -> 'ParameterDefinitionBuilder':
        """
        Set the format of the parameter
        :param format: The format of the parameter
        :return: The builder
        """
        new = pdbi(self._interop.SetFormat(format))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_custom_properties(self, custom_properties: str) -> 'ParameterDefinitionBuilder':
        """
        Set the custom properties of the parameter
        :param custom_properties: The custom properties of the parameter
        :return: The builder
        """
        new = pdbi(self._interop.SetCustomProperties(custom_properties))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_definition(self, parameter_id: str, name: str = None, description: str = None) -> 'ParameterDefinitionBuilder':
        """
        Add new parameter definition to the StreamPropertiesProducer. Configure it with the builder methods.
        :param parameter_id: The id of the parameter. Must match the parameter id used to send data.
        :param name: The human friendly display name of the parameter
        :param description: The description of the parameter
        :return: The builder
        """
        new = pdbi(self._interop.AddDefinition(parameter_id, name, description))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self
