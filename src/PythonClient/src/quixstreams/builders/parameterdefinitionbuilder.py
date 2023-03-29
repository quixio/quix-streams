import ctypes

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder import ParameterDefinitionBuilder as pdbi


@nativedecorator
class ParameterDefinitionBuilder(object):
    """Builder for creating ParameterDefinition for StreamTimeseriesProducer."""

    def __init__(self, net_pointer: ctypes.c_void_p):
        """Initializes a new instance of ParameterDefinitionBuilder.

        Args:
            net_pointer: Pointer to an instance of a .net ParameterDefinitionBuilder.
        """
        if net_pointer is None:
            raise Exception("ParameterDefinitionBuilder is none")

        self._interop = pdbi(net_pointer)

    def set_range(self, minimum_value: float, maximum_value: float) -> 'ParameterDefinitionBuilder':
        """Set the minimum and maximum range of the parameter.

        Args:
            minimum_value: The minimum value.
            maximum_value: The maximum value.

        Returns:
            The builder.
        """
        new = pdbi(self._interop.SetRange(minimum_value, maximum_value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_unit(self, unit: str) -> 'ParameterDefinitionBuilder':
        """Set the unit of the parameter.

        Args:
            unit: The unit of the parameter.

        Returns:
            The builder.
        """
        new = pdbi(self._interop.SetUnit(unit))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_format(self, format: str) -> 'ParameterDefinitionBuilder':
        """Set the format of the parameter.

        Args:
            format: The format of the parameter.

        Returns:
            The builder.
        """
        new = pdbi(self._interop.SetFormat(format))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def set_custom_properties(self, custom_properties: str) -> 'ParameterDefinitionBuilder':
        """Set the custom properties of the parameter.

        Args:
            custom_properties: The custom properties of the parameter.

        Returns:
            The builder.
        """
        new = pdbi(self._interop.SetCustomProperties(custom_properties))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_definition(self, parameter_id: str, name: str = None, description: str = None) -> 'ParameterDefinitionBuilder':
        """Add new parameter definition to the StreamTimeseriesProducer. Configure it with the builder methods.

        Args:
            parameter_id: The id of the parameter. Must match the parameter id used to send data.
            name: The human friendly display name of the parameter.
            description: The description of the parameter.

        Returns:
            Parameter definition builder to define the parameter properties
        """
        new = pdbi(self._interop.AddDefinition(parameter_id, name, description))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self
