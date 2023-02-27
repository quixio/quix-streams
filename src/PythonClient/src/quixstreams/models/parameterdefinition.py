import ctypes

from ..native.Python.QuixStreamsStreaming.Models.ParameterDefinition import ParameterDefinition as pdi


class ParameterDefinition(object):
    """
    Describes additional context for the parameter
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of ParameterDefinition

        NOTE: Do not initialize this class manually. Instances of it are available on StreamTimeseriesConsumer.definitions
        :param net_pointer: Pointer to an instance of a .net ParameterDefinition.
        """

        if net_pointer is None:
            raise Exception("ParameterDefinition constructor should not be invoked without a .net pointer")

        with (interop := pdi(net_pointer)):
            self.id: str = interop.get_Id()
            """Gets the globally unique identifier of the parameter"""

            self.name: str = interop.get_Name()
            """Gets the human friendly display name of the parameter"""

            self.format: str = interop.get_Format()
            """Gets the formatting to apply on the value for display purposes"""

            self.unit: str = interop.get_Unit()
            """Gets the unit of the parameter"""

            self.maximum_value: float = interop.get_MaximumValue()
            """Gets the maximum value of the parameter"""

            self.minimum_value: float = interop.get_MinimumValue()
            """Gets the minimum value of the parameter"""

            self.custom_properties: str = interop.get_CustomProperties()
            """
            Gets the optional field for any custom properties that do not exist on the parameter.
            For example this could be a json string, describing the optimal value range of this parameter
            """

            self.description: str = interop.get_Description()
            """Gets the description of the parameter"""

            self.location: str = interop.get_Location()
            """Gets the location of the parameter within the Parameter hierarchy. Example: "/", "car/chassis/suspension"""

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
