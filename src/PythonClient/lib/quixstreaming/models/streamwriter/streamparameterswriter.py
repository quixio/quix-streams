from datetime import datetime, timedelta
from typing import Union

from .parametersbufferwriter import ParametersBufferWriter
from quixstreaming.builders import ParameterDefinitionBuilder
from quixstreaming.models import ParameterData, ParameterDataRaw
from quixstreaming import __importnet
import Quix.Sdk.Streaming
import pandas as pd


class StreamParametersWriter(object):
    """
        Group all the Parameters properties, builders and helpers that allow to stream parameter values and parameter definitions to the platform.
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamWriter.StreamParametersWriter, net_object_stream_writer: Quix.Sdk.Streaming.IStreamWriter):
        """
            Initializes a new instance of StreamParametersWriter.

            Parameters:

            net_object (.net object): The .net object representing StreamParametersWriter.
        """

        self.__wrapped = net_object
        self.__wrapped_stream_writer = net_object_stream_writer

    def flush(self):
        """
        Flushes the parameter data and definitions from the buffer without waiting for buffer condition to fulfill for either
        """
        self.__wrapped.Flush()

    def add_definition(self, parameter_id: str, name: str = None, description: str = None) -> ParameterDefinitionBuilder:
        """
        Add new parameter definition to the StreamPropertiesWriter. Configure it with the builder methods.
        :param parameter_id: The id of the parameter. Must match the parameter id used to send data.
        :param name: The human friendly display name of the parameter
        :param description: The description of the parameter
        :return: ParameterDefinitionBuilder to define properties of the parameter or add additional parameters
        """
        return ParameterDefinitionBuilder(self.__wrapped.AddDefinition(parameter_id, name, description))

    def add_location(self, location: str) -> ParameterDefinitionBuilder:
        """
        Add a new location in the parameters groups hierarchy
        :param location: The group location
        :return: ParameterDefinitionBuilder to define the parameters under the specified location
        """
        return ParameterDefinitionBuilder(self.__wrapped.AddLocation(location))

    @property
    def default_location(self) -> str:
        """
            Gets the default Location of the parameters. Parameter definitions added with add_definition  will be inserted at this location.
            See add_location for adding definitions at a different location without changing default.
            Example: "/Group1/SubGroup2"
        """
        return self.__wrapped.DefaultLocation

    @default_location.setter
    def default_location(self, value: str):
        """
            Sets the default Location of the parameters. Parameter definitions added with add_definition will be inserted at this location.
            See add_location for adding definitions at a different location without changing default.
            Example: "/Group1/SubGroup2"
        """
        self.__wrapped.DefaultLocation = value

    @property
    def buffer(self) -> ParametersBufferWriter:
        """Get the buffer for writing parameter data"""
        return ParametersBufferWriter(self.__wrapped.Buffer)

    def write(self, packet: Union[ParameterData, pd.DataFrame]) -> None:
        """
            Writes the given packet to the stream without any buffering.

            :param packet: The packet containing ParameterData or panda DataFrame

            packet type panda.DataFrame
                Note 1: panda frame should contain 'time' label, else the first integer label will be taken as time.

                Note 2: Tags should be prefixed by TAG__ or they will be treated as parameters

                Examples
                -------
                Send a panda frame
                     pdf = panda.DataFrame({'time': [1, 5],
                     'panda_param': [123.2, 5]})

                     instance.write(pdf)

                Send a panda frame with multiple values
                     pdf = panda.DataFrame({'time': [1, 5, 10],
                     'panda_param': [123.2, None, 12],
                     'panda_param2': ["val1", "val2", None])

                     instance.write(pdf)

                Send a panda frame with tags
                     pdf = panda.DataFrame({'time': [1, 5, 10],
                     'panda_param': [123.2, 5, 12],,
                     'TAG__Tag1': ["v1", 2, None],
                     'TAG__Tag2': [1, None, 3]})

                     instance.write(pdf)

            for other type examples see the specific type
        """
        if isinstance(packet, ParameterData) or isinstance(packet, ParameterDataRaw):
            self.__wrapped.Write(packet.convert_to_net())
            return
        if isinstance(packet, pd.DataFrame):
            data = ParameterDataRaw.from_panda_frame(packet)
            self.__wrapped.Write(data.convert_to_net())
            return
        raise Exception("Write for the given type " + str(type(packet)) + " is not supported")

