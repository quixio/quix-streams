from typing import Union

from .timeseriesbufferwriter import TimeseriesBufferWriter
from ...builders import ParameterDefinitionBuilder
from ...models import TimeseriesData, TimeseriesDataRaw
import pandas as pd

from ...native.Python.QuixSdkStreaming.Models.StreamWriter.StreamParametersWriter import StreamParametersWriter as spwi
import ctypes
from ...helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamParametersWriter(object):
    """
        Group all the Parameters properties, builders and helpers that allow to stream parameter values and parameter definitions to the platform.
    """

    def __init__(self, output_topic, stream_writer, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamParametersWriter.

            Parameters:

            output_topic: The output topic the stream belongs to
            stream_writer: The stream the writer is created for
            net_pointer: Pointer to an instance of a .net StreamParametersWriter.
        """

        if net_pointer is None:
            raise Exception("StreamParametersWriter is none")

        self._interop = spwi(net_pointer)
        self._buffer = None
        self._stream_writer = stream_writer
        self._topic = output_topic

    def _finalizerfunc(self):
        if self._buffer is not None:
            self._buffer.dispose()

    def flush(self):
        """
        Flushes the timeseries data and definitions from the buffer without waiting for buffer condition to fulfill for either
        """
        self._interop.Flush()

    def add_definition(self, parameter_id: str, name: str = None, description: str = None) -> ParameterDefinitionBuilder:
        """
        Add new parameter definition to the StreamPropertiesWriter. Configure it with the builder methods.
        :param parameter_id: The id of the parameter. Must match the parameter id used to send data.
        :param name: The human friendly display name of the parameter
        :param description: The description of the parameter
        :return: ParameterDefinitionBuilder to define properties of the parameter or add additional parameters
        """
        return ParameterDefinitionBuilder(self._interop.AddDefinition(parameter_id, name, description))

    def add_location(self, location: str) -> ParameterDefinitionBuilder:
        """
        Add a new location in the parameters groups hierarchy
        :param location: The group location
        :return: ParameterDefinitionBuilder to define the parameters under the specified location
        """
        return ParameterDefinitionBuilder(self._interop.AddLocation(location))

    @property
    def default_location(self) -> str:
        """
            Gets the default Location of the parameters. Parameter definitions added with add_definition  will be inserted at this location.
            See add_location for adding definitions at a different location without changing default.
            Example: "/Group1/SubGroup2"
        """
        return self._interop.get_DefaultLocation()

    @default_location.setter
    def default_location(self, value: str):
        """
            Sets the default Location of the parameters. Parameter definitions added with add_definition will be inserted at this location.
            See add_location for adding definitions at a different location without changing default.
            Example: "/Group1/SubGroup2"
        """
        self._interop.set_DefaultLocation(value)

    @property
    def buffer(self) -> TimeseriesBufferWriter:
        """Get the buffer for writing timeseries data"""

        if self._buffer is None:
            self._buffer = TimeseriesBufferWriter(self._topic, self._stream_writer, self._interop.get_Buffer())
        return self._buffer

    def write(self, packet: Union[TimeseriesData, pd.DataFrame, TimeseriesDataRaw]) -> None:
        """
            Writes the given packet to the stream without any buffering.

            :param packet: The packet containing TimeseriesData or panda DataFrame

            packet type panda.DataFrame
                Note 1: panda data frame should contain 'time' label, else the first integer label will be taken as time.

                Note 2: Tags should be prefixed by TAG__ or they will be treated as parameters

                Examples
                -------
                Send a panda data frame
                     pdf = panda.DataFrame({'time': [1, 5],
                     'panda_param': [123.2, 5]})

                     instance.write(pdf)

                Send a panda data frame with multiple values
                     pdf = panda.DataFrame({'time': [1, 5, 10],
                     'panda_param': [123.2, None, 12],
                     'panda_param2': ["val1", "val2", None])

                     instance.write(pdf)

                Send a panda data frame with tags
                     pdf = panda.DataFrame({'time': [1, 5, 10],
                     'panda_param': [123.2, 5, 12],,
                     'TAG__Tag1': ["v1", 2, None],
                     'TAG__Tag2': [1, None, 3]})

                     instance.write(pdf)

            for other type examples see the specific type
        """
        if isinstance(packet, TimeseriesData):
            self._interop.Write(packet.get_net_pointer())
            return
        if isinstance(packet, TimeseriesDataRaw):
            self._interop.Write2(packet.get_net_pointer())
            return
        if isinstance(packet, pd.DataFrame):
            data = TimeseriesDataRaw.from_panda_dataframe(packet)
            with data:
                self._interop.Write2(data.get_net_pointer())
            return
        raise Exception("Write for the given type " + str(type(packet)) + " is not supported")

