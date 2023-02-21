from typing import Union

from .timeseriesbufferproducer import TimeseriesBufferProducer
from ...builders import ParameterDefinitionBuilder
from ...models import TimeseriesData, TimeseriesDataRaw
import pandas as pd

from ...native.Python.QuixSdkStreaming.Models.StreamProducer.StreamParametersProducer import StreamParametersProducer as sppi
import ctypes
from ...helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamParametersProducer(object):
    """
        Group all the Parameters properties, builders and helpers that allow to stream parameter values and parameter definitions to the platform.
    """

    def __init__(self, topic_producer, stream_producer, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamParametersProducer.

            Parameters:

            topic_producer: The output topic the stream belongs to
            stream_producer: The stream the writer is created for
            net_pointer: Pointer to an instance of a .net StreamParametersProducer.
        """

        if net_pointer is None:
            raise Exception("StreamParametersProducer is none")

        self._interop = sppi(net_pointer)
        self._buffer = None
        self._stream_producer = stream_producer
        self._topic = topic_producer

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
        Add new parameter definition to the StreamPropertiesProducer. Configure it with the builder methods.
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
    def buffer(self) -> TimeseriesBufferProducer:
        """Get the buffer for writing timeseries data"""

        if self._buffer is None:
            self._buffer = TimeseriesBufferProducer(self._topic, self._stream_producer, self._interop.get_Buffer())
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

