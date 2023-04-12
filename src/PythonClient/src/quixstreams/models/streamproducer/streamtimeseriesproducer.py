import ctypes
from typing import Union

import pandas as pd

from .timeseriesbufferproducer import TimeseriesBufferProducer
from ...builders import ParameterDefinitionBuilder
from ...helpers.nativedecorator import nativedecorator
from ...models import TimeseriesData, TimeseriesDataRaw
from ...native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer import StreamTimeseriesProducer as stspi


@nativedecorator
class StreamTimeseriesProducer(object):
    """
    Helper class for producing ParameterDefinition and TimeseriesData.
    """

    def __init__(self, stream_producer, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamTimeseriesProducer.

        Args:
            stream_producer: The Stream producer which owns this stream timeseries producer.
            net_pointer: Pointer to an instance of a .net StreamTimeseriesProducer.
        """

        if net_pointer is None:
            raise Exception("StreamTimeseriesProducer is none")

        self._interop = stspi(net_pointer)
        self._buffer = None
        self._stream_producer = stream_producer

    def _finalizerfunc(self):
        if self._buffer is not None:
            self._buffer.dispose()

    def flush(self):
        """
        Immediately publish timeseries data and definitions from the buffer without waiting for buffer condition to fulfill for either.
        """
        self._interop.Flush()

    def add_definition(self, parameter_id: str, name: str = None, description: str = None) -> ParameterDefinitionBuilder:
        """
        Add new parameter definition to the StreamTimeseriesProducer. Configure it with the builder methods.

        Args:
            parameter_id: The id of the parameter. Must match the parameter id used to send data.
            name: The human friendly display name of the parameter.
            description: The description of the parameter.

        Returns:
            ParameterDefinitionBuilder: Builder to define the parameter properties.
        """
        return ParameterDefinitionBuilder(self._interop.AddDefinition(parameter_id, name, description))

    def add_location(self, location: str) -> ParameterDefinitionBuilder:
        """
        Add a new location in the parameters groups hierarchy.

        Args:
            location: The group location.

        Returns:
            ParameterDefinitionBuilder: Builder to define the parameters under the specified location.
        """
        return ParameterDefinitionBuilder(self._interop.AddLocation(location))

    @property
    def default_location(self) -> str:
        """
        Gets the default location of the parameters. Parameter definitions added with add_definition will be inserted at this location.
        See add_location for adding definitions at a different location without changing default.

        Returns:
            str: The default location of the parameters, e.g., "/Group1/SubGroup2".
        """
        return self._interop.get_DefaultLocation()

    @default_location.setter
    def default_location(self, value: str):
        """
        Sets the default location of the parameters. Parameter definitions added with add_definition will be inserted at this location.
        See add_location for adding definitions at a different location without changing default.

        Args:
            value: The new default location of the parameters, e.g., "/Group1/SubGroup2".
        """
        self._interop.set_DefaultLocation(value)

    @property
    def buffer(self) -> TimeseriesBufferProducer:
        """
        Get the buffer for producing timeseries data.

        Returns:
            TimeseriesBufferProducer: The buffer for producing timeseries data.
        """
        if self._buffer is None:
            self._buffer = TimeseriesBufferProducer(self._stream_producer, self._interop.get_Buffer())
        return self._buffer

    def publish(self, packet: Union[TimeseriesData, pd.DataFrame, TimeseriesDataRaw]) -> None:
        """
        Publish the given packet to the stream without any buffering.

        Args:
            packet: The packet containing TimeseriesData, TimeseriesDataRaw, or pandas DataFrame.

        Note:
            - Pandas DataFrame should contain 'time' label, else the first integer label will be taken as time.
            - Tags should be prefixed by TAG__ or they will be treated as parameters.

        Examples:
            Send a pandas DataFrame:
                pdf = pandas.DataFrame({'time': [1, 5],
                'panda_param': [123.2, 5]})
                instance.publish(pdf)

            Send a pandas DataFrame with multiple values:
                pdf = pandas.DataFrame({'time': [1, 5, 10],
                'panda_param': [123.2, None, 12],
                'panda_param2': ["val1", "val2", None]})
                instance.publish(pdf)

            Send a pandas DataFrame with tags:
                pdf = pandas.DataFrame({'time': [1, 5, 10],
                'panda_param': [123.2, 5, 12],
                'TAG__Tag1': ["v1", 2, None],
                'TAG__Tag2': [1, None, 3]})
                instance.publish(pdf)

        Raises:
            Exception: If the given type is not supported for publishing.
        """
        if isinstance(packet, TimeseriesData):
            self._interop.Publish(packet.get_net_pointer())
            return
        if isinstance(packet, TimeseriesDataRaw):
            self._interop.Publish2(packet.get_net_pointer())
            return
        if isinstance(packet, pd.DataFrame):
            data = TimeseriesDataRaw.from_dataframe(packet)
            with data:
                self._interop.Publish2(data.get_net_pointer())
            return
        raise Exception("Publish for the given type " + str(type(packet)) + " is not supported")
