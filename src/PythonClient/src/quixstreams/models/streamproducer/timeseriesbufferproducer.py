import ctypes
from datetime import datetime, timedelta
from typing import Dict
from typing import Union

import pandas as pd

from ..netdict import NetDict
from ..timeseriesbuffer import TimeseriesBuffer
from ... import TimeseriesData
from ...builders import TimeseriesDataBuilder
from ...helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ...helpers.nativedecorator import nativedecorator
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer import TimeseriesBufferProducer as tsbpi


@nativedecorator
class TimeseriesBufferProducer(TimeseriesBuffer):
    """
    A class for producing timeseries data to a StreamProducer in a buffered manner.
    """

    def __init__(self, stream_producer, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TimeseriesBufferProducer.
        NOTE: Do not initialize this class manually, use StreamTimeseriesProducer.buffer to access an instance of it

        Args:
            stream_producer: The Stream producer which owns this timeseries buffer producer
            net_pointer: Pointer to an instance of a .net TimeseriesBufferProducer

        Raises:
            Exception: If TimeseriesBufferProducer is None
        """
        if net_pointer is None:
            raise Exception("TimeseriesBufferProducer is none")

        self._interop = tsbpi(net_pointer)
        TimeseriesBuffer.__init__(self, stream_producer, net_pointer)

    @property
    def default_tags(self) -> Dict[str, str]:
        """
        Get default tags injected for all parameters values sent by this buffer.

        Returns:
            Dict[str, str]: A dictionary containing the default tags
        """
        ptr = self._interop.get_DefaultTags()
        return NetDict.constructor_for_string_string(ptr)

    @property
    def epoch(self) -> datetime:
        """
        Get the default epoch used for parameter values.

        Returns:
            datetime: The default epoch used for parameter values
        """
        return dtc.datetime_to_python(self._interop.get_Epoch())

    @epoch.setter
    def epoch(self, value: datetime):
        """
        Set the default epoch used for parameter values. Datetime added on top of all the Timestamps.

        Args:
            value: The default epoch to set for parameter values
        """
        hptr = dtc.datetime_to_dotnet(value)
        self._interop.set_Epoch(hptr)

    def add_timestamp(self, time: Union[datetime, timedelta]) -> TimeseriesDataBuilder:
        """
        Start adding a new set of parameter values at the given timestamp.

        Args:
            time: The time to use for adding new parameter values.
                - datetime: The datetime to use for adding new parameter values. NOTE, epoch is not used
                - timedelta: The time since the default epoch to add the parameter values at

        Returns:
            TimeseriesDataBuilder: A TimeseriesDataBuilder instance for adding parameter values

        Raises:
            ValueError: If 'time' is None or not an instance of datetime or timedelta
        """
        if time is None:
            raise ValueError("'time' must not be None")
        if isinstance(time, datetime):
            try:
                netdate_hptr = dtc.datetime_to_dotnet(time)
                return TimeseriesDataBuilder(self._interop.AddTimestamp(netdate_hptr))
            finally:
                InteropUtils.free_hptr(netdate_hptr)  # dotnet will hold a reference to it, we no longer need it
        if isinstance(time, timedelta):
            nettimespan_uptr = dtc.timedelta_to_dotnet(time)
            return TimeseriesDataBuilder(self._interop.AddTimestamp2(nettimespan_uptr))
        raise ValueError("'time' must be either datetime or timedelta")

    def add_timestamp_nanoseconds(self, nanoseconds: int) -> TimeseriesDataBuilder:
        """
        Start adding a new set of parameter values at the given timestamp.

        Args:
            nanoseconds: The time in nanoseconds since the default epoch to add the parameter values at

        Returns:
            TimeseriesDataBuilder: A TimeseriesDataBuilder instance for adding parameter values
        """
        return TimeseriesDataBuilder(self._interop.AddTimestampNanoseconds(nanoseconds))

    def flush(self):
        """
        Immediately publishes the data from the buffer without waiting for the buffer condition to be fulfilled.
        """
        self._interop.Flush()

    def publish(self, packet: Union[TimeseriesData, pd.DataFrame]) -> None:
        """
        Publish the provided timeseries packet to the buffer.

        Args:
            packet: The packet containing TimeseriesData or panda DataFrame
                - packet type panda.DataFrame:
                    * Note 1: panda data frame should contain 'time' label, else the first integer label will be taken as time.
                    * Note 2: Tags should be prefixed by TAG__ or they will be treated as timeseries parameters

        Examples:
            Send a panda data frame:
                pdf = panda.DataFrame({'time': [1, 5],
                'panda_param': [123.2, 5]})

                instance.publish(pdf)

            Send a panda data frame with multiple values:
                pdf = panda.DataFrame({'time': [1, 5, 10],
                'panda_param': [123.2, None, 12],
                'panda_param2': ["val1", "val2", None]})

                instance.publish(pdf)

            Send a panda data frame with tags:
                pdf = panda.DataFrame({'time': [1, 5, 10],
                'panda_param': [123.2, 5, 12],,
                'TAG__Tag1': ["v1", 2, None],
                'TAG__Tag2': [1, None, 3]})

                instance.publish(pdf)

        Raises:
            Exception: If the packet type is not supported
        """
        if isinstance(packet, TimeseriesData):
            self._interop.Publish(packet.get_net_pointer())
            return
        if isinstance(packet, pd.DataFrame):
            data = TimeseriesData.from_panda_dataframe(packet)
            with data:
                self._interop.Publish(data.get_net_pointer())
            return
        raise Exception("Publish for the given type " + str(type(packet)) + " is not supported")
