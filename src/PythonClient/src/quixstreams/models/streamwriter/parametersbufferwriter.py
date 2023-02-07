from datetime import datetime, timedelta
from typing import Union
import ctypes
import pandas as pd
from typing import Dict

from ..parametersbuffer import ParametersBuffer
from ... import ParameterData
from ...builders import ParameterDataBuilder
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils

from ...native.Python.QuixSdkStreaming.Models.StreamWriter.ParametersBufferWriter import ParametersBufferWriter as pbwi
from ..netdict import NetDict
from ...helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ...helpers.nativedecorator import nativedecorator


@nativedecorator
class ParametersBufferWriter(ParametersBuffer):
    """
        Class used to write to StreamWriter in a buffered manner
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of ParametersBufferWriter.
            NOTE: Do not initialize this class manually, use StreamParametersWriter.buffer to access an instance of it

            Parameters:

            net_pointer: Pointer to an instance of a .net ParametersBufferWriter
        """
        if net_pointer is None:
            raise Exception("ParametersBufferWriter is none")

        ParametersBuffer.__init__(self, net_pointer)  # TODO is this necessary?
        self._interop = pbwi(net_pointer)

    @property
    def default_tags(self) -> Dict[str, str]:
        """Get default tags injected to all Parameters Values sent by the writer."""
        ptr = self._interop.get_DefaultTags()
        return NetDict.constructor_for_string_string(ptr)

    @property
    def epoch(self) -> datetime:
        """Get the default epoch used for parameter values"""
        return dtc.datetime_to_python(self._interop.get_Epoch())

    @epoch.setter
    def epoch(self, value: datetime):
        """Set the default epoch used for parameter values"""
        hptr = dtc.datetime_to_dotnet(value)
        self._interop.set_Epoch(hptr)

    def add_timestamp(self, time: Union[datetime, timedelta]) -> ParameterDataBuilder:
        """
        Start adding a new set of parameter values at the given timestamp.
        :param time: The time to use for adding new parameter values.
                     | datetime: The datetime to use for adding new parameter values. NOTE, epoch is not used
                     | timedelta: The time since the default epoch to add the parameter values at

        :return: ParameterDataBuilder
        """
        if time is None:
            raise ValueError("'time' must not be None")
        if isinstance(time, datetime):
            try:
                netdate_hptr = dtc.datetime_to_dotnet(time)
                return ParameterDataBuilder(self._interop.AddTimestamp(netdate_hptr))
            finally:
                InteropUtils.free_hptr(netdate_hptr)  # dotnet will hold a reference to it, we no longer need it
        if isinstance(time, timedelta):
            try:
                nettimespan_uptr = dtc.timedelta_to_dotnet(time)
                return ParameterDataBuilder(self._interop.AddTimestamp2(nettimespan_uptr))
            finally:
                InteropUtils.free_uptr(nettimespan_uptr)  # dotnet will hold a reference to it, we no longer need it
        raise ValueError("'time' must be either datetime or timedelta")

    def add_timestamp_nanoseconds(self, nanoseconds: int) -> ParameterDataBuilder:
        """
        Start adding a new set of parameter values at the given timestamp.
        :param nanoseconds: The time in nanoseconds since the default epoch to add the parameter values at
        :return: ParameterDataBuilder
        """
        return ParameterDataBuilder(self._interop.AddTimestampNanoseconds(nanoseconds))

    def flush(self):
        """Immediately writes the data from the buffer without waiting for buffer condition to fulfill"""
        self._interop.Flush()

    def write(self, packet: Union[ParameterData, pd.DataFrame]) -> None:
        """
            Writes the given packet to the stream without any buffering.

            :param packet: The packet containing ParameterData or panda DataFrame

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
        if isinstance(packet, ParameterData):
            self._interop.Write(packet.get_net_pointer())
            return
        if isinstance(packet, pd.DataFrame):
            data = ParameterData.from_panda_frame(packet)
            self._interop.Write(data.get_net_pointer())
            return
        raise Exception("Write for the given type " + str(type(packet)) + " is not supported")