from datetime import datetime, timedelta
from typing import Union
from quixstreaming.builders import ParameterDataBuilder
from quixstreaming.models.netdict import NetDict
from quixstreaming.helpers import *
from quixstreaming import __importnet, ParameterData
import Quix.Sdk.Streaming

import pandas as pd

from quixstreaming.models.parametersbuffer import ParametersBuffer


class ParametersBufferWriter(ParametersBuffer):
    """
        Class used to write to StreamWriter in a buffered manner
    """

    def __init__(self, net_object : Quix.Sdk.Streaming.Models.StreamWriter.ParametersBufferWriter):
        """
            Initializes a new instance of ParametersBufferWriter.
            NOTE: Do not initialize this class manually, use StreamParametersWriter.buffer to access an instance of it

            Parameters:

            net_object (.net object): The .net object representing a ParametersBufferWriter
        """
        if net_object is None:
            raise Exception("StreamEventsWriter is none")
        self.__wrapped = net_object
        ParametersBuffer.__init__(self, net_object)

    @property
    def default_tags(self) -> NetDict:
        """Get default tags injected to all Parameters Values sent by the writer."""
        return NetDict(self.__wrapped.DefaultTags)

    @property
    def epoch(self) -> datetime:
        """Get the default epoch used for parameter values"""
        return NetToPythonConverter.convert_datetime(self.__wrapped.Epoch)

    @epoch.setter
    def epoch(self, value: datetime):
        """Set the default epoch used for parameter values"""
        self.__wrapped.Epoch = PythonToNetConverter.convert_datetime(value)

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
            netdate = PythonToNetConverter.convert_datetime(time)
            return ParameterDataBuilder(self.__wrapped.AddTimestamp(netdate))
        if isinstance(time, timedelta):
            nettimespan = PythonToNetConverter.convert_timedelta(time)
            return ParameterDataBuilder(self.__wrapped.AddTimestamp(nettimespan))
        raise ValueError("'time' must be either datetime or timedelta")

    def add_timestamp_nanoseconds(self, nanoseconds: int) -> ParameterDataBuilder:
        """
        Start adding a new set of parameter values at the given timestamp.
        :param nanoseconds: The time in nanoseconds since the default epoch to add the parameter values at
        :return: ParameterDataBuilder
        """
        return ParameterDataBuilder(self.__wrapped.AddTimestampNanoseconds(nanoseconds))

    def flush(self):
        """Immediately writes the data from the buffer without waiting for buffer condition to fulfill"""
        self.__wrapped.Flush()

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
        if isinstance(packet, ParameterData):
            self.__wrapped.Write(packet.convert_to_net())
            return
        if isinstance(packet, pd.DataFrame):
            data = ParameterData.from_panda_frame(packet)
            self.__wrapped.Write(data.convert_to_net())
            return
        raise Exception("Write for the given type " + str(type(packet)) + " is not supported")