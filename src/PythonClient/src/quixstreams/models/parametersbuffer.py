from typing import Optional, Callable, Union
import ctypes

import pandas

from quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils

from ..models.parameterdata import ParameterData
from ..models.parameterdataraw import ParameterDataRaw
from ..models.parameterdatatimestamp import ParameterDataTimestamp

from ..native.Python.QuixSdkStreaming.Models.ParametersBuffer import ParametersBuffer as pbi

from ..helpers.nativedecorator import nativedecorator


@nativedecorator
class ParametersBuffer(object):
    """
        Class used for buffering parameters
        When none of the buffer conditions are configured, the buffer does not buffer at all
    """

    def __init__(self, stream, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of ParametersBuffer.
            NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

            Parameters:

            net_object (.net object): The .net object representing a ParametersBuffer
        """
        if net_pointer is None:
            raise Exception("ParametersBuffer is none")

        self._interop_pb = pbi(net_pointer)
        self._stream = stream

        def dummy():
            pass

        if self._interop_pb.get_CustomTriggerBeforeEnqueue() is not None:
            self._custom_trigger_before_enqueue = dummy  # just so it is not returning as None
        else:
            self._custom_trigger_before_enqueue = None

        if self._interop_pb.get_Filter() is not None:
            self._filter = dummy  # just so it is not returning as None
        else:
            self._filter = None

        if self._interop_pb.get_CustomTrigger() is not None:
            self._custom_trigger = dummy  # just so it is not returning as None
        else:
            self._custom_trigger = None

        # define events and their ref holder
        self._on_read = None
        self._on_read_ref = None  # keeping reference to avoid GC

        self._on_read_raw = None
        self._on_read_raw_ref = None  # keeping reference to avoid GC

        self._on_read_dataframe = None
        self._on_read_dataframe_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        self._on_read_dispose()
        self._on_read_raw_dispose()
        self._on_read_dataframe_dispose()


    # region on_read
    @property
    def on_read(self) -> Callable[[Union['StreamReader', 'StreamWriter'], ParameterData], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterData format.
        """
        return self._on_read

    @on_read.setter
    def on_read(self, value: Callable[[Union['StreamReader', 'StreamWriter'], ParameterData], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterData format.
        """
        self._on_read = value
        if self._on_read_ref is None:
            self._on_read_ref = self._interop_pb.add_OnRead(self._on_read_wrapper)

    def _on_read_wrapper(self, stream_hptr, data_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        data = ParameterData(net_pointer=data_hptr)
        self._on_read(self._stream, data)
        InteropUtils.free_hptr(stream_hptr)

    def _on_read_dispose(self):
        if self._on_read_ref is not None:
            self._interop_pb.remove_OnRead(self._on_read_ref)
            self._on_read_ref = None
    # endregion on_read
    
    # region on_read_raw
    @property
    def on_read_raw(self) -> Callable[[Union['StreamReader', 'StreamWriter'], ParameterDataRaw], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterDataRaw format.
        """
        return self._on_read_raw

    @on_read_raw.setter
    def on_read_raw(self, value: Callable[[Union['StreamReader', 'StreamWriter'], ParameterDataRaw], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterDataRaw format.
        """
        self._on_read_raw = value
        if self._on_read_raw_ref is None:
            self._on_read_raw_ref = self._interop_pb.add_OnReadRaw(self._on_read_raw_wrapper)

    def _on_read_raw_wrapper(self, stream_hptr, data_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        self._on_read_raw(self._stream, ParameterDataRaw(data_hptr))
        InteropUtils.free_hptr(stream_hptr)

    def _on_read_raw_dispose(self):
        if self._on_read_raw_ref is not None:
            self._interop_pb.remove_OnReadRaw(self._on_read_raw_ref)
            self._on_read_raw_ref = None
    # endregion on_read_raw
    
    # region on_read_dataframe
    @property
    def on_read_dataframe(self) -> Callable[[Union['StreamReader', 'StreamWriter'], pandas.DataFrame], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in Pandas' DataFrame format.
        """
        return self._on_read_dataframe

    @on_read_dataframe.setter
    def on_read_dataframe(self, value: Callable[[Union['StreamReader', 'StreamWriter'], pandas.DataFrame], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in Pandas' DataFrame format.
        """
        self._on_read_dataframe = value
        if self._on_read_dataframe_ref is None:
            self._on_read_dataframe_ref = self._interop_pb.add_OnReadRaw(self._on_read_dataframe_wrapper)

    def _on_read_dataframe_wrapper(self, stream_hptr, data_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        pdr = ParameterDataRaw(data_hptr)
        pdf = pdr.to_panda_frame()
        pdr.dispose()
        self._on_read_dataframe(self._stream, pdf)
        InteropUtils.free_hptr(stream_hptr)

    def _on_read_dataframe_dispose(self):
        if self._on_read_dataframe_ref is not None:
            self._interop_pb.remove_OnReadRaw(self._on_read_dataframe_ref)
            self._on_read_dataframe_ref = None
    # endregion on_read_dataframe


    @property
    def filter(self) -> Callable[[ParameterDataTimestamp], bool]:
        """
            Gets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
            Defaults to none (disabled).
        """
        return self._filter

    @filter.setter
    def filter(self, value: Callable[[ParameterDataTimestamp], bool]):
        """
            Sets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
            Defaults to none (disabled).
        """
        self._filter = value
        if value is None:
            self._interop_pb.set_Filter(None)
            return

        def callback(ts: ctypes.c_void_p) -> bool:
            converted_ts = ParameterDataTimestamp(ts)
            return value(converted_ts)

        self._interop_pb.set_Filter(callback)

    @property
    def custom_trigger(self) -> Callable[[ParameterData], bool]:
        """
            Gets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, ParameterBuffer.on_read is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        return self._custom_trigger

    @custom_trigger.setter
    def custom_trigger(self, value: Callable[[ParameterData], bool]):
        """
            Sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, ParameterBuffer.on_read is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        self._custom_trigger = value
        if value is None:
            self._interop_pb.set_CustomTrigger(None)
            return

        def callback(pd: ctypes.c_void_p) -> bool:
            converted_pd = ParameterData(pd)
            return value(converted_pd)

        self._interop_pb.set_CustomTrigger(callback)

    @property
    def packet_size(self) -> Optional[int]:
        """
            Gets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_read event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """
        return self._interop_pb.get_PacketSize()

    @packet_size.setter
    def packet_size(self, value: Optional[int]):
        """
            Sets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_read event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """

        self._interop_pb.set_PacketSize(value)

    @property
    def time_span_in_nanoseconds(self) -> Optional[int]:
        """
            Gets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
        """

        return self._interop_pb.get_TimeSpanInNanoseconds()

    @time_span_in_nanoseconds.setter
    def time_span_in_nanoseconds(self, value: Optional[int]):
        """
            Sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
        """

        if not isinstance(value, int):
            value = int(value)

        self._interop_pb.set_TimeSpanInNanoseconds(value)

    @property
    def time_span_in_milliseconds(self) -> Optional[int]:
        """
            Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """
        return self._interop_pb.get_TimeSpanInMilliseconds()

    @time_span_in_milliseconds.setter
    def time_span_in_milliseconds(self, value: Optional[int]):
        """
            Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """
        self._interop_pb.set_TimeSpanInMilliseconds(value)

    @property
    def buffer_timeout(self) -> Optional[int]:
        """
            Gets the maximum duration in milliseconds for which the buffer will be held before triggering on_read event.
            on_read event is trigger when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """
        return self._interop_pb.get_BufferTimeout()

    @buffer_timeout.setter
    def buffer_timeout(self, value: Optional[int]):
        """
            Sets the maximum duration in milliseconds for which the buffer will be held before triggering on_read event.
            on_read event is trigger when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """

        self._interop_pb.set_BufferTimeout(value)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop_pb.get_interop_pb_ptr__()
