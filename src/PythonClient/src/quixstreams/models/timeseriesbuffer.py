import ctypes
import traceback
from typing import Optional, Callable, Union

import pandas

from ..native.Python.InteropHelpers.InteropUtils import InteropUtils
from ..helpers.nativedecorator import nativedecorator
from ..models.timeseriesdata import TimeseriesData
from ..models.timeseriesdataraw import TimeseriesDataRaw
from ..models.timeseriesdatatimestamp import TimeseriesDataTimestamp
from ..native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs import TimeseriesDataRawReadEventArgs
from ..native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs import TimeseriesDataReadEventArgs
from ..native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer import TimeseriesBuffer as tsbi


@nativedecorator
class TimeseriesBuffer(object):
    """
        Class used for buffering parameters
        When none of the buffer conditions are configured, the buffer does not buffer at all
    """

    def __init__(self, stream, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of TimeseriesBuffer.
            NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

            Parameters:

            topic: The topic the stream for which this buffer is created for belongs to
            stream: The stream the buffer is created for
            net_object (.net object): The .net object representing a TimeseriesBuffer
        """
        if net_pointer is None:
            raise Exception("TimeseriesBuffer is none")

        self._interop_pb = tsbi(net_pointer)
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
        self._on_data_released = None
        self._on_data_released_ref = None  # keeping reference to avoid GC

        self._on_raw_released = None
        self._on_raw_released_ref = None  # keeping reference to avoid GC

        self._on_dataframe_released = None
        self._on_dataframe_released_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        self._on_data_released_dispose()
        self._on_raw_released_dispose()
        self._on_dataframe_released_dispose()

    # region on_data_released
    @property
    def on_data_released(self) -> Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.
        """
        return self._on_data_released

    @on_data_released.setter
    def on_data_released(self, value: Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.
        """
        self._on_data_released = value
        if self._on_data_released_ref is None:
            self._on_data_released_ref = self._interop_pb.add_OnDataReleased(self._on_data_released_wrapper)

    def _on_data_released_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := TimeseriesDataReadEventArgs(args_hptr)):
                data = TimeseriesData(net_pointer=args.get_Data())
                self._on_data_released(self._stream, data)
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_data_released_dispose(self):
        if self._on_data_released_ref is not None:
            self._interop_pb.remove_OnDataReleased(self._on_data_released_ref)
            self._on_data_released_ref = None

    # endregion on_data_released

    # region on_raw_released
    @property
    def on_raw_released(self) -> Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.
        """
        return self._on_raw_released

    @on_raw_released.setter
    def on_raw_released(self, value: Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.
        """
        self._on_raw_released = value
        if self._on_raw_released_ref is None:
            self._on_raw_released_ref = self._interop_pb.add_OnRawReleased(self._on_raw_released_wrapper)

    def _on_raw_released_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := TimeseriesDataRawReadEventArgs(args_hptr)):
                self._on_raw_released(self._stream, TimeseriesDataRaw(args.get_Data()))
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_raw_released_dispose(self):
        if self._on_raw_released_ref is not None:
            self._interop_pb.remove_OnRawReleased(self._on_raw_released_ref)
            self._on_raw_released_ref = None

    # endregion on_raw_released

    # region on_dataframe_released
    @property
    def on_dataframe_released(self) -> Callable[[Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.
        """
        return self._on_dataframe_released

    @on_dataframe_released.setter
    def on_dataframe_released(self, value: Callable[[Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.
        """
        self._on_dataframe_released = value
        if self._on_dataframe_released_ref is None:
            self._on_dataframe_released_ref = self._interop_pb.add_OnRawReleased(self._on_dataframe_released_wrapper)

    def _on_dataframe_released_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := TimeseriesDataRawReadEventArgs(args_hptr)):
                pdr = TimeseriesDataRaw(args.get_Data())
                pdf = pdr.to_dataframe()
                pdr.dispose()
                self._on_dataframe_released(self._stream, pdf)
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_dataframe_released_dispose(self):
        if self._on_dataframe_released_ref is not None:
            self._interop_pb.remove_OnRawReleased(self._on_dataframe_released_ref)
            self._on_dataframe_released_ref = None

    # endregion on_dataframe_released

    @property
    def filter(self) -> Callable[[TimeseriesDataTimestamp], bool]:
        """
            Gets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
            Defaults to none (disabled).
        """
        return self._filter

    @filter.setter
    def filter(self, value: Callable[[TimeseriesDataTimestamp], bool]):
        """
            Sets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
            Defaults to none (disabled).
        """
        self._filter = value
        if value is None:
            self._interop_pb.set_Filter(None)
            return

        def callback(ts: ctypes.c_void_p) -> bool:
            converted_ts = TimeseriesDataTimestamp(ts)
            return value(converted_ts)

        self._interop_pb.set_Filter(callback)

    @property
    def custom_trigger(self) -> Callable[[TimeseriesData], bool]:
        """
            Gets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_received is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        return self._custom_trigger

    @custom_trigger.setter
    def custom_trigger(self, value: Callable[[TimeseriesData], bool]):
        """
            Sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_received is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        self._custom_trigger = value
        if value is None:
            self._interop_pb.set_CustomTrigger(None)
            return

        def callback(pd: ctypes.c_void_p) -> bool:
            converted_pd = TimeseriesData(pd)
            return value(converted_pd)

        self._interop_pb.set_CustomTrigger(callback)

    @property
    def packet_size(self) -> Optional[int]:
        """
            Gets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_data_released event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """
        return self._interop_pb.get_PacketSize()

    @packet_size.setter
    def packet_size(self, value: Optional[int]):
        """
            Sets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_data_released event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """

        self._interop_pb.set_PacketSize(value)

    @property
    def time_span_in_nanoseconds(self) -> Optional[int]:
        """
            Gets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_data_released event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
        """

        return self._interop_pb.get_TimeSpanInNanoseconds()

    @time_span_in_nanoseconds.setter
    def time_span_in_nanoseconds(self, value: Optional[int]):
        """
            Sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_data_released event
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
            earliest and latest buffered timestamp surpasses this number the on_data_released event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """
        return self._interop_pb.get_TimeSpanInMilliseconds()

    @time_span_in_milliseconds.setter
    def time_span_in_milliseconds(self, value: Optional[int]):
        """
            Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_data_released event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """
        self._interop_pb.set_TimeSpanInMilliseconds(value)

    @property
    def buffer_timeout(self) -> Optional[int]:
        """
            Gets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
            on_data_released event is trigger when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """
        return self._interop_pb.get_BufferTimeout()

    @buffer_timeout.setter
    def buffer_timeout(self, value: Optional[int]):
        """
            Sets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
            on_data_released event is trigger when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """

        self._interop_pb.set_BufferTimeout(value)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop_pb.get_interop_pb_ptr__()
