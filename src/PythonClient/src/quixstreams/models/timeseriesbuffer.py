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
    Represents a class used to consume and produce stream messages in a buffered manner.
    When buffer conditions are not configured, it acts a pass-through, raising each message as arrives.
    """

    def __init__(self, stream, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TimeseriesBuffer.

        NOTE: Do not initialize this class manually, use StreamProducer.timeseries.buffer to create it.

        Args:
            stream: The stream the buffer is created for.
            net_pointer: Pointer to a .net TimeseriesBuffer object.
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
        Gets the handler for when the stream receives data.

        Returns:
            Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]: The event handler.
                The first parameter is the stream the data is received for, second is the data in TimeseriesData format.
        """

        return self._on_data_released

    @on_data_released.setter
    def on_data_released(self, value: Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]) -> None:
        """
        Sets the handler for when the stream receives data.

        Args:
            value: The event handler. The first parameter is the stream the data is received for, second is the data in TimeseriesData format.
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
        Gets the handler for when the stream receives raw data.

        Returns:
            Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]: The event handler.
                The first parameter is the stream the data is received for, second is the data in TimeseriesDataRaw format.
        """
        return self._on_raw_released

    @on_raw_released.setter
    def on_raw_released(self, value: Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]) -> None:
        """
        Sets the handler for when the stream receives raw data.

        Args:
            value: The event handler. The first parameter is the stream the data is received for, second is the data in TimeseriesDataRaw format.
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
        Gets the handler for when the stream receives data as a pandas DataFrame.

        Returns:
            Callable[[Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]: The event handler.
                The first parameter is the stream the data is received for, second is the data in pandas.DataFrame format.
        """
        return self._on_dataframe_released

    @on_dataframe_released.setter
    def on_dataframe_released(self, value: Callable[[Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]) -> None:
        """
        Sets the handler for when the stream receives data as a pandas DataFrame.

        Args:
            value: The event handler. The first parameter is the stream the data is received for, second is the data in pandas.DataFrame format.
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
        Sets the custom function to filter incoming data before adding to the buffer.

        The custom function takes a TimeseriesDataTimestamp object as input and returns
        a boolean value. If the function returns True, the data is added to the buffer,
        otherwise not. By default, this feature is disabled (None).

        Args:
            value: Custom filter function.
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
        Gets the custom trigger function, which is invoked after adding a new timestamp to the buffer.

        If the custom trigger function returns True, the buffer releases content and triggers relevant callbacks.
        By default, this feature is disabled (None).

        Returns:
            Callable[[TimeseriesData], bool]: Custom trigger function.
    """
        return self._custom_trigger

    @custom_trigger.setter
    def custom_trigger(self, value: Callable[[TimeseriesData], bool]):
        """
        Sets the custom trigger function, which is invoked after adding a new timestamp to the buffer.

        If the custom trigger function returns True, the buffer releases content and triggers relevant callbacks.
        By default, this feature is disabled (None).

        Args:
            value: Custom trigger function.

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
        Gets the maximum packet size in terms of values for the buffer.

        Each time the buffer has this amount of data, a callback method, such as `on_data_released`,
        is invoked, and the data is cleared from the buffer. By default, this feature
        is disabled (None).

        Returns:
            Optional[int]: Maximum packet size for the buffer.
        """
        return self._interop_pb.get_PacketSize()

    @packet_size.setter
    def packet_size(self, value: Optional[int]):
        """
        Sets the maximum packet size in terms of values for the buffer.

        Each time the buffer has this amount of data, a callback method, such as `on_data_released`,
        is invoked, and the data is cleared from the buffer. By default, this feature
        is disabled (None).

        Args:
            value: Maximum packet size for the buffer.
        """

        self._interop_pb.set_PacketSize(value)

    @property
    def time_span_in_nanoseconds(self) -> Optional[int]:
        """
        Gets the maximum time between timestamps for the buffer in nanoseconds.

        When the difference between the earliest and latest buffered timestamp surpasses
        this number, a callback method, such as `on_data_released`, is invoked, and the data is cleared
        from the buffer. By default, this feature is disabled (None).

        Returns:
            Optional[int]: Maximum time between timestamps in nanoseconds.

        """

        return self._interop_pb.get_TimeSpanInNanoseconds()

    @time_span_in_nanoseconds.setter
    def time_span_in_nanoseconds(self, value: Optional[int]):
        """
        Sets the maximum time between timestamps for the buffer in nanoseconds.

        When the difference between the earliest and latest buffered timestamp surpasses
        this number, a callback method, such as `on_data_released`, is invoked, and the data is cleared
        from the buffer. By default, this feature is disabled (None).

        Args:
            value: Maximum time between timestamps in nanoseconds.

        """

        if not isinstance(value, int):
            value = int(value)

        self._interop_pb.set_TimeSpanInNanoseconds(value)

    @property
    def time_span_in_milliseconds(self) -> Optional[int]:
        """
        Gets the maximum time between timestamps for the buffer in milliseconds.

        This property retrieves the maximum time between the earliest and latest buffered
        timestamp in milliseconds. If the difference surpasses this number, a callback method,
        such as on_data_released, is invoked, and the data is cleared from the buffer. Note that
        this property is a millisecond converter on top of time_span_in_nanoseconds, and both
        work with the same underlying value. Defaults to None (disabled).

        Returns:
            Optional[int]: The maximum time difference between timestamps in milliseconds, or None if disabled.
        """
        return self._interop_pb.get_TimeSpanInMilliseconds()

    @time_span_in_milliseconds.setter
    def time_span_in_milliseconds(self, value: Optional[int]):
        """
            Sets the maximum time between timestamps for the buffer in milliseconds.

            This property sets the maximum time between the earliest and latest buffered
            timestamp in milliseconds. If the difference surpasses this number, a callback method,
            such as on_data_released, is invoked, and the data is cleared from the buffer. Note that
            this property is a millisecond converter on top of time_span_in_nanoseconds, and both
            work with the same underlying value. Defaults to None (disabled).

            Args:
                value: The maximum time difference between timestamps in milliseconds, or None to disable.
        """
        self._interop_pb.set_TimeSpanInMilliseconds(value)

    @property
    def buffer_timeout(self) -> Optional[int]:
        """
        Gets the maximum duration in milliseconds for which the buffer will be held. When the configured value has elapsed
        or other buffer conditions are met, a callback method, such as on_data_released, is invoked.
        Defaults to None (disabled).

        Returns:
            Optional[int]: The maximum duration in milliseconds before invoking a callback method, or None if disabled.
        """
        return self._interop_pb.get_BufferTimeout()

    @buffer_timeout.setter
    def buffer_timeout(self, value: Optional[int]):
        """
        Sets the maximum duration in milliseconds for which the buffer will be held. When the configured value has elapsed
        or other buffer conditions are met, a callback method, such as on_data_released, is invoked.
        Defaults to None (disabled).

        Args:
            value: The maximum duration in milliseconds before invoking a callback method, or None to disable.
        """

        self._interop_pb.set_BufferTimeout(value)

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer.

        Returns:
            ctypes.c_void_p: The .net object pointer.
        """
        return self._interop_pb.get_interop_pb_ptr__()
