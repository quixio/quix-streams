import ctypes
from typing import Optional, Callable

from ..helpers.nativedecorator import nativedecorator
from ..models.timeseriesdata import TimeseriesData
from ..models.timeseriesdatatimestamp import TimeseriesDataTimestamp
from ..native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration import TimeseriesBufferConfiguration as tsbci


@nativedecorator
class TimeseriesBufferConfiguration(object):
    """
    Describes the configuration for parameter buffers
    When none of the buffer conditions are configured, the buffer immediate invokes the on_data_released
    """

    def __init__(self, net_pointer: ctypes.c_void_p = None):
        """
            Initializes a new instance of TimeseriesBufferConfiguration.

            :param _net_object: Can be ignored, here for internal purposes .net object: The .net object representing a TimeseriesBufferConfiguration.
        """
        if net_pointer is None:
            self._interop = tsbci(tsbci.Constructor())
        else:
            self._interop = tsbci(net_pointer)

        def dummy():
            pass

        if self._interop.get_CustomTriggerBeforeEnqueue() is not None:
            self._custom_trigger_before_enqueue = dummy  # just so it is not returning as None
        else:
            self._custom_trigger_before_enqueue = None

        if self._interop.get_Filter() is not None:
            self._filter = dummy  # just so it is not returning as None
        else:
            self._filter = None

        if self._interop.get_CustomTrigger() is not None:
            self._custom_trigger = dummy  # just so it is not returning as None
        else:
            self._custom_trigger = None

    def __str__(self):
        text = "packet_size: " + str(self.packet_size) + "\n"
        text += "time_span_in_nanoseconds: " + str(self.time_span_in_nanoseconds) + "\n"
        text += "buffer_timeout: " + str(self.buffer_timeout) + "\n"
        text += "custom_trigger_before_enqueue: " + str(self.custom_trigger_before_enqueue) + "\n"
        text += "custom_trigger: " + str(self.custom_trigger)
        return text

    @property
    def packet_size(self) -> Optional[int]:
        """
            Gets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_data_released event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """
        return self._interop.get_PacketSize()

    @packet_size.setter
    def packet_size(self, value: Optional[int]):
        """
            Sets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_data_released event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """
        self._interop.set_PacketSize(value)

    @property
    def time_span_in_nanoseconds(self) -> Optional[int]:
        """
            Gets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_data_released event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
        """
        return self._interop.get_TimeSpanInNanoseconds()

    @time_span_in_nanoseconds.setter
    def time_span_in_nanoseconds(self, value: Optional[int]):
        """
            Sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_data_released event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
        """

        self._interop.set_TimeSpanInNanoseconds(value)

    @property
    def time_span_in_milliseconds(self) -> Optional[int]:
        """
            Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_data_released event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """
        return self._interop.get_TimeSpanInMilliseconds()

    @time_span_in_milliseconds.setter
    def time_span_in_milliseconds(self, value: Optional[int]):
        """
            Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_data_released event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """

        self._interop.set_TimeSpanInMilliseconds(value)

    @property
    def buffer_timeout(self) -> Optional[int]:
        """
            Gets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
            on_data_released event is triggered when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """
        return self._interop.get_BufferTimeout()

    @buffer_timeout.setter
    def buffer_timeout(self, value: Optional[int]):
        """
            Sets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
            on_data_released event is triggered when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """

        self._interop.set_BufferTimeout(value)

    @property
    def custom_trigger_before_enqueue(self) -> Callable[[TimeseriesDataTimestamp], bool]:
        """
            Gets the custom function which is invoked before adding the timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked before adding the timestamp to it.
            Defaults to none (disabled).
        """
        return self._custom_trigger_before_enqueue

    @custom_trigger_before_enqueue.setter
    def custom_trigger_before_enqueue(self, value: Callable[[TimeseriesDataTimestamp], bool]):
        """
            Sets the custom function which is invoked before adding the timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked before adding the timestamp to it.
            Defaults to none (disabled).
        """
        self._custom_trigger_before_enqueue = value
        if value is None:
            self._interop.set_CustomTriggerBeforeEnqueue(None)
            return

        def callback(ts: ctypes.c_void_p) -> bool:
            converted_ts = TimeseriesDataTimestamp(ts)
            return value(converted_ts)

        self._interop.set_CustomTriggerBeforeEnqueue(callback)

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
            self._interop.set_Filter(None)
            return

        def callback(ts: ctypes.c_void_p) -> bool:
            converted_ts = TimeseriesDataTimestamp(ts)
            return value(converted_ts)

        self._interop.set_Filter(callback)

    @property
    def custom_trigger(self) -> Callable[[TimeseriesData], bool]:
        """
            Gets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        return self._custom_trigger

    @custom_trigger.setter
    def custom_trigger(self, value: Callable[[TimeseriesData], bool]):
        """
            Sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        self._custom_trigger = value
        if value is None:
            self._interop.set_CustomTrigger(None)
            return

        def callback(pd: ctypes.c_void_p) -> bool:
            converted_pd = TimeseriesData(pd)
            return value(converted_pd)

        self._interop.set_CustomTrigger(callback)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
