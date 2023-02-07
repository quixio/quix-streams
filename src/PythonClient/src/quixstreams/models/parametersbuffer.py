from typing import Optional, Callable
import ctypes

from ..eventhook import EventHook
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

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of ParametersBuffer.
            NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

            Parameters:

            net_object (.net object): The .net object representing a ParametersBuffer
        """
        if net_pointer is None:
            raise Exception("ParametersBuffer is none")

        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them
        self._interop_pb = pbi(net_pointer)

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

        def _on_read_net_handler(arg):
            self.on_read.fire(ParameterData(arg))

        def _on_first_sub():
            ref = self._interop_pb.add_OnRead(_on_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_unsub():
            # TODO do unsign with previous handler
            self._interop_pb.remove_OnRead(_on_read_net_handler)

        self.on_read = EventHook(_on_first_sub, _on_last_unsub, name="ParametersBuffer.on_read")
        """
        Raised when a parameter data package is read and buffer conditions are met

        Has one argument of type ParameterData
        """

        def _on_read_raw_net_handler(arg):
            self.on_read_raw.fire(ParameterDataRaw(arg))

        def _on_first_sub_raw():
            ref = self._interop_pb.add_OnReadRaw(_on_read_raw_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_unsub_raw():
            # TODO do unsign with previous handler
            self._interop_pb.remove_OnReadRaw(_on_read_raw_net_handler)

        self.on_read_raw = EventHook(_on_first_sub_raw, _on_last_unsub_raw, name="ParametersBuffer.on_read_raw")

        def _on_read_pandas_net_handler(arg):
            self.on_read_pandas.fire(ParameterDataRaw(arg).to_panda_frame())

        def _on_first_sub_pandas():
            ref = self._interop_pb.add_OnReadRaw(_on_read_pandas_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_unsub_pandas():
            # TODO do unsign with previous handler
            self._interop_pb.remove_OnReadRaw(_on_read_pandas_net_handler)

        self.on_read_pandas = EventHook(_on_first_sub_pandas, _on_last_unsub_pandas, name="ParametersBuffer.on_read_pandas")

    def _finalizerfunc(self):
        self._cfuncrefs = None

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
