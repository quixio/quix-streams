from quixstreaming.eventhook import EventHook

from quixstreaming import __importnet, ParameterData, ParameterDataRaw
from typing import Optional, Callable
import Quix.Sdk.Streaming

from quixstreaming.models.parameterdatatimestamp import ParameterDataTimestamp
import clr
from System import Func


class ParametersBuffer(object):
    """
        Class used for buffering parameters
        When none of the buffer conditions are configured, the buffer does not buffer at all
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.ParametersBuffer):
        """
            Initializes a new instance of ParametersBuffer.
            NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

            Parameters:

            net_object (.net object): The .net object representing a ParametersBuffer
        """
        if net_object is None:
            raise Exception("ParametersBuffer is none")
        self.__wrapped = net_object

        def dummy():
            pass

        # if self.__wrapped.CustomTriggerBeforeEnqueue is not None:
        #     self.__custom_trigger_before_enqueue = dummy  # just so it is not returning as None
        # else:
        #     self.__custom_trigger_before_enqueue = None

        if self.__wrapped.Filter is not None:
            self.__filter = dummy  # just so it is not returning as None
        else:
            self.__filter = None

        if self.__wrapped.CustomTrigger is not None:
            self.__custom_trigger = dummy  # just so it is not returning as None
        else:
            self.__custom_trigger = None

        def __on_read_net_handler(arg):
            self.on_read.fire(ParameterData(arg))

        def __on_first_sub():
            self.__wrapped.OnRead += __on_read_net_handler

        def __on_last_unsub():
            self.__wrapped.OnRead -= __on_read_net_handler

        self.on_read = EventHook(__on_first_sub, __on_last_unsub, name="ParametersBuffer.on_read")
        """
        Raised when a parameter data package is read and buffer conditions are met

        Has one argument of type ParameterData
        """

        def __on_read_raw_net_handler(arg):
            self.on_read_raw.fire(ParameterDataRaw(arg))

        def __on_first_sub_raw():
            self.__wrapped.OnReadRaw += __on_read_raw_net_handler

        def __on_last_unsub_raw():
            self.__wrapped.OnReadRaw -= __on_read_raw_net_handler

        self.on_read_raw = EventHook(__on_first_sub_raw, __on_last_unsub_raw, name="ParametersBuffer.on_read_raw")




        def __on_read_pandas_net_handler(arg):
            self.on_read_pandas.fire(ParameterDataRaw(arg).to_panda_frame())

        def __on_first_sub_pandas():
            self.__wrapped.OnReadRaw += __on_read_pandas_net_handler

        def __on_last_unsub_pandas():
            self.__wrapped.OnReadRaw -= __on_read_pandas_net_handler

        self.on_read_pandas = EventHook(__on_first_sub_pandas, __on_last_unsub_pandas, name="ParametersBuffer.on_read_pandas")



    @property
    def filter(self) -> Callable[[ParameterDataTimestamp], bool]:
        """
            Gets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
            Defaults to none (disabled).
        """
        return self.__filter

    @filter.setter
    def filter(self, value: Callable[[ParameterDataTimestamp], bool]):
        """
            Sets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
            Defaults to none (disabled).
        """
        self.__filter = value
        if value is None:
            self.__wrapped.Filter = None
            return

        def callback(ts: Quix.Sdk.Streaming.Models.ParameterDataTimestamp) -> bool:
            converted_ts = ParameterDataTimestamp(ts)
            return value(converted_ts)

        self.__wrapped.Filter = Func[Quix.Sdk.Streaming.Models.ParameterDataTimestamp, bool](callback)

    @property
    def custom_trigger(self) -> Callable[[ParameterData], bool]:
        """
            Gets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, ParameterBuffer.on_read is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        return self.__custom_trigger

    @custom_trigger.setter
    def custom_trigger(self, value: Callable[[ParameterData], bool]):
        """
            Sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, ParameterBuffer.on_read is invoked with the entire buffer content
            Defaults to none (disabled).
        """
        self.__custom_trigger = value
        if value is None:
            self.__wrapped.__custom_trigger = None
            return

        def callback(pd: Quix.Sdk.Streaming.Models.ParameterData) -> bool:
            converted_pd = ParameterData(pd)
            return value(converted_pd)

        self.__wrapped.CustomTrigger = Func[Quix.Sdk.Streaming.Models.ParameterData, bool](callback)

    @property
    def packet_size(self) -> Optional[int]:
        """
            Gets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_read event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """
        return self.__wrapped.PacketSize

    @packet_size.setter
    def packet_size(self, value: Optional[int]):
        """
            Sets the max packet size in terms of values for the buffer. Each time the buffer has this amount
            of data the on_read event is invoked and the data is cleared from the buffer.
            Defaults to None (disabled).
        """
        self.__wrapped.PacketSize = value

    @property
    def time_span_in_nanoseconds(self) -> Optional[int]:
        """
            Gets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
        """
        return self.__wrapped.TimeSpanInNanoseconds

    @time_span_in_nanoseconds.setter
    def time_span_in_nanoseconds(self, value: Optional[int]):
        """
            Sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
        """
        self.__wrapped.TimeSpanInNanoseconds = value

    @property
    def time_span_in_milliseconds(self) -> Optional[int]:
        """
            Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """
        return self.__wrapped.TimeSpanInMilliseconds

    @time_span_in_milliseconds.setter
    def time_span_in_milliseconds(self, value: Optional[int]):
        """
            Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
            earliest and latest buffered timestamp surpasses this number the on_read event
            is invoked and the data is cleared from the buffer.
            Defaults to none (disabled).
            Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.
        """
        self.__wrapped.TimeSpanInMilliseconds = value

    @property
    def buffer_timeout(self) -> Optional[int]:
        """
            Gets the maximum duration in milliseconds for which the buffer will be held before triggering on_read event.
            on_read event is trigger when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """
        return self.__wrapped.BufferTimeout

    @buffer_timeout.setter
    def buffer_timeout(self, value: Optional[int]):
        """
            Sets the maximum duration in milliseconds for which the buffer will be held before triggering on_read event.
            on_read event is trigger when the configured value has elapsed or other buffer condition is met.
            Defaults to none (disabled).
        """
        self.__wrapped.BufferTimeout = value
