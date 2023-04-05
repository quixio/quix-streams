import ctypes
import sys
import traceback
from datetime import datetime
from typing import Callable

from .helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from .helpers.enumconverter import EnumConverter as ec
from .helpers.nativedecorator import nativedecorator
from .models import *
from .models.streamproducer import *
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixStreamsTelemetry.Models.StreamEndType import StreamEndType as StreamEndTypeInterop
from .native.Python.QuixStreamsStreaming.IStreamProducer import IStreamProducer as spi


@nativedecorator
class StreamProducer(object):
    """
    Handles publishing stream to a topic
    """

    def __init__(self, topic_producer: 'TopicProducer', net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamProducer.

        NOTE: Do not initialize this class manually, use TopicProducer.get_or_create_stream or create_stream

        Args:
            topic_producer: The topic producer the stream producer publishes to.
            net_pointer: The .net object representing a StreamProducer.
        """

        if net_pointer is None:
            raise Exception("StreamProducer is none")

        self._interop = spi(net_pointer)
        self._topic = topic_producer
        self._streamTimeseriesProducer = None  # Holding reference to avoid GC
        self._streamEventsProducer = None  # Holding reference to avoid GC
        self._streamPropertiesProducer = None  # Holding reference to avoid GC

        # define events and their ref holder
        self._on_write_exception = None
        self._on_write_exception_ref = None  # keeping reference to avoid GC

        self._stream_id = self._interop.get_StreamId()

    def _finalizerfunc(self):
        if self._streamTimeseriesProducer is not None:
            self._streamTimeseriesProducer.dispose()
        if self._streamEventsProducer is not None:
            self._streamEventsProducer.dispose()
        if self._streamPropertiesProducer is not None:
            self._streamPropertiesProducer.dispose()
        self._on_write_exception_dispose()

    @property
    def topic(self) -> 'TopicProducer':
        """
        Gets the topic the stream is producing to.

        Returns:
            TopicProducer: The topic the stream is producing to.

        """
        return self._topic

    # region on_write_exception
    @property
    def on_write_exception(self) -> Callable[['StreamProducer', BaseException], None]:
        """
        Gets the handler for when a stream experiences exception during the asynchronous write process.

        Returns:
            Callable[['StreamProducer', BaseException], None]: The handler for exceptions during the asynchronous write process.
                The first parameter is the stream is received for, second is the exception.
        """
        return self._on_write_exception

    @on_write_exception.setter
    def on_write_exception(self, value: Callable[['StreamProducer', BaseException], None]) -> None:
        """
        Sets the handler for when a stream experiences exception during the asynchronous write process.

        Args:
            value: The handler for exceptions during the asynchronous write process.
                The first parameter is the stream is received for, second is the exception.
        """
        self._on_write_exception = value
        if self._on_write_exception_ref is None:
            self._on_write_exception_ref = self._interop.add_OnWriteException(self._on_write_exception_wrapper)

    def _on_write_exception_wrapper(self, stream_hptr, exception_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        try:
            # TODO fix arg to be handled as exception
            # self.on_write_exception.fire(self, BaseException(arg.Message, type(arg)))
            InteropUtils.free_hptr(stream_hptr)
            InteropUtils.free_hptr(exception_hptr)
        except:
            traceback.print_exc()

    def _on_write_exception_dispose(self):
        if self._on_write_exception_ref is not None:
            self._interop.remove_OnWriteException(self._on_write_exception_ref)
            self._on_write_exception_ref = None

    # endregion on_write_exception

    @property
    def stream_id(self) -> str:
        """
        Gets the unique id of the stream being produced.

        Returns:
            str: The unique id of the stream being produced.
        """
        return self._stream_id

    @property
    def epoch(self) -> datetime:
        """
        Gets the default Epoch used for Timeseries and Events.

        Returns:
            datetime: The default Epoch used for Timeseries and Events.
        """

        ptr = self._interop.get_Epoch()
        value = dtc.datetime_to_python(ptr)
        return value

    @epoch.setter
    def epoch(self, value: datetime):
        """
        Set the default Epoch used for Timeseries and Events.

        Args:
            value: The default Epoch value to set.
        """
        dotnet_value = dtc.datetime_to_dotnet(value)
        self._interop.set_Epoch(dotnet_value)

    @property
    def properties(self) -> StreamPropertiesProducer:
        """
        Gets the properties of the stream. The changes will automatically be sent after a slight delay.

        Returns:
            StreamPropertiesProducer: The properties of the stream.
        """
        if self._streamPropertiesProducer is None:
            self._streamPropertiesProducer = StreamPropertiesProducer(self._interop.get_Properties())
        return self._streamPropertiesProducer

    @property
    def timeseries(self) -> StreamTimeseriesProducer:
        """
        Gets the producer for publishing timeseries related information of the stream such as parameter definitions and values.

        Returns:
            StreamTimeseriesProducer: The producer for publishing timeseries related information of the stream.
        """

        if self._streamTimeseriesProducer is None:
            self._streamTimeseriesProducer = StreamTimeseriesProducer(self, self._interop.get_Timeseries())
        return self._streamTimeseriesProducer

    @property
    def events(self) -> StreamEventsProducer:
        """
        Gets the producer for publishing event related information of the stream such as event definitions and values.

        Returns:
            StreamEventsProducer: The producer for publishing event related information of the stream.
        """
        if self._streamEventsProducer is None:
            self._streamEventsProducer = StreamEventsProducer(self._interop.get_Events())
        return self._streamEventsProducer

    def close(self, end_type: StreamEndType = StreamEndType.Closed):
        """
        Closes the stream and flushes the pending data to stream.

        Args:
            end_type: The type of stream end. Defaults to StreamEndType.Closed.
        """

        dotnet_end_type = ec.enum_to_another(end_type, StreamEndTypeInterop)

        self._interop.Close(dotnet_end_type)

        self.dispose()
