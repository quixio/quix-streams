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
from .native.Python.QuixStreamsProcess.Models.StreamEndType import StreamEndType as StreamEndTypeInterop
from .native.Python.QuixStreamsStreaming.IStreamProducer import IStreamProducer as spi


@nativedecorator
class StreamProducer(object):
    """
        Handles writing stream to a topic
    """

    def __init__(self, topic_producer: 'TopicProducer', net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamProducer.
            NOTE: Do not initialize this class manually, use StreamingClient.create_stream to write streams

            Parameters:
            topic_producer: The topic producer the stream producer publishes to
            net_object (.net object): The .net object representing a StreamProducer
        """

        if net_pointer is None:
            raise Exception("StreamProducer is none")

        self._interop = spi(net_pointer)
        self._topic = topic_producer
        self._streamParametersWriter = None  # Holding reference to avoid GC
        self._streamEventsWriter = None  # Holding reference to avoid GC
        self._streamPropertiesWriter = None  # Holding reference to avoid GC

        # define events and their ref holder
        self._on_write_exception = None
        self._on_write_exception_ref = None  # keeping reference to avoid GC

        self._stream_id = self._interop.get_StreamId()

    def _finalizerfunc(self):
        if self._streamParametersWriter is not None:
            self._streamParametersWriter.dispose()
        if self._streamEventsWriter is not None:
            self._streamEventsWriter.dispose()
        if self._streamPropertiesWriter is not None:
            self._streamPropertiesWriter.dispose()
        self._on_write_exception_dispose()

    @property
    def topic(self) -> 'TopicProducer':
        """
        Gets the topic the stream is writing to
        """
        return self._topic

    # region on_write_exception
    @property
    def on_write_exception(self) -> Callable[['StreamProducer', BaseException], None]:
        """
        Gets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the stream
         is received for, second is the exception.
        """
        return self._on_write_exception

    @on_write_exception.setter
    def on_write_exception(self, value: Callable[['StreamProducer', BaseException], None]) -> None:
        """
        Sets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the stream
         is received for, second is the exception.
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
        """Gets the unique id the stream being written"""
        return self._stream_id

    @property
    def epoch(self) -> datetime:
        """Gets the default Epoch used for Parameters and Events"""

        ptr = self._interop.get_Epoch()
        value = dtc.datetime_to_python(ptr)
        return value

    @epoch.setter
    def epoch(self, value: datetime):
        """Set the default Epoch used for Parameters and Events"""
        dotnet_value = dtc.datetime_to_dotnet(value)
        self._interop.set_Epoch(dotnet_value)

    @property
    def properties(self) -> StreamPropertiesProducer:
        """Properties of the stream. The changes will automatically be sent after a slight delay"""
        if self._streamPropertiesWriter is None:
            self._streamPropertiesWriter = StreamPropertiesProducer(self._interop.get_Properties())
        return self._streamPropertiesWriter

    @property
    def parameters(self) -> StreamParametersProducer:
        """Helper for doing anything related to parameters of the stream. Use to send parameter definitions,
        groups or values """

        if self._streamParametersWriter is None:
            self._streamParametersWriter = StreamParametersProducer(self, self._interop.get_Parameters())
        return self._streamParametersWriter

    @property
    def events(self) -> StreamEventsProducer:
        """Helper for doing anything related to events of the stream. Use to send event definitions, groups or
        values. """
        if self._streamEventsWriter is None:
            self._streamEventsWriter = StreamEventsProducer(self._interop.get_Events())
        return self._streamEventsWriter

    def close(self, end_type: StreamEndType = StreamEndType.Closed):
        """
            Close the stream and flush the pending data to stream
        """

        dotnet_end_type = ec.enum_to_another(end_type, StreamEndTypeInterop)

        self._interop.Close(dotnet_end_type)

        refcount = sys.getrefcount(self)
        if refcount == -1:  # TODO figure out correct number
            self.dispose()
