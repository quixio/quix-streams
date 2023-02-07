import sys

from .eventhook import EventHook
from .models import *
from datetime import datetime
from .models.streamwriter import *

import ctypes

from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixSdkStreaming.IStreamWriter import IStreamWriter as swi
from .native.Python.QuixSdkProcess.Models.StreamEndType import StreamEndType as StreamEndTypeInterop
from .helpers.enumconverter import EnumConverter as ec
from .helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from .helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamWriter(object):
    """
        Handles writing stream to a topic
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamWriter.
            NOTE: Do not initialize this class manually, use StreamingClient.create_stream to write streams

            Parameters:

            net_object (.net object): The .net object representing a StreamWriter
        """

        if net_pointer is None:
            raise Exception("StreamWriter is none")

        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them
        self._interop = swi(net_pointer)
        self._streamParametersWriter = None  # Holding reference to avoid GC
        self._streamEventsWriter = None  # Holding reference to avoid GC
        self._streamPropertiesWriter = None  # Holding reference to avoid GC

        def _on_write_exception_net_handler(sender: ctypes.c_void_p, arg: ctypes.c_void_p):
            # TODO fix arg to be handled as exception
            self.on_write_exception.fire(self, BaseException(arg.Message, type(arg)))
            InteropUtils.free_hptr(sender)

        def _on_first_sub_on_write_exception_net():
            ref = self._interop.add_OnWriteException(_on_write_exception_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_unsub_on_write_exception_net():
            # TODO do unsign with previous handler
            self._interop.remove_OnWriteException(_on_write_exception_net_handler)

        self.on_write_exception = EventHook(_on_first_sub_on_write_exception_net, _on_last_unsub_on_write_exception_net, name="StreamWriter.on_write_exception")
        """Raised when an exception occurred during the writing processes
         Parameters:     
            exception (BaseException): The occurred exception
        """

        self._stream_id = self._interop.get_StreamId()

    def _finalizerfunc(self):
        if self._streamParametersWriter is not None:
            self._streamParametersWriter.dispose()
        if self._streamEventsWriter is not None:
            self._streamEventsWriter.dispose()
        if self._streamPropertiesWriter is not None:
            self._streamPropertiesWriter.dispose()
        self._cfuncrefs = None

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
    def properties(self) -> StreamPropertiesWriter:
        """Properties of the stream. The changes will automatically be sent after a slight delay"""
        if self._streamPropertiesWriter is None:
            self._streamPropertiesWriter = StreamPropertiesWriter(self._interop.get_Properties())
        return self._streamPropertiesWriter

    @property
    def parameters(self) -> StreamParametersWriter:
        """Helper for doing anything related to parameters of the stream. Use to send parameter definitions,
        groups or values """

        if self._streamParametersWriter is None:
            self._streamParametersWriter = StreamParametersWriter(self._interop.get_Parameters())
        return self._streamParametersWriter

    @property
    def events(self) -> StreamEventsWriter:
        """Helper for doing anything related to events of the stream. Use to send event definitions, groups or
        values. """
        if self._streamEventsWriter is None:
            self._streamEventsWriter = StreamEventsWriter(self._interop.get_Events())
        return self._streamEventsWriter

    def close(self, end_type: StreamEndType = StreamEndType.Closed):
        """
            Close the stream and flush the pending data to stream
        """

        dotnet_end_type = ec.enum_to_another(end_type, StreamEndTypeInterop)

        self._interop.Close(dotnet_end_type)

        refcount = sys.getrefcount(self)
        if refcount == 1:  # TODO figure out correct number
            self.dispose()

