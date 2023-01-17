from .eventhook import EventHook
from .models import *
from datetime import datetime
from .helpers import *
from .models.streamwriter import *
from . import __importnet
import Quix.Sdk.Streaming


class StreamWriter(object):
    """
        Handles writing stream to a topic
    """

    def __init__(self, wrapped: Quix.Sdk.Streaming.IStreamWriter):
        """
            Initializes a new instance of StreamWriter.
            NOTE: Do not initialize this class manually, use StreamingClient.create_stream to write streams

            Parameters:

            net_object (.net object): The .net object representing a StreamWriter
        """

        self.__wrapped = wrapped  # the wrapped .net IStreamWriter

        def __on_write_exception_net_handler(sender, arg):
            self.on_write_exception.fire(self, BaseException(arg.Message, type(arg)))

        def __on_first_sub_on_write_exception_net():
            self.__wrapped.OnWriteException += __on_write_exception_net_handler

        def __on_last_unsub_on_write_exception_net():
            self.__wrapped.OnWriteException -= __on_write_exception_net_handler

        self.on_write_exception = EventHook(__on_first_sub_on_write_exception_net, __on_last_unsub_on_write_exception_net, name="StreamWriter.on_write_exception")
        """Raised when an exception occurred during the writing processes
         Parameters:     
            exception (BaseException): The occurred exception
        """

    @property
    def stream_id(self) -> str:
        """Get the unique id the stream being written"""
        return self.__wrapped.StreamId

    @property
    def epoch(self) -> datetime:
        """Get the default Epoch used for Parameters and Events"""
        return NetToPythonConverter.convert_datetime(self.__wrapped.Epoch)

    @epoch.setter
    def epoch(self, value: datetime):
        """Set the default Epoch used for Parameters and Events"""
        self.__wrapped.Epoch = PythonToNetConverter.convert_datetime(value)

    @property
    def properties(self) -> StreamPropertiesWriter:
        """Properties of the stream. The changes will automatically be sent after a slight delay"""
        return StreamPropertiesWriter(self.__wrapped.Properties)

    @property
    def parameters(self) -> StreamParametersWriter:
        """Helper for doing anything related to parameters of the stream. Use to send parameter definitions,
        groups or values """
        return StreamParametersWriter(self.__wrapped.Parameters, self.__wrapped)

    @property
    def events(self) -> StreamEventsWriter:
        """Helper for doing anything related to events of the stream. Use to send event definitions, groups or
        values. """
        return StreamEventsWriter(self.__wrapped.Events)

    def close(self, end_type: StreamEndType = StreamEndType.Closed):
        """
            Close the stream and flush the pending data to stream
        """

        self.__wrapped.Close(end_type.convert_to_net())

