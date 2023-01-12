from .eventhook import EventHook
from .models import *
from . import __importnet
import Quix.Sdk.Streaming

from .models.streampackage import StreamPackage
from .models.streamreader import StreamPropertiesReader, StreamParametersReader
from .models.streamreader.streameventsreader import StreamEventsReader


class StreamReader(object):
    """
        Handles reading stream from a topic
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.IStreamReader):
        """
        Initializes a new instance of StreamReader.
        NOTE: Do not initialize this class manually, use StreamingClient.on_stream_received to read streams
        :param net_object: (.net object): The .net object representing a StreamReader
        """
        self.__wrapped = net_object  # the wrapped .net StreamReader

        self.on_stream_closed = EventHook(name="StreamReader.on_stream_closed")
        """Raised when stream close is read.       
         Parameters:
            sender (StreamReader): The StreamReader to which the event belongs to.            
            end_type (StreamEndType): The StreamEndType value read from the message.
        """

        def __on_stream_closed_net_handler(sender, end_type):
            self.on_stream_closed.fire(StreamEndType.convert_from_net(end_type))
        self.__wrapped.OnStreamClosed += __on_stream_closed_net_handler

        def __on_package_received_net_handler(sender, package):
            self.on_package_received.fire(self, StreamPackage(package))

        def __on_first_sub_on_package_received():
            self.__wrapped.OnPackageReceived += __on_package_received_net_handler

        def __on_last_unsub_on_package_received():
            self.__wrapped.OnPackageReceived -= __on_package_received_net_handler

        self.on_package_received = EventHook(__on_first_sub_on_package_received, __on_last_unsub_on_package_received, name="StreamReader.on_package_received")
        """Raised when stream package has been received.       
         Parameters:
            sender (StreamReader): The StreamReader to which the event belongs to.            
            package (StreamPackage): The Package received.
        """


    @property
    def stream_id(self) -> str:
        """Get the id of the stream being read"""
        return self.__wrapped.StreamId

    def dispose(self):
        """
            Disposes underlying .net instance.
        """
        self.__wrapped.Dispose()

    @property
    def properties(self) -> StreamPropertiesReader:
        """ Gets the reader for accessing the properties and metadata of the stream """
        return StreamPropertiesReader(self.__wrapped.Properties)

    @property
    def events(self) -> StreamEventsReader:
        """
        Gets the reader for accessing event related information of the stream such as definitions and event values
        """
        return StreamEventsReader(self.__wrapped.Events)

    @property
    def parameters(self) -> StreamParametersReader:
        """
        Gets the reader for accessing parameter related information of the stream such as definitions and parameter values
        """
        return StreamParametersReader(self.__wrapped.Parameters)
        