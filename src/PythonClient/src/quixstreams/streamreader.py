import sys

from .eventhook import EventHook

from .models.streampackage import StreamPackage
from .models.streamreader.streampropertiesreader import StreamPropertiesReader
from .models.streamreader.streamparametersreader import StreamParametersReader
from .models.streamreader.streameventsreader import StreamEventsReader

from .models.streamendtype import StreamEndType
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixSdkStreaming.IStreamReader import IStreamReader as sri
from .helpers.enumconverter import EnumConverter as ec
import ctypes
from .helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamReader(object):
    """
        Handles reading stream from a topic
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamReader.
        NOTE: Do not initialize this class manually, use StreamingClient.on_stream_received to read streams
        :param net_pointer: Pointer to an instance of a .net StreamReader
        """
        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them
        self._interop = sri(net_pointer)
        self._streamParametersReader = None  # Holding reference to avoid GC
        self._streamEventsReader = None  # Holding reference to avoid GC
        self._streamPropertiesReader = None  # Holding reference to avoid GC

        def _on_stream_closed_read_net_handler(sender, end_type):
            converted = ec.enum_to_another(end_type, StreamEndType)
            self.on_stream_closed.fire(converted)

            refcount = sys.getrefcount(self)
            print(f"REF COUNTER {refcount}")
            if refcount == 1: # TODO figure out correct number
                self.dispose()
            InteropUtils.free_hptr(sender)  # another pointer is assigned to the same object as current, we don't need it

        def _on_stream_closed_first_sub():
            ref = self._interop.add_OnStreamClosed(_on_stream_closed_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_stream_closed_last_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnStreamClosed(_on_stream_closed_read_net_handler)

        self.on_stream_closed = EventHook(_on_stream_closed_first_sub, _on_stream_closed_last_unsub, name="StreamReader.on_stream_closed")
        """Raised when stream close is read.       
         Parameters:
            sender (StreamReader): The StreamReader to which the event belongs to.            
            end_type (StreamEndType): The StreamEndType value read from the message.
        """

        def _on_package_received_net_handler(sender, package):
            self.on_package_received.fire(self, StreamPackage(package))
            InteropUtils.free_hptr(sender)   # another pointer is assigned to the same object as current, we don't need it

        def _on_first_sub_on_package_received():
            # TODO
            raise Exception("StreamReader.on_package_received is not yet fully implemented")
            ref = self._interop.add_OnPackageReceived(_on_package_received_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_unsub_on_package_received():
            # TODO do unsign with previous handler
            self._interop.remove_OnPackageReceived(_on_package_received_net_handler)

        self.on_package_received = EventHook(_on_first_sub_on_package_received, _on_last_unsub_on_package_received, name="StreamReader.on_package_received")
        """Raised when stream package has been received.       
         Parameters:
            sender (StreamReader): The StreamReader to which the event belongs to.            
            package (StreamPackage): The Package received.
        """

        self._streamId = None

    def _finalizerfunc(self):
        if self._streamParametersReader is not None:
            self._streamParametersReader.dispose()
        if self._streamEventsReader is not None:
            self._streamEventsReader.dispose()
        if self._streamPropertiesReader is not None:
            self._streamPropertiesReader.dispose()
        self._cfuncrefs = None

    @property
    def stream_id(self) -> str:
        """Get the id of the stream being read"""

        if self._streamId is None:
            self._streamId = self._interop.get_StreamId()
        return self._streamId

    @property
    def properties(self) -> StreamPropertiesReader:
        """ Gets the reader for accessing the properties and metadata of the stream """
        if self._streamPropertiesReader is None:
            self._streamPropertiesReader = StreamPropertiesReader(self._interop.get_Properties())
        return self._streamPropertiesReader

    @property
    def events(self) -> StreamEventsReader:
        """
        Gets the reader for accessing event related information of the stream such as definitions and event values
        """
        if self._streamEventsReader is None:
            self._streamEventsReader = StreamEventsReader(self._interop.get_Events())
        return self._streamEventsReader

    @property
    def parameters(self) -> StreamParametersReader:
        """
        Gets the reader for accessing parameter related information of the stream such as definitions and parameter values
        """
        if self._streamParametersReader is None:
            self._streamParametersReader = StreamParametersReader(self._interop.get_Parameters())
        return self._streamParametersReader

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
