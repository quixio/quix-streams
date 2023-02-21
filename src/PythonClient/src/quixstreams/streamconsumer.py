import sys
import traceback
from typing import Callable

from .models.streampackage import StreamPackage
from .models.streamconsumer.streampropertiesconsumer import StreamPropertiesConsumer
from .models.streamconsumer.streamparametersconsumer import StreamParametersConsumer
from .models.streamconsumer.streameventsconsumer import StreamEventsConsumer

from .models.streamendtype import StreamEndType
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixSdkStreaming.IStreamConsumer import IStreamConsumer as sci
from .native.Python.QuixSdkStreaming.StreamClosedEventArgs import StreamClosedEventArgs
from .native.Python.QuixSdkStreaming.PackageReceivedEventArgs import PackageReceivedEventArgs
from .helpers.enumconverter import EnumConverter as ec
import ctypes
from .helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamConsumer(object):
    """
        Handles reading stream from a topic
    """

    def __init__(self, net_pointer: ctypes.c_void_p, topic_consumer: 'StreamConsumer', on_close_cb_always: Callable[['StreamConsumer'], None]):
        """
        Initializes a new instance of StreamConsumer.
        NOTE: Do not initialize this class manually, use StreamingClient.on_stream_received to read streams
        :param net_pointer: Pointer to an instance of a .net StreamConsumer
        """
        self._interop = sci(net_pointer)
        self._topic_consumer = topic_consumer
        self._streamParametersReader = None  # Holding reference to avoid GC
        self._streamEventsReader = None  # Holding reference to avoid GC
        self._streamPropertiesReader = None  # Holding reference to avoid GC

        # define events and their ref holder
        self._on_stream_closed = None
        self._on_stream_closed_ref = None  # keeping reference to avoid GC
        
        self._on_package_received = None
        self._on_package_received_ref = None  # keeping reference to avoid GC

        self._streamId = None

        if on_close_cb_always is not None:
            def _on_close_cb_always_wrapper(sender_hptr, args_hptr):
                try:
                    # To avoid unnecessary overhead and complication, we're using the instances we already have
                    with (args := StreamClosedEventArgs(args_hptr)):
                        on_close_cb_always(self)

                    refcount = sys.getrefcount(self)
                    if refcount == -1:  # TODO figure out correct number
                        self.dispose()
                    InteropUtils.free_hptr(sender_hptr)
                except:
                    traceback.print_exc()

            self._on_close_cb_always_ref = self._interop.add_OnStreamClosed(_on_close_cb_always_wrapper)

    def _finalizerfunc(self):
        if self._streamParametersReader is not None:
            self._streamParametersReader.dispose()
        if self._streamEventsReader is not None:
            self._streamEventsReader.dispose()
        if self._streamPropertiesReader is not None:
            self._streamPropertiesReader.dispose()
        self._on_stream_closed_dispose()
        self._on_package_received_dispose()

    # region on_stream_closed
    @property
    def on_stream_closed(self) -> Callable[['TopicConsumer', 'StreamConsumer', 'StreamEndType'], None]:
        """
        Gets the handler for when the stream closes. First parameter is the stream which closes, second is the close type.
        """
        return self._on_stream_closed

    @on_stream_closed.setter
    def on_stream_closed(self, value: Callable[['TopicConsumer', 'StreamConsumer', 'StreamEndType'], None]) -> None:
        """
        Sets the handler for when the stream closes. First parameter is the stream which closes, second is the close type.
        """
        self._on_stream_closed = value
        if self._on_stream_closed_ref is None:
            self._on_stream_closed_ref = self._interop.add_OnStreamClosed(self._on_stream_closed_wrapper)

    def _on_stream_closed_wrapper(self, sender_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the instances we already have
        try:
            with (args := StreamClosedEventArgs(args_hptr)):
                converted = ec.enum_to_another(args.get_EndType(), StreamEndType)
                self.on_stream_closed(self._topic_consumer, self, converted)
            InteropUtils.free_hptr(sender_hptr)
        except:
            traceback.print_exc()

    def _on_stream_closed_dispose(self):
        if self._on_stream_closed_ref is not None:
            self._interop.remove_OnStreamClosed(self._on_stream_closed_ref)
            self._on_stream_closed_ref = None
    # endregion on_stream_closed
    
    # region on_package_received
    @property
    def on_package_received(self) -> Callable[['TopicConsumer', 'StreamConsumer', any], None]:
        """
        Gets the handler for when the stream receives a package of any type. First parameter is the stream which receives it, second is the package.
        """
        return self._on_package_received

    @on_package_received.setter
    def on_package_received(self, value: Callable[['TopicConsumer', 'StreamConsumer', any], None]) -> None:
        """
        Sets the handler for when the stream receives a package of any type. First parameter is the stream which receives it, second is the package.
        """
        # TODO
        raise Exception("StreamConsumer.on_package_received is not yet fully implemented")
        self._on_package_received = value
        if self._on_package_received_ref is None:
            self._on_package_received_ref = self._interop.add_OnPackageReceived(self._on_package_received_wrapper)

    def _on_package_received_wrapper(self, sender_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the instances we already have
        try:
            with (args := PackageReceivedEventArgs(args_hptr)):
                pass  # TODO
            InteropUtils.free_hptr(sender_hptr)
        except:
            traceback.print_exc()

    def _on_package_received_dispose(self):
        if self._on_package_received_ref is not None:
            self._interop.remove_OnPackageReceived(self._on_package_received_ref)
            self._on_package_received_ref = None
    # endregion on_package_received

    @property
    def stream_id(self) -> str:
        """Get the id of the stream being read"""

        if self._streamId is None:
            self._streamId = self._interop.get_StreamId()
        return self._streamId

    @property
    def properties(self) -> StreamPropertiesConsumer:
        """ Gets the reader for accessing the properties and metadata of the stream """
        if self._streamPropertiesReader is None:
            self._streamPropertiesReader = StreamPropertiesConsumer(self._topic_consumer, self, self._interop.get_Properties())
        return self._streamPropertiesReader

    @property
    def events(self) -> StreamEventsConsumer:
        """
        Gets the reader for accessing event related information of the stream such as definitions and event values
        """
        if self._streamEventsReader is None:
            self._streamEventsReader = StreamEventsConsumer(self._topic_consumer, self, self._interop.get_Events())
        return self._streamEventsReader

    @property
    def parameters(self) -> StreamParametersConsumer:
        """
        Gets the reader for accessing parameter related information of the stream such as definitions and parameter values
        """
        if self._streamParametersReader is None:
            self._streamParametersReader = StreamParametersConsumer(self._topic_consumer, self, self._interop.get_Parameters())
        return self._streamParametersReader

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
