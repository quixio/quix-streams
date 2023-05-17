import ctypes
import sys
import traceback
from typing import Any, Callable

from .helpers.enumconverter import EnumConverter as ec
from .helpers.nativedecorator import nativedecorator
from .models.streamconsumer.streameventsconsumer import StreamEventsConsumer
from .models.streamconsumer.streamtimeseriesconsumer import StreamTimeseriesConsumer
from .models.streamconsumer.streampropertiesconsumer import StreamPropertiesConsumer
from .models.streamendtype import StreamEndType
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixStreamsStreaming.IStreamConsumer import IStreamConsumer as sci
from .native.Python.QuixStreamsStreaming.PackageReceivedEventArgs import PackageReceivedEventArgs
from .native.Python.QuixStreamsStreaming.StreamClosedEventArgs import StreamClosedEventArgs
from .states.streamstate import StreamState
from .states.streamstatemanager import StreamStateManager

from typing import TypeVar

StreamStateType = TypeVar('StreamStateType')

@nativedecorator
class StreamConsumer(object):
    """
    Handles consuming stream from a topic.
    """

    def __init__(self, net_pointer: ctypes.c_void_p, topic_consumer: 'TopicConsumer', on_close_cb_always: Callable[['StreamConsumer'], None]):
        """
        Initializes a new instance of StreamConsumer.

        NOTE: Do not initialize this class manually, TopicProducer automatically creates this when a new stream is received.

        Args:
            net_pointer: Pointer to an instance of a .NET StreamConsumer.
            topic_consumer: The topic consumer which owns the stream consumer.
            on_close_cb_always: The callback function to be executed when the stream is closed.
        """
        self._interop = sci(net_pointer)
        self._topic = topic_consumer
        self._streamTimeseriesConsumer = None  # Holding reference to avoid GC
        self._streamEventsConsumer = None  # Holding reference to avoid GC
        self._streamPropertiesConsumer = None  # Holding reference to avoid GC

        # define events and their ref holder
        self._on_stream_closed = None
        self._on_stream_closed_ref = None  # keeping reference to avoid GC

        self._on_package_received = None
        self._on_package_received_ref = None  # keeping reference to avoid GC

        self._streamId = None

        self._stream_state_manager = None

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
        if self._streamTimeseriesConsumer is not None:
            self._streamTimeseriesConsumer.dispose()
        if self._streamEventsConsumer is not None:
            self._streamEventsConsumer.dispose()
        if self._streamPropertiesConsumer is not None:
            self._streamPropertiesConsumer.dispose()
        self._on_stream_closed_dispose()
        self._on_package_received_dispose()
        self._stream_state_manager.dispose()

    @property
    def topic(self) -> 'TopicConsumer':
        """
        Gets the topic the stream was raised for.

        Returns:
            TopicConsumer: The topic consumer instance associated with the stream.
        """
        return self._topic

    # region on_stream_closed
    @property
    def on_stream_closed(self) -> Callable[['StreamConsumer', 'StreamEndType'], None]:
        """
       Gets the handler for when the stream closes.

       Returns:
            Callable[['StreamConsumer', 'StreamEndType'], None]: The callback function to be executed when the stream closes.
                The first parameter is the stream that closes, and the second is the close type.
       """
        return self._on_stream_closed

    @on_stream_closed.setter
    def on_stream_closed(self, value: Callable[['StreamConsumer', 'StreamEndType'], None]) -> None:
        """
        Sets the handler for when the stream closes.

        Args:
            value: The new callback function to be executed when the stream closes.
                The first parameter is the stream that closes, and the second is the close type.
        """
        self._on_stream_closed = value
        if self._on_stream_closed_ref is None:
            self._on_stream_closed_ref = self._interop.add_OnStreamClosed(self._on_stream_closed_wrapper)

    def _on_stream_closed_wrapper(self, sender_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the instances we already have
        try:
            with (args := StreamClosedEventArgs(args_hptr)):
                converted = ec.enum_to_another(args.get_EndType(), StreamEndType)
                self.on_stream_closed(self, converted)
            InteropUtils.free_hptr(sender_hptr)
        except:
            traceback.print_exc()
        finally:
            self.dispose()

    def _on_stream_closed_dispose(self):
        if self._on_stream_closed_ref is not None:
            self._interop.remove_OnStreamClosed(self._on_stream_closed_ref)
            self._on_stream_closed_ref = None

    # endregion on_stream_closed

    # region on_package_received
    @property
    def on_package_received(self) -> Callable[['StreamConsumer', Any], None]:
        """
        Gets the handler for when the stream receives a package of any type.

        Returns:
            Callable[['StreamConsumer', Any], None]: The callback function to be executed when the stream receives a package.
                The first parameter is the stream that receives the package, and the second is the package itself.
        """
        return self._on_package_received

    @on_package_received.setter
    def on_package_received(self, value: Callable[['StreamConsumer', Any], None]) -> None:
        """
        Sets the handler for when the stream receives a package of any type.

        Args:
            value: The new callback function to be executed when the stream receives a package.
                The first parameter is the stream that receives the package, and the second is the package itself.
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
        """
        Get the ID of the stream being consumed.

        Returns:
            str: The ID of the stream being consumed.
        """
        if self._streamId is None:
            self._streamId = self._interop.get_StreamId()
        return self._streamId

    @property
    def properties(self) -> StreamPropertiesConsumer:
        """
        Gets the consumer for accessing the properties and metadata of the stream.

        Returns:
            StreamPropertiesConsumer: The stream properties consumer instance.
        """
        if self._streamPropertiesConsumer is None:
            self._streamPropertiesConsumer = StreamPropertiesConsumer(self, self._interop.get_Properties())
        return self._streamPropertiesConsumer

    @property
    def events(self) -> StreamEventsConsumer:
        """
        Gets the consumer for accessing event related information of the stream such as event definitions and values.

        Returns:
            StreamEventsConsumer: The stream events consumer instance.
        """
        if self._streamEventsConsumer is None:
            self._streamEventsConsumer = StreamEventsConsumer(self, self._interop.get_Events())
        return self._streamEventsConsumer

    @property
    def timeseries(self) -> StreamTimeseriesConsumer:
        """
        Gets the consumer for accessing timeseries related information of the stream such as parameter definitions and values.

        Returns:
            StreamTimeseriesConsumer: The stream timeseries consumer instance.
        """
        if self._streamTimeseriesConsumer is None:
            self._streamTimeseriesConsumer = StreamTimeseriesConsumer(self, self._interop.get_Timeseries())
        return self._streamTimeseriesConsumer

    def get_dict_state(self, state_name: str, default_value_factory: Callable[[str], StreamStateType] = None, state_type: StreamStateType = None) -> StreamState[StreamStateType]:
        """
        Creates a new application state of dictionary type with automatically managed lifecycle for the stream

        Args:
            state_name: The name of the state
            default_value_factory: The default value factory to create value when the key is not yet present in the state
            state_type: The type of the state

        Returns:
            StreamState: The stream state

         Example:
            >>> stream_consumer.get_dict_state('some_state')
            This will return a state where type is 'Any'

            >>> stream_consumer.get_dict_state('some_state', lambda missing_key: return {})
            this will return a state where type is a generic dictionary, with an empty dictionary as default value when
            key is not available. The lambda function will be invoked with 'get_state_type_check' key to determine type

            >>> stream_consumer.get_dict_state('some_state', lambda missing_key: return {}, Dict[str, float])
            this will return a state where type is a specific dictionary type, with default value

            >>> stream_consumer.get_dict_state('some_state', state_type=float)
            this will return a state where type is a float without default value, resulting in KeyError when not found
        """

        return self.get_state_manager().get_dict_state(state_name, default_value_factory, state_type)

    def get_state_manager(self) -> StreamStateManager:
        """
        Gets the manager for the stream states.

        Returns:
            StreamStateManager: The stream state manager
        """

        if self._stream_state_manager is None:
            self._stream_state_manager = StreamStateManager(self._interop.GetStateManager())

        return self._stream_state_manager

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .NET object pointer.

        Returns:
            ctypes.c_void_p: The .NET pointer
        """
        return self._interop.get_interop_ptr__()
